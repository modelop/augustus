#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Augustus.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language  permissions and
# limitations under the License.

"""A set of functions to set parameters given a configuration file."""

import gc
gc.disable()  # shut off circular garbage collection
import httplib
import logging
import os
import platform
import signal
import sys
import threading
from time import sleep
import traceback

import augustus.core.pmml41 as pmml
import augustus.core.config as config
import augustus.core.xmlbase as xmlbase
import augustus.engine.verification
from augustus.core.config import ConfigurationError
from augustus.core.defs import INVALID
from augustus.engine.engine import Engine
from augustus.engine.outputwriter import OutputWriter
from augustus.engine.segmentationschema import SegmentationScheme
from augustus.datastreams.datastreamer import getDataStreamer
from augustus.datastreams.modelwriter import getModelWriter
from augustus.logsetup.logsetup import setupLogging
from augustus.algorithms.eventweighting import UpdateScheme
from augustus.core.xmlbase import XML
# import augustus.runlib.timer as timer

_modelExceptionIdentifier = "EXCEPTION"
class ConfigurationError(Exception): pass  # Any configuration error

class ErrorCatcher(threading._Event):
    """ Thread safe container to place an exception caught in a child threads, to make it available to the main thread.
    """
    def __init__(self):
        self.error = None
        threading._Event.__init__(self)

    def postError(self):
        """ Save exception information from this thread and set my flag that an error occurred.

        The child thread must have a pointer to the errorCatcher object.  In the child thread:
            try:
                foo()
            except:
                errorCatcher.postError()  # set info for the parent thread...
                sys.exit(1)  # or whatever handling...

        In the main thread:
            errorCatcher = ErrorCatcher()
            setChildThreadErrorCatcher(errorCatcher)
            childThread.start()

            while notDone:
                doSomething()
                if errorCatcher.isSet():
                   # Handle error
        """
        self.error = sys.exc_info()
        self.set()  # Set threading._Event's flag.

    def __repr__(self):
        if self.error:
            return "".join(traceback.format_exception(*self.error))
        else:
            return "<ErrorCatcher: no error observed.>"


def getModel(configOptions):
    """Return a pmmlElement object: the root of the model.

    Arguments:

        configOptions (XML object, defined in xmlbase):
            The XML element <ModelInput>...</ModelInput> which
            contains the source location for the PMML model.
    """
    #xsd: Assume FromFile/FromFifo, with file name required.
    sourceElement = configOptions.child(lambda x: x.tag.startswith("From"))
    filename = sourceElement["name"]
    if sourceElement.tag.endswith("File"):
        selectmode = sourceElement.attrib.get(
            "selectmode", "lastAlphabetic")
        import glob
        filelist = glob.glob(filename)
        if len(filelist) > 1:
            filelist = [
                f for f in filelist if _modelExceptionIdentifier not in f]
        if len(filelist) == 0 :
            raise RuntimeError, "no files matched the given filename/glob: %s" % filename

        if selectmode == "mostRecent":
            filename = max(filelist, key=lambda x: os.stat(x).st_mtime)
        else:
            filelist.sort()
            filename = filelist[-1]

        if _modelExceptionIdentifier in filename:
            logging.getLogger().warning("Using a PMML model that was written on exception:File name: %s" % filename)

    try:
        # TODO: make lineNumbers optional (better diagnostics with them, better performance without them)
        model = xmlbase.loadfile(filename, pmml.X_ODG_PMML, lineNumbers=True)
    except:
        logging.getLogger().error("Error loading PMML model from %s." % filename)
        raise
    return model, filename

def getUpdateScheme(configOptions):
    """Return an UpdateScheme as specified by the user configOptions.

    Arguments:

        configOptions (XML object, defined in xmlbase):
            An XML element of type "Blending"; either
            <ConsumerBlending/> or <ProducerBlending/>; containing the
            weightings and default settings for the model update schemes.
    """
    if configOptions is None:
        return UpdateScheme("unweighted")

    params = configOptions.attrib.copy()
    scheme = "unweighted"
    if "method" in params:
        scheme = params["method"]

        if scheme == "computerTimeWindowSeconds":
            raise NotImplementedError

        elif scheme == "eventTimeWindow":
            scheme = "synchronized"

        del params["method"]

    if "windowLag" not in params:
        if scheme not in ("unweighted", "exponential"):
            params["windowLag"] = 0

    return UpdateScheme(scheme, **params)

def getOutputWriter(configOptions, pmmlFileName):
    """Set the scoring output location and output format for ``consumer``.

    The consumer data members that may be modified are:

        * output_filename (string):  The output file name
        * out (stream): stdout, stderr, or a file
        * __score_posting (tuple): the host and URL to post scores.

    Arguments:

        eonfigOptions (XML object, defined in xmlbase):
            The XML element <Output>...</Output> which
            contains the destination for the scoring output and the
            definition of the report format.
    """
    if configOptions is None:
        return None

    child = configOptions.child(lambda x: x.tag.startswith("To"))
    mode = child.attrib.get("type", "XML").lower()

    if isinstance(child, config.ToFile):
        output_filename = child["name"]
        if child.attrib.get("overwrite", False):
            out = open(output_filename, 'w')
        else:
            out = open(output_filename, 'a')
    elif isinstance(child, config.ToHTTP):
        port = int(child.attrib.get("port", ""))
        host = child.attrib.get("host", "127.0.0.1")
        url = child.attrib.get("url", "/SCORE")

        class _HTTPWrapper:
            def __init__(self, host, port, url):
                self.name = "<http connection %s:%d%s>" % (host, port, url)
                self._url = url
                try:
                    self._connection = httplib.HTTPConnection(host, port)
                except:
                    logging.getLogger().error("Could not access %s:%d for scoring output." %(host, port))
                    raise
            def write(self, message):
                try:
                    self._connection.request(
                        "Post", self._url, message,
                        {"Content-type":"application/x-www-form-urlencoded",
                        "Accept":"text/plain"})
                except:
                    logging.getLogger().error("An error occurred while sending results over HTTP..")
                    for elem in sys.exc_info():
                        logging.getLogger().error("...%s" % str(elem) )

        out = _HTTPWrapper(port, host, url)

    elif isinstance(child, config.ToStandardError):
        out = sys.stderr
    elif isinstance(child, config.ToStandardOut):
        out = sys.stdout
    else: 
        raise ConfigurationError, \
            "Must define an output destination for scoring output."
    setup = dict(pmmlFileName=pmmlFileName)
    child = configOptions.child("ReportTag", exception=False)
    if child:
        setup.update(dict(reportName=child.attrib.get("name","Report")))

    child = configOptions.child("EventTag", exception=False)
    if child:
        setup.update(dict(
            eventName=child.attrib.get("name", "Event"),
            pseudoEventName=child.attrib.get("pseudoName", "pseudoEvent")))

    return OutputWriter(out, mode=mode, **setup)

################################################################### main
def main(config_file=None, rethrowExceptions=False):
    """From the configuration, set up the Augustus engine.
    
    Set up segments, PMML tree, I/O, and logging for the
    ProducerConsumer.  Identify what task is to be done:
    Producing, Consuming, or Automatic Incremental Model
    updates (AIM).  Identify the model type and start that
    model, passing the segments, PMML tree, and I/O information.

    Arguments:

        config_file (string):
            Path to the configuration file.
    """
    # Get the configuration settings (as an XML instance)
    if isinstance(config_file, config.AugustusConfiguration):
        configRoot = config_file
    else:
        configRoot = xmlbase.loadfile(config_file, config.Config, lineNumbers=True)

    setupLogging(configRoot.matches(lambda x: x.tag in ("Logging", "Metadata")))
    logger = logging.getLogger()
    metadata = logging.getLogger('metadata')

    logger.info("Loading PMML model.")
    child = configRoot.child(config.ModelInput, exception=False)
    metadata.startTiming("Time to load PMML model")
    pmmlModel, pmmlFileName = getModel(child)
    metadata.stopTiming("Time to load PMML model")
    metadata.data["PMML model file"] = pmmlFileName

    logger.info("Setting up data input.")
    child = configRoot.child(config.DataInput, exception=False)
    dataStreamer = getDataStreamer(child)

    child = configRoot.child(config.ConsumerBlending, exception=False)
    consumerUpdateScheme = getUpdateScheme(child)

    child = configRoot.child(config.ModelSetup, exception=False)
    # Default Model setup parameters
    modelWriter = getModelWriter(None)
    engineSettings = dict(maturityThreshold=0)
    producerParameters = {}
    filenameOnException = None
    producerUpdateScheme = getUpdateScheme(None)
    segmentationScheme = SegmentationScheme(None, pmmlModel)
    aggregateUpdateFlag = updateFlag = False

    # Model setup
    if child:
        logger.info("Setting up model updating/producing.")
        modelWriter = getModelWriter(child)
        segmentationScheme = SegmentationScheme(child.child(config.SegmentationSchema, exception=False), pmmlModel)
        if modelWriter is not None:
            engineSettings['lockAllSegments'] = child.attrib.get("mode", None) == "lockExisting"
            producerParameters['resume'] = child.attrib.get("mode", None) == "updateExisting"
            updateFlag = child.attrib.get("updateEvery", "event") in ("event", "both")
            aggregateUpdateFlag = child.attrib.get("updateEvery", "event") in ("aggregate", "both")
            filenameOnException = "".join([modelWriter.baseName, _modelExceptionIdentifier, ".pmml"])
            child = child.child(config.ProducerBlending, exception=False)
            producerUpdateScheme = getUpdateScheme(child)
            if child and child.exists(config.MaturityThreshold):
                maturityConfig = child.child(config.MaturityThreshold)
                engineSettings['maturityThreshold'] = int(maturityConfig.attrib.get("threshold", 1))
                engineSettings['lockingThreshold'] = \
                    None if "lockingThreshold" not in \
                    maturityConfig.attrib \
                    else int(maturityConfig["lockingThreshold"])
            if engineSettings['lockAllSegments'] and not segmentationScheme._generic and not segmentationScheme.whiteList:
                logger.warning("The model is locked and no new segments are specified...new model files will be unchanged.")
        else:
            logger.warning("There is no outputFile attribute in the ModelSetup; no new model file will be created.")

    # Set up output
    child = configRoot.child(config.Output, exception=False)
    outputWriter = getOutputWriter(child, pmmlFileName)
    child = configRoot.child(config.EventSettings, exception=False)
    if child is not None:
        logger.info("Setting up output.")
        # not in a dictionary to reduce the number of lookups while looping
        scoreFlag = child.attrib['score']
        outputFlag = child.attrib['output']
    else:
        scoreFlag = outputFlag = False
    child = configRoot.child(config.AggregationSettings, exception=False)
    if child is not None:
        aggregateScoreFlag = child.attrib['score']
        aggregateOutputFlag = child.attrib['output']
        aggregationSettings = child.attrib
    else:
        aggregationSettings = None

    metadata.data['Update model'] = "true" if updateFlag or aggregateUpdateFlag else "false"

    # build engine once without a data stream
    engine = Engine(pmmlModel, None, producerUpdateScheme, consumerUpdateScheme, segmentationScheme, **engineSettings)
    engine.initialize(producerParameters=producerParameters)
    if outputWriter: outputWriter.open()

    # score fake data from <ModelVerifications>
    modelVerificationConfig = configRoot.child(config.ModelVerification, exception=False)
    if modelVerificationConfig is not None:
        augustus.engine.verification.verify(modelVerificationConfig, engine, logger, outputWriter)

    # start of real data
    logger.info("Setting up Augustus's main engine.")
    engine.resetDataStream(dataStreamer)
    dataStreamer.start_streaming()

    metadata.data['Events'] = 0
    logger.info("Calculating.")
    metadata.startTiming("Run time")

    try:
        while True:
            try:
                score = engine.event(score=scoreFlag, update=updateFlag)
                metadata.data['Events'] += 1
                if outputWriter and outputFlag:
                    try:
                        outputWriter.write(score)
                    except IOError:
                        ## FIXME: this exception should be raised to the top level; I do not
                        ## undersand why it is handled here, nor why a 'good' model is written...--tanya
                        if modelWriter:
                            modelWriter.write(pmmlModel)
                        break
                if modelWriter:
                    modelWriter.serialize(pmmlModel, metadata.data['Events'])

                if aggregationSettings:
                    if engine.checkPseudoeventReadiness(aggregationSettings):
                        score = engine.pseudoevent(score=aggregateScoreFlag, update=aggregateUpdateFlag)
                        if outputWriter and outputOnAggregate:
                            outputWriter.write(score)
                        
            except StopIteration:
                if modelWriter:
                    if modelWriter.serialization:
                        modelWriter.serialize(pmmlModel, metadata.data['Events'])
                    else:
                        modelWriter.write(pmmlModel)
                break

        if aggregationSettings is not None and aggregationSettings['atEnd']:
            score = engine.pseudoevent(score=aggregateScoreFlag, update=aggregateUpdateFlag)
            if outputWriter and aggregateOutputFlag:
                outputWriter.write(score)

    except (Exception, KeyboardInterrupt), err:
        if rethrowExceptions: raise

        logger.error("Shutting down on exception...")
        excinfo = sys.exc_info()
        logger.error("...%s" % excinfo[0])
        logger.error("...%s" % excinfo[1])
        logger.error("...%s" % traceback.format_exc())
        if filenameOnException:
            logger.error("Writing last model in location %s" % filenameOnException)
            pmmlModel.write(filenameOnException)

        sys.exit("Shutting down on exception; for more information check the logfile (if logging is enabled)...\n%s" % traceback.format_exc())

    metadata.stopTiming("Run time")

    if outputWriter: outputWriter.close()
    if modelWriter and modelWriter.thread: 
        while modelWriter.thread.isAlive(): sleep(0)
    metadata.flush()
    logger.info("Augustus is finished.")
    logging.shutdown()

########################################################### Command Line
if __name__ == "__main__":

    from augustus import version
    from augustus.version import __version__
    from optparse import OptionParser, make_option
    version._python_check()
    usage = "usage: %prog [options]"
    version = "%prog " + __version__
    options = [
        make_option(
            "-c","--config", default="config.xml",
            help="configuration file path and name. default: config.xml")]
    parser = OptionParser(usage=usage, version=version, option_list=options)
    (options, arguments) = parser.parse_args()
    main(options.config)
