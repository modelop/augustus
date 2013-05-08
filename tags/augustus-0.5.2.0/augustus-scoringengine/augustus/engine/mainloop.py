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
import httplib
import logging
import os
import platform
import signal
import sys
import threading
from time import sleep
import traceback
import random
import numpy.random
import glob

import augustus.core.pmml41 as pmml
import augustus.core.config as config
import augustus.core.xmlbase as xmlbase
import augustus.engine.verification
from augustus.core.config import ConfigurationError
from augustus.core.defs import Atom, INVALID, MISSING, IMMATURE, MATURE, LOCKED, UNINITIALIZED, NameSpace, NameSpaceReadOnly
from augustus.engine.engine import Engine
from augustus.engine.outputwriter import OutputWriter
from augustus.engine.segmentationschema import SegmentationScheme
from augustus.datastreams.datastreamer import getDataStreamer
from augustus.datastreams.httpstreamer import AugustusHTTPDataStream
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

        if filename.startswith("http://") or filename.startswith("https://"):
            pass

        else:
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
            out = open(output_filename, "w")
        else:
            out = open(output_filename, "a")
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

################################################################### main, refactored into a class
class MainLoop:
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

    def __init__(self, config_file=None, rethrowExceptions=False, dataStream=None):
        # Get the configuration settings (as an XML instance)
        if isinstance(config_file, config.AugustusConfiguration):
            configRoot = config_file
        else:
            configRoot = xmlbase.loadfile(config_file, config.Config, lineNumbers=True)

        if "randomSeed" in configRoot.attrib:
            augustusRandomSeed = configRoot.attrib["randomSeed"]
            random.seed(augustusRandomSeed)
            numpy.random.seed(augustusRandomSeed + 1)
        else:
            augustusRandomSeed = "unspecified"

        setupLogging(configRoot.matches(lambda x: x.tag in ("Logging", "Metadata")))
        logger = logging.getLogger()
        metadata = logging.getLogger("metadata")

        for l in logger, metadata:
            if "initialization" in l.differentLevel:
                l.setLevel(l.differentLevel["initialization"])
            else:
                l.setLevel(l.naturalLevel)

        logger.info("Loading PMML model.")
        modelInput = configRoot.child(config.ModelInput, exception=False)
        metadata.startTiming("Time to load PMML model")
        pmmlModel, pmmlFileName = getModel(modelInput)
        metadata.stopTiming("Time to load PMML model")
        metadata.data["PMML model file"] = pmmlFileName

        logger.info("Setting up data input.")
        child = configRoot.child(config.DataInput, exception=False)
        if dataStream is None:
            fromHTTP = child.child(config.FromHTTP, exception=False)
            if fromHTTP is None:
                dataStreamer = getDataStreamer(child)                
            else:
                dataStreamer = AugustusHTTPDataStream(fromHTTP)
        else:
            dataStreamer = dataStream

        child = configRoot.child(config.ConsumerBlending, exception=False)
        consumerUpdateScheme = getUpdateScheme(child)

        child = configRoot.child(config.ModelSetup, exception=False)
        # Default Model setup parameters
        modelWriter = getModelWriter(None)
        engineSettings = {"maturityThreshold": modelInput.attrib.get("maturityThreshold", 0),
                          "augustusRandomSeed": augustusRandomSeed,
                          "hasProducer": True,
                          }
        filenameOnException = None
        producerUpdateScheme = getUpdateScheme(None)
        segmentationScheme = SegmentationScheme(None, pmmlModel)
        aggregateUpdateFlag = updateFlag = False

        producerAlgorithm = config.producerAlgorithmDefaults
        for pa in producerAlgorithm.values():
            if pa.validate() is not None:
                raise Exception, "Programmer error in producerAlgorithmDefaults"
        if child is not None:
            for pa in child.matches(config.ProducerAlgorithm):
                producerAlgorithm[pa.attrib["model"]] = pa

        # Model setup
        if child:
            logger.info("Setting up model updating/producing.")
            modelWriter = getModelWriter(child)
            segmentationScheme = SegmentationScheme(child.child(config.SegmentationSchema, exception=False), pmmlModel)
            if modelWriter is not None:
                engineSettings["lockAllSegments"] = child.attrib.get("mode", None) == "lockExisting"
                if child.attrib.get("mode", None) == "updateExisting":
                    for pa in producerAlgorithm.values():
                        pa.parameters["updateExisting"] = True
                if child.attrib.get("mode", None) == "replaceExisting":
                    for pa in producerAlgorithm.values():
                        pa.parameters["updateExisting"] = False

                updateFlag = child.attrib.get("updateEvery", "event") in ("event", "both")
                aggregateUpdateFlag = child.attrib.get("updateEvery", "event") in ("aggregate", "both")
                filenameOnException = "".join([modelWriter.baseName, _modelExceptionIdentifier, ".pmml"])
                child = child.child(config.ProducerBlending, exception=False)
                producerUpdateScheme = getUpdateScheme(child)
                if child and child.exists(config.MaturityThreshold):
                    maturityConfig = child.child(config.MaturityThreshold)
                    engineSettings["maturityThreshold"] = int(maturityConfig.attrib.get("threshold", 1))
                    engineSettings["lockingThreshold"] = \
                        None if "lockingThreshold" not in \
                        maturityConfig.attrib \
                        else int(maturityConfig["lockingThreshold"])
                if engineSettings["lockAllSegments"] and segmentationScheme is not None and not segmentationScheme._generic and not segmentationScheme._whiteList:
                    logger.warning("The model is locked and no new segments are specified...new model files will be unchanged.")
            else:
                logger.warning("There is no outputFile attribute in the ModelSetup; no new model file will be created.")
        else:
            engineSettings["hasProducer"] = False

        # Set up output
        child = configRoot.child(config.Output, exception=False)
        outputWriter = getOutputWriter(child, pmmlFileName)
        child = configRoot.child(config.EventSettings, exception=False)
        if child is not None:
            logger.info("Setting up output.")
            # not in a dictionary to reduce the number of lookups while looping
            scoreFlag = child.attrib["score"]
            outputFlag = child.attrib["output"]
        else:
            scoreFlag = outputFlag = False
        child = configRoot.child(config.AggregationSettings, exception=False)
        if child is not None:
            aggregateScoreFlag = child.attrib["score"]
            aggregateOutputFlag = child.attrib["output"]
            aggregationSettings = child.attrib
        else:
            aggregateScoreFlag = False
            aggregateOutputFlag = False
            aggregationSettings = None

        metadata.data["Update model"] = "true" if updateFlag or aggregateUpdateFlag else "false"

        # build engine once without a data stream
        engine = Engine(pmmlModel, None, producerUpdateScheme, consumerUpdateScheme, segmentationScheme, producerAlgorithm, **engineSettings)
        engine.initialize()
        if outputWriter: outputWriter.open()

        for l in logger, metadata:
            if "verification" in l.differentLevel:
                l.eventLogLevel = l.differentLevel["verification"]
                l.setLevel(l.differentLevel["verification"])
            else:
                l.eventLogLevel = l.naturalLevel
                l.setLevel(l.naturalLevel)

        # score fake data from <ModelVerifications>
        modelVerificationConfig = configRoot.child(config.ModelVerification, exception=False)
        if modelVerificationConfig is not None:
            augustus.engine.verification.verify(modelVerificationConfig, engine, logger, outputWriter)

        # verification can increment aggregate variables, but
        # aggregates should all start at zero at the start of real
        # processing, whether verification happened or not
        engine.flushAggregates()

        if isinstance(dataStreamer, AugustusHTTPDataStream):
            if outputWriter is None:
                dataStreamer.respond = False
            if dataStreamer.respond:
                dataStreamer.setupOutput(outputWriter)

        for l in logger, metadata:
            if "eventloop" in l.differentLevel:
                l.eventLogLevel = l.differentLevel["eventloop"]
                l.setLevel(l.differentLevel["eventloop"])
            else:
                l.eventLogLevel = l.naturalLevel
                l.setLevel(l.naturalLevel)

        # possibly set up custom processing
        customProcessing = configRoot.child(config.CustomProcessing, exception=False)
        if customProcessing is not None:
            constants = engine.pmmlModel.child(pmml.Extension, exception=False)
            if constants is None:
                constants = NameSpaceReadOnly()
            else:
                constants = constants.child(pmml.X_ODG_CustomProcessingConstants, exception=False)
                if constants is None:
                    constants = NameSpaceReadOnly()
                else:
                    constants = constants.nameSpace

            atoms = {"INVALID": INVALID, "MISSING": MISSING, "IMMATURE": IMMATURE, "MATURE": MATURE, "LOCKED": LOCKED, "UNINITIALIZED": UNINITIALIZED}
            for thing in pmml.OutputField.__dict__.values() + pmml.X_ODG_OutputField.__dict__.values():
                if isinstance(thing, Atom):
                    atoms[repr(thing)] = thing

            customProcessing.initialize(pmmlModel, engine.pmmlModel, constants, [s.userFriendly for s in engine.segmentRecords], atoms, logger, metadata, consumerUpdateScheme, producerUpdateScheme)
            engine.customProcessing = customProcessing
            engine.reinitialize()

        else:
            # only shut off circular garbage collection if there is no CustomProcessing or AugustusInterface
            gc.disable()

        self.dataStreamer = dataStreamer
        self.logger = logger
        self.engine = engine
        self.metadata = metadata
        self.aggregationSettings = aggregationSettings
        self.rethrowExceptions = rethrowExceptions
        self.scoreFlag = scoreFlag
        self.updateFlag = updateFlag
        self.outputWriter = outputWriter
        self.outputFlag = outputFlag
        self.modelWriter = modelWriter
        self.filenameOnException = filenameOnException
        self.pmmlModel = pmmlModel
        self.aggregateScoreFlag = aggregateScoreFlag
        self.aggregateUpdateFlag = aggregateUpdateFlag
        self.aggregateOutputFlag = aggregateOutputFlag
        self.customProcessing = customProcessing

    def doBegin(self):
        """Executes code before the event stream; necessary for setting up metadata."""

        # start of real data
        self.logger.info("Setting up Augustus's main engine.")
        self.engine.resetDataStream(self.dataStreamer)
        self.dataStreamer.initialize()

        if self.customProcessing is not None:
            out = self.customProcessing.doBegin()
            if out is not None and self.outputWriter and self.outputFlag:
                self.outputWriter.write(out)

        self.metadata.data["Events"] = 0
        self.logger.info("Calculating.")
        self.metadata.startTiming("Run time")

    def doEvent(self):
        """Executes one event, returning False if there was an error that should stop the loop."""

        score = self.engine.event(score=self.scoreFlag, update=self.updateFlag)

        self.metadata.data["Events"] += 1
        if self.outputWriter and self.outputFlag:
            try:
                self.outputWriter.write(score)
            except IOError:
                ## FIXME: this exception should be raised to the top level; I do not
                ## undersand why it is handled here, nor why a 'good' model is written...--tanya
                if self.modelWriter:
                    self.logger.info("About to write the model to PMML.")
                    self.modelWriter.write(self.pmmlModel)
                    self.logger.info("Done writing PMML.")
                return False

        if self.modelWriter:
            self.logger.debug("Writing a copy of the current model to PMML (model serialization).")
            self.modelWriter.serialize(self.pmmlModel, self.metadata.data["Events"])
            self.logger.debug("Done writing PMML.")

        if self.aggregationSettings:
            if self.engine.checkPseudoeventReadiness(self.aggregationSettings):
                score = self.engine.pseudoevent(score=self.aggregateScoreFlag, update=self.aggregateUpdateFlag)
                if self.outputWriter and self.aggregateOutputFlag:
                    self.outputWriter.write(score)

        return True

    def doEnd(self):
        """Executes code after successful event processing.  For file-based producers, this is when the producer algorithm starts."""

        numEvents = self.engine.eventNumber
        if numEvents < 2:
            self.logger.error("Processed %d events before encountering StopIteration: check your data stream configuration (including file type)!" % numEvents)
        else:
            self.logger.warning("Processed %d events before encountering StopIteration." % numEvents)

        for l in self.logger, self.metadata:
            if "produce" in l.differentLevel:
                l.setLevel(l.differentLevel["produce"])
            else:
                l.setLevel(l.naturalLevel)

        self.engine.produce()

        for l in self.logger, self.metadata:
            if "shutdown" in l.differentLevel:
                l.setLevel(l.differentLevel["shutdown"])
            else:
                l.setLevel(l.naturalLevel)

        if self.customProcessing is not None:
            out = self.customProcessing.doEnd()
            if out is not None and self.outputWriter and self.outputFlag:
                self.outputWriter.write(out)

        if self.modelWriter:
            if self.modelWriter.serialization:
                self.modelWriter.serialize(self.pmmlModel, self.metadata.data["Events"])
            else:
                self.logger.info("About to write the model to PMML.")
                self.modelWriter.write(self.pmmlModel)
                self.logger.info("Done writing.")

    def doShutdown(self):
        """Executes code after doEnd or an exception, if Augustus is handling exceptions (rethrowExceptions is False)."""

        self.metadata.stopTiming("Run time")

        if self.outputWriter: self.outputWriter.close()
        if self.modelWriter and self.modelWriter.thread: 
            while self.modelWriter.thread.isAlive(): sleep(0)
        self.metadata.flush()
        self.logger.info("Augustus is finished.")
        logging.shutdown()

    def run(self):
        """Executes all events (the doBegin, doEvent ... doEvent, doEnd, doShutdown lifecycle)."""

        self.doBegin()

        try:
            stillGoing = True
            while stillGoing:
                try:
                    stillGoing = self.doEvent()

                except StopIteration:
                    self.doEnd()
                    stillGoing = False

            # outside "while stillGoing" but inside try ... except (Exception, KeyboardInterrupt)"
            if self.aggregationSettings is not None and self.aggregationSettings["atEnd"]:
                score = self.engine.pseudoevent(score=self.aggregateScoreFlag, update=self.aggregateUpdateFlag)
                if self.outputWriter and self.aggregateOutputFlag:
                    self.outputWriter.write(score)

        except (Exception, KeyboardInterrupt), err:
            if self.rethrowExceptions: raise

            for l in self.logger, self.metadata:
                if "shutdown" in l.differentLevel:
                    l.setLevel(l.differentLevel["shutdown"])
                else:
                    l.setLevel(l.naturalLevel)

            if self.customProcessing is not None:
                self.customProcessing.doException()

            self.logger.error("Shutting down on exception after %d successful events..." % self.engine.eventNumber)
            excinfo = sys.exc_info()
            self.logger.error("...%s" % excinfo[0])
            self.logger.error("...%s" % excinfo[1])
            self.logger.error("...%s" % traceback.format_exc())
            if self.filenameOnException:
                self.logger.error("Writing last model in location %s" % self.filenameOnException)
                self.pmmlModel.write(self.filenameOnException)

            sys.exit("Shutting down on exception; for more information check the logfile (if logging is enabled)...\n%s" % traceback.format_exc())

        self.doShutdown()

def main(config_file=None, rethrowExceptions=False, dataStream=None):
    mainLoop = MainLoop(config_file, rethrowExceptions, dataStream)
    mainLoop.run()
