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

"""The main loop of the Augustus process."""

import os
import sys
import time
import glob
import logging
import traceback
import random
import numpy.random
import gc
import codecs

import augustus.core.xmlbase as xmlbase
import augustus.core.config as config
import augustus.core.pmml41 as pmml
from augustus.core.defs import Atom, INVALID, MISSING, IMMATURE, MATURE, LOCKED, UNINITIALIZED, NameSpaceReadOnly
from augustus.core.config import ConfigurationError
from augustus.datastreams.datastreamer import DataStreamer
from augustus.datastreams.httpstreamer import AugustusHTTPDataStream
from augustus.datastreams.modelwriter import getModelWriter
from augustus.logsetup.logsetup import setupLogging
from augustus.engine.engine import Engine
from augustus.engine.outputwriter import OutputWriter
from augustus.engine.segmentationschema import SegmentationScheme
from augustus.engine.verification import verify
from augustus.algorithms.eventweighting import UpdateScheme

class MainLoop(object):
    """Read the configuration file, set up an Augustus job, and run it.

    Arguments:

        configuration (string or XML object):
            Literal configuration string, path to a configuration
            file, or configuration XML tree structure (must be
            validated).  Must be provided.

        model (optional string or XML object):
            Literal PMML string, path to a PMML file, or a validated
            XML tree structure.  If provided, this overrides what is
            in the configuration.

        dataStream (optional class with 'initialize', 'next', and 'get'):
            Explicit data stream, overriding that which is in the
            configuration file.

        rethrowExceptions (optional bool, default is False):
            If an exception is caught by MainLoop, re-throw it to the
            containing loop.  Use this if you're running Augustus in
            a larger program and want to handle exceptions on your
            own.

    Notes:

        If the configuration contains a "randomSeed" directive,
        MainLoop's __init__ will globally change the random seed of
        Python's random library and numpy.random.

        MainLoop's __init__ will globally change the numpy error
        handling.

        Unless the configuration has a <CustomProcessing> block (which
        is always the case within an AugustusInterface), MainLoop's
        __init__ will globally disable circular garbage collection.

    """

    _modelExceptionIdentifier = "EXCEPTION"

    def __init__(self, configuration, model=None, dataStream=None, rethrowExceptions=None):
        self.model = model
        self.dataStream = dataStream
        self.rethrowExceptions = rethrowExceptions
        self.fileNameOnException = None

        # get the configuration, in whatever form you find it
        if isinstance(configuration, config.AugustusConfiguration):
            pass
        elif isinstance(configuration, basestring):
            try:
                configuration = xmlbase.loadfile(configuration, config.Config, lineNumbers=True)
            except IOError:
                configuration = xmlbase.load(configuration, config.Config, lineNumbers=True)
        else:
            raise ConfigurationError("Configuration must be a pre-validated XML object, a fileName, or a literal configuration string.")
    
        # set up logging
        setupLogging(configuration.matches(lambda x: isinstance(x, (config.Logging, config.Metadata))))
        self.logger = logging.getLogger()
        self.metadata = logging.getLogger("metadata")

        # begin "initialization" phase
        for l in self.logger, self.metadata:
            if "initialization" in l.differentLevel:
                l.setLevel(l.differentLevel["initialization"])
            else:
                l.setLevel(l.naturalLevel)

        # get the model, in whatever form you find it
        self.logger.info("Loading PMML model.")
        self.metadata.startTiming("Time to load PMML model")
        modelFileName = "(none)"
        maturityThreshold = 0
        if self.model is None:
            modelInput = configuration.child(config.ModelInput, exception=None)
            if modelInput is None:
                raise ConfigurationError("If a model is not provided to MainLoop explicitly, it must be present in the configuration file.")

            fileLocation = modelInput["fileLocation"]
            if not fileLocation.startswith("http://") and not fileLocation.startswith("https://"):
                fileList = glob.glob(fileLocation)
                if len(fileList) > 1:
                    fileList = [f for f in fileList if self._modelExceptionIdentifier not in f]
                if len(fileList) == 0:
                    raise IOError("No files matched the ModelInput fileLocation \"%s\"." % fileLocation)

                selectmode = modelInput.attrib.get("selectmode", "lastAlphabetic")
                if selectmode == "mostRecent":
                    fileLocation = max(fileList, key=lambda x: os.stat(x).st_mtime)
                elif selectmode == "lastAlphabetic":
                    fileList.sort()
                    fileLocation = fileList[-1]
                else:
                    assert False

                if self._modelExceptionIdentifier in fileLocation:
                    self.logger.warning("Using a PMML model that was written on exception (fileName \"%s\")" % fileLocation)

            self.model = xmlbase.loadfile(fileLocation, pmml.X_ODG_PMML, lineNumbers=True)

            if "maturityThreshold" in modelInput.attrib: maturityThreshold = modelInput["maturityThreshold"]

        elif isinstance(self.model, pmml.PMML):
            pass
        elif isinstance(self.model, basestring):
            try:
                self.model, modelFileName = xmlbase.loadfile(self.model, pmml.X_ODG_PMML, lineNumbers=True), self.model
            except IOError:
                self.model = xmlbase.load(self.model, pmml.X_ODG_PMML, lineNumbers=True)
        else:
            raise ConfigurationError("Model must be a pre-validated XML object, a fileName, or a literal PMML string.")
        self.metadata.stopTiming("Time to load PMML model")
        self.metadata.data["PMML model file"] = modelFileName

        # globally set random number seeds
        if "randomSeed" in configuration.attrib:
            augustusRandomSeed = configuration["randomSeed"]
            random.seed(augustusRandomSeed)
            numpy.random.seed(augustusRandomSeed + 1)
        else:
            augustusRandomSeed = "unspecified"

        # globally set numpy error handling
        numpy.seterr(divide="raise", over="raise", under="ignore", invalid="raise")

        # update schemes (producerUpdateScheme may be redefined below)
        consumerUpdateScheme = self._getUpdateScheme(configuration.child(config.ConsumerBlending, exception=False))
        producerUpdateScheme = self._getUpdateScheme(None)

        # set up scoring output
        outputConfig = configuration.child(config.Output, exception=False)
        if outputConfig is None:
            self.outputWriter = None
        else:
            outputParams = {"pmmlFileName": modelFileName, "mode": outputConfig.destination.attrib.get("type", "XML").lower()}

            if isinstance(outputConfig.destination, config.ToFile):
                if outputConfig.destination.attrib.get("overwrite", False):
                    outputStream = codecs.open(outputConfig.destination["name"], "w", encoding="utf-8")
                else:
                    outputStream = codecs.open(outputConfig.destination["name"], "a", encoding="utf-8")
            elif isinstance(outputConfig.destination, config.ToStandardError):
                outputStream = sys.stderr
            elif isinstance(outputConfig.destination, config.ToStandardOut):
                outputStream = sys.stdout
            else:
                assert False

            reportTag = outputConfig.child("ReportTag", exception=False)
            if reportTag:
                outputParams["reportName"] = reportTag.attrib.get("name", "Report")

            eventTag = outputConfig.child("EventTag", exception=False)
            if eventTag:
                outputParams["eventName"] = eventTag.attrib.get("name", "Event")
                outputParams["pseudoEventName"] = eventTag.attrib.get("pseudoName", "pseudoEvent")

            self.outputWriter = OutputWriter(outputStream, **outputParams)

        # initialize for the case of no output model
        engineSettings = {"maturityThreshold": maturityThreshold, "augustusRandomSeed": augustusRandomSeed}
        self.modelWriter = None
        segmentationScheme = SegmentationScheme(None, self.model)
        self.updateFlag = False
        self.aggregateUpdateFlag = False

        producerAlgorithm = dict(config.producerAlgorithmDefaults)
        for pa in producerAlgorithm.values():
            validationResult = pa.validate()
            assert validationResult is None

        # set up output model, if present in the configuration
        modelSetup = configuration.child(config.ModelSetup, exception=False)
        engineSettings["hasProducer"] = modelSetup is not None
        if engineSettings["hasProducer"]:
            self.logger.info("Setting up model updating/producing.")

            producerBlending = modelSetup.child(config.ProducerBlending, exception=False)
            producerUpdateScheme = self._getUpdateScheme(producerBlending)
            if producerBlending is not None and producerBlending.contains(config.MaturityThreshold):
                maturityConfig = producerBlending.child(config.MaturityThreshold)
                engineSettings["maturityThreshold"] = int(maturityConfig.attrib.get("threshold", 1))
                try:
                    engineSettings["lockingThreshold"] = int(maturityConfig.attrib["lockingThreshold"])
                except KeyError:
                    engineSettings["lockingThreshold"] = None

            engineSettings["lockAllSegments"] = modelSetup.attrib.get("mode", None) == "lockExisting"
            if engineSettings["lockAllSegments"] and segmentationScheme is not None and not segmentationScheme._generic and not segmentationScheme._whiteList:
                self.logger.warning("The model is locked and no new segments are specified...new model files will be unchanged.")

            self.modelWriter = getModelWriter(modelSetup)
            if self.modelWriter is not None:
                if self.modelWriter.baseName is None:
                    self.fileNameOnException = self._modelExceptionIdentifier + ".pmml"
                else:
                    self.fileNameOnException = "".join([self.modelWriter.baseName, self._modelExceptionIdentifier, ".pmml"])
            else:
                self.logger.warning("There is no outputFile attribute in the ModelSetup; no new model file will be created.")

            segmentationScheme = SegmentationScheme(modelSetup.child(config.SegmentationSchema, exception=False), self.model)
            self.updateFlag = modelSetup.attrib.get("updateEvery", "event") in ("event", "both")
            self.aggregateUpdateFlag = modelSetup.attrib.get("updateEvery", "event") in ("aggregate", "both")

            for pa in modelSetup.matches(config.ProducerAlgorithm):
                producerAlgorithm[pa["model"]] = pa
            if modelSetup.attrib.get("mode", None) == "updateExisting":
                for pa in producerAlgorithm.values():
                    pa.parameters["updateExisting"] = True
            if modelSetup.attrib.get("mode", None) == "replaceExisting":
                for pa in producerAlgorithm.values():
                    pa.parameters["updateExisting"] = False

        # to score or not to score
        eventSettings = configuration.child(config.EventSettings, exception=False)
        if eventSettings is not None:
            self.logger.info("Setting up output.")
            self.scoreFlag = eventSettings["score"]
            self.outputFlag = eventSettings["output"]
        else:
            self.scoreFlag = False
            self.outputFlag = False

        aggregationConfig = configuration.child(config.AggregationSettings, exception=False)
        if aggregationConfig is not None:
            self.aggregateScoreFlag = aggregationConfig["score"]
            self.aggregateOutputFlag = aggregationConfig["output"]
            self.aggregationSettings = dict(aggregationConfig.attrib)
        else:
            self.aggregateScoreFlag = False
            self.aggregateOutputFlag = False
            self.aggregationSettings = None

        self.metadata.data["Update model"] = "true" if self.updateFlag or self.aggregateUpdateFlag else "false"

        # build a scoring engine once without a dataStream (to evaluate any verification blocks)
        self.engine = Engine(self.model, None, producerUpdateScheme, consumerUpdateScheme, segmentationScheme, producerAlgorithm, **engineSettings)
        self.engine.initialize()
        if self.outputWriter is not None: self.outputWriter.open()

        # begin "verification" phase
        for l in self.logger, self.metadata:
            if "verification" in l.differentLevel:
                l.eventLogLevel = l.differentLevel["verification"]
                l.setLevel(l.differentLevel["verification"])
            else:
                l.eventLogLevel = l.naturalLevel
                l.setLevel(l.naturalLevel)

        # evaluate verification blocks
        modelVerificationConfig = configuration.child(config.ModelVerification, exception=False)
        if modelVerificationConfig is not None:
            verify(modelVerificationConfig, self.engine, self.logger, self.outputWriter)

        # verification can increment aggregate variables, but
        # aggregates should all start at zero at the start of real
        # processing, whether verification happened or not
        self.engine.flushAggregates()

        # get the dataStream, in whatever form you find it
        self.logger.info("Setting up data input.")
        if self.dataStream is None:
            configDataInput = configuration.child(config.DataInput, exception=None)
            if configDataInput is None:
                raise ConfigurationError("If a dataStream is not provided to MainLoop explicitly, it must be present in the configuration file.")
            if configDataInput.contains(config.FromFile):
                self.dataStream = DataStreamer(configDataInput.child(config.FromFile), self.engine.pmmlModel)
            elif configDataInput.contains(config.FromStandardIn):
                self.dataStream = DataStreamer(configDataInput.child(config.FromStandardIn), self.engine.pmmlModel)
            elif configDataInput.contains(config.FromHTTP):
                self.dataStream = AugustusHTTPDataStream(configDataInput.child(config.FromHTTP))
                if self.outputWriter is None:
                    self.dataStream.respond = False
                if self.dataStream.respond:
                    self.dataStream.setupOutput(self.outputWriter)
            else:
                assert False

        # begin "eventLoop" phase
        for l in self.logger, self.metadata:
            if "eventloop" in l.differentLevel:
                l.eventLogLevel = l.differentLevel["eventloop"]
                l.setLevel(l.differentLevel["eventloop"])
            else:
                l.eventLogLevel = l.naturalLevel
                l.setLevel(l.naturalLevel)

        # possibly set up custom processing
        self.customProcessing = configuration.child(config.CustomProcessing, exception=False)
        if self.customProcessing is not None:
            constants = self.engine.pmmlModel.child(pmml.Extension, exception=False)
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

            self.customProcessing.initialize(self.model, self.engine.pmmlModel, constants, [s.userFriendly for s in self.engine.segmentRecords], atoms, self.logger, self.metadata, consumerUpdateScheme, producerUpdateScheme)
            self.engine.customProcessing = self.customProcessing
            self.engine.reinitialize()

        else:
            # only turn off circular garbage collection if there is no CustomProcessing or AugustusInterface
            gc.disable()

    def _getUpdateScheme(self, configuration):
        """Return an UpdateScheme as specified by the user configuration.

        Arguments:

            configuration (XML object, defined in xmlbase):
                An XML element of type "Blending"; either
                <ConsumerBlending/> or <ProducerBlending/>; containing the
                weightings and default settings for the model update schemes.
        """

        if configuration is None: return UpdateScheme("unweighted")

        params = dict(configuration.attrib)
        scheme = "unweighted"

        if "method" in params:
            scheme = params["method"]
            if scheme == "eventTimeWindow": scheme = "synchronized"
            del params["method"]

        if scheme in ("window", "synchronized") and "windowLag" not in params:
            params["windowLag"] = 0

        return UpdateScheme(scheme, **params)

    def doBegin(self):
        """Executes code before the event stream; necessary for setting up metadata."""

        # start of real data
        self.logger.info("Setting up Augustus's main engine.")
        self.engine.resetDataStream(self.dataStream)
        self.dataStream.initialize()

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
                if self.modelWriter:
                    self.logger.info("About to write the model to PMML.")
                    self.modelWriter.write(self.model)
                    self.logger.info("Done writing PMML.")
                return False

        if self.modelWriter is not None and self.modelWriter.serialization is not None:
            self.logger.debug("Writing a copy of the current model to PMML (model serialization).")
            self.modelWriter.serialize(self.model, self.metadata.data["Events"])
            self.logger.debug("Done writing PMML.")

        if self.aggregationSettings is not None:
            if self.engine.checkPseudoeventReadiness(self.aggregationSettings):
                score = self.engine.pseudoevent(score=self.aggregateScoreFlag, update=self.aggregateUpdateFlag)
                if self.outputWriter and self.aggregateOutputFlag:
                    self.outputWriter.write(score)

        return True

    def doEnd(self):
        """Executes code after successful event processing.  For file-based producers, this is when the producer algorithm starts."""

        numEvents = self.engine.eventNumber
        self.logger.info("Processed %d events before encountering StopIteration." % numEvents)

        # begin "produce" phase
        for l in self.logger, self.metadata:
            if "produce" in l.differentLevel:
                l.setLevel(l.differentLevel["produce"])
            else:
                l.setLevel(l.naturalLevel)

        self.engine.produce()

        # begin "shutdown" phase
        for l in self.logger, self.metadata:
            if "shutdown" in l.differentLevel:
                l.setLevel(l.differentLevel["shutdown"])
            else:
                l.setLevel(l.naturalLevel)

        if self.customProcessing is not None:
            out = self.customProcessing.doEnd()
            if out is not None and self.outputWriter and self.outputFlag:
                self.outputWriter.write(out)

        if self.modelWriter is not None:
            if self.modelWriter.serialization:
                self.modelWriter.serialize(self.model, self.metadata.data["Events"])
            else:
                self.logger.info("About to write the model to PMML.")
                self.modelWriter.write(self.model)
                self.logger.info("Done writing.")

    def doShutdown(self):
        """Executes code after doEnd or an exception, if Augustus is handling exceptions (rethrowExceptions is False)."""

        self.metadata.stopTiming("Run time")

        if self.outputWriter: self.outputWriter.close()
        if self.modelWriter and self.modelWriter.thread: 
            while self.modelWriter.thread.isAlive(): time.sleep(0)
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
            if self.fileNameOnException is not None:
                self.logger.error("Writing last model in location %s" % self.fileNameOnException)
                self.model.write(self.fileNameOnException)

            sys.exit("Shutting down on exception; for more information check the logfile (if logging is enabled)...\n%s" % traceback.format_exc())

        self.doShutdown()

# the actual main loop, called by the Augustus executable script
def main(configuration, dataStream=None, rethrowExceptions=False):
    mainLoop = MainLoop(configuration, dataStream=dataStream, rethrowExceptions=rethrowExceptions)
    mainLoop.run()
