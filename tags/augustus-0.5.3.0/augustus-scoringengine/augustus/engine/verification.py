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

"""Implements <ModelVerification> tests."""

import augustus.core.pmml41 as pmml
from augustus.core.defs import MISSING
from augustus.engine.segmentrecord import SELECTONLY
from augustus.engine.outputwriter import OutputSegment, OutputRecord

class ModelVerificationError(Exception): pass

def verify(modelVerificationConfig, engine, logger, outputWriter):
    modelsWithVerification = []

    if modelVerificationConfig.attrib.get("checkModel", True):
        if engine.pmmlModel.exists(pmml.ModelVerification):
            engine.pmmlModel.segmentRecord = None
            modelsWithVerification.append(engine.pmmlModel)

    if modelVerificationConfig.attrib.get("checkSegments", True):
        for segment in engine.segmentRecords:
            if segment.pmmlModel.exists(pmml.ModelVerification):
                segment.pmmlModel.segmentRecord = segment

                if segment.pmmlModel not in modelsWithVerification:
                    modelsWithVerification.append(segment.pmmlModel)

    haltOnFailures = (modelVerificationConfig.attrib.get("onFailures", "halt") == "halt")
    reportInScores = modelVerificationConfig.attrib.get("reportInScores", False)

    for model in modelsWithVerification:
        logger.info("Evaluating ModelVerification in model %s%s." % (model.tag, model.fileAndLine()))

        for verificationBlockCounter, verification in enumerate(model.matches(pmml.ModelVerification)):
            verification.initialize()

            # the verificaiton block acts as a fake data stream
            engine.resetDataStream(verification)

            segmentRecord = model.segmentRecord
            while True:
                try:
                    if segmentRecord is None:
                        outputRecord = engine.event(score=True, update=False, explicitSegments=None, predictedName=verification.predictedName)
                    else:
                        outputRecord = engine.event(score=True, update=False, explicitSegments=[segmentRecord], predictedName=verification.predictedName)

                    if len(outputRecord.segments) == 0:
                        results = [(f, MISSING) for f in verification.predicted]
                    else:
                        results = outputRecord.segments[0].fields

                    if reportInScores and outputWriter:
                        verificationErrorsBlock = OutputRecord(outputRecord.eventNumber, SELECTONLY)
                        verificationErrorsSegment = OutputSegment(None)
                        verificationErrorsBlock.segments = [verificationErrorsSegment]

                    for name, resultValue in results:
                        if name in verification.column:
                            expectedValue = verification.get(name)

                            if isinstance(resultValue, (float, int, long)):
                                try:
                                    expectedValue = float(expectedValue)
                                except AttributeError:
                                    verified = False
                                else:
                                    zeroThreshold = verification.zeroThreshold[name]

                                    if abs(expectedValue) <= zeroThreshold:
                                        verified = (abs(resultValue) <= zeroThreshold)

                                    else:
                                        precision = verification.precision[name]
                                        verified = ((expectedValue * (1. - precision)) <= resultValue <= (expectedValue * (1. + precision)))

                            else:
                                verified = (expectedValue == resultValue)

                            if not verified:
                                modelName = model.attrib.get("modelName", None)
                                if modelName is not None:
                                    modelName = ", modelName \"%s\"" % modelName
                                else:
                                    modelName = ""

                                errorString = "Verification failure in ModelVerification %d%s in model %s%s%s, row %d (\"<%s>\"): %s (\"%s\") expected to be %s, calculated to be %s" % (verificationBlockCounter, verification.fileAndLine(), model.tag, model.fileAndLine(), modelName, verification.table.index, verification.table.rows[verification.table.index].content(), name, verification.column[name], str(expectedValue), str(resultValue))

                                if haltOnFailures:
                                    raise ModelVerificationError(errorString)
                                else:
                                    logger.error(errorString)
                                    if reportInScores and outputWriter:
                                        verificationErrorsSegment.fields.append((name, resultValue))

                    if reportInScores and outputWriter:
                        eventTags = [("model", "%s on line %s" % (model.tag, str(getattr(model, "lineStart", "unknown"))))]
                        modelName = model.attrib.get("modelName", None)
                        if modelName is not None:
                            eventTags.append(("modelName", modelName))
                        eventTags.append(("block", str(verificationBlockCounter)))

                        outputWriter.write(verificationErrorsBlock, eventTags=eventTags, eventName="ModelVerificationFailures")

                except StopIteration:
                    break
        
        logger.info("Done with ModelVerification in model %s%s." % (model.tag, model.fileAndLine()))
