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
# See the License for the specific language governing permissions and
# limitations under the License.

"""Defines the Naive Bayes producer and consumer algorithms."""

from augustus.core.python3transition import *

# system includes
import math
import numpy

# local includes
from augustus.core.defs import Atom, INVALID, MISSING
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm
from augustus.algorithms.eventweighting import SUMX

import augustus.core.pmml41 as pmml

SCORE_predictedValue = pmml.OutputField.predictedValue
SCORE_probability = pmml.OutputField.probability
SCORE_residual = pmml.OutputField.residual

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerNaiveBayesModel(ConsumerAlgorithm):
    def initialize(self):
        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        self.resetLoggerLevels()
        probability = self.model.evaluate(get)

        if probability is INVALID:
            self.lastScore = INVALID
            self.logger.debug("ConsumerNaiveBayesModel.score: returning INVALID score")
            return self.lastScore

        winner = None
        winningProb = -1.
        for value, prob in probability.items():
            if prob > winningProb:
                winner = value
                winningProb = prob

        actualValue = get(self.model.bayesOutput.attrib["fieldName"])
        if not isinstance(actualValue, Atom):
            actualValue = str(actualValue)

        indicator = (1. if actualValue == winner else 0.)

        output = {SCORE_predictedValue: winner, SCORE_probability: winningProb, SCORE_residual: indicator - winningProb}

        # OutputField's "value" parameter can only be a string (no knowledge of type), so it's important that "value" (below) is also a string
        for value in self.model.targetCategories:
            p = probability[value]
            indicator = (1. if actualValue == value else 0.)

            output[SCORE_probability, value] = p
            output[SCORE_residual, value] = indicator - p

        self.lastScore = output
        return self.lastScore

#########################################################################################
######################################################################### producer ######
#########################################################################################

class ProducerNaiveBayesModel(ProducerAlgorithm):
    defaultParams = {"updateExisting": "false"}

    def initialize(self, **params):
        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        self.model = self.segmentRecord.pmmlModel

        ### field names for get()
        self.inputFields = []
        for bayesInput in self.model.bayesInputs:
            self.inputFields.append(bayesInput.attrib["fieldName"])

        self.outputField = self.model.bayesOutput.attrib["fieldName"]

        self.first = True

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def firstUpdate(self):
        ### if we're not updating an existing model, be sure to zero-out all of its PMML and consumer caches
        if not self.updateExisting:
            for bayesInput in self.model.bayesInputs:
                # PMML
                for tvcDict in bayesInput.tvcMap.values():
                    for tvc in tvcDict.values():
                        tvc.attrib["count"] = 0.

                # consumer cache
                for pcDict in bayesInput.pairCounts.values():
                    for pcKey in pcDict.keys():
                        pcDict[pcKey] = 0.

                for denomKey in bayesInput.denominators.keys():
                    bayesInput.denominators[denomKey] = 0.

            # PMML
            for tvc in self.model.bayesOutput.targetValueCounts.tvcMap.values():
                tvc.attrib["count"] = 0.

            # consumer cache
            self.model.targetCounts *= 0.

        ### create updators for everything
        self.inputPairUpdators = {}
        self.inputDenomUpdators = {}
        self.outputUpdators = {}
        
        for inputField, bayesInput in self.model.bayesInput.items():
            self.inputPairUpdators[inputField] = {}

            for inputValue, tvcDict in bayesInput.tvcMap.items():
                self.inputPairUpdators[inputField][inputValue] = {}

                for outputValue, tvc in tvcDict.items():
                    self.inputPairUpdators[inputField][inputValue][outputValue] = self.engine.producerUpdateScheme.updator(SUMX)
                    self.inputPairUpdators[inputField][inputValue][outputValue].initialize({SUMX: tvc.attrib["count"]})

            self.inputDenomUpdators[inputField] = {}
            for outputValue, summation in bayesInput.denominators.items():
                self.inputDenomUpdators[inputField][outputValue] = self.engine.producerUpdateScheme.updator(SUMX)
                self.inputDenomUpdators[inputField][outputValue].initialize({SUMX: summation})

        for outputValue, count in zip(self.model.targetCategories, self.model.targetCounts):
            self.outputUpdators[outputValue] = self.engine.producerUpdateScheme.updator(SUMX)
            self.outputUpdators[outputValue].initialize({SUMX: count})

        self.first = False

    def update(self, syncNumber, get):
        self.resetLoggerLevels()
        if self.first: self.firstUpdate()

        ### get the output value
        outputValue = get(self.outputField)
        if outputValue is INVALID or outputValue is MISSING:
            self.logger.debug("NaiveBayes.update: returning False (INVALID or MISSING data)")
            return False
        # output values are compared as strings because that is how they're referenced by TargetValueCount["value"] and OutputField["value"]
        outputValue = str(outputValue)

        ### if we have not seen this output value, make a new element in all representations
        ### this happens relatively rarely
        if outputValue not in self.outputUpdators:
            # updator
            self.outputUpdators[outputValue] = self.engine.producerUpdateScheme.updator(SUMX)

            # PMML
            tvc = pmml.newInstance("TargetValueCount", attrib={"value": outputValue, "count": 0.})
            targetValueCounts = self.model.bayesOutput.targetValueCounts
            targetValueCounts.tvcMap[outputValue] = tvc
            targetValueCounts.children.append(tvc)

            # consumer cache
            self.model.targetIndex[outputValue] = len(self.model.targetCategories)
            self.model.targetCategories.append(outputValue)
            self.model.targetCounts = numpy.append(self.model.targetCounts, 0.)

        ### update the output values histogram
        ### this happens very frequently
        tvcMap = self.model.bayesOutput.targetValueCounts.tvcMap
        targetCounts = self.model.targetCounts
        targetIndex = self.model.targetIndex
        for value, updator in self.outputUpdators.items():
            # updator
            if value == outputValue:
                updator.increment(syncNumber, 1.)
            else:
                updator.increment(syncNumber, 0.)
            newcount = updator.sum()

            # PMML
            tvcMap[value].attrib["count"] = newcount

            # consumer cache
            targetCounts[targetIndex[value]] = newcount

        ### get the input value; INVALID input -> skip all input fields, MISSING input -> skip only the missing field
        inputValues = [bi.evaluate(get) for bi in self.model.bayesInputs]
        if INVALID in inputValues:
            self.logger.debug("NaiveBayes.update: returning False (INVALID Bayes input fields)")
            return False

        for inputField, inputValue in zip(self.inputFields, inputValues):
            if inputValue is not MISSING:
                bayesInput = self.model.bayesInput[inputField]
                inputPairUpdator = self.inputPairUpdators[inputField]
                inputDenomUpdator = self.inputDenomUpdators[inputField]

                ### if we have not seen this input value, make a new element in all representations
                ### this happens relatively rarely
                if inputValue not in inputPairUpdator:
                    # updator
                    inputPairUpdator[inputValue] = {}

                    # PMML
                    tv = pmml.newInstance("TargetValueCounts")
                    tv.tvcMap = {}

                    pc = pmml.newInstance("PairCounts", attrib={"value": inputValue}, children=[tv])
                    pc.targetValueCounts = tv

                    bayesInput.pcMap[inputValue] = pc
                    bayesInput.tvcMap[inputValue] = {}
                    bayesInput.children.append(pc)

                    # consumer cache
                    bayesInput.pairCounts[inputValue] = {}

                ### advance local pointers one level deeper
                inputPairUpdator = inputPairUpdator[inputValue]
                pcMap = bayesInput.pcMap[inputValue]
                tvcMap = bayesInput.tvcMap[inputValue]
                pairCounts = bayesInput.pairCounts[inputValue]

                ### if we have not seen this input value/output value combination, make a new element
                if outputValue not in inputPairUpdator:
                    # updator
                    inputPairUpdator[outputValue] = self.engine.producerUpdateScheme.updator(SUMX)

                    # PMML
                    tvc = pmml.newInstance("TargetValueCount", attrib={"value": outputValue, "count": 0.})
                    tvcMap[outputValue] = tvc
                    pcMap.targetValueCounts.children.append(tvc)

                    # consumer cache
                    pairCounts[outputValue] = 0.

                ### update the output values histogram for this input value
                ### this happens very frequently
                for value, updator in inputPairUpdator.items():
                    # updator
                    if value == outputValue:
                        updator.increment(syncNumber, 1.)
                    else:
                        updator.increment(syncNumber, 0.)
                    newcount = updator.sum()
                    
                    # PMML
                    tvcMap[value].attrib["count"] = newcount

                    # consumer cache
                    pairCounts[value] = newcount

                ### if this inputField has not seen this outputValue, make new elements (there is no corresponding PMML)
                denominator = bayesInput.denominators
                if outputValue not in inputDenomUpdator:
                    # updator
                    inputDenomUpdator[outputValue] = self.engine.producerUpdateScheme.updator(SUMX)

                    # consumer cache
                    denominator[outputValue] = 0.

                ### update the denominator histogram for this inputField (there is no corresponding PMML)
                ### this happens very frequently
                for value, updator in inputDenomUpdator.items():
                    # updator
                    if value == outputValue:
                        updator.increment(syncNumber, 1.)
                    else:
                        updator.increment(syncNumber, 0.)

                    # consumer cache
                    denominator[value] = updator.sum()

        # print "outputValue", outputValue
        # print "inputValues", dict(zip(self.inputFields, inputValues))
        # print
        # for bi in self.model.bayesInputs:
        #     print bi.xml()
        #     print "pairCounts", bi.pairCounts
        #     print "denominators", bi.denominators
        #     print
        # print self.model.bayesOutput.xml()
        # raw_input()

        return True
