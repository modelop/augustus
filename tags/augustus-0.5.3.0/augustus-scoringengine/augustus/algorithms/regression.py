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

"""Defines the regression producer and consumer algorithms."""

from augustus.core.python3transition import *

# system includes
import math
import numpy

# local includes
from augustus.core.defs import Atom, INVALID, MISSING, InvalidDataError
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm
from augustus.algorithms.eventweighting import OLS

import augustus.core.pmml41 as pmml

SCORE_predictedValue = pmml.OutputField.predictedValue
SCORE_probability = pmml.OutputField.probability
SCORE_residual = pmml.OutputField.residual
SCORE_standardError = pmml.OutputField.standardError   # FIXME: DMG says RegressionModels should output a standardError, but how???

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerRegressionModel(ConsumerAlgorithm):
    def initialize(self):
        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        self.resetLoggerLevels()

        try:
            yvalue = self.model.yvalue(get)
        except InvalidDataError:
            self.lastScore = INVALID
            self.logger.debug("ConsumerRegressionModel.score: returning INVALID score")
            return self.lastScore

        if self.model.functionName is pmml.RegressionModel.REGRESSION:
            prob = INVALID
            if yvalue is not MISSING:
                try:
                    prob = self.model.prob(yvalue)
                except (OverflowError, ZeroDivisionError):
                    pass

            residual = INVALID
            if yvalue is not MISSING and self.model.targetFieldOptype is pmml.RegressionModel.CONTINUOUS:
                actualValue = get(self.model.targetFieldName)
                if actualValue is not INVALID and actualValue is not MISSING:
                    residual = actualValue - yvalue

            self.lastScore = {SCORE_predictedValue: yvalue, SCORE_probability: prob, SCORE_residual: residual}

            for key, value in self.lastScore.items():
                if isinstance(value, float) and (numpy.isinf(value) or numpy.isnan(value)):
                    self.lastScore[key] = INVALID
            return self.lastScore

        else:
            index = None
            winner = MISSING
            prob = INVALID

            if MISSING not in yvalue:
                index, winner = self.model.winner(yvalue)
                try:
                    prob = self.model.prob(yvalue)
                except FloatingPointError:
                    pass
                else:
                    if prob is not INVALID and (numpy.isnan(prob).any() or numpy.isinf(prob).any()):
                        prob = INVALID

            residual = INVALID
            if prob is not INVALID and self.model.targetFieldName is not None:
                actualValue = get(self.model.targetFieldName)
                if actualValue is not INVALID and actualValue is not MISSING:
                    actualValue = str(actualValue)   # all self.model.targetValues are strings; make this the same

                    indicator = numpy.array([1. if v == actualValue else 0. for v in self.model.targetValues])
                    residual = indicator - prob

            output = {SCORE_predictedValue: winner}
            if prob is not INVALID:
                output[SCORE_probability] = prob[index]
            else:
                output[SCORE_probability] = INVALID

            if residual is not INVALID:
                output[SCORE_residual] = residual[index]
            else:
                output[SCORE_residual] = INVALID

            # OutputField's "value" parameter can only be a string (no knowledge of type), so it's important that "value" (below) is also a string
            for i, value in enumerate(self.model.targetValues):
                if prob is not INVALID:
                    output[SCORE_probability, value] = prob[i]
                else:
                    output[SCORE_probability, value] = INVALID

                if residual is not INVALID:
                    output[SCORE_residual, value] = residual[i]
                else:
                    output[SCORE_residual, value] = INVALID

            self.lastScore = output

            for key, value in self.lastScore.items():
                if isinstance(value, float) and (numpy.isinf(value) or numpy.isnan(value)):
                    self.lastScore[key] = INVALID

            return self.lastScore

#########################################################################################
######################################################################### producer ######
#########################################################################################

class ProducerRegressionModel(ProducerAlgorithm):
    defaultParams = {"updateExisting": "false", "dependentField": None}

    def initialize(self, **params):
        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])
        if self.updateExisting:
            raise NotImplementedError("Updating from existing RegressionModels not implemented; use mode='replaceExisting'")

        if "dependentField" in params:
            self.dependentField = params["dependentField"]
            del params["dependentField"]
        else:
            self.dependentField = self.defaultParams["dependentField"]
        if self.dependentField == "": self.dependentField = None

        self.model = self.segmentRecord.pmmlModel

        self.predicted = []
        for miningField in self.model.child(pmml.MiningSchema).matches(pmml.MiningField):
            name = miningField.attrib["name"]
            usageType = miningField.attrib.get("usageType", "active")
            if usageType == "predicted":
                self.predicted.append(name)

        if len(self.predicted) == 0:
            self.dependentField = INVALID

        else:
            if self.dependentField is None:
                # by default, take the first 'predicted' feature
                self.dependentField = self.predicted[0]
            else:
                if self.dependentField not in self.predicted:
                    raise RuntimeError("DependentField feature not found among the 'predicted' features in the RegressionModel's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())

        self.regressionTable = self.model.regressionTables[0]

        p = 1 + len(self.regressionTable.numericTerms) + len(self.regressionTable.categoricalTerms) + len(self.regressionTable.predictorTerms)
        self.updator = self.engine.producerUpdateScheme.updator(OLS(p))

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def update(self, syncNumber, get):
        self.resetLoggerLevels()

        if self.model.functionName is not pmml.RegressionModel.REGRESSION:
            raise NotImplementedError("Only regression-type RegressionModels can be produced (both can be scored).")

        # get the dependent field and invert the logistic or exponential normalization
        if self.dependentField is INVALID:
            raise RuntimeError("No predicted field found in the RegressionModel's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())
        y = get(self.dependentField)
        if y is INVALID or y is MISSING:
            return False
        try:
            y = self.model.probinv(y)
        except (ValueError, ZeroDivisionError, OverflowError):
            return False

        # get the values of the RegressionTable terms, apart from their coefficients
        try:
            vals = self.regressionTable.values(get)
        except InvalidDataError:
            return False
        if vals is MISSING:
            return False
        numericX, categoryX, predictorX = vals
        if MISSING in categoryX: return False     # consumer allows categorical terms to contain MISSING, producer does not

        # put it into the least-squares calculator as [dependent, 1 (for intercept), independent_1, independent_2, ...]
        self.updator.increment(syncNumber, [y, 1.] + numericX + categoryX + predictorX)
        parameters = self.updator.ordinaryLeastSquares()

        if parameters is INVALID:
            return False

        # unpack the new coefficients and put them into the model (PMML attributes and consumer's cache)
        index = 0
        self.regressionTable.attrib["intercept"] = parameters[index]
        index += 1

        for i, term in enumerate(self.regressionTable.numericTerms):
            term.attrib["coefficient"] = parameters[index]
            self.regressionTable.numericCoefficients[i] = parameters[index]
            index += 1

        for i, term in enumerate(self.regressionTable.categoricalTerms):
            term.attrib["coefficient"] = parameters[index]
            self.regressionTable.categoryCoefficients[i] = parameters[index]
            index += 1

        for i, term in enumerate(self.regressionTable.predictorTerms):
            term.attrib["coefficient"] = parameters[index]
            self.regressionTable.predictorCoefficients[i] = parameters[index]
            index += 1

        return True
