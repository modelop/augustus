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

"""Defines the rule-set producer and consumer algorithms."""

# system includes
import math

# local includes
from augustus.core.defs import INVALID, MISSING
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm

import augustus.core.pmml41 as pmml

SCORE_predictedValue = pmml.OutputField.predictedValue
# SCORE_probability = pmml.OutputField.probability    # implement Targets!
# SCORE_residual = pmml.OutputField.residual
SCORE_entityId = pmml.OutputField.entityId

SCORE_weight = pmml.X_ODG_OutputField.weight

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerRuleSetModel(ConsumerAlgorithm):
    def initialize(self):
        """Initialize a rule-set consumer."""

        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        """Score one event with the rule-set model, returning a scores dictionary."""

        self.resetLoggerLevels()
        score, weight, entity = self.model.ruleset.evaluate(get)

        entityId = INVALID
        if entity is not None:
            entityId = entity.attrib["id"]

        self.lastScore = {SCORE_predictedValue: score, SCORE_weight: weight, SCORE_entityId: entityId}
        return self.lastScore

#########################################################################################
######################################################################### producer ######
#########################################################################################

class ProducerRuleSetModel(ProducerAlgorithm):
    defaultParams = {}

    def initialize(self, **params):
        raise NotImplementedError("RuleSetModel does not have a dedicated producer; use the TreeModelProducer on rule-sets instead")

    def update(self, syncNumber, get):
        self.resetLoggerLevels()
        raise NotImplementedError("RuleSetModel does not have a dedicated producer; use the TreeModelProducer on rule-sets instead")
