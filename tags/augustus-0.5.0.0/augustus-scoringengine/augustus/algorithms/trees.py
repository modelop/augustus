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

"""Defines the decision tree producer and consumer algorithms."""

# system includes
import random
import math
import new
import heapq

# local includes
from augustus.core.defs import Atom, INVALID, MISSING
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm
from augustus.algorithms.eventweighting import UpdateScheme, COUNT, SUM1, SUMX, SUMXX

import augustus.core.pmml41 as pmml

SCORE_predictedValue = pmml.OutputField.predictedValue
# SCORE_probability = pmml.OutputField.probability    # implement Targets!
# SCORE_residual = pmml.OutputField.residual
SCORE_entityId = pmml.OutputField.entityId

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerTreeModel(ConsumerAlgorithm):
    def initialize(self, **params):
        """Initialize a tree model consumer."""

        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        """Score one event with the tree model, returning a scores dictionary."""

        node, metadata = self.model.evaluate(get)

        if node is None:
            predictedValue = MISSING
            entityId = MISSING
        else:
            predictedValue = node.score
            entityId = node.attrib["id"]

        ### someday, you'll need this:
        # penalty = 1.
        # if metadata.missingValuePenalty is not None:
        #     penalty = metadata.missingValuePenalty**metadata.unknowns

        return {SCORE_predictedValue: predictedValue, SCORE_entityId: entityId}

#########################################################################################
######################################################################### producer ######
#########################################################################################

class Feature:
    CATEGORICAL = Atom("Categorical")
    CONTINUOUS = Atom("Continuous")
    ORDINALSTRING = Atom("OrdinalString")

    STRING = Atom("String")
    INTEGER = Atom("Integer")
    FLOAT = Atom("Float")

    def __init__(self, name, optype, dataType, producerUpdateScheme):
        self.name = name

        if optype == "categorical":
            self.values = set()
            self.optype = self.CATEGORICAL
            self.dataType = self.STRING if dataType == "string" else dataType
            self.mature = False
            self.maturityCounter = 0

        elif optype == "continuous":
            self.updator = producerUpdateScheme.updator(SUM1, SUMX, SUMXX)
            self.optype = self.CONTINUOUS
            self.dataType = {"integer": self.INTEGER, "float": self.FLOAT, "double": self.FLOAT}.get(dataType, dataType)
            self.mature = False
            self.maturityCounter = 0

        else:
            self.values = map(optype, optype.values)
            self.optype = self.ORDINALSTRING
            self.dataType = self.STRING if dataType == "string" else dataType
            self.mature = True

    def increment(self, syncValue, get):
        value = get(self.name)
        if value is not INVALID and value is not MISSING:
            if self.optype is self.CATEGORICAL:
                self.values.add(value)

                if self.maturityCounter < self.maturityThreshold:
                    self.maturityCounter += 1
                else:
                    self.mature = True

            elif self.optype is self.CONTINUOUS:
                self.updator.increment(syncValue, value)

                if self.maturityCounter < self.maturityThreshold:
                    self.maturityCounter += 1
                else:
                    self.mature = True

    def randomSplit(self):
        if self.optype is self.CATEGORICAL:
            return SplitEqual(self.name, random.choice(tuple(self.values)))

        elif self.optype is self.CONTINUOUS:
            if self.dataType is self.INTEGER:
                return SplitGreaterThan(self.name, int(round(random.gauss(self.updator.mean(), math.sqrt(self.updator.variance())))))
            else:
                return SplitGreaterThan(self.name, random.gauss(self.updator.mean(), math.sqrt(self.updator.variance())))

        elif self.optype is self.ORDINALSTRING:
            return SplitGreaterThan(self.name, random.choice(tuple(self.values)))

class Split:
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.classifications = set()
        self.counts = {(None, None): 0., (None, True): 0., (None, False): 0.}
        self.mature = False
        self.maturityCounter = 0
        self.__hash = hash(id(self))

    def increment(self, syncValue, get, classification):
        result = self.decision(get)

        if classification not in self.classifications:
            self.classifications.add(classification)
            self.counts[classification, None] = 0.    # make these updators?
            self.counts[classification, True] = 0.
            self.counts[classification, False] = 0.

        self.counts[None, None] += 1.
        self.counts[None, result] += 1.
        
        self.counts[classification, None] += 1.
        self.counts[classification, result] += 1.

        self.maturityCounter += 1
        if self.maturityCounter >= self.maturityThreshold:
            self.mature = True

    def entropy(self, result):
        output = 0.

        for classification in self.classifications:
            try:
                frac = self.counts[classification, result] / self.counts[None, result]
            except ZeroDivisionError:
                continue
            try:
                output -= frac*math.log(frac, 2)
            except ValueError:
                pass

        return output

    def fraction(self, result):
        try:
            return self.counts[None, result] / self.counts[None, None]
        except ZeroDivisionError:
            return 0.

    def gain(self):
        return self.entropy(None) - self.fraction(True)*self.entropy(True) - self.fraction(False)*self.entropy(False)

    def score(self, result):
        bestClassification = None
        bestCount = None
        for classification in self.classifications:
            count = self.counts[classification, result]

            if bestClassification is None or count > bestCount:
                bestClassification = classification
                bestCount = count

        return bestClassification

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __hash__(self):
        return self.__hash

class SplitEqual(Split):
    def decision(self, get):
        return get(self.name) == self.value

    def expression(self):
        return "(%s == \"%s\")" % (self.name, self.value)

    def trueSimplePredicate(self):
        output = new.instance(pmml.SimplePredicate)
        output.tag = "SimplePredicate"
        output.attrib = {"field": self.name, "operator": "equal", "value": self.value}
        output.children = []
        output.needsValue = False
        return output

    def falseSimplePredicate(self):
        output = new.instance(pmml.SimplePredicate)
        output.tag = "SimplePredicate"
        output.attrib = {"field": self.name, "operator": "notEqual", "value": self.value}
        output.children = []
        output.needsValue = False
        return output

class SplitGreaterThan(Split):
    def decision(self, get):
        return get(self.name) > self.value

    def expression(self):
        return "(%s > %s)" % (self.name, str(self.value))

    def trueSimplePredicate(self):
        output = new.instance(pmml.SimplePredicate)
        output.tag = "SimplePredicate"
        output.attrib = {"field": self.name, "operator": "greaterThan", "value": self.value}
        output.children = []
        output.needsValue = False
        return output

    def falseSimplePredicate(self):
        output = new.instance(pmml.SimplePredicate)
        output.tag = "SimplePredicate"
        output.attrib = {"field": self.name, "operator": "lessOrEqual", "value": self.value}
        output.children = []
        output.needsValue = False
        return output

class World:
    def __init__(self, level, split):
        self.level = level
        self.split = split
        self.true_matureSplits = []
        self.false_matureSplits = []
        self.true_immatureSplits = []
        self.false_immatureSplits = []
        self.true_outworlds = {}
        self.false_outworlds = {}

    def expressions(self, expr, output):
        if self.split is None:
            thisExpression = expr + ["True"]
        else:
            thisExpression = expr + [self.split.expression()]

        output.append(thisExpression)

        for world in self.true_outworlds.values() + self.false_outworlds.values():
            world.expressions(thisExpression, output)

    def increment(self, syncValue, get, classification, matureFeatures, producer):
        if self.split is None:
            decision = True
        else:
            decision = self.split.decision(get)

        if decision:
            matureSplits = self.true_matureSplits
            immatureSplits = self.true_immatureSplits
            outworlds = self.true_outworlds
        else:
            matureSplits = self.false_matureSplits
            immatureSplits = self.false_immatureSplits
            outworlds = self.false_outworlds

        while len(matureSplits) + len(immatureSplits) <= producer.trialsToKeep:
            split = random.choice(matureFeatures).randomSplit()
            split.maturityThreshold = producer.splitMaturityThreshold
            immatureSplits.append(split)

        for split in matureSplits:
            split.increment(syncValue, get, classification)
        for split in immatureSplits:
            split.increment(syncValue, get, classification)

        newImmatureSplits = []
        for split in immatureSplits:
            if split.mature:
                matureSplits.append(split)
            else:
                newImmatureSplits.append(split)

        for split in matureSplits:
            split.gainCache = split.gain()
        
        # len(matureSplits) is never much larger than trialsToKeep, so find the best trialsToKeep by sorting
        if len(matureSplits) > producer.trialsToKeep:
            matureSplits.sort(lambda a, b: cmp(b.gainCache, a.gainCache))
            newMatureSplits = matureSplits[0:producer.trialsToKeep]
        else:
            newMatureSplits = matureSplits  # just pass a reference around when not doing anything

        if decision:
            self.true_matureSplits = newMatureSplits
            self.true_immatureSplits = newImmatureSplits
        else:
            self.false_matureSplits = newMatureSplits
            self.false_immatureSplits = newImmatureSplits

        if self.level < producer.treeDepth:
            # worldsToSplit is typically much smaller than len(matureSplits), so find the best worldsToSplit by heapq (not sorting!)
            branchableSplits = heapq.nlargest(producer.worldsToSplit, newMatureSplits, lambda x: x.maturityCounter)

            for split, world in outworlds.items():
                if split not in branchableSplits:
                    del outworlds[split]     # I am become death, the destroyer of worlds!

            for split in branchableSplits:
                if split not in outworlds:
                    outworlds[split] = World(self.level + 1, split)   # and creator of worlds...

            for world in outworlds.values():
                world.increment(syncValue, get, classification, matureFeatures, producer)

    def bestTree(self, parent, bestClassification, producer):
        if self.split is None:
            pmmlTrue = new.instance(pmml.pmmlTrue)
            pmmlTrue.tag = "True"
            pmmlTrue.attrib = {}
            pmmlTrue.children = []

            trueNode = new.instance(pmml.Node)
            trueNode.tag = "Node"
            trueNode.attrib = {"score": bestClassification}
            trueNode.children = [pmmlTrue]
            trueNode.test = pmmlTrue.createTest()

            parent[producer.nodeIndex] = trueNode

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer)

        else:
            trueNode = new.instance(pmml.Node)
            trueNode.tag = "Node"
            trueNode.attrib = {"score": self.split.score(True)}
            trueNode.children = [self.split.trueSimplePredicate()]
            trueNode.test = trueNode.children[0].createTest()

            falseNode = new.instance(pmml.Node)
            falseNode.tag = "Node"
            falseNode.attrib = {"score": self.split.score(False)}
            falseNode.children = [self.split.falseSimplePredicate()]
            falseNode.test = falseNode.children[0].createTest()

            parent.children.append(trueNode)
            parent.children.append(falseNode)

            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer)
                max(self.false_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(falseNode, bestClassification, producer)

    def bestRule(self, parent, bestClassification, producer):
        if self.split is None:
            pmmlTrue = new.instance(pmml.pmmlTrue)
            pmmlTrue.tag = "True"
            pmmlTrue.attrib = {}
            pmmlTrue.children = []

            if len(self.true_outworlds) > 0:
                trueRule = new.instance(pmml.CompoundRule)
                trueRule.tag = "CompoundRule"
                trueRule.attrib = {}
            else:
                trueRule = new.instance(pmml.SimpleRule)
                trueRule.tag = "SimpleRule"
                trueRule.attrib = {"score": bestClassification}

            trueRule.children = [pmmlTrue]
            trueRule.test = pmmlTrue.createTest()

            parent[producer.nodeIndex] = trueRule

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(trueRule, bestClassification, producer)

        else:
            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                trueRule = new.instance(pmml.CompoundRule)
                trueRule.tag = "CompoundRule"
                trueRule.attrib = {}

                falseRule = new.instance(pmml.CompoundRule)
                falseRule.tag = "CompoundRule"
                falseRule.attrib = {}

            else:
                trueRule = new.instance(pmml.SimpleRule)
                trueRule.tag = "SimpleRule"
                trueRule.attrib = {"score": self.split.score(True)}

                falseRule = new.instance(pmml.SimpleRule)
                falseRule.tag = "SimpleRule"
                falseRule.attrib = {"score": self.split.score(False)}

            trueRule.children = [self.split.trueSimplePredicate()]
            trueRule.test = trueRule.children[0].createTest()
            falseRule.children = [self.split.falseSimplePredicate()]
            falseRule.test = falseRule.children[0].createTest()

            parent.children.append(trueRule)
            parent.children.append(falseRule)

            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(trueRule, bestClassification, producer)
                max(self.false_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(falseRule, bestClassification, producer)

class ProducerTreeModel(ProducerAlgorithm):
    TREEMODEL = Atom("TreeModel")
    RULESETMODEL = Atom("RuleSetModel")

    def initialize(self, **params):
        """An event-based tree-producing algorithm.

        Although it does not iterate over the data as the standard
        CART algorithm does, it converges to an approximate tree by
        keeping alternate hypotheses in mind and collecting data for
        all active hypotheses.
        """

        if "resume" in params:
            self.resume = int(params["resume"])
            del params["resume"]
        else:
            self.resume = False

        if self.resume:
            raise NotImplementedError, "Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'"

        if "featureMaturityThreshold" in params:
            self.featureMaturityThreshold = int(params["featureMaturityThreshold"])
            del params["featureMaturityThreshold"]
        else:
            self.featureMaturityThreshold = 10

        if "splitMaturityThreshold" in params:
            self.splitMaturityThreshold = int(params["splitMaturityThreshold"])
            del params["splitMaturityThreshold"]
        else:
            self.splitMaturityThreshold = 30

        if "trialsToKeep" in params:
            self.trialsToKeep = int(params["trialsToKeep"])
            del params["trialsToKeep"]
        else:
            self.trialsToKeep = 50

        if "worldsToSplit" in params:
            self.worldsToSplit = int(params["worldsToSplit"])
            del params["worldsToSplit"]
        else:
            self.worldsToSplit = 3

        if "treeDepth" in params:
            self.treeDepth = int(params["treeDepth"])
            del params["treeDepth"]
        else:
            self.treeDepth = 3

        if "classification" in params:
            self.classification = int(params["classification"])
            del params["classification"]
        else:
            self.classification = None

        self.model = self.segmentRecord.pmmlModel

        if isinstance(self.model, pmml.TreeModel):
            self.modelType = self.TREEMODEL
            self.nodeIndex = self.model.index(pmml.Node)

        elif isinstance(self.model, pmml.RuleSetModel):
            self.ruleSet = self.model.child(pmml.RuleSet)
            self.modelType = self.RULESETMODEL
            self.nodeIndex = self.ruleSet.index(lambda x: isinstance(x, (pmml.SimpleRule, pmml.CompoundRule)), exception=False)
            if self.nodeIndex is None:
                self.nodeIndex = len(self.ruleSet.children)
                self.ruleSet.children.append(None)

        self.features = []
        self.predicted = []
        for miningField in self.model.child(pmml.MiningSchema).matches(pmml.MiningField):
            name = miningField.attrib["name"]
            usageType = miningField.attrib.get("usageType", "active")
            if usageType == "active":
                dataType = self.model.dataContext.dataType[name]
                optype = self.model.dataContext.optype[name]
                if optype == "ordinal" and dataType == "string":
                    optype = self.model.dataContext.cast[name]

                feature = Feature(name, optype, dataType, self.engine.producerUpdateScheme)
                feature.maturityThreshold = self.featureMaturityThreshold
                self.features.append(feature)

            if usageType == "predicted":
                self.predicted.append(name)

        if len(self.predicted) == 0:
            self.classification = INVALID

        else:
            if self.classification is None:
                # by default, take the first 'predicted' feature
                self.classification = self.predicted[0]
            else:
                if self.classification not in self.predicted:
                    raise RuntimeError, "Classification feature not found among the 'predicted' features in the decision tree's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()
        
        self.topWorld = World(0, None)
        self.counts = {}

    def update(self, syncNumber, get):
        if self.classification is INVALID:
            raise RuntimeError, "Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()

        values = [get(feature.name) for feature in self.features]
        if INVALID in values or MISSING in values: return False

        classification = get(self.classification)

        if classification not in self.counts:
            self.counts[classification] = 0
        self.counts[classification] += 1

        bestClassification = None
        bestCount = None
        for c, count in self.counts.items():
            if bestClassification is None or count > bestCount:
                bestClassification = c
                bestCount = count

        matureFeatures = []
        for feature in self.features:
            feature.increment(syncNumber, get)
            if feature.mature:
                matureFeatures.append(feature)

        if len(matureFeatures) > 0:
            self.topWorld.increment(syncNumber, get, classification, matureFeatures, self)

        if self.modelType is self.TREEMODEL:
            self.topWorld.bestTree(self.model, bestClassification, self)
        elif self.modelType is self.RULESETMODEL:
            self.topWorld.bestRule(self.ruleSet, bestClassification, self)

        return True
