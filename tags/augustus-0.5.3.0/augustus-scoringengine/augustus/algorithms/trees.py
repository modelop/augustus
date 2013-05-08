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
import heapq
import array
import numpy
import logging

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
    def initialize(self):
        """Initialize a tree model consumer."""

        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        """Score one event with the tree model, returning a scores dictionary."""

        self.resetLoggerLevels()
        node, metadata = self.model.evaluate(get)

        if node is None:
            predictedValue = MISSING
            entityId = MISSING
        else:
            entityId = node.attrib["id"]
            if node.regression is None:
                predictedValue = node.score
            else:
                predictedValue = node.regression.evaluate(get)

        ### someday, you'll need this:
        # penalty = 1.
        # if metadata.missingValuePenalty is not None:
        #     penalty = metadata.missingValuePenalty**metadata.unknowns

        self.lastScore = {SCORE_predictedValue: predictedValue, SCORE_entityId: entityId}

        return self.lastScore

#########################################################################################
######################################################################### producer ######
#########################################################################################

class Feature(object):
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
            try:
                stdev = math.sqrt(self.updator.variance())
            except ValueError:
                stdev = 0.

            if self.dataType is self.INTEGER:
                return SplitGreaterThan(self.name, int(round(random.gauss(self.updator.mean(), stdev))))
            else:
                return SplitGreaterThan(self.name, random.gauss(self.updator.mean(), stdev))

        elif self.optype is self.ORDINALSTRING:
            return SplitGreaterThan(self.name, random.choice(tuple(self.values)))

class Split(object):
    regression = False

    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.classifications = set()
        if self.regression:
            self.none_sum1 = 0.
            self.none_sumx = 0.
            self.none_sumxx = 0.
            self.true_sum1 = 0.
            self.true_sumx = 0.
            self.true_sumxx = 0.
            self.false_sum1 = 0.
            self.false_sumx = 0.
            self.false_sumxx = 0.
        else:
            self.counts = {(None, None): 0., (None, True): 0., (None, False): 0.}
        self.mature = False
        self.maturityCounter = 0
        self.__hash = hash(id(self))

    def increment(self, syncValue, get, classification):
        if self.regression:
            self.none_sum1 += 1.
            self.none_sumx += classification
            self.none_sumxx += classification**2

            if self.decision(get):
                self.true_sum1 += 1.
                self.true_sumx += classification
                self.true_sumxx += classification**2
            else:
                self.false_sum1 += 1.
                self.false_sumx += classification
                self.false_sumxx += classification**2

        else:
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
            except (ValueError, OverflowError):
                pass

        return output

    def fraction(self, result):
        try:
            return self.counts[None, result] / self.counts[None, None]
        except ZeroDivisionError:
            return 0.

    def gain(self):
        if self.regression:
            self.none_variance = (self.none_sumxx / self.none_sum1) - (self.none_sumx / self.none_sum1)**2

            try:
                self.true_variance = (self.true_sumxx / self.true_sum1) - (self.true_sumx / self.true_sum1)**2
            except ZeroDivisionError:
                self.true_variance = 1.

            try:
                self.false_variance = (self.false_sumxx / self.false_sum1) - (self.false_sumx / self.false_sum1)**2
            except ZeroDivisionError:
                self.false_variance = 1.

            return self.none_sum1*self.none_variance - self.true_sum1*self.true_variance - self.false_sum1*self.false_variance

        else:
            return self.entropy(None) - self.fraction(True)*self.entropy(True) - self.fraction(False)*self.entropy(False)

    def score(self, result):
        if self.regression:
            self.none_mean = self.none_sumx / self.none_sum1

            if result:
                try:
                    return self.true_sumx / self.true_sum1
                except ZeroDivisionError:
                    return self.none_mean

            else:
                try:
                    return self.false_sumx / self.false_sum1
                except ZeroDivisionError:
                    return self.none_mean

        else:
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

    def trueSimplePredicate(self, dataContext):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "equal", "value": self.value})
        output.post_validate()
        output.top_validate(dataContext)
        return output

    def falseSimplePredicate(self, dataContext):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "notEqual", "value": self.value})
        output.post_validate()
        output.top_validate(dataContext)
        return output

class SplitGreaterThan(Split):
    def decision(self, get):
        return get(self.name) > self.value

    def expression(self):
        return "(%s > %s)" % (self.name, str(self.value))

    def trueSimplePredicate(self, dataContext):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "greaterThan", "value": self.value})
        output.post_validate()
        output.top_validate(dataContext)
        return output

    def falseSimplePredicate(self, dataContext):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "lessOrEqual", "value": self.value})
        output.post_validate()
        output.top_validate(dataContext)
        return output

class World(object):
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
                    del outworlds[split]

            for split in branchableSplits:
                if split not in outworlds:
                    outworlds[split] = World(self.level + 1, split)

            for world in outworlds.values():
                world.increment(syncValue, get, classification, matureFeatures, producer)

    def bestTree(self, parent, bestClassification, producer, name):
        if self.split is None:
            pmmlTrue = pmml.newInstance("True")
            trueNode = pmml.newInstance("Node", attrib={"score": bestClassification, "id": name + "-1"}, children=[pmmlTrue])

            trueNode.configure()
            trueNode.test = pmmlTrue.createTest()

            parent[producer.nodeIndex] = trueNode
            parent.configure()

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer, name + "-1")

        else:
            trueNode = pmml.newInstance("Node", attrib={"score": self.split.score(True), "id": name + "-1"}, children=[self.split.trueSimplePredicate(producer.model.dataContext)])
            falseNode = pmml.newInstance("Node", attrib={"score": self.split.score(False), "id": name + "-2"}, children=[self.split.falseSimplePredicate(producer.model.dataContext)])

            trueNode.configure()
            falseNode.configure()
            trueNode.test = trueNode.children[0].createTest()
            falseNode.test = falseNode.children[0].createTest()

            parent.children.append(trueNode)
            parent.children.append(falseNode)
            parent.configure()

            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer, name + "-1")
                max(self.false_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(falseNode, bestClassification, producer, name + "-2")

    def bestRule(self, parent, bestClassification, producer, name):
        if self.split is None:
            pmmlTrue = pmml.newInstance("True")
            if len(self.true_outworlds) > 0:
                trueRule = pmml.newInstance("CompoundRule", children=[pmmlTrue])
            else:
                trueRule = pmml.newInstance("SimpleRule", attrib={"score": bestClassification, "id": name + "-1"}, children=[pmmlTrue])

            trueRule.configure()
            trueRule.test = pmmlTrue.createTest()

            parent[producer.nodeIndex] = trueRule
            parent.configure()

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(trueRule, bestClassification, producer, name + "-1")

        else:
            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                trueRule = pmml.newInstance("CompoundRule", children=[self.split.trueSimplePredicate(producer.model.dataContext)])
                falseRule = pmml.newInstance("CompoundRule", children=[self.split.falseSimplePredicate(producer.model.dataContext)])
            else:
                trueRule = pmml.newInstance("SimpleRule", attrib={"score": self.split.score(True), "id": name + "-1"}, children=[self.split.trueSimplePredicate(producer.model.dataContext)])
                falseRule = pmml.newInstance("SimpleRule", attrib={"score": self.split.score(False), "id": name + "-2"}, children=[self.split.falseSimplePredicate(producer.model.dataContext)])

            trueRule.configure()
            falseRule.configure()
            trueRule.test = trueRule.children[0].createTest()
            falseRule.test = falseRule.children[0].createTest()

            parent.children.append(trueRule)
            parent.children.append(falseRule)
            parent.configure()

            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(trueRule, bestClassification, producer, name + "-1")
                max(self.false_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(falseRule, bestClassification, producer, name + "-2")

class ProducerTreeModel(ProducerAlgorithm):
    TREEMODEL = Atom("TreeModel")
    RULESETMODEL = Atom("RuleSetModel")

    defaultParams = {"updateExisting": "false", "featureMaturityThreshold": "10", "splitMaturityThreshold": "30", "trialsToKeep": "50", "worldsToSplit": "3", "treeDepth": "3", "classifierField": ""}

    def initialize(self, **params):
        """An event-based tree-producing algorithm.

        Although it does not iterate over the data as the standard
        CART algorithm does, it converges to an approximate tree by
        keeping alternate hypotheses in mind and collecting data for
        all active hypotheses.
        """

        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if self.updateExisting:
            raise NotImplementedError("Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'")

        if "featureMaturityThreshold" in params:
            self.featureMaturityThreshold = int(params["featureMaturityThreshold"])
            del params["featureMaturityThreshold"]
        else:
            self.featureMaturityThreshold = int(self.defaultParams["featureMaturityThreshold"])

        if "splitMaturityThreshold" in params:
            self.splitMaturityThreshold = int(params["splitMaturityThreshold"])
            del params["splitMaturityThreshold"]
        else:
            self.splitMaturityThreshold = int(self.defaultParams["splitMaturityThreshold"])

        if "trialsToKeep" in params:
            self.trialsToKeep = int(params["trialsToKeep"])
            del params["trialsToKeep"]
        else:
            self.trialsToKeep = int(self.defaultParams["trialsToKeep"])

        if "worldsToSplit" in params:
            self.worldsToSplit = int(params["worldsToSplit"])
            del params["worldsToSplit"]
        else:
            self.worldsToSplit = int(self.defaultParams["worldsToSplit"])

        if "treeDepth" in params:
            self.treeDepth = int(params["treeDepth"])
            del params["treeDepth"]
        else:
            self.treeDepth = int(self.defaultParams["treeDepth"])

        if "classifierField" in params:
            self.classifierField = params["classifierField"]
            del params["classifierField"]
        else:
            self.classifierField = self.defaultParams["classifierField"]
        if self.classifierField == "": self.classifierField = None

        self.model = self.segmentRecord.pmmlModel
        self.regression = False

        if isinstance(self.model, pmml.TreeModel):
            self.modelType = self.TREEMODEL
            self.nodeIndex = self.model.index(pmml.Node)

            if self.model.attrib["functionName"] == "regression":
                self.regression = True

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
            self.classifierField = INVALID

        else:
            if self.classifierField is None:
                # by default, take the first 'predicted' feature
                self.classifierField = self.predicted[0]
            else:
                if self.classifierField not in self.predicted:
                    raise RuntimeError("ClassifierField feature not found among the 'predicted' features in the decision tree's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())
        
        self.topWorld = World(0, None)

        if self.regression:
            self.sum1 = 0.
            self.sumx = 0.
        else:
            self.counts = {}

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def update(self, syncNumber, get):
        self.resetLoggerLevels()
        Split.regression = self.regression

        if self.classifierField is INVALID:
            raise RuntimeError("Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())

        values = [get(feature.name) for feature in self.features]
        if INVALID in values or MISSING in values:
            self.logger.debug("ProducerTreeModel.update: returning False (INVALID or MISSING data)")
            return False

        classification = get(self.classifierField)
        if classification is INVALID or classification is MISSING:
            self.logger.debug("ProducerTreeModel.update: returning False (INVALID or MISSING classification field)")
            return False

        if self.regression:
            self.sum1 += 1.
            self.sumx += classification
            bestClassification = self.sumx / self.sum1

        else:
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
            self.topWorld.bestTree(self.model, bestClassification, self, "Node")
        elif self.modelType is self.RULESETMODEL:
            self.topWorld.bestRule(self.ruleSet, bestClassification, self, "Node")

        return True

#########################################################################################
############################################################## iterative producers ######
#########################################################################################

class Node(object):
    pmmlTrue = pmml.pmmlTrue()

    def __init__(self, features, data):
        self.features = features
        self.data = data
        
        self.selectionCache = {}
        self.selectionValue = {}

        self.subNodes = None

    def gainterm(self, feature):
        if feature in self.featureSplitExceptions:
            return self.featureSplitExceptions[feature](self, feature)

        elif self.categorical[feature]:
            return self.splitCategorical(feature)

        else:
            return self.splitOrdinal(feature)

    def _stringifyScore(self):
        if self.lookup[self.classifierField] is not False:
            for sd in self.scoreDistributions:
                sd.attrib["value"] = self.lookup[self.classifierField][sd.attrib["value"]]
            self.score = self.lookup[self.classifierField][self.score]

    def split(self, depthCounter, logger):
        self.extension = None

        if self.maxTreeDepth is not None and depthCounter >= self.maxTreeDepth:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "reached maximum tree depth of %d, stopping tree-building" % self.maxTreeDepth)
            self._stringifyScore()
            return
        if len(self.features) == 0:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no feature are left to split, stopping tree-building")
            self._stringifyScore()
            return

        if self.minRecordCount is not None and self.sdTotal < self.minRecordCount:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "node has only %d records, not enough to continue tree-building" % self.sdTotal)
            self._stringifyScore()
            return

        if logger is not None:
            logger.debug(("    " * depthCounter) + "depth %d: determining best feature for split in a dataset with %d records" % (depthCounter, len(self.data[self.classifierField])))

        # calculate the entropy without any selections first
        s = self.unsplitValue()

        # now try on all the different possible changes to the entropy (the gain)
        bestFeature = None
        bestGain = 0.
        for feature in self.features:
            gainterm = self.gainterm(feature)
            if gainterm is not None:
                gain = s + gainterm
                if bestFeature is None or gain > bestGain:
                    bestFeature = feature
                    bestGain = gain

        if bestFeature is None:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no features are granular enough to split, stopping tree-building")
            self._stringifyScore()
            return

        if bestGain <= self.minGain:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no split produces a gain greater than %g (best is %g), stopping tree-building" % (self.minGain, bestGain))
            self._stringifyScore()
            return

        ###### build the PMML elements

        self._stringifyScore()

        order = [v for f, v in self.selectionCache.keys() if f == bestFeature]
        order.sort()

        self.extension = pmml.newInstance("Extension", attrib={"name": "gain", "value": bestGain})
        self.predicates = []

        for value in order:
            selValue = self.selectionValue[bestFeature]

            # complete splits
            if selValue is None:
                if self.lookup[bestFeature] is False:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "equal", "value": value})
                else:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "equal", "value": self.lookup[bestFeature][value]})

            # subset splits
            elif isinstance(selValue, numpy.ndarray):
                predicate = pmml.newInstance("SimpleSetPredicate", attrib={"field": bestFeature, "booleanOperator": "isIn" if value else "isNotIn"})

                thetype = self.dataType[bestFeature]
                if thetype == "integer": thetype = "int"
                elif thetype in ("float", "double"): thetype = "real"
                else: thetype = "string"
                thearray = pmml.newInstance("Array", attrib={"type": thetype, "n": len(selValue)})

                if self.lookup[bestFeature] is False:
                    thearray.value = list(selValue)
                else:
                    thearray.value = list(self.lookup[bestFeature][selValue])

                predicate.children.append(thearray)

            # singleton splits
            elif self.categorical[bestFeature]:
                if self.lookup[bestFeature] is False:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "equal" if value else "notEqual", "value": selValue})
                else:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "equal" if value else "notEqual", "value": self.lookup[bestFeature][selValue]})

            # ordinal splits
            else:
                if self.lookup[bestFeature] is False:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "greaterThan" if value else "lessOrEqual", "value": selValue})
                else:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": bestFeature, "operator": "greaterThan" if value else "lessOrEqual", "value": self.lookup[bestFeature][selValue]})

            self.predicates.append(predicate)

        ###### make subnodes

        self.subNodes = []

        completeSplit = ((bestFeature in self.featureSplitExceptions and self.featureSplitExceptions[bestFeature] == self.completeSplit) or \
                         (self.categorical[bestFeature] and self.splitCategorical == self.completeSplit))

        for value in order:
            subData = {}
            subData[self.classifierField] = self.data[self.classifierField][self.selectionCache[bestFeature, value]]

            if completeSplit:
                subFeatures = list(set(self.features).difference(set([bestFeature])))
            else:
                subFeatures = self.features

            for f in subFeatures:
                subData[f] = self.data[f][self.selectionCache[bestFeature, value]]

            # create a subnode with the same class as this one (either NodeClassification or NodeRegression)
            self.subNodes.append(self.__class__(subFeatures, subData))

        del self.selectionCache
        del self.selectionValue
        del self.cvalues
        del self.data
        del self.features

        if logger is not None:
            logger.debug(("    " * depthCounter) + "feature \"%s\" will generate %d branches from this node (gain is %g)" % (bestFeature, len(self.subNodes), bestGain))

        # print
        # print "Finished building node at depth", depthCounter
        # self._memoryProfile()

        for i, subNode in enumerate(self.subNodes):
            if logger is not None:
                logger.debug(("    " * depthCounter) + "recursing in branch %d out of %d" % (i+1, len(self.subNodes)))

            subNode.split(depthCounter + 1, logger)

    # def _memoryProfile(self, obj=None):
    #     import sys
    #
    #     if obj is not None:
    #         total = sys.getsizeof(obj)
    #         if isinstance(obj, (list, tuple)):
    #             for item in obj:
    #                 total += self._memoryProfile(item)
    #         elif isinstance(obj, numpy.ndarray):
    #             total += obj.nbytes
    #         elif isinstance(obj, dict):
    #             total += self._memoryProfile(obj.keys())
    #             total += self._memoryProfile(obj.values())
    #         elif isinstance(obj, pmml.PMML):
    #             for key in "tag", "attrib", "children", "value", "text":
    #                 if key in obj.__dict__:
    #                     total += self._memoryProfile(obj.__dict__[key])
    #         elif isinstance(obj, (basestring, int, long, float)):
    #             pass
    #         return total
    #
    #     if "masterList" not in Node.__dict__:
    #         Node.__dict__["masterList"] = {}
    #
    #     for name in self.__dict__:
    #         if name not in Node.__dict__:
    #             if name != "subNodes":
    #                 if name not in Node.masterList:
    #                     Node.masterList[name] = (0, 0, "")
    #                 Node.masterList[name] = (Node.masterList[name][0] + 1, Node.masterList[name][1] + self._memoryProfile(self.__dict__[name]), repr(type(self.__dict__[name])))
    #
    #     names = Node.masterList.keys()
    #     names.sort()
    #     for name in names:
    #         print "    %s (%s): %d / %d = %g" % (name, Node.masterList[name][2], Node.masterList[name][1], Node.masterList[name][0], 1.*Node.masterList[name][1]/Node.masterList[name][0])

    def tree(self, name):
        if self.subNodes is not None:
            output = pmml.newInstance("Node", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children=[self.extension, self.pmmlTrue])
            output.predicateIndex = 1
            if self.scoreDistributions is not None:
                output.children.extend(self.scoreDistributions)

            for i, subNode in enumerate(self.subNodes):
                node = subNode.tree("%s-%d" % (name, i+1))
                node[node.predicateIndex] = self.predicates[i]
                output.children.append(node)

        else:
            output = pmml.newInstance("Node", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children=[self.pmmlTrue])
            output.predicateIndex = 0
            if self.scoreDistributions is not None:
                output.children.extend(self.scoreDistributions)

        return output

    def rule(self, name):
        if self.subNodes is not None:
            output = pmml.newInstance("CompoundRule", children=[self.extension, self.pmmlTrue])
            output.predicateIndex = 1

            for i, subNode in enumerate(self.subNodes):
                node = subNode.rule("%s-%d" % (name, i+1))
                node[node.predicateIndex] = self.predicates[i]
                output.children.append(node)

        else:
            output = pmml.newInstance("SimpleRule", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children=[self.pmmlTrue])
            output.predicateIndex = 0
            if self.scoreDistributions is not None:
                output.children.extend(self.scoreDistributions)

        return output

class NodeClassification(Node):
    def __init__(self, features, data):
        Node.__init__(self, features, data)

        self.cvalues = numpy.unique(data[self.classifierField])
        bestCount = 0
        self.score = None
        self.scoreDistributions = []
        self.sdTotal = 0

        for cvalue in self.cvalues:
            selection = (data[self.classifierField] == cvalue)
            ccount = len(numpy.nonzero(selection)[0])
            if self.score is None or ccount > bestCount:
                self.score = cvalue
                bestCount = ccount

            sd = pmml.newInstance("ScoreDistribution", attrib={"value": cvalue, "recordCount": ccount})
            self.scoreDistributions.append(sd)
            self.sdTotal += ccount

        def sorter(a, b):
            aRC = a.attrib["recordCount"]
            bRC = b.attrib["recordCount"]
            if aRC == bRC:
                return cmp(a.attrib["value"], b.attrib["value"])
            else:
                return cmp(bRC, aRC)
        self.scoreDistributions.sort(sorter)

    def unsplitValue(self):
        s = 0.
        for cvalue in self.cvalues:
            try:
                frac = float(len(numpy.nonzero(self.data[self.classifierField] == cvalue)[0])) / len(self.data[self.classifierField])
            except ZeroDivisionError:
                pass
            else:
                try:
                    s -= frac * math.log(frac, 2)
                except (ValueError, OverflowError):
                    pass
        return s

    def _prepareCategorical(self, feature, values, return_valueClassProbs=False, mostPopularCValue=None):
        totalForClass = {}
        for sd in self.scoreDistributions:
            totalForClass[sd.attrib["value"]] = sd.attrib["recordCount"]
        
        numTotal = float(len(self.data[feature]))
        singleNumInSelection = {}
        singleNumInSelWithClass = {}
        valueClassProbs = []

        singleSelection = numpy.empty(len(self.data[feature]), dtype=numpy.bool)

        for v in values:
            numpy.equal(self.data[feature], v, singleSelection)
            singleNumInSelection[v] = float(len(numpy.nonzero(singleSelection)[0]))

            classification = self.data[self.classifierField][singleSelection]
            singleNumInSelWithClass[v] = {}
            for c in self.cvalues:
                singleNumInSelWithClass[v][c] = float(len(numpy.nonzero(classification == c)[0]))

            if return_valueClassProbs:
                try:
                    classProb = float(len(numpy.nonzero(classification == mostPopularCValue)[0]))/len(classification)
                except ZeroDivisionError:
                    classProb = 0.
                valueClassProbs.append((v, classProb))

        if return_valueClassProbs:
            return totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass, valueClassProbs
        else:
            return totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass

    def completeSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass = self._prepareCategorical(feature, values)

        normalizedEntropyGainTerm = 0.
        for v in values:
            self.selectionCache[feature, v] = (self.data[feature] == v)
            self.selectionValue[feature] = None

            selectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = singleNumInSelWithClass[v][c] / singleNumInSelection[v]
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        selectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            normalizedEntropyGainTerm -= (singleNumInSelection[v]/numTotal)*selectionEntropy

        return normalizedEntropyGainTerm

    def subsetSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None
        elif len(self.scoreDistributions) == 2:
            # according to a theorem in the CART book, this is equivalent for 2 classification values
            return self.fastSubsetSplit(feature)

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass = self._prepareCategorical(feature, values)

        bestOutput = None
        bestSubset = None

        # only need to check the first half of the possible subsets (2**N / 2) because the second half are mirror images
        try:
            iterator = xrange(2**(len(values) - 1))
        except OverflowError:
            raise RuntimeError("Attempting to split feature \"%s\", which has %d unique values and therefore %d subsets to try: problem too large, use splitCategorical='fast' instead" % (feature, len(values), 2**(len(values) - 1)))

        for subsetNumber in iterator:
            # produce a subset from the subsetNumber
            binaryDigits = numpy.array([(subsetNumber >> i) % 2 == 1 for i in xrange(len(values) - 1, -1, -1)])
            subset = values[binaryDigits]

            numInSelection = sum([singleNumInSelection[v] for v in subset])
            numNotInSelection = numTotal - numInSelection

            numInSelWithClass = {}
            numNotInSelWithClass = {}
            for c in self.cvalues:
                numInSelWithClass[c] = sum([singleNumInSelWithClass[v][c] for v in subset])
                numNotInSelWithClass[c] = totalForClass[c] - numInSelWithClass[c]

            selectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numInSelWithClass[c] / numInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        selectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            antiSelectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numNotInSelWithClass[c] / numNotInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        antiSelectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            normalizedEntropyGainTerm = -(numInSelection/numTotal)*selectionEntropy - (numNotInSelection/numTotal)*antiSelectionEntropy

            if bestOutput is None or normalizedEntropyGainTerm > bestOutput:
                bestOutput = normalizedEntropyGainTerm
                bestSubset = subset

        try:
            bestSelection = numpy.in1d(self.data[feature], bestSubset, assume_unique=(len(values) == len(self.data[feature])))
        except AttributeError:
            bestSelection = numpy.array([v in bestSubset for v in self.data[feature]])

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = bestSubset

        return bestOutput

    def fastSubsetSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        mostPopularCValue = None
        popularity = 0
        for sd in self.scoreDistributions:
            if sd.attrib["recordCount"] > popularity:
                mostPopularCValue = sd.attrib["value"]
                popularity = sd.attrib["recordCount"]

        totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass, valueClassProbs = self._prepareCategorical(feature, values, return_valueClassProbs=True, mostPopularCValue=mostPopularCValue)

        bestOutput = None
        bestSubset = []  # never actually test the empty set/complete set: gain of this pair is zero
        numInSelection_best = 0.
        numInSelWithClass_best = dict([(c, 0.) for c in self.cvalues])

        # we want to consider these values in the order of decreasing class probability (for the most popular class)
        valueClassProbs.sort(lambda a, b: cmp(b[1], a[1]))

        # differs from subsetSplit because if a value didn't help, it is never considered again
        for value, cp in valueClassProbs:
            subset = bestSubset + [value]

            numInSelection = numInSelection_best + singleNumInSelection[value]
            numNotInSelection = numTotal - numInSelection

            numInSelWithClass = dict(numInSelWithClass_best)
            numNotInSelWithClass = {}
            for c in self.cvalues:
                numInSelWithClass[c] += singleNumInSelWithClass[value][c]
                numNotInSelWithClass[c] = totalForClass[c] - numInSelWithClass[c]

            selectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numInSelWithClass[c] / numInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        selectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            antiSelectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numNotInSelWithClass[c] / numNotInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        antiSelectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            normalizedEntropyGainTerm = -(numInSelection/numTotal)*selectionEntropy - (numNotInSelection/numTotal)*antiSelectionEntropy

            if bestOutput is None or normalizedEntropyGainTerm > bestOutput:
                bestOutput = normalizedEntropyGainTerm
                bestSubset = subset   # bestSubset is built up by appending values when they help
                numInSelection_best = numInSelection
                numInSelWithClass_best = numInSelWithClass

        try:
            bestSelection = numpy.in1d(self.data[feature], bestSubset, assume_unique=(len(values) == len(self.data[feature])))
        except AttributeError:
            bestSelection = numpy.array([v in bestSubset for v in self.data[feature]])

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = numpy.array(bestSubset)

        return bestOutput

    def singletonSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        totalForClass, numTotal, singleNumInSelection, singleNumInSelWithClass = self._prepareCategorical(feature, values)

        bestOutput = None
        bestValue = None

        for v in values:
            selectionEntropy = 0.
            antiSelectionEntropy = 0.

            numInSelection = singleNumInSelection[v]
            numNotInSelection = numTotal - numInSelection

            for c in self.cvalues:
                numInSelWithClass_c = singleNumInSelWithClass[v][c]
                numNotInSelWithClass_c = totalForClass[c] - numInSelWithClass_c

                try:
                    frac = numInSelWithClass_c / numInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        selectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

                try:
                    frac = numNotInSelWithClass_c / numNotInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        antiSelectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            normalizedEntropyGainTerm = -(numInSelection/numTotal)*selectionEntropy - (numNotInSelection/numTotal)*antiSelectionEntropy

            if bestOutput is None or normalizedEntropyGainTerm > bestOutput:
                bestOutput = normalizedEntropyGainTerm
                bestValue = v

        self.selectionCache[feature, True] = (self.data[feature] == bestValue)
        self.selectionCache[feature, False] = numpy.logical_not(self.selectionCache[feature, True])
        self.selectionValue[feature] = bestValue
        return bestOutput

    def fastOrdinalSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        # find a threshold between two data points that maximizes entropic gain
        #      * without evaluating all points
        #      * that is deterministic
        #      * that's no worse than O(n log(n)) where n is the number of data points

        # sort values so that we can use running sums
        sortedIndices = numpy.argsort(self.data[feature], kind="heapsort")
            
        categories = self.data[self.classifierField][sortedIndices]
        values = self.data[feature][sortedIndices]

        try:
            uniqueValues, backwardIndices = numpy.unique(values[::-1], return_index=True)
        except TypeError:  # Numpy 1.3 and below
            if not hasattr(self, "numpy_version"):
                self.numpy_version = map(int, numpy.__version__.split("."))
            if self.numpy_version < [1, 3, 0]:
                backwardIndices, uniqueValues = numpy.unique1d(values[::-1], return_index=True)
            else:
                uniqueValues, backwardIndices = numpy.unique1d(values[::-1], return_index=True)

        if len(uniqueValues) < 2:
            return None

        forwardIndices = len(values) - 1 - backwardIndices
        del backwardIndices

        numAtAndBeforeIndexWithClass = {}
        catselection = numpy.empty(len(categories), dtype=numpy.bool)
        for c in self.cvalues:
            numpy.equal(categories, c, catselection)
            numAtAndBeforeIndexWithClass[c] = numpy.cumsum(catselection)
        del catselection

        def normalizedEntropyGainTerm(index):
            index = forwardIndices[index]

            numInSelection = float(index + 1)
            numNotInSelection = len(values) - numInSelection

            selectionEntropy = 0.
            antiSelectionEntropy = 0.
            for c in self.cvalues:
                numInSelWithClass = numAtAndBeforeIndexWithClass[c][index]
                numNotInSelWithClass = numAtAndBeforeIndexWithClass[c][-1] - numInSelWithClass

                try:
                    frac = 1. * numInSelWithClass / numInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        selectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

                try:
                    frac = 1. * numNotInSelWithClass / numNotInSelection
                except ZeroDivisionError:
                    pass
                else:
                    try:
                        antiSelectionEntropy -= frac * math.log(frac, 2)
                    except (ValueError, OverflowError):
                        pass

            return -(numInSelection/len(values))*selectionEntropy - (numNotInSelection/len(values))*antiSelectionEntropy

        resphi = 2. - (1. + math.sqrt(5.))/2.
        def goldenSection(a, b, c, fb):
            if c - b > b - a:
                x = int(round(b + resphi*(c - b)))
            else:
                x = int(round(b - resphi*(b - a)))
            if x in (a, b, c):
                i = numpy.argmax([normalizedEntropyGainTerm(a), fb, normalizedEntropyGainTerm(c)])
                return [a, b, c][i]

            fx = normalizedEntropyGainTerm(x)

            if fx > fb:
                if c - b > b - a:
                    return goldenSection(b, x, c, fx)
                else:
                    return goldenSection(a, x, b, fx)
            else:
                if c - b > b - a:
                    return goldenSection(a, b, x, fb)
                else:
                    return goldenSection(x, b, c, fb)

        # all of the work starts here
        low = 0
        high = len(uniqueValues) - 2
        mid = (low + high) / 2
        cut = goldenSection(low, mid, high, normalizedEntropyGainTerm(mid))

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if issubclass(self.data[feature].dtype.type, numpy.floating) and cut + 1 < len(uniqueValues):
            cutValue = (uniqueValues[cut] + uniqueValues[cut + 1])/2.
        else:
            cutValue = uniqueValues[cut]

        selection = (self.data[feature] <= cutValue)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = cutValue

        return normalizedEntropyGainTerm(cut)

    def exhaustiveSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        uniqueValues = numpy.unique(self.data[feature])
        if len(uniqueValues) < 2:
            return None
            
        numTotal = 0.
        numInSelWithClass = {}
        numNotInSelWithClass = {}
        for sd in self.scoreDistributions:
            numTotal += sd.attrib["recordCount"]
            numInSelWithClass[sd.attrib["value"]] = 0.
            numNotInSelWithClass[sd.attrib["value"]] = sd.attrib["recordCount"]

        numInSelection = 0.
        numNotInSelection = numTotal

        # sort values so that we can use running sums
        sortedIndices = numpy.argsort(self.data[feature], kind="heapsort")

        categories = self.data[self.classifierField][sortedIndices]
        values = self.data[feature][sortedIndices]

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if issubclass(self.data[feature].dtype.type, numpy.floating):
            uniqueValues = (uniqueValues[1:] + uniqueValues[:-1])/2.
        else:
            uniqueValues = uniqueValues[:-1]
        
        bestOutput = None
        bestCut = None

        index = 0
        for cut in uniqueValues:
            while values[index] <= cut:
                numInSelection += 1.
                numNotInSelection -= 1.
                numInSelWithClass[categories[index]] += 1.
                numNotInSelWithClass[categories[index]] -= 1.
                index += 1
                if index >= len(values): break

            selectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numInSelWithClass[c] / numInSelection
                except ZeroDivisionError:
                    continue
                try:
                    selectionEntropy -= frac * math.log(frac, 2)
                except (ValueError, OverflowError):
                    pass

            antiSelectionEntropy = 0.
            for c in self.cvalues:
                try:
                    frac = numNotInSelWithClass[c] / numNotInSelection
                except ZeroDivisionError:
                    continue
                try:
                    antiSelectionEntropy -= frac * math.log(frac, 2)
                except (ValueError, OverflowError):
                    pass

            normalizedEntropyGainTerm = -(numInSelection/numTotal)*selectionEntropy - (numNotInSelection/numTotal)*antiSelectionEntropy

            if bestOutput is None or normalizedEntropyGainTerm > bestOutput:
                bestOutput = normalizedEntropyGainTerm
                bestCut = cut

        self.selectionCache[feature, False] = (self.data[feature] <= bestCut)
        self.selectionCache[feature, True] = numpy.logical_not(self.selectionCache[feature, False])
        self.selectionValue[feature] = bestCut

        return bestOutput

    def medianSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        if len(self.data[feature]) < 2 or numpy.ptp(self.data[feature]) == 0.:
            return None

        median = numpy.median(self.data[feature])
        selection = (self.data[feature] <= median)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = median

        try:
            fraction = float(len(numpy.nonzero(selection)[0]))/float(len(self.data[self.classifierField]))
        except ZeroDivisionError:
            fraction = 0.
        classification = self.data[self.classifierField][selection]
        catselection = numpy.empty(len(classification), dtype=numpy.bool)

        selectionEntropy = 0.
        for cvalue in self.cvalues:
            numpy.equal(classification, cvalue, catselection)
            try:
                frac = float(len(numpy.nonzero(catselection)[0]))/float(len(classification))
            except ZeroDivisionError:
                continue
            try:
                selectionEntropy -= frac*math.log(frac, 2)
            except (ValueError, OverflowError):
                pass

        classification = self.data[self.classifierField][numpy.logical_not(selection)]
        catselection = numpy.empty(len(classification), dtype=numpy.bool)

        antiSelectionEntropy = 0.
        for cvalue in self.cvalues:
            numpy.equal(classification, cvalue, catselection)
            try:
                frac = float(len(numpy.nonzero(catselection)[0]))/float(len(classification))
            except ZeroDivisionError:
                continue
            try:
                antiSelectionEntropy -= frac*math.log(frac, 2)
            except (ValueError, OverflowError):
                pass

        return -fraction*selectionEntropy - (1. - fraction)*antiSelectionEntropy

class NodeRegression(Node):
    def __init__(self, features, data):
        Node.__init__(self, features, data)

        self.score = numpy.mean(data[self.classifierField])
        self.sdTotal = len(data[self.classifierField])
        self.scoreDistributions = None
        self.cvalues = None
    
    def unsplitValue(self):
        return len(self.data[self.classifierField]) * numpy.var(self.data[self.classifierField])

    def _prepareCategorical(self, feature, values):
        singleSelection = numpy.empty(len(self.data[feature]), dtype=numpy.bool)

        sum1 = {}
        sumx = {}
        sumxx = {}
        for v in values:
            numpy.equal(self.data[feature], v, singleSelection)

            singleResponse = self.data[self.classifierField][singleSelection]
            sum1[v] = len(singleResponse)
            sumx[v] = numpy.sum(singleResponse)
            numpy.square(singleResponse, singleResponse)
            sumxx[v] = numpy.sum(singleResponse)

        sum1Total = sum(sum1.values())
        sumxTotal = sum(sumx.values())
        sumxxTotal = sum(sumxx.values())

        return sum1, sumx, sumxx, sum1Total, sumxTotal, sumxxTotal

    def completeSplit(self, feature):
        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        nvariances = 0.
        for v in values:
            self.selectionCache[feature, v] = (self.data[feature] == v)
            self.selectionValue[feature] = None

            subdata = self.data[self.classifierField][self.selectionCache[feature, v]]
            nvariances -= len(subdata) * numpy.var(subdata)
            
        return nvariances

    def subsetSplit(self, feature):
        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        bestOutput = None
        bestSubset = None

        # only need to check the first half of the possible subsets (2**N / 2) because the second half are mirror images
        try:
            iterator = xrange(2**(len(values) - 1))
        except OverflowError:
            raise RuntimeError("Attempting to split feature \"%s\", which has %d unique values and therefore %d subsets to try: problem too large, use splitCategorical='fast' instead" % (feature, len(values), 2**(len(values) - 1)))

        sum1, sumx, sumxx, sum1Total, sumxTotal, sumxxTotal = self._prepareCategorical(feature, values)

        for subsetNumber in iterator:
            # produce a subset from the subsetNumber
            binaryDigits = numpy.array([(subsetNumber >> i) % 2 == 1 for i in xrange(len(values) - 1, -1, -1)])
            subset = values[binaryDigits]

            this_sum1 = sum([sum1[v] for v in subset])
            this_sumx = sum([sumx[v] for v in subset])
            this_sumxx = sum([sumxx[v] for v in subset])

            that_sum1 = sum1Total - this_sum1
            that_sumx = sumxTotal - this_sumx
            that_sumxx = sumxxTotal - this_sumxx

            nvariances = -((this_sumxx - this_sumx**2/this_sum1) if this_sum1 > 0. else 0.) + \
                         -((that_sumxx - that_sumx**2/that_sum1) if that_sum1 > 0. else 0.)

            if bestOutput is None or nvariances > bestOutput:
                bestOutput = nvariances
                bestSubset = subset

        try:
            bestSelection = numpy.in1d(self.data[feature], bestSubset, assume_unique=(len(values) == len(self.data[feature])))
        except AttributeError:
            bestSelection = numpy.array([v in bestSubset for v in self.data[feature]])

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = bestSubset

        return bestOutput

    def fastSubsetSplit(self, feature):
        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        sum1, sumx, sumxx, sum1Total, sumxTotal, sumxxTotal = self._prepareCategorical(feature, values)
        
        # we want to consider these values in the order of the means of their associated responses
        valueResponseMeans = [(v, sumx[v]/sum1[v] if sum1[v] > 0. else None) for v in values]

        # sort by decreasing response-means (Nones go to the end)
        valueResponseMeans.sort(lambda a, b: cmp(b[1], a[1]))

        bestOutput = None
        bestSubset = []
        best_this_sum1 = 0.
        best_this_sumx = 0.
        best_this_sumxx = 0.

        for value, rm in valueResponseMeans:
            subset = bestSubset + [value]

            this_sum1 = best_this_sum1 + sum1[value]
            this_sumx = best_this_sumx + sumx[value]
            this_sumxx = best_this_sumxx + sumxx[value]

            that_sum1 = sum1Total - this_sum1
            that_sumx = sumxTotal - this_sumx
            that_sumxx = sumxxTotal - this_sumxx

            nvariances = -((this_sumxx - this_sumx**2/this_sum1) if this_sum1 > 0. else 0.) + \
                         -((that_sumxx - that_sumx**2/that_sum1) if that_sum1 > 0. else 0.)

            if bestOutput is None or nvariances > bestOutput:
                bestOutput = nvariances
                bestSubset = subset
                best_this_sum1 = this_sum1
                best_this_sumx = this_sumx
                best_this_sumxx = this_sumxx
                
        try:
            bestSelection = numpy.in1d(self.data[feature], bestSubset, assume_unique=(len(values) == len(self.data[feature])))
        except AttributeError:
            bestSelection = numpy.array([v in bestSubset for v in self.data[feature]])

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = numpy.array(bestSubset)

        return bestOutput

    def singletonSplit(self, feature):
        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        sum1, sumx, sumxx, sum1Total, sumxTotal, sumxxTotal = self._prepareCategorical(feature, values)

        bestOutput = None
        bestValue = None

        for v in values:
            nvariances = -((sumxx[v] - sumx[v]**2/sum1[v]) if sum1[v] > 0. else 0.) + \
                         -(((sumxxTotal - sumxx[v]) - (sumxTotal - sumx[v])**2/(sum1Total - sum1[v])) if (sum1Total - sum1[v]) > 0. else 0.)
            
            if bestOutput is None or nvariances > bestOutput:
                bestOutput = nvariances
                bestValue = v

        self.selectionCache[feature, True] = (self.data[feature] == bestValue)
        self.selectionCache[feature, False] = numpy.logical_not(self.selectionCache[feature, True])
        self.selectionValue[feature] = bestValue
        return bestOutput

    def fastOrdinalSplit(self, feature):
        # sort values so that we can use running sums
        sortedIndices = numpy.argsort(self.data[feature], kind="heapsort")

        response = self.data[self.classifierField][sortedIndices]
        values = self.data[feature][sortedIndices]

        try:
            uniqueValues, backwardIndices = numpy.unique(values[::-1], return_index=True)
        except TypeError:  # Numpy 1.3 and below
            if not hasattr(self, "numpy_version"):
                self.numpy_version = map(int, numpy.__version__.split("."))
            if self.numpy_version < [1, 3, 0]:
                backwardIndices, uniqueValues = numpy.unique1d(values[::-1], return_index=True)
            else:
                uniqueValues, backwardIndices = numpy.unique1d(values[::-1], return_index=True)

        if len(uniqueValues) < 2:
            return None

        forwardIndices = len(values) - 1 - backwardIndices
        del backwardIndices

        forward_sum1 = numpy.arange(len(response), dtype=numpy.double)
        forward_sumx = numpy.cumsum(response)
        forward_sumxx = numpy.cumsum(response * response)
        backward_sum1 = forward_sum1[-1] - forward_sum1
        backward_sumx = forward_sumx[-1] - forward_sumx
        backward_sumxx = forward_sumxx[-1] - forward_sumxx

        def nvariances(index):
            index = forwardIndices[index]

            this_sum1 = forward_sum1[index]
            this_sumx = forward_sumx[index]
            this_sumxx = forward_sumxx[index]
            that_sum1 = backward_sum1[index]
            that_sumx = backward_sumx[index]
            that_sumxx = backward_sumxx[index]

            return -((this_sumxx - this_sumx**2/this_sum1) if this_sum1 > 0. else 0.) + \
                   -((that_sumxx - that_sumx**2/that_sum1) if that_sum1 > 0. else 0.)

        resphi = 2. - (1. + math.sqrt(5.))/2.
        def goldenSection(a, b, c, fb):
            if c - b > b - a:
                x = int(round(b + resphi*(c - b)))
            else:
                x = int(round(b - resphi*(b - a)))
            if x in (a, b, c):
                i = numpy.argmax([nvariances(a), fb, nvariances(c)])
                return [a, b, c][i]

            fx = nvariances(x)

            if fx > fb:
                if c - b > b - a:
                    return goldenSection(b, x, c, fx)
                else:
                    return goldenSection(a, x, b, fx)
            else:
                if c - b > b - a:
                    return goldenSection(a, b, x, fb)
                else:
                    return goldenSection(x, b, c, fb)

        # all of the work starts here
        low = 0
        high = len(uniqueValues) - 2
        mid = (low + high) / 2
        cut = goldenSection(low, mid, high, nvariances(mid))

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if issubclass(self.data[feature].dtype.type, numpy.floating) and cut + 1 < len(uniqueValues):
            cutValue = (uniqueValues[cut] + uniqueValues[cut + 1])/2.
        else:
            cutValue = uniqueValues[cut]

        selection = (self.data[feature] <= cutValue)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = cutValue

        return nvariances(cut)

    def exhaustiveSplit(self, feature):
        uniqueValues = numpy.unique(self.data[feature])
        if len(uniqueValues) < 2:
            return None

        # sort values so that we can use running sums
        sortedIndices = numpy.argsort(self.data[feature], kind="heapsort")

        response = self.data[self.classifierField][sortedIndices]
        values = self.data[feature][sortedIndices]

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if issubclass(self.data[feature].dtype.type, numpy.floating):
            uniqueValues = (uniqueValues[1:] + uniqueValues[:-1])/2.
        else:
            uniqueValues = uniqueValues[:-1]
        
        bestOutput = None
        bestCut = None

        this_sum1 = 0.
        this_sumx = 0.
        this_sumxx = 0.
        sum1Total = len(response)
        sumxTotal = numpy.sum(response)
        sumxxTotal = numpy.sum(response * response)

        index = 0
        for cut in uniqueValues:
            while values[index] <= cut:
                this_sum1 += 1.
                this_sumx += response[index]
                this_sumxx += response[index]**2
                index += 1
                if index >= len(values): break

            that_sum1 = sum1Total - this_sum1
            that_sumx = sumxTotal - this_sumx
            that_sumxx = sumxxTotal - this_sumxx

            nvariances = -((this_sumxx - this_sumx**2/this_sum1) if this_sum1 > 0. else 0.) + \
                         -((that_sumxx - that_sumx**2/that_sum1) if that_sum1 > 0. else 0.)

            if bestOutput is None or nvariances > bestOutput:
                bestOutput = nvariances
                bestCut = cut

        self.selectionCache[feature, False] = (self.data[feature] <= bestCut)
        self.selectionCache[feature, True] = numpy.logical_not(self.selectionCache[feature, False])
        self.selectionValue[feature] = bestCut
        
        return bestOutput

    def medianSplit(self, feature):
        median = numpy.median(self.data[feature])
        selection = (self.data[feature] <= median)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = median

        subdata = self.data[self.classifierField][selection]
        nvariances = -len(subdata) * numpy.var(subdata)
        
        subdata = self.data[self.classifierField][numpy.logical_not(selection)]
        nvariances -= len(subdata) * numpy.var(subdata)

        return nvariances

class ProducerIterative(ProducerAlgorithm):
    """The standard tree-building algorithms: ID3, C45, and CART."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minGain": "0.", "minRecordCount": "0", "splitOrdinal": "fast", "splitCategorical": "fast", "classifierField": "", "pruningDataFraction": "0.", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        self.model = self.segmentRecord.pmmlModel

        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if self.updateExisting:
            raise NotImplementedError("Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'")

        if "maxTreeDepth" in params:
            try:
                self.maxTreeDepth = int(params["maxTreeDepth"])
            except ValueError:
                self.maxTreeDepth = None
            if self.maxTreeDepth <= 0:
                self.maxTreeDepth = None
            del params["maxTreeDepth"]
        else:
            self.maxTreeDepth = int(self.defaultParams["maxTreeDepth"])

        if "minGain" in params:
            try:
                self.minGain = float(params["minGain"])
            except ValueError:
                self.minGain = 0.
            del params["minGain"]
        else:
            self.minGain = float(self.defaultParams["minGain"])

        if "minRecordCount" in params:
            try:
                self.minRecordCount = int(params["minRecordCount"])
            except ValueError:
                self.minRecordCount = 0
            del params["minRecordCount"]
        else:
            self.minRecordCount = int(self.defaultParams["minRecordCount"])

        if "splitOrdinal" in params:
            self.splitOrdinal = params["splitOrdinal"]
            del params["splitOrdinal"]
        else:
            self.splitOrdinal = self.defaultParams["splitOrdinal"]
        if self.splitOrdinal not in ("fast", "exhaustive", "median"):
            raise NotImplementedError("The only valid splitOrdinal values are ('fast', 'exhaustive', 'median')")

        if "splitCategorical" in params:
            self.splitCategorical = params["splitCategorical"]
            del params["splitCategorical"]
        else:
            self.splitCategorical = self.defaultParams["splitCategorical"]
        if self.splitCategorical not in ("complete", "subset", "fast", "singleton"):
            raise NotImplementedError("The only valid splitCategoricals are ('complete', 'subset', 'fast', 'singleton')")
        
        if "classifierField" in params:
            self.classifierField = params["classifierField"]
            del params["classifierField"]
        else:
            self.classifierField = self.defaultParams["classifierField"]
        if self.classifierField == "": self.classifierField = None

        if "pruningDataFraction" in params:
            try:
                self.pruningDataFraction = float(params["pruningDataFraction"])
            except ValueError:
                self.pruningDataFraction = 0.
            del params["pruningDataFraction"]
        else:
            self.pruningDataFraction = float(self.defaultParams["pruningDataFraction"])
        if self.pruningDataFraction > 0.:
            raise NotImplementedError("Pruning has not yet been implemented; use truncation (maxTreeDepth, minGain, minRecordCount) instead")

        if "pruningThreshold" in params:
            try:
                self.pruningThreshold = float(params["pruningThreshold"])
            except ValueError:
                self.pruningThreshold = 0.
            del params["pruningThreshold"]
        else:
            self.pruningThreshold = float(self.defaultParams["pruningThreshold"])

        if isinstance(self.model, pmml.TreeModel):
            self.nodeIndex = self.model.index(pmml.Node)
            if self.splitCategorical == "complete":
                self.model.attrib["splitCharacteristic"] = "multiSplit"
            else:
                self.model.attrib["splitCharacteristic"] = "binarySplit"

        elif isinstance(self.model, pmml.RuleSetModel):
            self.ruleSet = self.model.child(pmml.RuleSet)
            self.nodeIndex = self.ruleSet.index(lambda x: isinstance(x, (pmml.SimpleRule, pmml.CompoundRule)), exception=False)
            if self.nodeIndex is None:
                self.nodeIndex = len(self.ruleSet.children)
                self.ruleSet.children.append(None)

        self.regression = (self.model.attrib["functionName"] == "regression")
            
        self.features = []
        self.categorical = {}
        self.predicted = []
        self.data = {}
        self.lookup = {}
        self.dataType = {}
        self.featureSplitExceptions = {}
        for miningField in self.model.child(pmml.MiningSchema).matches(pmml.MiningField):
            name = miningField.attrib["name"]
            usageType = miningField.attrib.get("usageType", "active")
            self.dataType[name] = self.model.dataContext.dataType[name]
            self.lookup[name] = (self.dataType[name] not in ("integer", "float", "double", "boolean"))

            if usageType == "active":
                optype = self.model.dataContext.optype[name]

                self.features.append(name)
                self.categorical[name] = (optype == "categorical")
                self.data[name] = []

                featureSplitName = "split_" + name
                if featureSplitName in params:
                    self.featureSplitExceptions[name] = params[featureSplitName]
                    del params[featureSplitName]

                    if optype == "categorical":
                        if self.featureSplitExceptions[name] == "complete":
                            self.featureSplitExceptions[name] = NodeRegression.completeSplit if self.regression else NodeClassification.completeSplit
                            self.model.attrib["splitCharacteristic"] = "multiSplit"
                        elif self.featureSplitExceptions[name] == "subset":
                            self.featureSplitExceptions[name] = NodeRegression.subsetSplit if self.regression else NodeClassification.subsetSplit
                        elif self.featureSplitExceptions[name] == "fast":
                            self.featureSplitExceptions[name] = NodeRegression.fastSubsetSplit if self.regression else NodeClassification.fastSubsetSplit
                        elif self.featureSplitExceptions[name] == "singleton":
                            self.featureSplitExceptions[name] = NodeRegression.singletonSplit if self.regression else NodeClassification.singletonSplit
                        else:
                            raise NotImplementedError("The only valid split methods for feature \"%s\", which is %s, are ('complete', 'subset', 'fast', 'singleton')" % (name, optype))

                    else:
                        if self.featureSplitExceptions[name] == "fast":
                            self.featureSplitExceptions[name] = NodeRegression.fastOrdinalSplit if self.regression else NodeClassification.fastOrdinalSplit
                        elif self.featureSplitExceptions[name] == "exhaustive":
                            self.featureSplitExceptions[name] = NodeRegression.exhaustiveSplit if self.regression else NodeClassification.exhaustiveSplit
                        elif self.featureSplitExceptions[name] == "median":
                            self.featureSplitExceptions[name] = NodeRegression.medianSplit if self.regression else NodeClassification.medianSplit
                        else:
                            raise NotImplementedError("The only valid split methods for feature \"%s\", which is %s, are ('fast', 'exhaustive', 'median')" % (name, optype))

            if usageType == "predicted":
                self.predicted.append(name)

        if len(self.predicted) == 0:
            self.classifierField = INVALID

        else:
            if self.classifierField is None:
                # by default, take the first 'predicted' feature
                self.classifierField = self.predicted[0]
            else:
                if self.classifierField not in self.predicted:
                    raise RuntimeError("ClassifierField feature not found among the 'predicted' features in the decision tree's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())

        if self.regression and self.dataType[self.classifierField] not in ("integer", "float", "double"):
            raise RuntimeError("Regression trees require a numeric predicted field: \"%s\" is \"%s\"" % (self.classifierField, self.dataType[self.classifierField]))

        self.data[self.classifierField] = []

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def update(self, syncNumber, get):
        self.resetLoggerLevels()

        if self.classifierField is INVALID:
            raise RuntimeError("Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine())
        
        values = [get(feature) for feature in self.features]
        if INVALID in values or MISSING in values:
            self.logger.debug("ProducerIterativeTree.update: returning False (INVALID or MISSING classification field)")
            return False

        classification = get(self.classifierField)
        if classification is INVALID or classification is MISSING:
            self.logger.debug("ProducerIterativeTree.update: returning False (INVALID or MISSING classification field)")
            return False

        for value, feature in zip(values, self.features):
            self.data[feature].append(value)
        self.data[self.classifierField].append(classification)

        return True

    def produce(self):
        self.resetLoggerLevels()

        if self.logger.getEffectiveLevel() <= logging.INFO:
            self.logger.info("ProducerIterativeTree.produce: accumulated dataset has %d records" % len(self.data[self.classifierField]))

        for name in self.data:
            if self.lookup[name]:
                self.logger.debug("ProducerIterativeTree.produce: converting feature \"%s\" to a NumPy array (with string-to-integer conversion)" % name)

                try:
                    lookup, data = numpy.unique(self.data[name], return_inverse=True)
                except TypeError:
                    if map(int, numpy.__version__.split(".")) < [1, 3, 0]:
                        lookup = numpy.unique1d(self.data[name])
                        data = numpy.array([numpy.searchsorted(lookup, d) for d in self.data[name]])
                    else:
                        lookup, data = numpy.unique1d(self.data[name], return_inverse=True)

                self.lookup[name], self.data[name] = lookup, data

            else:
                self.logger.debug("ProducerIterativeTree.produce: converting feature \"%s\" to a NumPy array" % name)
                self.data[name] = numpy.array(self.data[name])

        Node.maxTreeDepth = self.maxTreeDepth
        Node.minGain = self.minGain
        Node.minRecordCount = self.minRecordCount

        Node.lookup = self.lookup
        Node.dataType = self.dataType
        Node.categorical = self.categorical
        Node.classifierField = self.classifierField
        Node.featureSplitExceptions = self.featureSplitExceptions

        if self.splitOrdinal == "fast":
            NodeRegression.splitOrdinal = NodeRegression.fastOrdinalSplit
            NodeClassification.splitOrdinal = NodeClassification.fastOrdinalSplit
        elif self.splitOrdinal == "exhaustive":
            NodeRegression.splitOrdinal = NodeRegression.exhaustiveSplit
            NodeClassification.splitOrdinal = NodeClassification.exhaustiveSplit
        elif self.splitOrdinal == "median":
            NodeRegression.splitOrdinal = NodeRegression.medianSplit
            NodeClassification.splitOrdinal = NodeClassification.medianSplit

        if self.splitCategorical == "complete":
            NodeRegression.splitCategorical = NodeRegression.completeSplit
            NodeClassification.splitCategorical = NodeClassification.completeSplit
        elif self.splitCategorical == "subset":
            NodeRegression.splitCategorical = NodeRegression.subsetSplit
            NodeClassification.splitCategorical = NodeClassification.subsetSplit
        elif self.splitCategorical == "fast":
            NodeRegression.splitCategorical = NodeRegression.fastSubsetSplit
            NodeClassification.splitCategorical = NodeClassification.fastSubsetSplit
        elif self.splitCategorical == "singleton":
            NodeRegression.splitCategorical = NodeRegression.singletonSplit
            NodeClassification.splitCategorical = NodeClassification.singletonSplit

        logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG
        self.logger.debug("ProducerIterativeTree.produce: starting to build tree")

        if self.regression:
            node = NodeRegression(self.features, self.data)
        else:
            node = NodeClassification(self.features, self.data)
        depthCounter = 0
        node.split(depthCounter, self.logger if logDebug else None)

        self.logger.debug("ProducerIterativeTree.produce: finished building tree")

        # eliminate these large datasets before entering into the next segment
        del self.lookup
        del self.data

        if isinstance(self.model, pmml.TreeModel):
            self.model[self.nodeIndex] = node.tree("Node-1")

        elif isinstance(self.model, pmml.RuleSetModel):
            self.ruleSet[self.nodeIndex] = node.rule("Node-1")

        # now that the model has been made, you no longer need the structure that made it
        del node

class ProducerC45(ProducerIterative):
    """The special case of ProducerIterative known as C4.5 (ID3 is the sub-case of no continuous features)."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minGain": "0.", "minRecordCount": "0", "fast": "false", "classifierField": "", "pruningDataFraction": "0.", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        if "fast" in params:
            self.fast = pmml.boolCheck(params["fast"])
            del params["fast"]
        else:
            self.fast = pmml.boolCheck(self.defaultParams["fast"])

        if "splitOrdinal" in params:
            raise TypeError("Parameter 'splitOrdinal' cannot be set in algorithm 'c45' because it is always equal to \"exhaustive\" (or \"fast\" if you set fast to \"true\")")
        else:
            if self.fast:
                params["splitOrdinal"] = "fast"
            else:
                params["splitOrdinal"] = "exhaustive"

        if "splitCategorical" in params:
            raise TypeError("Parameter 'splitCategorical' cannot be set in algorithm 'c45' because it is always equal to \"subset\" (or \"fast\" if you set fast to \"true\")")
        else:
            if self.fast:
                params["splitCategorical"] = "fast"
            else:
                params["splitCategorical"] = "subset"

        ProducerIterative.initialize(self, **params)

class ProducerCART(ProducerIterative):
    """The special case of ProducerIterative known as CART."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minGain": "0.", "minRecordCount": "0", "fast": "false", "classifierField": "", "pruningDataFraction": "0.", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        if "fast" in params:
            self.fast = pmml.boolCheck(params["fast"])
            del params["fast"]
        else:
            self.fast = pmml.boolCheck(self.defaultParams["fast"])

        if "splitOrdinal" in params:
            raise TypeError("Parameter 'splitOrdinal' cannot be set in algorithm 'cart' because it is always equal to \"exhaustive\" (or \"fast\" if you set fast to \"true\")")
        else:
            if self.fast:
                params["splitOrdinal"] = "fast"
            else:
                params["splitOrdinal"] = "exhaustive"

        if "splitCategorical" in params:
            raise TypeError("Parameter 'splitCategorical' cannot be set in algorithm 'cart' because it is always equal to \"subset\" (or \"fast\" if you set fast to \"true\")")
        else:
            if self.fast:
                params["splitCategorical"] = "fast"
            else:
                params["splitCategorical"] = "subset"

        ProducerIterative.initialize(self, **params)
