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
            predictedValue = node.score
            entityId = node.attrib["id"]

        ### someday, you'll need this:
        # penalty = 1.
        # if metadata.missingValuePenalty is not None:
        #     penalty = metadata.missingValuePenalty**metadata.unknowns

        self.lastScore = {SCORE_predictedValue: predictedValue, SCORE_entityId: entityId}
        return self.lastScore

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
            except (ValueError, OverflowError):
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
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "equal", "value": self.value})
        output.needsValue = False
        return output

    def falseSimplePredicate(self):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "notEqual", "value": self.value})
        output.needsValue = False
        return output

class SplitGreaterThan(Split):
    def decision(self, get):
        return get(self.name) > self.value

    def expression(self):
        return "(%s > %s)" % (self.name, str(self.value))

    def trueSimplePredicate(self):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "greaterThan", "value": self.value})
        output.needsValue = False
        return output

    def falseSimplePredicate(self):
        output = pmml.newInstance("SimplePredicate", attrib={"field": self.name, "operator": "lessOrEqual", "value": self.value})
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
            pmmlTrue = pmml.newInstance("True")
            trueNode = pmml.newInstance("Node", attrib={"score": bestClassification}, children=[pmmlTrue])
            trueNode.test = pmmlTrue.createTest()

            parent[producer.nodeIndex] = trueNode

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer)

        else:
            trueNode = pmml.newInstance("Node", attrib={"score": self.split.score(True)}, children=[self.split.trueSimplePredicate()])
            trueNode.test = trueNode.children[0].createTest()

            falseNode = pmml.newInstance("Node", attrib={"score": self.split.score(False)}, children=[self.split.falseSimplePredicate()])
            falseNode.test = falseNode.children[0].createTest()

            parent.children.append(trueNode)
            parent.children.append(falseNode)

            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(trueNode, bestClassification, producer)
                max(self.false_outworlds.values(), key=lambda x: x.split.gainCache).bestTree(falseNode, bestClassification, producer)

    def bestRule(self, parent, bestClassification, producer):
        if self.split is None:
            pmmlTrue = pmml.newInstance("True")
            if len(self.true_outworlds) > 0:
                trueRule = pmml.newInstance("CompoundRule", children=pmmlTrue)
            else:
                trueRule = pmml.newInstance("SimpleRule", attrib={"score": bestClassification}, children=pmmlTrue)

            trueRule.test = pmmlTrue.createTest()
            parent[producer.nodeIndex] = trueRule

            if len(self.true_outworlds) > 0:
                # find the single best world using max (not sorting!)
                max(self.true_outworlds.values(), key=lambda x: x.split.gainCache).bestRule(trueRule, bestClassification, producer)

        else:
            if len(self.true_outworlds) > 0 and len(self.false_outworlds) > 0:
                trueRule = pmml.newInstance("CompoundRule", children=[self.split.trueSimplePredicate()])
                falseRule = pmml.newInstance("CompoundRule", children=[self.split.falseSimplePredicate()])
            else:
                trueRule = pmml.newInstance("SimpleRule", attrib={"score": self.split.score(True)}, children=[self.split.trueSimplePredicate()])
                falseRule = pmml.newInstance("SimpleRule", attrib={"score": self.split.score(False)}, children=[self.split.falseSimplePredicate()])

            trueRule.test = trueRule.children[0].createTest()
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
            raise NotImplementedError, "Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'"

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

        if isinstance(self.model, pmml.TreeModel):
            self.modelType = self.TREEMODEL
            self.nodeIndex = self.model.index(pmml.Node)

            if self.model.attrib["functionName"] == "regression":
                raise NotImplementedError, "TreeModels with functionName 'regression' have not been implemented (they can be consumed, but not produced)"

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
                    raise RuntimeError, "ClassifierField feature not found among the 'predicted' features in the decision tree's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()
        
        self.topWorld = World(0, None)
        self.counts = {}

        if len(params) > 0:
            raise TypeError, "Unrecognized parameters %s" % params

    def update(self, syncNumber, get):
        self.resetLoggerLevels()

        if self.classifierField is INVALID:
            raise RuntimeError, "Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()

        values = [get(feature.name) for feature in self.features]
        if INVALID in values or MISSING in values:
            self.logger.debug("ProducerTreeModel.update: returning False (INVALID or MISSING data)")
            return False

        classification = get(self.classifierField)
        if classification is INVALID or classification is MISSING:
            self.logger.debug("ProducerTreeModel.update: returning False (INVALID or MISSING classification field)")
            return False

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

#########################################################################################
############################################################## iterative producers ######
#########################################################################################

class Node:
    pmmlTrue = pmml.pmmlTrue()
    objectiveMetric = None
    splitOrdinal = None
    splitCategorical = None
    featureSplitExceptions = {}

    # stopping conditions
    maxTreeDepth = None
    minObjective = None
    minRecordCount = None

    def __init__(self, features, categorical, data, classifierField):
        self.features = features
        self.categorical = categorical
        self.data = data
        self.classifierField = classifierField
        
        self.cvalues = numpy.unique(data[classifierField])
        bestCount = 0
        self.score = None
        self.scoreDistributions = []
        self.sdTotal = 0

        for cvalue in self.cvalues:
            selection = (data[classifierField] == cvalue)
            ccount = sum(selection)
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

        self.selectionCache = {}
        self.selectionValue = {}

        self.subNodes = None

    def classProb(self, selection, cvalue):
        classification = self.data[self.classifierField][selection]
        try:
            return float(sum(classification == cvalue))/float(len(classification))
        except ZeroDivisionError:
            return 0.

    def entropyMetric(self, selection=None):
        if selection is None:
            classification = self.data[self.classifierField]
        else:
            classification = self.data[self.classifierField][selection]

        output = 0.
        for cvalue in self.cvalues:
            try:
                frac = float(sum(classification == cvalue))/float(len(classification))
            except ZeroDivisionError:
                continue
            try:
                output -= frac*math.log(frac, 2)
            except (ValueError, OverflowError):
                pass
        return output

    def giniMetric(self, selection=None):
        if selection is None:
            classification = self.data[self.classifierField]
        else:
            classification = self.data[self.classifierField][selection]

        output = 1.
        for cvalue in self.cvalues:
            try:
                frac = float(sum(classification == cvalue))/float(len(classification))
            except ZeroDivisionError:
                continue
            try:
                output -= frac**2
            except (ValueError, OverflowError):
                pass
        return output

    def fraction(self, selection):
        try:
            return float(sum(selection))/float(len(self.data[self.classifierField]))
        except ZeroDivisionError:
            return 0.

    def gainterm(self, feature):
        if feature in self.featureSplitExceptions:
            return self.featureSplitExceptions[feature](self, feature)

        elif self.categorical[feature]:
            return self.splitCategorical(feature)

        else:
            return self.splitOrdinal(feature)

    def completeSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        values = numpy.unique(self.data[feature])       
        if len(values) < 2:
            return None

        output = 0.
        for value in values:
            selection = (self.data[feature] == value)
            self.selectionCache[feature, value] = selection
            self.selectionValue[feature] = None
            output -= self.fraction(selection) * self.objectiveMetric(selection)
        return output

    def calculatePair(self, selection):
        fraction = self.fraction(selection)
        output = -fraction * self.objectiveMetric(selection)

        selection = numpy.logical_not(selection)
        fraction = 1. - fraction
        output -= fraction * self.objectiveMetric(selection)

        return output

    def subsetSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None
        elif len(self.scoreDistributions) == 2:
            # according to a theorem in the CART book, this is equivalent for 2 classification values
            return self.fastSubsetSplit(feature)

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        bestOutput = None
        bestSelection = None
        bestSubset = None

        singleSelection = {}
        for v in values:
            singleSelection[v] = (self.data[feature] == v)

        # only need to check the first half of the possible subsets (2**N / 2) because the second half are mirror images
        try:
            iterator = xrange(2**(len(values) - 1))
        except OverflowError:
            raise RuntimeError, "Attempting to split feature \"%s\", which has %d unique values and therefore %d subsets to try: problem too large, use splitCategorical='fast' instead" % (feature, len(values), 2**(len(values) - 1))

        for subsetNumber in iterator:
            # produce a subset from the subsetNumber
            binaryDigits = numpy.array([(subsetNumber >> i) % 2 == 1 for i in xrange(len(values) - 1, -1, -1)])
            subset = values[binaryDigits]

            # make a selection map as a logical-or of each value in the subset
            selection = numpy.zeros(len(self.data[feature]), dtype=numpy.bool)
            for v in subset:
                selection = numpy.logical_or(selection, singleSelection[v])

            output = self.calculatePair(selection)

            if bestOutput is None or output > bestOutput:
                bestOutput = output
                bestSelection = selection
                bestSubset = subset

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = list(bestSubset)

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

        bestOutput = None
        bestSelection = None
        bestSubset = []  # never actually test the empty set/complete set: gain of this pair is zero

        singleSelection = {}
        valueClassProbs = []
        for v in values:
            singleSelection[v] = (self.data[feature] == v)
            valueClassProbs.append((v, self.classProb(singleSelection[v], mostPopularCValue)))

        # we want to consider these values in the order of decreasing class probability (for the most popular class)
        valueClassProbs.sort(lambda a, b: cmp(b[1], a[1]))

        # differs from subsetSplit because if a value didn't help, it is never considered again
        for value, cp in valueClassProbs:
            subset = bestSubset + [value]

            selection = numpy.zeros(len(self.data[feature]), dtype=numpy.bool)
            for v in subset:
                selection = numpy.logical_or(selection, singleSelection[v])

            output = self.calculatePair(selection)

            if bestOutput is None or output > bestOutput:
                bestOutput = output
                bestSelection = selection
                bestSubset = subset   # bestSubset is built up by appending values when they help

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = list(bestSubset)

        return bestOutput

    def singletonSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        values = numpy.unique(self.data[feature])
        if len(values) < 2:
            return None

        bestOutput = None
        bestSelection = None
        bestValue = None

        for value in values:
            selection = (self.data[feature] == value)

            output = self.calculatePair(selection)

            if bestOutput is None or output > bestOutput:
                bestOutput = output
                bestSelection = selection
                bestValue = value

        self.selectionCache[feature, True] = bestSelection
        self.selectionCache[feature, False] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = bestValue
        return bestOutput

    def sortby(self, feature):
        if self.data[feature].dtype == numpy.object:
            return numpy.sort(numpy.unique(self.data[feature]), kind="quicksort")
        else:
            return numpy.sort(numpy.unique(self.data[feature]), kind="heapsort")
        
    def fastOrdinalSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        # find a threshold between two data points that maximizes objectiveMetric gain
        #      * without evaluating all points
        #      * that is deterministic
        #      * that's no worse than O(n log(n)) where n is the number of data points

        sortedValues = self.sortby(feature)
        if len(sortedValues) < 2:
            return None

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if self.data[feature].dtype == "d":
            sortedValues = (sortedValues[1:] + sortedValues[:-1])/2.
        else:
            sortedValues = sortedValues[:-1]

        f = {}  # cache values
        resphi = 2. - (1. + math.sqrt(5.))/2.
        def goldenSection(a, b, c):
            if c - b > b - a:
                x = int(round(b + resphi*(c - b)))
            else:
                x = int(round(b - resphi*(b - a)))
            if x in (a, b, c):
                if a not in f: f[a] = self.calculatePair((self.data[feature] <= sortedValues[a]))
                if b not in f: f[b] = self.calculatePair((self.data[feature] <= sortedValues[b]))
                if c not in f: f[c] = self.calculatePair((self.data[feature] <= sortedValues[c]))

                i = numpy.argmax([f[a], f[b], f[c]])
                return [a, b, c][i]

            if x not in f: f[x] = self.calculatePair((self.data[feature] <= sortedValues[x]))
            if b not in f: f[b] = self.calculatePair((self.data[feature] <= sortedValues[b]))

            if f[x] > f[b]:
                if c - b > b - a:
                    return goldenSection(b, x, c)
                else:
                    return goldenSection(a, x, b)
            else:
                if c - b > b - a:
                    return goldenSection(a, b, x)
                else:
                    return goldenSection(x, b, c)

        # all of the work starts here
        low = 0
        high = len(sortedValues) - 1
        mid = (low + high) / 2
        cut = goldenSection(low, mid, high)

        selection = (self.data[feature] <= sortedValues[cut])
        if cut not in f:
            f[cut] = self.calculatePair(selection)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = sortedValues[cut]

        return f[cut]

    def exhaustiveSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        sortedValues = self.sortby(feature)
        if len(sortedValues) < 2:
            return None

        # use midpoints so that the cut threshold is not exactly on any of the training data (for floating-point only)
        if self.data[feature].dtype == "d":
            sortedValues = (sortedValues[1:] + sortedValues[:-1])/2.
        else:
            sortedValues = sortedValues[:-1]

        bestOutput = None
        bestSelection = None
        bestCut = None

        for cut in sortedValues:
            selection = (self.data[feature] <= cut)
            output = self.calculatePair(selection)

            if bestOutput is None or output > bestOutput:
                bestOutput = output
                bestSelection = selection
                bestCut = cut

        self.selectionCache[feature, False] = bestSelection
        self.selectionCache[feature, True] = numpy.logical_not(bestSelection)
        self.selectionValue[feature] = bestCut

        return bestOutput

    def medianSplit(self, feature):
        if len(self.scoreDistributions) < 2:
            return None

        sortedValues = self.sortby(feature)
        if len(sortedValues) < 2:
            return None
        
        median = sortedValues[(len(sortedValues) - 1)/2 + 1]
        if self.data[feature].dtype == "d":
            median += 0.5 * (sortedValues[(len(sortedValues) - 1)/2 + 1] - median)

        selection = (self.data[feature] <= median)

        self.selectionCache[feature, False] = selection
        self.selectionCache[feature, True] = numpy.logical_not(selection)
        self.selectionValue[feature] = median

        return self.calculatePair(selection)

    def split(self, depthCounter, logger):
        if self.maxTreeDepth is not None and depthCounter >= self.maxTreeDepth:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "reached maximum tree depth of %d, stopping tree-building" % self.maxTreeDepth)
            return
        if len(self.features) == 0:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no feature are left to split, stopping tree-building")
            return

        if self.minRecordCount is not None and self.sdTotal < self.minRecordCount:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "node has only %d records, not enough to continue tree-building" % self.sdTotal)
            return

        if logger is not None:
            logger.debug(("    " * depthCounter) + "depth %d: determining best feature for split in a dataset with %d records" % (depthCounter, len(self.data[self.classifierField])))

        s = self.objectiveMetric()
        
        bestFeature = None
        self.bestGain = 0.
        for feature in self.features:
            gainterm = self.gainterm(feature)
            if gainterm is not None:
                gain = s + gainterm
                if bestFeature is None or gain > self.bestGain:
                    bestFeature = feature
                    self.bestGain = gain

        if bestFeature is None:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no features are granular enough to split, stopping tree-building")
            return

        if self.bestGain <= self.minObjective:
            if logger is not None:
                logger.debug(("    " * depthCounter) + "no split produces a gain greater than %g (best is %g), stopping tree-building" % (self.minObjective, self.bestGain))
            return

        if (bestFeature in self.featureSplitExceptions and self.featureSplitExceptions[bestFeature] == self.completeSplit) or \
           (self.categorical[bestFeature] and self.splitCategorical == self.completeSplit):
            subFeatures = list(set(self.features).difference(set([bestFeature])))
        else:
            subFeatures = self.features

        self.subNodes = []
        self.cutVar = []
        self.cutCat = []
        self.cutVal = []

        for feature, value in self.selectionCache.keys():
            if feature == bestFeature:
                subData = {}
                subData[self.classifierField] = self.data[self.classifierField][self.selectionCache[feature, value]]
                for f in subFeatures:
                    subData[f] = self.data[f][self.selectionCache[feature, value]]

                self.subNodes.append(Node(subFeatures, self.categorical, subData, self.classifierField))
                self.cutVar.append(feature)

                if self.data[feature].dtype == "b":
                    self.cutCat.append("true" if value else "false")
                    self.cutVal.append(None)

                else:
                    self.cutCat.append(value)
                    self.cutVal.append(self.selectionValue[feature])

        del self.selectionCache
        del self.selectionValue

        if logger is not None:
            logger.debug(("    " * depthCounter) + "feature \"%s\" will generate %d branches from this node (gain is %g)" % (bestFeature, len(self.subNodes), self.bestGain))

        for i, subNode in enumerate(self.subNodes):
            if logger is not None:
                logger.debug(("    " * depthCounter) + "recursing in branch %d out of %d" % (i+1, len(self.subNodes)))

            subNode.split(depthCounter + 1, logger)

    def tree(self, name):
        if self.subNodes is not None:
            extension = pmml.newInstance("Extension", attrib={"name": "gain", "value": self.bestGain, "extender": "ODG"})
            output = pmml.newInstance("Node", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children = [extension, self.pmmlTrue])
            output.predicateIndex = 1
            output.children.extend(self.scoreDistributions)

            for i, subNode, var, cat, val in zip(range(len(self.subNodes)), self.subNodes, self.cutVar, self.cutCat, self.cutVal):
                node = subNode.tree("%s-%d" % (name, i+1))

                if val is None:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "equal", "value": cat})

                elif isinstance(val, list):
                    predicate = pmml.newInstance("SimpleSetPredicate", attrib={"field": var, "booleanOperator": "isIn" if cat else "isNotIn"})
                    setarray = pmml.newInstance("Array", attrib={"type": "string", "n": len(val)})
                    setarray.value = val
                    predicate.children.append(setarray)

                elif self.categorical[var]:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "equal" if cat else "notEqual", "value": val})

                else:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "greaterThan" if cat else "lessOrEqual", "value": val})

                node[node.predicateIndex] = predicate
                output.children.append(node)

        else:
            output = pmml.newInstance("Node", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children=[self.pmmlTrue])
            output.predicateIndex = 0
            output.children.extend(self.scoreDistributions)

        return output

    def rule(self, name):
        if self.subNodes is not None:
            extension = pmml.newInstance("Extension", attrib={"name": "gain", "value": self.bestGain, "extender": "ODG"})
            output = pmml.newInstance("CompoundRule", children=[extension, self.pmmlTrue])
            output.predicateIndex = 1

            for i, subNode, var, cat, val in zip(range(len(self.subNodes)), self.subNodes, self.cutVar, self.cutCat, self.cutVal):
                node = subNode.rule("%s-%d" % (name, i+1))

                if val is None:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "equal", "value": cat})

                elif isinstance(val, list):
                    predicate = pmml.newInstance("SimpleSetPredicate", attrib={"field": var, "booleanOperator": "isIn" if cat else "isNotIn"})
                    setarray = pmml.newInstance("Array", attrib={"type": "string", "n": len(val)})
                    setarray.value = val
                    predicate.children.append(setarray)

                elif self.categorical[var]:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "equal" if cat else "notEqual", "value": val})

                else:
                    predicate = pmml.newInstance("SimplePredicate", attrib={"field": var, "operator": "greaterThan" if cat else "lessOrEqual", "value": val})

                node[node.predicateIndex] = predicate
                output.children.append(node)

        else:
            output = pmml.newInstance("SimpleRule", attrib={"score": self.score, "id": name, "recordCount": self.sdTotal}, children=[self.pmmlTrue])
            output.predicateIndex = 0
            output.children.extend(self.scoreDistributions)

        return output

class ProducerIterative(ProducerAlgorithm):
    """The standard tree-building algorithms: ID3, C45, and CART."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minObjective": "0.", "minRecordCount": "0", "objectiveMetric": "entropy", "splitOrdinal": "fast", "splitCategorical": "fast", "classifierField": "", "pruningDataFraction": "0.", "pruningObjective": "entropy", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        self.model = self.segmentRecord.pmmlModel

        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if self.updateExisting:
            raise NotImplementedError, "Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'"

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

        if "minObjective" in params:
            try:
                self.minObjective = float(params["minObjective"])
            except ValueError:
                self.minObjective = 0.
            del params["minObjective"]
        else:
            self.minObjective = float(self.defaultParams["minObjective"])

        if "minRecordCount" in params:
            try:
                self.minRecordCount = int(params["minRecordCount"])
            except ValueError:
                self.minRecordCount = 0
            del params["minRecordCount"]
        else:
            self.minRecordCount = int(self.defaultParams["minRecordCount"])

        if "objectiveMetric" in params:
            self.objectiveMetric = params["objectiveMetric"]
            del params["objectiveMetric"]
        else:
            self.objectiveMetric = self.defaultParams["objectiveMetric"]
        if self.objectiveMetric not in ("entropy", "gini"):
            raise NotImplementedError, "The only valid objectiveMetrics are ('entropy', 'gini')"

        if "splitOrdinal" in params:
            self.splitOrdinal = params["splitOrdinal"]
            del params["splitOrdinal"]
        else:
            self.splitOrdinal = self.defaultParams["splitOrdinal"]
        if self.splitOrdinal not in ("fast", "exhaustive", "median"):
            raise NotImplementedError, "The only valid splitOrdinal values are ('fast', 'exhaustive', 'median')"

        if "splitCategorical" in params:
            self.splitCategorical = params["splitCategorical"]
            del params["splitCategorical"]
        else:
            self.splitCategorical = self.defaultParams["splitCategorical"]
        if self.splitCategorical not in ("complete", "subset", "fast", "singleton"):
            raise NotImplementedError, "The only valid splitCategoricals are ('complete', 'subset', 'fast', 'singleton')"
        
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
            raise NotImplementedError, "Pruning has not yet been implemented; use truncation (maxTreeDepth, minObjective, minRecordCount) instead"

        if "pruningObjective" in params:
            self.pruningObjective = params["pruningObjective"]
            del params["pruningObjective"]
        else:
            self.pruningObjective = self.defaultParams["pruningObjective"]
        if self.pruningObjective not in ("entropy", "gini"):
            raise NotImplementedError, "The only valid pruningObjectives are ('entropy', 'gini')"

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

        self.features = []
        self.categorical = {}
        self.predicted = []
        self.data = {}
        self.featureSplitExceptions = {}
        for miningField in self.model.child(pmml.MiningSchema).matches(pmml.MiningField):
            name = miningField.attrib["name"]
            usageType = miningField.attrib.get("usageType", "active")
            if usageType == "active":
                dataType = self.model.dataContext.dataType[name]
                optype = self.model.dataContext.optype[name]

                self.features.append(name)
                self.categorical[name] = (optype == "categorical")

                if dataType == "boolean":
                    self.data[name] = array.array("b")
                elif dataType == "integer":
                    self.data[name] = array.array("l")
                elif dataType in ("float", "double"):
                    self.data[name] = array.array("d")
                else:
                    self.data[name] = []

                featureSplitName = "split_" + name
                if featureSplitName in params:
                    self.featureSplitExceptions[name] = params[featureSplitName]
                    del params[featureSplitName]

                    if optype == "categorical":
                        if self.featureSplitExceptions[name] == "complete":
                            self.featureSplitExceptions[name] = Node.completeSplit
                        elif self.featureSplitExceptions[name] == "subset":
                            self.featureSplitExceptions[name] = Node.subsetSplit
                        elif self.featureSplitExceptions[name] == "fast":
                            self.featureSplitExceptions[name] = Node.fastSubsetSplit
                        elif self.featureSplitExceptions[name] == "singleton":
                            self.featureSplitExceptions[name] = Node.singletonSplit
                        else:
                            raise NotImplementedError, "The only valid split methods for feature \"%s\", which is %s, are ('complete', 'subset', 'fast', 'singleton')" % (name, optype)

                    else:
                        if self.featureSplitExceptions[name] == "fast":
                            self.featureSplitExceptions[name] = Node.fastOrdinalSplit
                        elif self.featureSplitExceptions[name] == "exhaustive":
                            self.featureSplitExceptions[name] = Node.exhaustiveSplit
                        elif self.featureSplitExceptions[name] == "median":
                            self.featureSplitExceptions[name] = Node.medianSplit
                        else:
                            raise NotImplementedError, "The only valid split methods for feature \"%s\", which is %s, are ('fast', 'exhaustive', 'median')" % (name, optype)

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
                    raise RuntimeError, "ClassifierField feature not found among the 'predicted' features in the decision tree's MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()

        self.data[self.classifierField] = []

        if len(params) > 0:
            raise TypeError, "Unrecognized parameters %s" % params

    def update(self, syncNumber, get):
        self.resetLoggerLevels()

        if self.classifierField is INVALID:
            raise RuntimeError, "Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()
        
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
            if isinstance(self.data[name], array.array) and self.data[name].typecode == "b":
                self.data[name] = numpy.array(self.data[name], dtype="b")
            elif isinstance(self.data[name], array.array) and self.data[name].typecode == "l":
                self.data[name] = numpy.array(self.data[name], dtype="l")
            elif isinstance(self.data[name], array.array) and self.data[name].typecode == "d":
                self.data[name] = numpy.array(self.data[name], dtype="d")
            else:
                self.data[name] = numpy.array(self.data[name])

        Node.maxTreeDepth = self.maxTreeDepth
        Node.minObjective = self.minObjective
        Node.minRecordCount = self.minRecordCount

        if self.objectiveMetric == "entropy":
            Node.objectiveMetric = Node.entropyMetric
        elif self.objectiveMetric == "gini":
            Node.objectiveMetric = Node.giniMetric

        if self.splitOrdinal == "fast":
            Node.splitOrdinal = Node.fastOrdinalSplit
        elif self.splitOrdinal == "exhaustive":
            Node.splitOrdinal = Node.exhaustiveSplit
        elif self.splitOrdinal == "median":
            Node.splitOrdinal = Node.medianSplit

        if self.splitCategorical == "complete":
            Node.splitCategorical = Node.completeSplit
        elif self.splitCategorical == "subset":
            Node.splitCategorical = Node.subsetSplit
        elif self.splitCategorical == "fast":
            Node.splitCategorical = Node.fastSubsetSplit
        elif self.splitCategorical == "singleton":
            Node.splitCategorical = Node.singletonSplit

        Node.featureSplitExceptions = self.featureSplitExceptions

        logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG

        self.logger.debug("ProducerIterativeTree.produce: starting to build tree")
        node = Node(self.features, self.categorical, self.data, self.classifierField)
        depthCounter = 0
        node.split(depthCounter, self.logger if logDebug else None)
        self.logger.debug("ProducerIterativeTree.produce: finished building tree")

        if isinstance(self.model, pmml.TreeModel):
            self.model[self.nodeIndex] = node.tree("Node-1")

        elif isinstance(self.model, pmml.RuleSetModel):
            self.ruleSet[self.nodeIndex] = node.rule("Node-1")

class ProducerC45(ProducerIterative):
    """The special case of ProducerIterative known as C4.5 (ID3 is the sub-case of no continuous features)."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minObjective": "0.", "minRecordCount": "0", "fast": "false", "classifierField": "", "pruningDataFraction": "0.", "pruningObjective": "entropy", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        if "objectiveMetric" in params:
            raise TypeError, "Parameter 'objectiveMetric' cannot be set in algorithm 'c45' because it is always equal to \"entropy\""
        else:
            params["objectiveMetric"] = "entropy"

        if "fast" in params:
            self.fast = pmml.boolCheck(params["fast"])
            del params["fast"]
        else:
            self.fast = pmml.boolCheck(self.defaultParams["fast"])

        if "splitOrdinal" in params:
            raise TypeError, "Parameter 'splitOrdinal' cannot be set in algorithm 'c45' because it is always equal to \"exhaustive\" (or \"fast\" if you set fast to \"true\")"
        else:
            if self.fast:
                params["splitOrdinal"] = "fast"
            else:
                params["splitOrdinal"] = "exhaustive"

        if "splitCategorical" in params:
            raise TypeError, "Parameter 'splitCategorical' cannot be set in algorithm 'c45' because it is always equal to \"subset\" (or \"fast\" if you set fast to \"true\")"
        else:
            if self.fast:
                params["splitCategorical"] = "fast"
            else:
                params["splitCategorical"] = "subset"

        ProducerIterative.initialize(self, **params)

class ProducerCART(ProducerIterative):
    """The special case of ProducerIterative known as CART."""

    defaultParams = {"updateExisting": "false", "maxTreeDepth": "5", "minObjective": "0.", "minRecordCount": "0", "fast": "false", "classifierField": "", "pruningDataFraction": "0.", "pruningObjective": "entropy", "pruningThreshold": "0.2"}

    def initialize(self, **params):
        if "objectiveMetric" in params:
            raise TypeError, "Parameter 'objectiveMetric' cannot be set in algorithm 'cart' because it is always equal to \"gini\""
        else:
            params["objectiveMetric"] = "gini"

        if "fast" in params:
            self.fast = pmml.boolCheck(params["fast"])
            del params["fast"]
        else:
            self.fast = pmml.boolCheck(self.defaultParams["fast"])

        if "splitOrdinal" in params:
            raise TypeError, "Parameter 'splitOrdinal' cannot be set in algorithm 'cart' because it is always equal to \"exhaustive\" (or \"fast\" if you set fast to \"true\")"
        else:
            if self.fast:
                params["splitOrdinal"] = "fast"
            else:
                params["splitOrdinal"] = "exhaustive"

        if "splitCategorical" in params:
            raise TypeError, "Parameter 'splitCategorical' cannot be set in algorithm 'cart' because it is always equal to \"subset\" (or \"fast\" if you set fast to \"true\")"
        else:
            if self.fast:
                params["splitCategorical"] = "fast"
            else:
                params["splitCategorical"] = "subset"

        ProducerIterative.initialize(self, **params)
