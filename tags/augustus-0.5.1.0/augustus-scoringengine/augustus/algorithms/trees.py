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
import array
import numpy

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

    defaultParams = {"updateExisting": "false", "featureMaturityThreshold": "10", "splitMaturityThreshold": "30", "trialsToKeep": "50", "worldsToSplit": "3", "treeDepth": "3", "classification": ""}

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

        if "classification" in params:
            self.classification = params["classification"]
            del params["classification"]
        else:
            self.classification = self.defaultParams["classification"]
        if self.classification == "": self.classification = None

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

        if len(params) > 0:
            raise TypeError, "Unrecognized parameters %s" % params

    def update(self, syncNumber, get):
        if self.classification is INVALID:
            raise RuntimeError, "Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()

        values = [get(feature.name) for feature in self.features]
        if INVALID in values or MISSING in values: return False

        classification = get(self.classification)
        if classification is INVALID or classification is MISSING: return False

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
##################################################################### ID3 producer ######
#########################################################################################

class Node:
    pmmlTrue = pmml.pmmlTrue()

    def __init__(self, features, categorical, data, classification):
        self.features = features
        self.categorical = categorical
        self.data = data
        self.classification = classification
        
        self.cvalues = numpy.unique(data[classification])
        bestCount = 0
        self.score = None
        for cvalue in self.cvalues:
            selection = (data[classification] == cvalue)
            ccount = sum(selection)
            if self.score is None or ccount > bestCount:
                self.score = cvalue
                bestCount = ccount
            
        self.selectionCache = {}
        self.selectionValue = {}

        self.subNodes = None

    def entropy(self, selection=None):
        if selection is None:
            classification = self.data[self.classification]
        else:
            classification = self.data[self.classification][selection]

        output = 0.
        for cvalue in self.cvalues:
            try:
                frac = float(sum(classification == cvalue))/float(len(classification))
            except ZeroDivisionError:
                continue
            try:
                output -= frac*math.log(frac, 2)
            except ValueError:
                pass
        return output

    def fraction(self, selection):
        try:
            return float(sum(selection))/float(len(self.data[self.classification]))
        except ZeroDivisionError:
            return 0.

    def gainterm(self, feature):
        if self.categorical[feature]:
            # categorical features are easy: calculate the entropy gain for each possible value
            # (we assume that the number of possible values is much smaller than the number of data points)

            values = numpy.unique(self.data[feature])
            if len(values) < 2:
                return None

            output = 0.
            for value in values:
                selection = (self.data[feature] == value)
                self.selectionCache[feature, value] = selection
                self.selectionValue[feature] = None

                output -= self.fraction(selection) * self.entropy(selection)
            return output

        else:
            # continuous features are hard: there could be as many unique values as there are values
            # in the set of data points (n), and there are n - 1 ways to put a threshold between them

            # we want to generate a new categorical feature from the best (highest gain) threshold
            # this new feature has two values: lessOrEqual ("False") and greaterThan ("True")

            # calculates the entropy gain, O(n)
            def calculate(x):
                output = 0.

                selection = (self.data[feature] <= x)
                fraction = self.fraction(selection)
                output -= fraction * self.entropy(selection)

                selection = numpy.logical_not(selection)
                fraction = 1. - fraction
                output -= fraction * self.entropy(selection)

                return output

            # find a threshold between two data points that maximizes entropy gain
            #      * without evaluating all points
            #      * that is deterministic
            #      * that's no worse than O(n log(n)) where n is the number of data points

            if self.data[feature].dtype == numpy.object:
                sortedValues = numpy.sort(numpy.unique(self.data[feature]), kind="quicksort")
            else:
                sortedValues = numpy.sort(numpy.unique(self.data[feature]), kind="heapsort")

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
                    if a not in f: f[a] = calculate(sortedValues[a])
                    if b not in f: f[b] = calculate(sortedValues[b])
                    if c not in f: f[c] = calculate(sortedValues[c])

                    i = numpy.argmax([f[a], f[b], f[c]])
                    return [a, b, c][i]
                
                if x not in f: f[x] = calculate(sortedValues[x])
                if b not in f: f[b] = calculate(sortedValues[b])

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
            if cut not in f:
                f[cut] = calculate(sortedValues[cut])

            selection = (self.data[feature] <= sortedValues[cut])
            self.selectionCache[feature, False] = selection
            self.selectionCache[feature, True] = numpy.logical_not(selection)
            self.selectionValue[feature] = sortedValues[cut]

            return f[cut]

    def split(self, depthRemaining):
        if depthRemaining == 0: return
        if len(self.features) == 0: return

        s = self.entropy()

        bestFeature = None
        self.bestGain = 0.
        for feature in self.features:
            gainterm = self.gainterm(feature)
            if gainterm is not None:
                gain = s + gainterm
                if bestFeature is None or gain > self.bestGain:
                    bestFeature = feature
                    self.bestGain = gain

        if bestFeature is None: return
        if self.bestGain == 0.: return

        if self.categorical[bestFeature]:
            subFeatures = list(set(self.features).difference(set([bestFeature])))
        else:
            subFeatures = self.features

        self.subNodes = []
        self.cutVar = []
        self.cutCat = []
        self.cutVal = []

        for (feature, value) in self.selectionCache.keys():
            if feature == bestFeature:
                subData = {}
                subData[self.classification] = self.data[self.classification][self.selectionCache[feature, value]]
                for f in subFeatures:
                    subData[f] = self.data[f][self.selectionCache[feature, value]]

                self.subNodes.append(Node(subFeatures, self.categorical, subData, self.classification))
                self.cutVar.append(feature)

                if self.data[feature].dtype == "b":
                    self.cutCat.append("true" if value else "false")
                    self.cutVal.append(None)

                else:
                    self.cutCat.append(value)
                    self.cutVal.append(self.selectionValue[feature])

        del self.selectionCache
        del self.selectionValue

        for subNode in self.subNodes:
            subNode.split(depthRemaining - 1)

    def tree(self):
        if self.subNodes is not None:
            extension = new.instance(pmml.Extension)
            extension.tag = "Extension"
            extension.attrib = {"name": "gain", "value": self.bestGain}
            extension.children = []

            output = new.instance(pmml.Node)
            output.tag = "Node"
            output.attrib = {"score": self.score}
            output.children = [extension, self.pmmlTrue]
            output.predicateIndex = 1

            for subNode, var, cat, val in zip(self.subNodes, self.cutVar, self.cutCat, self.cutVal):
                node = subNode.tree()

                predicate = new.instance(pmml.SimplePredicate)
                predicate.tag = "SimplePredicate"
                if val is None:
                    predicate.attrib = {"field": var, "operator": "equal", "value": cat}
                else:
                    predicate.attrib = {"field": var, "operator": "greaterThan" if cat else "lessOrEqual", "value": val}
                predicate.children = []

                node[node.predicateIndex] = predicate

                output.children.append(node)

        else:
            output = new.instance(pmml.Node)
            output.tag = "Node"
            output.attrib = {"score": self.score}
            output.children = [self.pmmlTrue]
            output.predicateIndex = 0

        return output

    def rule(self):
        if self.subNodes is not None:
            extension = new.instance(pmml.Extension)
            extension.tag = "Extension"
            extension.attrib = {"name": "gain", "value": self.bestGain}
            extension.children = []

            output = new.instance(pmml.CompoundRule)
            output.tag = "CompoundRule"
            output.attrib = {}
            output.children = [extension, self.pmmlTrue]
            output.predicateIndex = 1

            for subNode, var, cat, val in zip(self.subNodes, self.cutVar, self.cutCat, self.cutVal):
                node = subNode.rule()

                predicate = new.instance(pmml.SimplePredicate)
                predicate.tag = "SimplePredicate"
                if val is None:
                    predicate.attrib = {"field": var, "operator": "equal", "value": cat}
                else:
                    predicate.attrib = {"field": var, "operator": "greaterThan" if cat else "lessOrEqual", "value": val}
                predicate.children = []

                node[node.predicateIndex] = predicate

                output.children.append(node)
        else:
            output = new.instance(pmml.SimpleRule)
            output.tag = "SimpleRule"
            output.attrib = {"score": self.score}
            output.children = [self.pmmlTrue]
            output.predicateIndex = 0

        return output

class ProducerID3(ProducerAlgorithm):
    """The standard ID3 tree-building algorithm."""

    defaultParams = {"updateExisting": "false", "treeMaxDepth": "3", "classification": ""}

    def initialize(self, **params):
        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if self.updateExisting:
            raise NotImplementedError, "Updating from existing TreeModels/RuleSetModels not implemented; use mode='replaceExisting'"

        if "treeMaxDepth" in params:
            self.treeMaxDepth = int(params["treeMaxDepth"])
            del params["treeMaxDepth"]
        else:
            self.treeMaxDepth = int(self.defaultParams["treeMaxDepth"])

        if "classification" in params:
            self.classification = params["classification"]
            del params["classification"]
        else:
            self.classification = self.defaultParams["classification"]
        if self.classification == "": self.classification = None

        self.model = self.segmentRecord.pmmlModel

        if isinstance(self.model, pmml.TreeModel):
            self.nodeIndex = self.model.index(pmml.Node)
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

        self.data[self.classification] = []

        if len(params) > 0:
            raise TypeError, "Unrecognized parameters %s" % params

    def update(self, syncNumber, get):
        if self.classification is INVALID:
            raise RuntimeError, "Cannot produce a decision tree with no 'predicted' features in the MiningSchema%s" % self.model.child(pmml.MiningSchema).fileAndLine()
        
        values = [get(feature) for feature in self.features]
        if INVALID in values or MISSING in values: return False

        classification = get(self.classification)
        if classification is INVALID or classification is MISSING: return False

        for value, feature in zip(values, self.features):
            self.data[feature].append(value)
        self.data[self.classification].append(classification)

        return True

    def produce(self):
        for name in self.data:
            if isinstance(self.data[name], array.array) and self.data[name].typecode == "b":
                self.data[name] = numpy.array(self.data[name], dtype="b")
            elif isinstance(self.data[name], array.array) and self.data[name].typecode == "l":
                self.data[name] = numpy.array(self.data[name], dtype="l")
            elif isinstance(self.data[name], array.array) and self.data[name].typecode == "d":
                self.data[name] = numpy.array(self.data[name], dtype="d")
            else:
                self.data[name] = numpy.array(self.data[name])

        node = Node(self.features, self.categorical, self.data, self.classification)
        node.split(self.treeMaxDepth)

        if isinstance(self.model, pmml.TreeModel):
            self.model[self.nodeIndex] = node.tree()
            self.model.attrib["splitCharacteristic"] = "multiSplit"

        elif isinstance(self.model, pmml.RuleSetModel):
            self.ruleSet[self.nodeIndex] = node.rule()
