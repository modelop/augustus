#!/usr/bin/env python

# Copyright (C) 2006-2013  Open Data ("Open Data" refers to
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

"""This module defines the Node class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlPredicate import PmmlPredicate
from augustus.core.DataColumn import DataColumn

class Node(PmmlBinding):
    """Node implements a a Node in a TreeModel.

    The missingValueStrategy and noTrueChildStrategy are represented
    as ad-hoc singleton objects in this class.

      - C{LAST_PREDICTION}
      - C{NULL_PREDICTION}
      - C{DEFAULT_CHILD}
      - C{WEIGHTED_CONFIDENCE}
      - C{AGGREGATE_NODES}
      - C{NONE}
      - C{RETURN_NULL_PREDICTION}
      - C{RETURN_LAST_PREDICTION}

    U{PMML specification<http://www.dmg.org/v4-1/TreeModel.html>}.
    """

    # missingValueStrategy
    LAST_PREDICTION = object()
    NULL_PREDICTION = object()
    DEFAULT_CHILD = object()
    WEIGHTED_CONFIDENCE = object()
    AGGREGATE_NODES = object()
    NONE = object()

    # noTrueChildStrategy
    RETURN_NULL_PREDICTION = object()
    RETURN_LAST_PREDICTION = object()

    def evaluatePredicate(self, dataTable, functionTable, performanceTable, returnUnknowns):
        """Evaluate this Node's PREDICATE (PmmlPredicate) to produce a selection.

        @type dataTable: DataTable
        @param dataTable: A DataTable containing all rows that match this node in the tree and those above it.
        @type functionTable: FunctionTable
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type returnUnknowns: bool
        @param returnUnknowns: If True, return a 3-tuple of selection, unknowns, and encounteredUnknowns; otherwise, return just the selection.
        @rtype: 1d Numpy array of bool or 3-tuple of 1d Numpy arrays of bool
        @return: The selected rows or the selection, unknowns, and encounteredUnknowns, as returned by the PREDICATE.
        """

        return self.childOfClass(PmmlPredicate).evaluate(dataTable, functionTable, performanceTable, returnUnknowns)

    def applyScoreLeaf(self, selection, score, performanceTable):
        """Walk on a leaf, applying the score to the rows of the DataTable that remain.
        
        @type selection: 1d Numpy array of bool
        @param selection: The rows selected by this leaf.
        @type score: dict
        @param score: A dictionary that maps PMML score "features" to DataColumns.  The None key is "predictedValue" and is the only one guaranteed to exist.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        """

        performanceTable.begin("set scores")

        bestRecordCount = None
        bestScoreDistribution = None
        for scoreDistribution in self.childrenOfTag("ScoreDistribution"):
            recordCount = int(scoreDistribution["recordCount"])
            if bestRecordCount is None or recordCount > bestRecordCount:
                bestRecordCount = recordCount
                bestScoreDistribution = scoreDistribution

        scoreValue = self.get("score")

        if bestScoreDistribution is not None:
            if scoreValue is None:
                scoreValue = bestScoreDistribution["value"]

            confidence = bestScoreDistribution.get("confidence")
            if confidence is not None and "confidence" in score:
                performanceTable.begin("confidence")
                data = score["confidence"].data
                mask = score["confidence"].mask
                data[selection] = float(confidence)
                NP("logical_and", mask, NP("logical_not", selection), mask)
                performanceTable.end("confidence")

            probability = bestScoreDistribution.get("probability")
            if probability is not None and "probability" in score:
                performanceTable.begin("probability")
                data = score["probability"].data
                mask = score["probability"].mask
                data[selection] = float(probability)
                NP("logical_and", mask, NP("logical_not", selection), mask)
                performanceTable.end("probability")

        if scoreValue is not None:
            performanceTable.begin("predictedValue")
            data = score[None].data
            mask = score[None].mask
            data[selection] = score[None].fieldType.stringToValue(scoreValue)
            NP("logical_and", mask, NP("logical_not", selection), mask)
            performanceTable.end("predictedValue")

        if "entity" in score:
            performanceTable.begin("entity")
            data = score["entity"].data
            mask = score["entity"].mask
            for index in xrange(len(data)):
                if selection[index]:
                    data[index] = self
            NP("logical_and", mask, NP("logical_not", selection), mask)
            performanceTable.end("entity")

        entityId = self.get("id")
        if entityId is not None and "entityId" in score:
            performanceTable.begin("entityId")
            data = score["entityId"].data
            mask = score["entityId"].mask
            entityId = score["entityId"].fieldType.stringToValue(entityId)
            data[selection] = entityId
            NP("logical_and", mask, NP("logical_not", selection), mask)
            performanceTable.end("entityId")

        performanceTable.end("set scores")

    def applyScore(self, dataTable, functionTable, performanceTable, selection, score, missingValueStrategy, missingValuePenalty, noTrueChildStrategy):
        """Walk through the tree by one Node, splitting the DataTable
        on the way down and merging it on the way back up.
        
        @type dataTable: DataTable
        @param dataTable: A DataTable containing all rows that match this node in the tree and those above it.
        @type functionTable: FunctionTable
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type selection: 1d Numpy array of bool
        @param selection: The rows in this DataTable that match this Node.
        @type score: dict
        @param score: A dictionary that maps PMML score "features" to DataColumns.  The None key is "predictedValue" and is the only one guaranteed to exist.
        @type missingValueStrategy: singleton Python object, defined in the Node class
        @param missingValueStrategy: The tree's global missing value strategy.
        @type missingValuePenalty: number
        @param missingValuePenalty: The tree's global missing value penalty.
        @type noTrueChildStrategy: singleton Python object, defined in the Node class
        @param noTrueChildStrategy: The tree's global no-true-child strategy.
        """

        if not selection.any():
            return

        subNodes = self.childrenOfClass(Node)
        if len(subNodes) == 0:
            self.applyScoreLeaf(selection, score, performanceTable)

        else:
            performanceTable.begin("split downward")

            subTable = dataTable.subTable(selection)
            subScore = {}
            for name, field in score.items():
                if field.mask is None:
                    subScore[name] = DataColumn(field.fieldType, field.data[selection], None)
                else:
                    subScore[name] = DataColumn(field.fieldType, field.data[selection], field.mask[selection])
                subScore[name]._unlock()

            unset = NP("ones", len(subTable), dtype=NP.dtype(bool))

            performanceTable.end("split downward")

            for subNode in subNodes:
                subSelection, subUnknowns, subEncounteredUnknowns = subNode.evaluatePredicate(subTable, functionTable, performanceTable, returnUnknowns=True)

                performanceTable.begin("logical_and")
                NP("logical_and", subSelection, unset, subSelection)
                NP("logical_and", subSelection, NP("logical_not", subUnknowns), subSelection)
                NP("logical_and", subUnknowns, unset, subUnknowns)
                NP("logical_and", subEncounteredUnknowns, unset, subEncounteredUnknowns)
                NP("logical_and", unset, NP("logical_not", subSelection), unset)
                performanceTable.end("logical_and")

                subNode.applyScore(subTable, functionTable, performanceTable, subSelection, subScore, missingValueStrategy, missingValuePenalty, noTrueChildStrategy)

                if "penaltyProduct" in subScore:
                    subScore["penaltyProduct"].data[subEncounteredUnknowns] *= missingValuePenalty
                
                if subUnknowns.any():
                    if missingValueStrategy is self.LAST_PREDICTION:
                        self.applyScoreLeaf(subUnknowns, subScore, performanceTable)
                        NP("logical_and", unset, NP("logical_not", subUnknowns), unset)

                    elif missingValueStrategy is self.NULL_PREDICTION:
                        NP("logical_and", unset, NP("logical_not", subUnknowns), unset)

                    elif missingValueStrategy is self.DEFAULT_CHILD:
                        defaultChild = self.xpath("@defaultChild")
                        if len(defaultChild) == 0:
                            raise defs.PmmlValidationError("When missingValueStrategy is \"defaultChild\", every non-leaf node must have a defaultChild attribute")
                        defaultChild = defaultChild[0]

                        defaultNode = self.xpath("pmml:Node[@id='%s']" % defaultChild)
                        if len(defaultNode) == 0:
                            raise defs.PmmlValidationError("The defaultChild \"%s\" is not found (no such id at this level)" % defaultChild)
                        defaultNode = defaultNode[0]

                        NP("logical_and", unset, NP("logical_not", subUnknowns), unset)
                        defaultNode.applyScore(subTable, functionTable, performanceTable, subUnknowns, subScore, missingValueStrategy, missingValuePenalty, noTrueChildStrategy)

                    elif missingValueStrategy is self.WEIGHTED_CONFIDENCE:
                        # this involves evaluating an ensemble of subtrees and choosing among them: too hard
                        raise NotImplementedError("missingValueStrategy=\"weightedConfidence\"")

                    elif missingValueStrategy is self.AGGREGATE_NODES:
                        # this involves evaluating an ensemble of subtrees and agregating over them: too hard
                        raise NotImplementedError("missingValueStrategy=\"aggregateNodes\"")

                    elif missingValueStrategy is self.NONE:
                        pass

                if not unset.any():
                    break

            if noTrueChildStrategy is self.RETURN_LAST_PREDICTION and unset.any():
                self.applyScoreLeaf(unset, subScore, performanceTable)

            performanceTable.begin("merge upward")

            for name, field in score.items():
                field.data[selection] = subScore[name].data
                if field.mask is not None:
                    field.mask[selection] = subScore[name].mask

            performanceTable.end("merge upward")
