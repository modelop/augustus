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

"""This module defines the TreeModel class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlModel import PmmlModel
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataColumn import DataColumn
from augustus.pmml.model.trees.Node import Node

class TreeModel(PmmlModel):
    """TreeModel implements decision and regression tree models in
    PMML, which choose an outcome based on a set of predicates.

    U{PMML specification<http://www.dmg.org/v4-1/TreeModel.html>}.

    @type subFields: dict
    @param subFields: To globally turn on the calculation of "entity", "entityId", "confidence", or "probability", set C{subFields["XXX"]} to True.
    """

    subFields = {"entity": False, "entityId": False, "confidence": False, "probability": False}

    def calculateScore(self, dataTable, functionTable, performanceTable):
        """Calculate the score of this model.

        This method is called by C{calculate} to separate operations
        that are performed by all models (in C{calculate}) from
        operations that are performed by specific models (in
        C{calculateScore}).

        @type subTable: DataTable
        @param subTable: The DataTable representing this model's lexical scope.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataColumn
        @return: A DataColumn containing the score.
        """

        performanceTable.begin("TreeModel")

        performanceTable.begin("set up")

        missingValueStrategy = self.get("missingValueStrategy", defaultFromXsd=True)
        if missingValueStrategy == "lastPrediction":
            missingValueStrategy = Node.LAST_PREDICTION
        elif missingValueStrategy == "nullPrediction":
            missingValueStrategy = Node.NULL_PREDICTION
        elif missingValueStrategy == "defaultChild":
            missingValueStrategy = Node.DEFAULT_CHILD
        elif missingValueStrategy == "weightedConfidence":
            missingValueStrategy = Node.WEIGHTED_CONFIDENCE
        elif missingValueStrategy == "aggregateNodes":
            missingValueStrategy = Node.AGGREGATE_NODES
        elif missingValueStrategy == "none":
            missingValueStrategy = Node.NONE

        missingValuePenalty = self.get("missingValuePenalty", defaultFromXsd=True, convertType=True)

        noTrueChildStrategy = self.get("noTrueChildStrategy", defaultFromXsd=True)
        if noTrueChildStrategy == "returnNullPredication":
            noTrueChildStrategy = Node.RETURN_NULL_PREDICTION
        elif noTrueChildStrategy == "returnLastPrediction":
            noTrueChildStrategy = Node.RETURN_LAST_PREDICTION

        if self["functionName"] == "classification":
            fieldType = FakeFieldType("string", "categorical")
        elif self["functionName"] == "regression":
            fieldType = FakeFieldType("double", "continuous")
        else:
            raise defs.PmmlValidationError("TreeModel functionName may only be \"classification\" or \"regression\", not \"%s\"" % self["functionName"])

        performanceTable.end("set up")

        score = {None: DataColumn(fieldType, NP("empty", len(dataTable), dtype=fieldType.dtype), NP("ones", len(dataTable), dtype=defs.maskType))}
        score[None]._unlock()

        if self.subFields["entity"]:
            fieldType = FakeFieldType("object", "any")
            score["entity"] = DataColumn(fieldType, NP("empty", len(dataTable), dtype=fieldType.dtype), NP("ones", len(dataTable), dtype=defs.maskType))
            score["entity"]._unlock()

        if self.subFields["entityId"]:
            fieldType = FakeFieldType("string", "categorical")
            score["entityId"] = DataColumn(fieldType, NP("empty", len(dataTable), dtype=fieldType.dtype), NP("ones", len(dataTable), dtype=defs.maskType))
            score["entityId"]._unlock()

        if self.subFields["confidence"]:
            fieldType = FakeFieldType("double", "continuous")
            score["confidence"] = DataColumn(fieldType, NP("empty", len(dataTable), dtype=fieldType.dtype), NP("ones", len(dataTable), dtype=defs.maskType))
            score["confidence"]._unlock()

            fieldType = FakeFieldType("double", "continuous")
            score["penaltyProduct"] = DataColumn(fieldType, NP("ones", len(dataTable), dtype=fieldType.dtype), None)
            score["penaltyProduct"]._unlock()

        if self.subFields["probability"]:
            fieldType = FakeFieldType("double", "continuous")
            score["probability"] = DataColumn(fieldType, NP("empty", len(dataTable), dtype=fieldType.dtype), NP("ones", len(dataTable), dtype=defs.maskType))
            score["probability"]._unlock()

        node = self.childOfClass(Node)
        selection = node.evaluatePredicate(dataTable, functionTable, performanceTable, returnUnknowns=False)
        node.applyScore(dataTable, functionTable, performanceTable, selection, score, missingValueStrategy, missingValuePenalty, noTrueChildStrategy)

        if "confidence" in score:
            score["confidence"]._data *= score["penaltyProduct"].data
            del score["penaltyProduct"]

        for field in score.values():
            if not field.mask.any():
                field._mask = None
            else:
                field._mask *= defs.INVALID
            field._lock()

        performanceTable.end("TreeModel")
        return score
