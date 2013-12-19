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

"""This module defines the MiningModel class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlModel import PmmlModel
from augustus.core.PmmlPredicate import PmmlPredicate
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataTable import DataTable
from augustus.core.DataColumn import DataColumn

class MiningModel(PmmlModel):
    """MiningModel implements segmentation, the application of a large
    pool of models to a dataset, with models selected for individual
    data records by the data's features.

    U{PMML specification<http://www.dmg.org/v4-1/MultipleModels.html>}.
    """

    scoreType = FakeFieldType("object", "any")
    scoreTypeSegment = FakeFieldType("object", "any")
    scoreTypeCardinality = FakeFieldType("integer", "continuous")

    SELECT_ALL = object()
    MEDIAN = object()

    SUM = object()
    AVERAGE = object()
    WEIGHTED_AVERAGE = object()

    MAJORITY_VOTE = object()
    WEIGHTED_MAJORITY_VOTE = object()

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

        segmentation = self.childOfTag("Segmentation")
        if segmentation is None:
            return dataTable

        multipleModelMethod = segmentation.get("multipleModelMethod")

        if multipleModelMethod == "selectAll":
            return self._selectAllMedianMajority(dataTable, functionTable, performanceTable, segmentation, self.SELECT_ALL)

        if multipleModelMethod == "median":
            return self._selectAllMedianMajority(dataTable, functionTable, performanceTable, segmentation, self.MEDIAN)

        elif multipleModelMethod == "majorityVote":
            return self._selectAllMedianMajority(dataTable, functionTable, performanceTable, segmentation, self.MAJORITY_VOTE)

        elif multipleModelMethod == "weightedMajorityVote":
            return self._selectAllMedianMajority(dataTable, functionTable, performanceTable, segmentation, self.WEIGHTED_MAJORITY_VOTE)

        elif multipleModelMethod == "selectFirst":
            return self._selectFirst(dataTable, functionTable, performanceTable, segmentation)

        elif multipleModelMethod == "sum":
            return self._sumAverageWeighted(dataTable, functionTable, performanceTable, segmentation, self.SUM)

        elif multipleModelMethod == "average":
            return self._sumAverageWeighted(dataTable, functionTable, performanceTable, segmentation, self.AVERAGE)

        elif multipleModelMethod == "weightedAverage":
            return self._sumAverageWeighted(dataTable, functionTable, performanceTable, segmentation, self.WEIGHTED_AVERAGE)

        elif multipleModelMethod == "max":
            return self._selectMax(dataTable, functionTable, performanceTable, segmentation)

        else:
            raise NotImplementedError("multipleModelMethod \"%s\" has not been implemented" % multipleModelMethod)

    def _selectAllMedianMajority(self, dataTable, functionTable, performanceTable, segmentation, which):
        """Used by C{calculateScore}."""

        if which is self.SELECT_ALL:
            performanceLabel = "Segmentation selectAll"
        elif which is self.MEDIAN:
            performanceLabel = "Segmentation median"
        elif which is self.MAJORITY_VOTE:
            performanceLabel = "Segmentation majorityVote"
        elif which is self.WEIGHTED_MAJORITY_VOTE:
            performanceLabel = "Segmentation weightedMajorityVote"
        performanceTable.begin(performanceLabel)

        scores = [[] for x in xrange(len(dataTable))]
        if which is self.SELECT_ALL:
            segments = [[] for x in xrange(len(dataTable))]

        newOutputData = {}
        for segment in segmentation.childrenOfTag("Segment", iterator=True):
            performanceTable.pause(performanceLabel)
            selection = segment.childOfClass(PmmlPredicate).evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause(performanceLabel)
            if not selection.any():
                continue

            segmentName = segment.get("id")
            indexes = NP("nonzero", selection)[0]

            subTable = dataTable.subTable(selection)
            subModel = segment.childOfClass(PmmlModel)

            performanceTable.pause(performanceLabel)
            subModel.calculate(subTable, functionTable, performanceTable)
            performanceTable.unpause(performanceLabel)

            if which is self.MEDIAN and subTable.score.fieldType.dataType in ("string", "boolean", "object"):
                raise defs.PmmlValidationError("Segmentation with multipleModelMethod=\"median\" cannot be applied to models that produce dataType \"%s\"" % subTable.score.fieldType.dataType)

            scoreData = subTable.score.data
            scoreMask = subTable.score.mask
            indexesUsed = indexes
            if which is self.SELECT_ALL:
                for subIndex, index in enumerate(indexes):
                    if scoreMask is None or scoreMask[subIndex] == defs.VALID:
                        scores[index].append(scoreData[subIndex])
                        segments[index].append(segmentName)

            elif which is self.MEDIAN:
                for subIndex, index in enumerate(indexes):
                    if scoreMask is None or scoreMask[subIndex] == defs.VALID:
                        scores[index].append(scoreData[subIndex])

            elif which in (self.MAJORITY_VOTE, self.WEIGHTED_MAJORITY_VOTE):
                if which is self.MAJORITY_VOTE:
                    weight = 1.0
                else:
                    weight = float(segment.get("weight", 1.0))
                for subIndex, index in enumerate(indexes):
                    if scoreMask is None or scoreMask[subIndex] == defs.VALID:
                        newValue = scoreData[subIndex]
                        score = scores[index]
                        found = False
                        for pair in score:
                            if pair[0] == newValue:
                                pair[1] += weight
                                found = True
                                break
                        if not found:
                            score.append([newValue, weight])

            if which is self.SELECT_ALL:
                for fieldName, dataColumn in subTable.output.items():
                    newData = newOutputData.get(fieldName)
                    if newData is None:
                        newData = [[] for x in xrange(len(dataTable))]
                        newOutputData[fieldName] = newData

                    dataColumnData = dataColumn.data
                    dataColumnMask = dataColumn.mask
                    for subIndex, index in enumerate(indexes):
                        if scoreMask is None or scoreMask[subIndex] == defs.VALID:
                            if dataColumnMask is None or dataColumnMask[subIndex] == defs.VALID:
                                newData[index].append(dataColumnData[subIndex])
                            else:
                                newData[index].append(None)

        if which is self.SELECT_ALL:
            for fieldName, newData in newOutputData.items():
                finalNewData = NP("empty", len(dataTable), dtype=NP.dtype(object))
                for index, newDatum in enumerate(newData):
                    finalNewData[index] = tuple(newDatum)
                dataTable.output[fieldName] = DataColumn(self.scoreType, finalNewData, None)

            finalScoresData = NP("empty", len(dataTable), dtype=NP.dtype(object))
            for index, score in enumerate(scores):
                finalScoresData[index] = tuple(score)
            finalScores = DataColumn(self.scoreType, finalScoresData, None)

            if self.name is None:
                performanceTable.end(performanceLabel)
                return {None: finalScores}
            else:
                finalSegmentsData = NP("empty", len(dataTable), dtype=NP.dtype(object))
                for index, segment in enumerate(segments):
                    finalSegmentsData[index] = tuple(segment)

                performanceTable.end(performanceLabel)
                return {None: finalScores, "segment": DataColumn(self.scoreTypeSegment, finalSegmentsData, None)}

        elif which is self.MEDIAN:
            finalScoresData = NP("empty", len(dataTable), dtype=NP.dtype(object))
            finalScoresMask = NP("empty", len(dataTable), dtype=defs.maskType)
            for index, score in enumerate(scores):
                if len(score) > 0:
                    finalScoresData[index] = NP("median", score)
                    finalScoresMask[index] = defs.VALID
                else:
                    finalScoresMask[index] = defs.INVALID

            if not finalScoresMask.any():
                finalScoresMask = None
            finalScores = DataColumn(self.scoreType, finalScoresData, finalScoresMask)

            performanceTable.end(performanceLabel)
            return {None: finalScores}

        elif which in (self.MAJORITY_VOTE, self.WEIGHTED_MAJORITY_VOTE):
            finalScoresData = NP("empty", len(dataTable), dtype=NP.dtype(object))
            finalScoresMask = NP("empty", len(dataTable), dtype=defs.maskType)
            cardinality = NP("empty", len(dataTable), dtype=self.scoreTypeCardinality.dtype)

            for index, score in enumerate(scores):
                bestN, bestValue = None, None
                for value, N in score:
                    if bestN is None or N > bestN:
                        bestN = N
                        bestValue = value
                if bestN is not None:
                    finalScoresData[index] = bestValue
                    finalScoresMask[index] = defs.VALID
                    cardinality[index] = bestN
                else:
                    finalScoresMask[index] = defs.INVALID
                    cardinality[index] = 0

            if not finalScoresMask.any():
                finalScoresMask = None
            finalScores = DataColumn(self.scoreType, finalScoresData, finalScoresMask)

            if self.name is None:
                performanceTable.end(performanceLabel)
                return {None: finalScores}
            else:
                finalCardinality = DataColumn(self.scoreTypeCardinality, cardinality, None)

                performanceTable.end(performanceLabel)
                return {None: finalScores, "cardinality": finalCardinality}

    def _selectFirst(self, dataTable, functionTable, performanceTable, segmentation):
        """Used by C{calculateScore}."""

        performanceTable.begin("Segmentation selectFirst")

        scoresData = NP("empty", len(dataTable), dtype=NP.dtype(object))
        scoresMask = NP("zeros", len(dataTable), dtype=defs.maskType)
        unfilled = NP("ones", len(dataTable), dtype=NP.dtype(bool))
        segments = NP("empty", len(dataTable), dtype=NP.dtype(object))

        newOutputData = []
        for segment in segmentation.childrenOfTag("Segment", iterator=True):
            performanceTable.pause("Segmentation selectFirst")
            selection = segment.childOfClass(PmmlPredicate).evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("Segmentation selectFirst")
            NP("logical_and", selection, unfilled, selection)
            if not selection.any():
                continue

            subTable = dataTable.subTable(selection)
            subModel = segment.childOfClass(PmmlModel)
            performanceTable.pause("Segmentation selectFirst")

            subModel.calculate(subTable, functionTable, performanceTable)
            performanceTable.unpause("Segmentation selectFirst")

            scoresData[selection] = subTable.score.data
            if subTable.score.mask is not None:
                scoresMask[selection] = subTable.score.mask
            else:
                scoresMask[selection] = defs.VALID

            segmentName = segment.get("id")
            if segmentName is not None:
                segments[selection] = segmentName

            for fieldName, dataColumn in subTable.output.items():
                if fieldName not in dataTable.output:
                    data = NP("empty", len(dataTable), dtype=dataColumn.fieldType.dtype)
                    data[selection] = dataColumn.data

                    mask = NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.MISSING)
                    if dataColumn.mask is None:
                        mask[selection] = defs.VALID
                    else:
                        mask[selection] = dataColumn.mask

                    newDataColumn = DataColumn(dataColumn.fieldType, data, mask)
                    newDataColumn._unlock()
                    dataTable.output[fieldName] = newDataColumn
                    newOutputData.append(newDataColumn)

                else:
                    newDataColumn = dataTable.output[fieldName]

                    newDataColumn.data[selection] = dataColumn.data
                    if dataColumn.mask is None:
                        newDataColumn.mask[selection] = defs.VALID
                    else:
                        newDataColumn.mask[selection] = dataColumn.mask

            unfilled -= selection
            if not unfilled.any():
                break

        for newDataColumn in newOutputData:
            if not newDataColumn.mask.any():
                newDataColumn._mask = None
            newDataColumn._lock()

        if not scoresMask.any():
            scoresMask = None

        scores = DataColumn(self.scoreType, scoresData, scoresMask)

        if self.name is None:
            performanceTable.end("Segmentation selectFirst")
            return {None: scores}
        else:
            performanceTable.end("Segmentation selectFirst")
            return {None: scores, "segment": DataColumn(self.scoreTypeSegment, segments, None)}

    def _sumAverageWeighted(self, dataTable, functionTable, performanceTable, segmentation, which):
        """Used by C{calculateScore}."""

        if which is self.SUM:
            performanceLabel = "Segmentation sum"
        elif which is self.AVERAGE:
            performanceLabel = "Segmentation average"
        elif which is self.WEIGHTED_AVERAGE:
            performanceLabel = "Segmentation weightedAverage"
        performanceTable.begin(performanceLabel)

        scoresData = NP("zeros", len(dataTable), dtype=NP.dtype(object))
        if which is not self.SUM:
            denominator = NP("zeros", len(dataTable), dtype=NP.dtype(float))
        invalid = NP("zeros", len(dataTable), dtype=NP.dtype(bool))

        for segment in segmentation.childrenOfTag("Segment", iterator=True):
            performanceTable.pause(performanceLabel)
            selection = segment.childOfClass(PmmlPredicate).evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause(performanceLabel)
            if not selection.any():
                continue
            
            subTable = dataTable.subTable(selection)
            subModel = segment.childOfClass(PmmlModel)
            performanceTable.pause(performanceLabel)
            subModel.calculate(subTable, functionTable, performanceTable)
            performanceTable.unpause(performanceLabel)

            if subTable.score.fieldType.dataType in ("string", "boolean", "object"):
                raise defs.PmmlValidationError("Segmentation with multipleModelMethod=\"%s\" cannot be applied to models that produce dataType \"%s\"" % (self.childOfTag("Segmentation").get("multipleModelMethod"), subTable.score.fieldType.dataType))

            # ignore invalid in matches (like the built-in "+" and "avg" Apply functions)
            if subTable.score.mask is not None:
                NP("logical_and", selection, NP(subTable.score.mask == defs.VALID), selection)

            if which is self.SUM:
                scoresData[selection] += subTable.score.data
            if which is self.AVERAGE:
                scoresData[selection] += subTable.score.data
                denominator[selection] += 1.0
            elif which is self.WEIGHTED_AVERAGE:
                weight = float(segment.get("weight", 1.0))
                scoresData[selection] += (subTable.score.data * weight)
                denominator[selection] += weight

            if subTable.score.mask is not None:
                invalid[selection] = NP("logical_or", invalid[selection], NP(subTable.score.mask != defs.VALID))

        if which is not self.SUM:
            NP("logical_or", invalid, NP(denominator == 0.0), invalid)
            valid = NP("logical_not", invalid)
            scoresData[valid] /= denominator[valid]

        if invalid.any():
            scoresMask = NP(NP("array", invalid, dtype=defs.maskType) * defs.INVALID)
        else:
            scoresMask = None
        
        scores = DataColumn(self.scoreType, scoresData, scoresMask)

        performanceTable.end(performanceLabel)
        return {None: scores}

    def _selectMax(self, dataTable, functionTable, performanceTable, segmentation):
        """Used by C{calculateScore}."""

        performanceTable.begin("Segmentation max")

        scoresData = NP("empty", len(dataTable), dtype=NP.dtype(object))
        filled = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
        unfilled = NP("ones", len(dataTable), dtype=NP.dtype(bool))

        newOutputData = []
        for segment in segmentation.childrenOfTag("Segment", iterator=True):
            performanceTable.pause("Segmentation max")
            selection = segment.childOfClass(PmmlPredicate).evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("Segmentation max")
            if not selection.any():
                continue
            
            subTable = dataTable.subTable(selection)
            subModel = segment.childOfClass(PmmlModel)
            performanceTable.pause("Segmentation max")
            subModel.calculate(subTable, functionTable, performanceTable)
            performanceTable.unpause("Segmentation max")

            if subTable.score.fieldType.dataType in ("string", "boolean", "object"):
                raise defs.PmmlValidationError("Segmentation with multipleModelMethod=\"max\" cannot be applied to models that produce dataType \"%s\"" % subTable.score.fieldType.dataType)

            # ignore invalid in matches (like the built-in "min" Apply function)
            if subTable.score.mask is not None:
                NP("logical_and", selection, NP(subTable.score.mask == defs.VALID), selection)

            selectionFilled = NP("logical_and", selection, filled)
            selectionUnfilled = NP("logical_and", selection, unfilled)
            filled_selection = filled[selection]
            unfilled_selection = unfilled[selection]

            left, right = subTable.score.data[filled_selection], scoresData[selectionFilled]
            condition = NP(left > right)
            scoresData[selectionFilled] = NP("where", condition, left, right)
            scoresData[selectionUnfilled] = subTable.score.data[unfilled_selection]

            for fieldName, dataColumn in subTable.output.items():
                if fieldName not in dataTable.output:
                    data = NP("empty", len(dataTable), dtype=dataColumn.fieldType.dtype)
                    data[selectionUnfilled] = dataColumn.data

                    mask = NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.MISSING)
                    if dataColumn.mask is None:
                        mask[selectionUnfilled] = defs.VALID
                    else:
                        mask[selectionUnfilled] = dataColumn.mask

                    newDataColumn = DataColumn(dataColumn.fieldType, data, mask)
                    newDataColumn._unlock()
                    dataTable.output[fieldName] = newDataColumn
                    newOutputData.append(newDataColumn)

                else:
                    newDataColumn = dataTable.output[fieldName]

                    newDataColumn.data[selectionFilled] = NP("where", condition, dataColumn.data[filled_selection], newDataColumn.data[selectionFilled])
                    newDataColumn.data[selectionUnfilled] = dataColumn.data[unfilled_selection]

                    if dataColumn.mask is None:
                        newDataColumn.mask[selectionUnfilled] = defs.VALID
                    else:
                        newDataColumn.mask[selectionUnfilled] = dataColumn.mask

            filled += selectionUnfilled
            unfilled -= selectionUnfilled

        for newDataColumn in newOutputData:
            if not newDataColumn.mask.any():
                newDataColumn._mask = None
            newDataColumn._lock()
            
        if filled.all():
            scoresMask = None
        else:
            scoresMask = NP(NP("logical_not", filled) * defs.MISSING)
        
        scores = DataColumn(self.scoreType, scoresData, scoresMask)

        performanceTable.end("Segmentation max")
        return {None: scores}
