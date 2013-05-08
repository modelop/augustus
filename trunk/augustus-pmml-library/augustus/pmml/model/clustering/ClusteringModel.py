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

"""This module defines the ClusteringModel class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlModel import PmmlModel
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataColumn import DataColumn
from augustus.core.PmmlArray import PmmlArray

from augustus.pmml.model.clustering.ComparisonMeasure import ComparisonMeasure
from augustus.pmml.model.clustering.ClusteringField import ClusteringField
from augustus.pmml.model.clustering.PmmlClusteringMetric import PmmlClusteringMetric
from augustus.pmml.model.clustering.PmmlClusteringMetricBinary import PmmlClusteringMetricBinary

class ClusteringModel(PmmlModel):
    """ClusteringModel implements cluster models in PMML, which map
    regions of a vector space to the nearest cluster center.

    U{PMML specification<http://www.dmg.org/v4-1/ClusteringModel.html>}.

    @type subFields: dict
    @param subFields: To globally turn on the calculation of "predictedDisplayValue", "entity", "clusterId", "entityId", "clusterAffinity", "affinity", or "all", set C{subFields["XXX"]} to True.
    @type propagateInvalid: bool
    @param propagateInvalid: To globally turn on propagation of INVALID fields to INVALID scores, set this to True.  Otherwise, bad data are handled by missing value weights.
    """

    subFields = {"predictedDisplayValue": False, "entity": False, "clusterId": False, "entityId": False, "clusterAffinity": False, "affinity": False, "all": False}

    class _State(object):
        pass

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

        performanceTable.begin("ClusteringModel")

        performanceTable.begin("set up")

        distributionBased = (self["modelClass"] == "distributionBased")
        clusteringFields = self.xpath("pmml:ClusteringField[not(@isCenterField='false')]")
        fieldWeights = [clusteringField.get("fieldWeight", defaultFromXsd=True, convertType=True) for clusteringField in clusteringFields]
        for fieldWeight in fieldWeights:
            if fieldWeight < 0.0:
                raise defs.PmmlValidationError("ClusteringField fieldWeights must all be non-negative (encountered %g)" % fieldWeight)
        clusters = self.xpath("pmml:Cluster")
        comparisonMeasure = self.childOfClass(ComparisonMeasure)
        defaultCompareFunction = comparisonMeasure.get("compareFunction", defaultFromXsd=True)
        metric = comparisonMeasure.childOfClass(PmmlClusteringMetric)
        metrictag = metric.t

        performanceTable.end("set up")

        for clusteringField in clusteringFields:
            dataType = dataTable.fields[clusteringField["field"]].fieldType.dataType
            if dataType == "string":
                raise defs.PmmlValidationError("ClusteringField \"%s\" has dataType \"%s\", which cannot be used for clustering" % (clusteringField["field"], dataType))

        missingValueWeights = self.childOfTag("MissingValueWeights")
        if missingValueWeights is None:
            adjustM = None

        else:
            performanceTable.begin("MissingValueWeights")

            missingWeights = missingValueWeights.childOfClass(PmmlArray).values(convertType=True)

            sumNMqi = NP("zeros", len(dataTable), dtype=NP.dtype(float))
            for clusteringField, missingWeight in zip(clusteringFields, missingWeights):
                clusteringField.addToAdjustM(dataTable, functionTable, performanceTable, sumNMqi, missingWeight)

            adjustM = NP(sum(missingWeights) / sumNMqi)
            adjustM[NP(sumNMqi == 0.0)] = 1.0

            performanceTable.end("MissingValueWeights")

        anyInvalid = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
        for clusteringField in clusteringFields:
            mask = dataTable.fields[clusteringField["field"]].mask
            if mask is not None:
                NP("logical_or", anyInvalid, NP(mask == defs.INVALID), anyInvalid)

        bestClusterId = None
        bestClusterAffinity = None
        allClusterAffinities = {}

        for index, cluster in enumerate(clusters):
            array = cluster.childOfClass(PmmlArray)
            if array is None:
                raise defs.PmmlValidationError("Cluster must have an array to designate its center")

            centerStrings = array.values(convertType=False)
            if len(centerStrings) != len(clusteringFields):
                raise defs.PmmlValidationError("Cluster array has %d components, but there are %d ClusteringFields with isCenterField=true" % (len(centerStrings), len(clusteringFields)))

            performanceTable.begin(metrictag)

            if distributionBased:
                matrix = cluster.xpath("pmml:Covariances/pmml:Matrix")
                if len(matrix) != 1:
                    raise defs.PmmlValidationError("In distribution-based clustering, all clusters must have a Covariances/Matrix")
                try:
                    covarianceMatrix = NP("array", matrix[0].values(), dtype=NP.dtype(float))
                except ValueError:
                    raise defs.PmmlValidationError("Covariances/Matrix must contain real numbers for distribution-based clustering")

            else:
                covarianceMatrix = None

            state = self._State()
            metric.initialize(state, len(dataTable), len(clusteringFields), distributionBased)

            for clusteringField, centerString, fieldWeight in zip(clusteringFields, centerStrings, fieldWeights):
                if isinstance(metric, PmmlClusteringMetricBinary):
                    metric.accumulateBinary(state, dataTable.fields[clusteringField["field"]], centerString, distributionBased)
                else:
                    performanceTable.pause(metrictag)
                    cxy = clusteringField.compare(dataTable, functionTable, performanceTable, centerString, defaultCompareFunction, anyInvalid)
                    performanceTable.unpause(metrictag)
                    metric.accumulate(state, cxy, fieldWeight, distributionBased)

            distance = metric.finalizeDistance(state, adjustM, distributionBased, covarianceMatrix)
            del state

            performanceTable.end(metrictag)

            if index == 0:
                bestClusterId = NP("ones", len(dataTable), dtype=NP.dtype(int))   # 1-based index
                bestClusterAffinity = distance

            better = NP(distance < bestClusterAffinity)
            bestClusterId[better] = index + 1   # 1-based index
            bestClusterAffinity[better] = distance[better]

            allClusterAffinities[cluster.get("id", "%d" % (index + 1))] = distance

        if not anyInvalid.any():
            scoreMask = None
        else:
            scoreMask = NP(anyInvalid * defs.INVALID)

        performanceTable.begin("set scores")
        score = {}

        performanceTable.begin("predictedValue")
        fieldType = FakeFieldType("string", "categorical")
        clusterIdentifiers = NP("empty", len(dataTable), dtype=fieldType.dtype)
        for index, cluster in enumerate(clusters):
            value = fieldType.stringToValue(cluster.get("id", "%d" % (index + 1)))
            clusterIdentifiers[NP(bestClusterId == (index + 1))] = value
        score[None] = DataColumn(fieldType, clusterIdentifiers, scoreMask)
        performanceTable.end("predictedValue")

        if self.subFields["predictedDisplayValue"]:
            performanceTable.begin("predictedDisplayValue")
            fieldType = FakeFieldType("string", "categorical")
            clusterNames = NP("empty", len(dataTable), dtype=fieldType.dtype)
            for index, cluster in enumerate(clusters):
                value = fieldType.stringToValue(cluster.get("name", ""))
                clusterNames[NP(bestClusterId == (index + 1))] = value
            score["predictedDisplayValue"] = DataColumn(fieldType, clusterNames, scoreMask)
            performanceTable.end("predictedDisplayValue")

        if self.subFields["entity"]:
            performanceTable.begin("entity")
            fieldType = FakeFieldType("object", "any")
            entities = NP("empty", len(dataTable), dtype=fieldType.dtype)
            for index, cluster in enumerate(clusters):
                value = fieldType.stringToValue(cluster.get("name", ""))
                indexPlusOne = index + 1
                for i in xrange(len(entities)):
                    if bestClusterId[i] == indexPlusOne:
                        entities[i] = cluster
            score["entity"] = DataColumn(fieldType, entities, scoreMask)
            performanceTable.end("entity")

        if self.subFields["clusterId"]:
            performanceTable.begin("clusterId")
            fieldType = FakeFieldType("integer", "continuous")
            score["clusterId"] = DataColumn(fieldType, bestClusterId, scoreMask)
            performanceTable.end("clusterId")

        if self.subFields["entityId"]:
            performanceTable.begin("entityId")
            fieldType = FakeFieldType("integer", "continuous")
            score["entityId"] = DataColumn(fieldType, bestClusterId, scoreMask)
            performanceTable.end("entityId")

        if self.subFields["clusterAffinity"]:
            performanceTable.begin("clusterAffinity")
            fieldType = FakeFieldType("double", "continuous")
            score["clusterAffinity"] = DataColumn(fieldType, bestClusterAffinity, scoreMask)
            performanceTable.end("clusterAffinity")

        if self.subFields["affinity"]:
            performanceTable.begin("affinity")
            fieldType = FakeFieldType("double", "continuous")
            score["affinity"] = DataColumn(fieldType, bestClusterAffinity, scoreMask)
            performanceTable.end("affinity")

        if self.subFields["all"]:
            performanceTable.begin("all")
            fieldType = FakeFieldType("double", "continuous")
            for identifier, distance in allClusterAffinities.items():
                score["all.%s" % identifier] = DataColumn(fieldType, distance, scoreMask)
            performanceTable.end("all")

        performanceTable.end("set scores")
        performanceTable.end("ClusteringModel")
        return score
