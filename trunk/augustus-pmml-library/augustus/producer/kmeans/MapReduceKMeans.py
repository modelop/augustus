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

"""This module defines the MapReduceKMeans class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.mapreduce.MapReduceApplication import MapReduceApplication

class MapReduceKMeans(MapReduceApplication):
    """MapReduceKMeans is a MapReduceApplication that implements
    distributed k-means.
    """

    loggerName = "Augustus.MapReduceKMeans"
    gatherOutput = True
    chain = False
    imports = {"math": None,
               "augustus.core.NumpyInterface": ["NP"]}

    def beginMapperTask(self):
        self.fieldNames = map(str, self.metadata["ClusteringModel"].xpath("pmml:ClusteringField/@field"))
        self.clusters = self.metadata["ClusteringModel"].xpath("pmml:Cluster")

    def mapper(self, dataTable):
        dataTable = dataTable.subTable()  # ensure that the results of this calculation do not get propagated

        self.metadata["ClusteringModel"].calculate(dataTable, performanceTable=self.performanceTable)

        data = dataTable.score.data
        mask = dataTable.score.mask
        stringToValue = dataTable.score.fieldType.stringToValue
        for index, cluster in enumerate(self.clusters):
            clusterName = cluster.get("id", "%d" % (index + 1))
            value = stringToValue(clusterName)

            selection = NP(data == value)
            if mask is not None:
                NP("logical_and", selection, NP(mask == defs.VALID), selection)

            denominator = selection.sum()

            numer = dict((fieldName, 0.0) for fieldName in self.fieldNames)
            denom = dict((fieldName, 0.0) for fieldName in self.fieldNames)

            for fieldName in self.fieldNames:
                numer[fieldName] += dataTable.fields[fieldName].data[selection].sum()
                denom[fieldName] += denominator

            self.emit(clusterName, {"numer": numer, "denom": denom})

    def beginReducerTask(self):
        self.fieldNames = self.metadata["ClusteringModel"].xpath("pmml:ClusteringField/@field")
        self.clusterVectors = self.metadata["clusterVectors"]

    def beginReducerKey(self, key):
        self.numer = dict((fieldName, 0.0) for fieldName in self.fieldNames)
        self.denom = dict((fieldName, 0.0) for fieldName in self.fieldNames)

    def reducer(self, key, record):
        numer = record["numer"]
        denom = record["denom"]
        for fieldName in self.fieldNames:
            self.numer[fieldName] += numer[fieldName]
            self.denom[fieldName] += denom[fieldName]
        self.logger.debug("Cluster \"%s\", adding partial numer %r denom %r", key, numer, denom)

    def endReducerKey(self, key):
        for clusterName in self.clusterVectors.keys():
            if clusterName == key:
                newPosition = NP("array", [self.numer[fieldName] / self.denom[fieldName] if self.denom[fieldName] > 0.0 else 0.0 for fieldName in self.fieldNames], dtype=NP.dtype(float))

                self.emit(clusterName, newPosition)
                break

    def endIteration(self, outputRecords, outputKeyValues):
        for index, cluster in enumerate(self.metadata["ClusteringModel"].xpath("pmml:Cluster")):
            clusterName = cluster.get("id", "%d" % (index + 1))
            self.logger.info("Cluster \"%s\" new position: %r", clusterName, outputKeyValues[clusterName])

        biggestDifference = 0.0
        for index, cluster in enumerate(self.metadata["ClusteringModel"].xpath("pmml:Cluster")):
            clusterName = cluster.get("id", "%d" % (index + 1))

            difference = outputKeyValues[clusterName] - self.metadata["clusterVectors"][clusterName]
            self.logger.info("Cluster \"%s\" difference: %r", clusterName, difference)

            self.metadata["clusterVectors"][clusterName] = outputKeyValues[clusterName]
            cluster.childOfTag("Array").text = " ".join(map(repr, self.metadata["clusterVectors"][clusterName]))

            scalarDifference = math.sqrt(NP(difference**2).sum())
            if scalarDifference > biggestDifference:
                biggestDifference = scalarDifference

        if biggestDifference <= self.allChangeThreshold:
            return True
        else:
            return False
