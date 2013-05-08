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

"""This module defines the KMeans class."""

import random
import copy
import logging

from augustus.core.defs import defs
from augustus.strict import *
from augustus.core.NumpyInterface import NP
from augustus.core.FakePerformanceTable import FakePerformanceTable
from augustus.mapreduce.MapReduce import MapReduce
from augustus.producer.kmeans.MapReduceKMeans import MapReduceKMeans

class KMeans(object):
    """The KMeans class implements Lloyd's algorithm for k-means
    clustering.

    It is a stateful object: to run the algorithm, you create a KMeans
    object, run its random seed generator (C{randomSeeds}), optionally
    run its seed improver (C{smallTrials}), and then run the full
    algorithm locally (C{optimize}) or on Hadoop (C{hadoopOptimize}).

    Alternatively, there is a C{calc} method for running the whole
    process.
    """

    def __init__(self, clusterCenters, fieldNames, metric=None, compareFunction="absDiff", allChangeThreshold=1e-5, iterationLimit=1000):
        """Initialize a KMeans object by creating a blank PMML
        ClusteringModel.

        @type clusterCenters: int or list of strings
        @param clusterCenters: Number of cluters (k) or their names.
        @type fieldNames: list of strings
        @param fieldNames: Names of the fields to use for clustering.
        @type metric: PmmlClusteringMetric or None
        @param metric: The metric to use or None for SquaredEuclidean.
        @type compareFunction: string
        @param compareFunction: The comparison function to use.
        @type allChangeThreshold: number
        @param allChangeThreshold: The maximum change allowed for all cluster centers before stopping the k-means algorithm.
        @type iterationLimit: int
        @param iterationLimit: The maximum number of iterations allowed.
        """

        if isinstance(clusterCenters, (int, long)):
            clusterCenters = [repr(i + 1) for i in xrange(clusterCenters)]

        E = modelLoader.elementMaker()
        self.clusteringModel = E.ClusteringModel(functionName="clustering", modelClass="centerBased", numberOfClusters=repr(len(clusterCenters)))

        self.clusteringModel.append(E.Extension(name="producer", value="KMeans"))
        parametersMarkup = E.Extension(name="parameters")
        for param in "allChangeThreshold", "iterationLimit":
            value = eval(param)
            self.__dict__[param] = value
            parametersMarkup.append(E.Extension(name=param, value=repr(value)))
        self.clusteringModel.append(parametersMarkup)
        self.clusteringModel.append(E.Extension(name="iterations.smallTrials", value="0"))
        self.clusteringModel.append(E.Extension(name="iterations", value="0"))

        miningSchema = E.MiningSchema()
        for fieldName in fieldNames:
            miningSchema.append(E.MiningField(name=fieldName))
        self.clusteringModel.append(miningSchema)

        if metric is None:
            metric = E.squaredEuclidean

        comparisonMeasure = E.ComparisonMeasure(metric, kind="distance")
        self.clusteringModel.append(comparisonMeasure)
        
        for fieldName in fieldNames:
            self.clusteringModel.append(E.ClusteringField(field=fieldName, compareFunction=compareFunction))

        for clusterCenter in clusterCenters:
            cluster = E.Cluster(E.Array(" ".join(["0.0"] * len(fieldNames)), n=repr(len(fieldNames)), type="real"), id=clusterCenter, name=clusterCenter)
            self.clusteringModel.append(cluster)

        modelLoader.validate(self.clusteringModel)

    def calc(self, inputData, inputMask=None, performanceTable=None):
        """Build a DataTable from the input data and then run k-means
        clustering on it to produce a ClusteringModel.

        This method is intended for interactive use, since it is more
        laborious to construct a DataTable by hand.

        Modifies and returns C{self.clusteringModel}.

        @type inputData: dict
        @param inputData: Dictionary from field names to data, as required by the DataTable constructor.
        @type inputMask: dict or None
        @param inputMask: Dictionary from field names to missing value masks, as required by the DataTable constructor.
        @type inputState: DataTableState or None
        @param inputState: Calculation state, used to continue a calculation over many C{calc} calls.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: PmmlBinding
        @return: The PMML model representing the result of the k-means clustering.
        """

        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        performanceTable.begin("make DataTable")
        dataTable = DataTable(self.clusteringModel, inputData, inputMask, None)
        performanceTable.end("make DataTable")

        self.smallTrials(dataTable, performanceTable=performanceTable)

        self.optimize([dataTable], performanceTable=performanceTable)
        return self.clusteringModel

    def explicitSeeds(self, values):
        """Set explicit cluster center seeds.

        Modifies C{self.clusteringModel}.

        @type values: dict
        @param values: A mapping from cluster name (string) to vector (list of numbers).
        """

        for index, cluster in enumerate(self.clusteringModel.xpath("pmml:Cluster")):
            clusterName = cluster.get("id", "%d" % (index + 1))
            clusterVector = values[clusterName]
            cluster.childOfTag("Array").text = " ".join(map(repr, clusterVector))

    def randomSeeds(self, dataTable):
        """Randomly pick cluster center seeds from a dataset.

        Modifies C{self.clusteringModel}.

        @type dataTable: DataTable
        @param dataTable: The input data.
        """

        fieldNames = self.clusteringModel.xpath("pmml:ClusteringField/@field")
        clusterArrays = self.clusteringModel.xpath("pmml:Cluster/pmml:Array")
        numberOfClusters = len(clusterArrays)

        indexes = random.sample(xrange(len(dataTable)), numberOfClusters)
        for i, index in enumerate(indexes):
            clusterVector = [dataTable.fields[fieldName].value(index) for fieldName in fieldNames]
            clusterArrays[i].text = " ".join(map(repr, clusterVector))

    def smallTrials(self, dataTable, numberOfTrials=5, recordsPerTrial=100, performanceTable=None):
        """Improve the initial seed with a few small trials on random subsets of the data.

        Modifies C{self.clusteringModel}.

        @type dataTable: DataTable
        @param dataTable: The input data.
        @type numberOfTrials: int
        @param numberOfTrials: The number of independent trials with the same number of C{recordsPerTrial}.  The trial with the smallest sum of in-cluster variances wins.
        @type recordsPerTrial: int
        @param recordsPerTrial: The number of rows to randomly select from the DataTable in each trial.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        """

        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        performanceTable.begin("smallTrials")

        mapReduce = self.mapReduce()
        
        self.KMeansMapReduceApplication.metadata["ClusteringModel"] = copy.deepcopy(self.KMeansMapReduceApplication.metadata["ClusteringModel"])

        bestVariance = None
        bestSeed = None
        for trialNumber in xrange(numberOfTrials):
            indexes = random.sample(xrange(len(dataTable)), recordsPerTrial)
            subTable = dataTable.subTable(NP("array", indexes, dtype=NP.dtype(int)))

            self.randomSeeds(dataTable)
            mapReduce.metadata["ClusteringModel"] = self.clusteringModel

            outputRecords, outputKeyValues, numberOfIterations = mapReduce.run([subTable], parallel=False, frozenClass=False, numberOfMappers=1, numberOfReducers=1, iterationLimit=self.iterationLimit)

            for extension in self.clusteringModel.xpath("pmml:Extension[@name='iterations.smallTrials']"):
                extension["value"] = repr(int(extension["value"]) + numberOfIterations)

            mapReduce.metadata["ClusteringModel"]["modelName"] = "smallTrials"
            mapReduce.metadata["ClusteringModel"].subFields = dict(mapReduce.metadata["ClusteringModel"].subFields)
            mapReduce.metadata["ClusteringModel"].subFields.update({"affinity": True})
            mapReduce.metadata["ClusteringModel"].calculate(subTable)

            data = subTable.fields["smallTrials.affinity"].data
            mask = subTable.fields["smallTrials.affinity"].mask
            if mask is None:
                variance = NP(data**2).sum() / float(len(subTable))
            else:
                selection = NP(mask == defs.VALID)
                denom = NP("count_nonzero", selection)
                if denom > 0:
                    variance = NP(data[selection]**2).sum() / float(denom)
                else:
                    variance = None
            if variance is not None and (bestVariance is None or variance < bestVariance):
                bestVariance = variance
                bestSeed = mapReduce.metadata["clusterVectors"]

        if bestSeed is not None:
            self.explicitSeeds(bestSeed)

        performanceTable.end("smallTrials")

    def optimize(self, dataTables, numberOfMappers=1, numberOfReducers=1, performanceTable=None):
        """Attempt to optimize the current set of clusters with
        Lloyd's algorithm (k-means clustering).

        Modifies C{self.clusteringModel}.

        Behind the scenes, the algorithm is run in a pure Python
        map-reduce framework.  If C{numberOfMappers} or
        C{numberOfReducers} is greater than 1, the algorithm will be
        parallelized with threads.  Splitting the data among multiple
        mappers requires a list of DataTables, rather than a single
        DataTable.
        
        @type dataTables: list of DataTables
        @param dataTables: The input data.
        @type numberOfMappers: int
        @param numberOfMappers: Requested number of mappers.  Input data will be divided evenly among them.
        @type numberOfReducers: int
        @param numberOfReducers: Requested number of reducers.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        """

        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        mapReduce = self.mapReduce()
        outputRecords, outputKeyValues, numberOfIterations = mapReduce.run(dataTables, parallel=(numberOfMappers > 1 or numberOfReducers > 1), frozenClass=False, numberOfMappers=numberOfMappers, numberOfReducers=numberOfReducers, iterationLimit=self.iterationLimit, performanceTable=PerformanceTable())

        parent = self.clusteringModel.getparent()
        if parent is not None:
            index = parent.index(self.clusteringModel)
            parent[index] = mapReduce.metadata["ClusteringModel"]

        self.clusteringModel = mapReduce.metadata["ClusteringModel"]
        for extension in self.clusteringModel.xpath("pmml:Extension[@name='iterations']"):
            extension["value"] = repr(int(extension["value"]) + numberOfIterations)

        performanceTable.absorb(mapReduce.performanceTable)

    def hadoopOptimize(self, inputHdfsDirectory, outputHdfsDirectory, numberOfReducers=1, cmdenv=None, loggingLevel=logging.WARNING, overwrite=True, verbose=True):
        """A variant of the C{optimize} command that runs on Hadoop.

        Modifies C{self.clusteringModel}.

        @type inputHdfsDirectory: string
        @param inputHdfsDirectory: The name of the HDFS directory to use as input.  It should contain SequenceFiles generated by C{MapReduce.hadoopPopulate}.
        @type outputHdfsDirectory: string
        @param outputHdfsDirectory: The name of the HDFS directory to use as output.  If it exists and C{overwrite} is True, it will be overwritten.
        @type numberOfReducers: int
        @param numberOfReducers: Desired number of reducers.
        @type cmdenv: dict or None
        @param cmdenv: Environment variables to pass to the mapper and reducer processes.
        @type loggingLevel: logging level
        @param loggingLevel: The level of log output that will go to Hadoop's standard error.
        @type overwrite: bool
        @param overwrite: If C{outputHdfsDirectory} exists and this is True, the contents will be overwritten.
        @type verbose: bool
        @param verbose: If True, let Hadoop print its output to C{sys.stdout}.
        @raise IOError: If any I/O related error occurs, this function raises an error.
        """

        mapReduce = self.mapReduce()
        outputRecords, outputKeyValues, numberOfIterations = mapReduce.hadoopRun(inputHdfsDirectory, outputHdfsDirectory, iterationLimit=self.iterationLimit, numberOfReducers=numberOfReducers, cmdenv=cmdenv, loggingLevel=loggingLevel, overwrite=overwrite, verbose=verbose)

        parent = self.clusteringModel.getparent()
        if parent is not None:
            index = parent.index(self.clusteringModel)
            parent[index] = mapReduce.metadata["ClusteringModel"]

        self.clusteringModel = mapReduce.metadata["ClusteringModel"]

        for extension in self.clusteringModel.xpath("pmml:Extension[@name='iterations']"):
            extension["value"] = repr(int(extension["value"]) + numberOfIterations)

    def mapReduce(self):
        """Build a MapReduce-Ready K-means producer.

        Used by C{optimize} and C{hadoopOptimize}.

        @rtype: MapReduce
        @return: An instance of MapReduce that can either be run in pure-Python mode or submitted to Hadoop.
        """

        class KMeansMapReduceApplication(MapReduceKMeans):
            metadata = {}
            allChangeThreshold = self.allChangeThreshold

        KMeansMapReduceApplication.metadata["ClusteringModel"] = self.clusteringModel

        clusterVectors = {}
        for index, cluster in enumerate(self.clusteringModel.xpath("pmml:Cluster")):
            clusterName = cluster.get("id", "%d" % (index + 1))
            clusterVectors[clusterName] = NP("array", cluster.childOfTag("Array").values(), dtype=NP.dtype(float))
        KMeansMapReduceApplication.metadata["clusterVectors"] = clusterVectors

        self.KMeansMapReduceApplication = KMeansMapReduceApplication

        return MapReduce(KMeansMapReduceApplication)
