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

"""Defines the k-means clustering producer and consumer algorithms."""

# system includes
import math
import numpy
import random

# local includes
from augustus.core.defs import Atom, INVALID, MISSING
from augustus.algorithms.defs import ConsumerAlgorithm, ProducerAlgorithm
from augustus.algorithms.eventweighting import COUNT, SUM1, SUMX, SUMXX, COVARIANCE

import augustus.core.pmml41 as pmml

SCORE_predictedValue = pmml.OutputField.predictedValue
SCORE_clusterId = pmml.OutputField.clusterId
SCORE_clusterAffinity = pmml.OutputField.clusterAffinity
SCORE_entityId = pmml.OutputField.entityId
SCORE_affinity = pmml.OutputField.affinity

#########################################################################################
######################################################################### consumer ######
#########################################################################################

class ConsumerClusteringModel(ConsumerAlgorithm):
    def initialize(self):
        """Initialize a clustering model consumer."""

        self.model = self.segmentRecord.pmmlModel

    def score(self, syncNumber, get):
        """Score one event with the clustering model, returning a scores dictionary."""

        self.resetLoggerLevels()

        vector = [get(field) for field in self.model.fields]
        if INVALID in vector:
            self.lastScore = INVALID
            self.logger.debug("ClusteringModel.score: returning INVALID score")
            return self.lastScore
        
        result = self.model.closestCluster(vector)
        if result is INVALID:
            self.lastScore = INVALID
            self.logger.debug("ClusteringModel.score: returning INVALID score")
            return self.lastScore

        clusterId, clusterNumber, clusterAffinity = result
        self.lastScore = {SCORE_predictedValue: clusterId,
                          SCORE_clusterId: clusterNumber, SCORE_entityId: clusterNumber,
                          SCORE_clusterAffinity: clusterAffinity, SCORE_affinity: clusterAffinity,
                          }
        return self.lastScore

#########################################################################################
######################################################################### producer ######
#########################################################################################

class TrialClusterSet(object):
    """A set of cluster centroids that aspires to be the solution to
    the clustering problem.

    Only called by ProducerClusteringModel.
    """

    def __init__(self, numberOfClusters, randomization, updateScheme):
        self.updator = updateScheme.updator(COUNT, SUM1, SUMX, SUMXX)
        self.clusters = [TrialCluster(randomization, updateScheme) for i in xrange(numberOfClusters)]
        self.rethrowPattern = [False] * numberOfClusters

    def __repr__(self):
        return "<TrialClusterSet %d %g>" % (self.updator.count(), self.updator.mean())

    def __str__(self):
        return " ".join(map(repr, self.clusters))

    def rethrowInvalid(self, randomization, updateScheme):
        rethrowPattern = [self.clusters[i].anyInvalid() for i in xrange(len(self.clusters))]
        if (True in rethrowPattern) and (rethrowPattern == self.rethrowPattern):
            rethrowPattern = [True] * len(self.clusters)

        for i in xrange(len(self.clusters)):
            if rethrowPattern[i]:
                self.clusters[i] = TrialCluster(randomization, updateScheme)

        self.rethrowPattern = rethrowPattern

    def update(self, syncNumber, vector, model, moving, weight):
        anyMissing = MISSING in vector

        # find the cluster to which this vector of data is closest
        shortestDistance = None
        closestCluster = None
        for cluster in self.clusters:
            if moving:
                clusterVector = cluster.vector()
            else:
                clusterVector = cluster.initialPosition

            if anyMissing:
                distance = model.metric.metricMissing(vector, clusterVector)
            else:
                distance = model.metric.metric(vector, clusterVector)

            if distance is INVALID: return False

            if shortestDistance is None or distance < shortestDistance:
                shortestDistance = distance
                closestCluster = cluster

        # XSD requires at least one cluster, so shortestDistance and closestCluster cannot be None

        # the Lamarckian step: clusters approach the center of the data points in its realm
        # (giraffes stretch their necks to reach the high leaves)
        if not anyMissing:
            closestCluster.increment(syncNumber, vector, weight)

        # quality of this cluster set as a whole
        if weight is None:
            self.updator.increment(syncNumber, shortestDistance)
        else:
            self.updator.counters[COUNT] += 1
            self.updator.counters[SUM1] += weight
            self.updator.counters[SUMX] += weight * shortestDistance
            self.updator.counters[SUMXX] += weight * shortestDistance**2

        return True

    def reset(self):
        self.updator.initialize({COUNT: 0, SUM1: 0., SUMX: 0., SUMXX: 0.})
        for cluster in self.clusters:
            cluster.reset()

    def converged(self, closeEnough):
        for cluster in self.clusters:
            if not cluster.converged(closeEnough):
                return False
        return True

class TrialCluster(object):
    """A single cluster centroid.

    Only called by ProducerClusteringModel.
    """

    def __init__(self, randomization, updateScheme):
        self.counter = updateScheme.updator(COUNT)

        randomPoint = randomization()
        if isinstance(randomPoint, numpy.matrix):
            randomPoint = [randomPoint[i,0] for i in xrange(len(randomPoint))]

        # set up a vector of updators with their initial value at the random point
        self.fields = []
        self.initialPosition = []
        for ri in randomPoint:
            u = updateScheme.updator(SUM1, SUMX)
            u.initialize({SUM1: 1., SUMX: ri})
            self.fields.append(u)
            self.initialPosition.append(ri)

    def __repr__(self):
        return "<TrialCluster %s>" % [u.mean() for u in self.fields]

    def anyInvalid(self):
        return INVALID in [u.mean() for u in self.fields]

    def vector(self):
        return [u.mean() for u in self.fields]

    def increment(self, syncNumber, vector, weight):
        if weight is None:
            for i, u in enumerate(self.fields):
                u.increment(syncNumber, vector[i])
        else:
            for i, u in enumerate(self.fields):
                u.counters[SUM1] += weight
                u.counters[SUMX] += weight * vector[i]
        self.counter.increment(syncNumber, 1)

    def reset(self):
        for u, i in zip(self.fields, xrange(len(self.initialPosition))):
            if u.counters[SUM1] != 0.:
                try:
                    self.initialPosition[i] = u.mean()
                except TypeError:
                    pass
            u.initialize({SUM1: 0., SUMX: 0.})
        self.counter.initialize({COUNT: 0})

    def converged(self, closeEnough):
        d = 0.
        for u, i in zip(self.fields, self.initialPosition):
            if u.counters[SUM1] == 0.:
                return False
            d += (u.mean() - i)**2
        return d <= closeEnough**2

    def count(self):
        return self.counter.count()

class ProducerClusteringModel(ProducerAlgorithm):
    """An event-based clustering algorithm.

    Although it does not iterate over the data as k-means does, it
    converges quickly because it creates large ensembles of trial
    clustering solutions.
    """

    defaultParams = {"updateExisting": "false", "numberOfTrials": "10", "numberToKeep": "3", "maturityThreshold": "100", "initialStability": "100", "overrideSignificance": "5."}

    def initialize(self, **params):
        """Initialize a clustering model producer."""

        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if "numberOfTrials" in params:
            self.numberOfTrials = int(params["numberOfTrials"])
            del params["numberOfTrials"]
        else:
            self.numberOfTrials = int(self.defaultParams["numberOfTrials"])

        if "numberToKeep" in params:
            self.numberToKeep = int(params["numberToKeep"])
            del params["numberToKeep"]
        else:
            self.numberToKeep = int(self.defaultParams["numberToKeep"])

        if "maturityThreshold" in params:
            self.maturityThreshold = int(params["maturityThreshold"])
            del params["maturityThreshold"]
        else:
            self.maturityThreshold = int(self.defaultParams["maturityThreshold"])

        if "initialStability" in params:
            self.initialStability = int(params["initialStability"])
            del params["initialStability"]
        else:
            self.initialStability = int(self.defaultParams["initialStability"])

        if "overrideSignificance" in params:
            self.overrideSignificance = float(params["overrideSignificance"])
            del params["overrideSignificance"]
            if self.overrideSignificance == 0.:
                self.overrideSignificance = None
        else:
            self.overrideSignificance = float(self.defaultParams["overrideSignificance"])

        self.model = self.segmentRecord.pmmlModel
        self.dataDistribution = self.engine.producerUpdateScheme.updator(COVARIANCE(self.model.numberOfFields))

        self.distanceMeasure = (self.model.child(pmml.ComparisonMeasure).attrib["kind"] == "distance")
            
        # put PartialSums in the model if they're not already there; pick up old values if you're resuming
        extension = self.model.child(pmml.Extension, exception=False)
        if extension is None:
            extension = pmml.Extension()
            self.model.children.append(extension)

        if self.updateExisting:
            self.sumOfDistances = extension.child(lambda x: isinstance(x, pmml.X_ODG_PartialSums) and x.attrib.get("name", None) == "SumOfDistances", exception=False)
        else:
            index = extension.index(lambda x: isinstance(x, pmml.X_ODG_PartialSums) and x.attrib.get("name", None) == "SumOfDistances", exception=False)
            if index is not None:
                del extension[index]
            self.sumOfDistances = None

        if self.sumOfDistances is None:
            self.sumOfDistances = pmml.X_ODG_PartialSums(name="SumOfDistances", COUNT=0, SUM1=0., SUMX=0., SUMXX=0.)
            extension.children.append(self.sumOfDistances)

        self.partialSums = {}
        for theid, cluster in zip(self.model.ids, self.model.cluster):
            for i, field in enumerate(self.model.fields):
                fullname = "%s.%s" % (theid, field)

                if self.updateExisting:
                    partialSum = extension.child(lambda x: isinstance(x, pmml.X_ODG_PartialSums) and x.attrib.get("name", None) == fullname, exception=False)
                else:
                    index = extension.index(lambda x: isinstance(x, pmml.X_ODG_PartialSums) and x.attrib.get("name", None) == fullname, exception=False)
                    if index is not None:
                        del extension[index]
                    partialSum = None

                if partialSum is None:
                    partialSum = pmml.X_ODG_PartialSums(name=fullname, SUM1=1., SUMX=cluster.value[i])
                    extension.children.append(partialSum)

                self.partialSums[fullname] = partialSum
                    
        # create the first trial using the values constructed above (they come from the PMML file if updateExisting is True)
        trialFromPmml = TrialClusterSet.__new__(TrialClusterSet)
        trialFromPmml.updator = self.engine.producerUpdateScheme.updator(COUNT, SUM1, SUMX, SUMXX)
        trialFromPmml.updator.initialize({COUNT: self.sumOfDistances.attrib["COUNT"], SUM1: self.sumOfDistances.attrib["SUM1"], SUMX: self.sumOfDistances.attrib["SUMX"], SUMXX: self.sumOfDistances.attrib["SUMXX"]})

        trialFromPmml.clusters = []
        for theid, cluster in zip(self.model.ids, self.model.cluster):
            trialCluster = TrialCluster.__new__(TrialCluster)
            trialCluster.counter = self.engine.producerUpdateScheme.updator(COUNT)
            trialCluster.fields = []
            trialCluster.initialPosition = []
            for field in self.model.fields:
                partialSum = self.partialSums["%s.%s" % (theid, field)]
                u = self.engine.producerUpdateScheme.updator(SUM1, SUMX)
                u.initialize({SUM1: partialSum.attrib["SUM1"], SUMX: partialSum.attrib["SUMX"]})
                trialCluster.fields.append(u)
            trialCluster.initialPosition = list(cluster.value)
            trialFromPmml.clusters.append(trialCluster)

        self.trials = [trialFromPmml]

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def update(self, syncNumber, get):
        """Update the clustering model.

        This will move all clusters
        closer to their nearest data points (Lamarckian step), but it
        may not change the current champion (Darwinian step).
        """

        self.resetLoggerLevels()

        vector = [get(field) for field in self.model.fields]
        if INVALID in vector:
            self.logger.debug("ClusteringModel.update: returning False (INVALID data)")
            return False

        weight = None
        if self.model.weightField is not None:
            weight = get(self.model.weightField)
        if weight is INVALID or weight is MISSING:
            self.logger.debug("ClusteringModel.update: returning False (INVALID or MISSING weight)")
            return False

        trans = numpy.matrix(numpy.identity(len(self.model.fields)))
        shift = numpy.matrix(numpy.zeros(len(self.model.fields))).T

        if MISSING not in vector:
            if self.distanceMeasure:
                # characterize the data so that you can generate random numbers with the same distribution
                self.dataDistribution.increment(syncNumber, vector)

                try:
                    covariance = self.dataDistribution.covariance()
                except ZeroDivisionError:
                    covariance = INVALID

                if covariance is not INVALID:
                    shift = self.dataDistribution.covmean()
                    try:
                        trans = numpy.linalg.cholesky(covariance)
                    except numpy.linalg.LinAlgError:
                        pass # FIXME: at least make trans a diagonal matrix with stdev entries (or 1/stdev)!

            else:
                raise NotImplementedError("Currently, only clusters with ComparisonMeasure.kind == 'distance' metrics can be produced.")

        randomization = lambda: ((trans * (numpy.matrix(numpy.random.randn(len(shift))).T)) + shift)

        while len(self.trials) < self.numberOfTrials:
            self.trials.append(TrialClusterSet(self.model.numberOfClusters, randomization, self.engine.producerUpdateScheme))

        for trial in self.trials:
            if not trial.update(syncNumber, vector, self.model, trial.updator.count() > self.initialStability, weight):
                self.logger.debug("ClusteringModel.update: returning False (INVALID distance calculation)")
                return False

        if self.overrideSignificance is None:
            self.trials.sort(lambda a, b: cmp(a.updator.mean(), b.updator.mean()))
        else:
            sig = self.overrideSignificance
            def sorter(a, b):
                amean = a.updator.mean()
                bmean = b.updator.mean()
                avar = a.updator.variance()
                bvar = b.updator.variance()
                if avar is INVALID or avar < 0.: avar = 0.
                if bvar is INVALID or bvar < 0.: bvar = 0.

                return cmp(amean + sig*math.sqrt(avar)/a.updator.counters[SUM1], bmean + sig*math.sqrt(bvar)/b.updator.counters[SUM1])

            self.trials.sort(sorter)

        # the Darwinian step: keep only the N fittest (after they become mature)
        immature = []
        keep = []
        for trial in self.trials:
            if trial.updator.count() < self.maturityThreshold:
                immature.append(trial)
            elif len(keep) < self.numberToKeep:
                keep.append(trial)

        self.trials = immature + keep

        # write the current winner to the PMML file
        if len(keep) > 0:
            best = keep[0]
            self.sumOfDistances.attrib["COUNT"] = best.updator.counters[COUNT]
            self.sumOfDistances.attrib["SUM1"] = best.updator.counters[SUM1]
            self.sumOfDistances.attrib["SUMX"] = best.updator.counters[SUMX]
            self.sumOfDistances.attrib["SUMXX"] = best.updator.counters[SUMXX]

            for trialCluster, pmmlCluster, theid in zip(best.clusters, self.model.cluster, self.model.ids):
                pmmlCluster.value = trialCluster.vector()
                pmmlCluster.attrib["n"] = len(pmmlCluster.value)

                for field, u in zip(self.model.fields, trialCluster.fields):
                    partialSum = self.partialSums["%s.%s" % (theid, field)]
                    partialSum.attrib["SUM1"] = u.counters[SUM1]
                    partialSum.attrib["SUMX"] = u.counters[SUMX]

            return True

        else:
            self.logger.debug("ClusteringModel.update: returning False (no mature clusters yet)")
            return False

#########################################################################################
################################################################# k-means producer ######
#########################################################################################

class ProducerKMeans(ProducerAlgorithm):
    """The standard k-means clustering algorithm."""

    SYNCNUMBER = Atom("SyncNumber")
    RANDOM_DATAPOINTS = Atom("Random_DataPoints")
    RANDOM_DATAWEIGHTED = Atom("Random_DataWeighted")
    RANDOM_DATACOVARIANCE = Atom("Random_DataCovariance")
    RANDOM_UNITRECT = Atom("Random_UnitRect")

    defaultParams = {"updateExisting": "false", "quickConvergeSteps": "()", "numberOfClusters": "unset", "seedSource": "dataPoints", "numberOfTrials": "20", "numberToConverge": "5", "maxIterations": "unset", "closeEnough": "0"}

    def initialize(self, **params):
        if "updateExisting" in params:
            self.updateExisting = pmml.boolCheck(params["updateExisting"])
            del params["updateExisting"]
            if self.updateExisting:
                raise NotImplementedError("Updating from existing ClusterModels using 'kmeans' not implemented; use mode='replaceExisting'")
        else:
            self.updateExisting = pmml.boolCheck(self.defaultParams["updateExisting"])

        if "quickConvergeSteps" in params:
            try:
                self.quickConvergeSteps = eval(params["quickConvergeSteps"])
                if not isinstance(self.quickConvergeSteps, tuple):
                    raise RuntimeError
                self.quickConvergeSteps = map(int, self.quickConvergeSteps)
            except err:
                raise RuntimeError("quickConvergeSteps must be a tuple of numbers of events")
            del params["quickConvergeSteps"]
        else:
            self.quickConvergeSteps = eval(self.defaultParams["quickConvergeSteps"])

        if "numberOfClusters" in params:
            self.numberOfClusters = params["numberOfClusters"]
            del params["numberOfClusters"]
        else:
            self.numberOfClusters = self.defaultParams["numberOfClusters"]
        try:
            self.numberOfClusters = int(self.numberOfClusters)
            if self.numberOfClusters <= 0: raise ValueError
        except ValueError:
            if self.numberOfClusters == "unset":
                self.numberOfClusters = None
            else:
                raise RuntimeError("numberOfClusters must be a positive integer or \"unset\", not \"%s\"" % self.numberOfClusters)

        if "seedSource" in params:
            self.seedSource = params["seedSource"]
            del params["seedSource"]
        else:
            self.seedSource = self.defaultParams["seedSource"]
        if self.seedSource == "dataPoints":
            self.seedSource = self.RANDOM_DATAPOINTS
        elif self.seedSource == "dataWeighted":
            self.seedSource = self.RANDOM_DATAWEIGHTED
        elif self.seedSource == "dataCovariance":
            self.seedSource = self.RANDOM_DATACOVARIANCE
        elif self.seedSource == "unitRect":
            self.seedSource = self.RANDOM_UNITRECT
        else:
            raise NotImplementedError("The seedSource must be one of 'dataPoints', 'dataCovariance', 'unitRect'")

        if "numberOfTrials" in params:
            self.numberOfTrials = int(params["numberOfTrials"])
            del params["numberOfTrials"]
        else:
            self.numberOfTrials = int(self.defaultParams["numberOfTrials"])

        if "numberToConverge" in params:
            self.numberToConverge = int(params["numberToConverge"])
            del params["numberToConverge"]
        else:
            self.numberToConverge = int(self.defaultParams["numberToConverge"])

        if self.numberToConverge > self.numberOfTrials:
            raise RuntimeError("numberToConverge (%d) must not be greater than numberOfTrials (%d)" % (self.numberToConverge, self.numberOfTrials))

        if "maxIterations" in params:
            self.maxIterations = params["maxIterations"]
            del params["maxIterations"]
        else:
            self.maxIterations = self.defaultParams["maxIterations"]
        try:
            self.maxIterations = int(self.maxIterations)
            if self.maxIterations <= 0: raise ValueError
        except ValueError:
            if self.maxIterations == "unset":
                self.maxIterations = None
            else:
                raise RuntimeError("maxIterations must be a positive integer or \"unset\", not \"%s\"" % self.maxIterations)

        if "closeEnough" in params:
            self.closeEnough = float(params["closeEnough"])
            del params["closeEnough"]
        else:
            self.closeEnough = float(self.defaultParams["closeEnough"])

        self.model = self.segmentRecord.pmmlModel
        self.dataDistribution = self.engine.producerUpdateScheme.updator(COVARIANCE(self.model.numberOfFields))
        self.distanceMeasure = (self.model.child(pmml.ComparisonMeasure).attrib["kind"] == "distance")

        if self.seedSource == self.RANDOM_DATAWEIGHTED and self.model.weightField is None:
            self.seedSource = self.RANDOM_DATAPOINTS

        # get rid of any PartialSums objects, since they would be misleading (this algorithm doesn't use them)
        extension = self.model.child(pmml.Extension, exception=False)
        if extension is not None:
            newChildren = []
            for child in extension.children:
                if not isinstance(child, pmml.X_ODG_PartialSums):
                    newChildren.append(child)
            extension.children = newChildren

        self.buffer = {self.SYNCNUMBER: []}
        for field in self.model.fields:
            self.buffer[field] = []

        if self.model.weightField is not None:
            self.buffer[self.model.weightField] = []

        if len(params) > 0:
            raise TypeError("Unrecognized parameters %s" % params)

    def update(self, syncNumber, get):
        self.resetLoggerLevels()

        vector = [get(field) for field in self.model.fields]
        if INVALID in vector:
            self.logger.debug("KMeansClustering.update: returning False (INVALID data)")
            return False

        if self.model.weightField is not None:
            weight = get(self.model.weightField)
            if weight is INVALID or weight is MISSING:
                self.logger.debug("KMeansClustering.update: returning False (INVALID or MISSING weight)")
                return False
            self.buffer[self.model.weightField].append(weight)

        self.buffer[self.SYNCNUMBER].append(syncNumber)
        for i, field in enumerate(self.model.fields):
            self.buffer[field].append(vector[i])

        if self.distanceMeasure and MISSING not in vector:
            self.dataDistribution.increment(syncNumber, vector)

        return True

    def produce(self):
        self.resetLoggerLevels()

        extension = self.model.child(pmml.Extension, exception=False)
        if extension is None:
            extension = pmml.Extension()
            self.model.children.append(extension)

        convergence = extension.child(pmml.X_ODG_Convergence, exception=False)
        if convergence is None:
            convergence = pmml.X_ODG_Convergence()
            extension.children.append(convergence)

        numRecords = len(self.buffer[self.SYNCNUMBER])

        if self.logDebug:
            self.logger.debug("KMeansClustering.produce: this segment has %d data records; setting up for cluster production." % numRecords)

        if numRecords == 0:
            self.logger.debug("KMeansClustering.produce: no data in this segment, so there are no clusters to produce.")
            return

        if self.numberOfClusters is not None:
            if self.numberOfClusters > numRecords:
                self.logger.info("KMeansClustering.produce: number of desired clusters (%d) exceeds number of data records (%d), reducing number of clusters to match." % (self.model.numberOfClusters, numRecords))
                self.model.changeNumberOfClusters(numRecords)
            elif self.numberOfClusters != self.model.numberOfClusters:
                self.model.changeNumberOfClusters(self.numberOfClusters)

        elif self.model.numberOfClusters > numRecords:
            self.logger.info("KMeansClustering.produce: number of desired clusters (%d) exceeds number of data records (%d), reducing number of clusters to match." % (self.model.numberOfClusters, numRecords))
            self.model.changeNumberOfClusters(numRecords)

        # special case that should be easy, but it can cause the standard k-means algorithm to infinite loop:
        if self.model.numberOfClusters == numRecords:
            self.logger.debug("KMeansClustering.produce: number of records equals the number of clusters (%d), so we skip the standard algorithm and just assign data points to clusters" % numRecords)
            for i, pmmlCluster in enumerate(self.model.cluster):
                pmmlCluster.value = [self.buffer[field][i] for field in self.model.fields]
                pmmlCluster.attrib["n"] = len(pmmlCluster.value)
            return

        self.trans = numpy.matrix(numpy.identity(len(self.model.fields)))
        self.shift = numpy.matrix(numpy.zeros(len(self.model.fields))).T

        if self.distanceMeasure:
            # characterize the data so that you can generate random numbers with the same distribution
            try:
                covariance = self.dataDistribution.covariance()
            except ZeroDivisionError:
                covariance = INVALID

            if covariance is not INVALID:
                self.shift = self.dataDistribution.covmean()
                try:
                    self.trans = numpy.linalg.cholesky(covariance)
                except numpy.linalg.LinAlgError:
                    pass # FIXME: at least make trans a diagonal matrix with stdev entries (or 1/stdev)!

        else:
            raise NotImplementedError("Currently, only clusters with ComparisonMeasure.kind == 'distance' metrics can be produced.")

        # make a new set of trials
        if self.seedSource is ProducerKMeans.RANDOM_DATAPOINTS:
            # pick a random point from the dataset
            def randomization():
                i = random.randint(0, len(self.buffer[self.SYNCNUMBER]) - 1)
                return [self.buffer[field][i] for field in self.model.fields if field is not self.SYNCNUMBER]
            self.randomization = randomization

        elif self.seedSource == ProducerKMeans.RANDOM_DATAWEIGHTED:
            # pick a random point from the dataset, weighted by their weights
            sumOfWeights = numpy.cumsum(self.buffer[self.model.weightField])
            def randomization():
                x = random.uniform(0., sumOfWeights[-1])
                i = numpy.where(sumOfWeights > x)[0][0]
                return [self.buffer[field][i] for field in self.model.fields if field is not self.SYNCNUMBER]
            self.randomization = randomization

        elif self.seedSource == ProducerKMeans.RANDOM_DATACOVARIANCE:
            # generate a random point from a distribution with a covariance like the data
            self.randomization = lambda: ((self.trans * (numpy.matrix(numpy.random.randn(len(self.shift))).T)) + self.shift)

        elif self.seedSource == ProducerKMeans.RANDOM_UNITRECT:
            # generate a random point in the unit rectangle
            self.randomization = lambda: [random.random() for i in xrange(len(self.shift))]

        self.trials = [TrialClusterSet(self.model.numberOfClusters, self.randomization, self.engine.producerUpdateScheme) for i in xrange(self.numberOfTrials)]

        # prepare small subsamples to run first to improve convergence when the whole dataset gets used
        allIndices = range(len(self.buffer[self.SYNCNUMBER]))
        quickConvergeSamples = []
        for numEvents in self.quickConvergeSteps:
            if numEvents > len(allIndices):
                numEvents = len(allIndices)
            quickConvergeSamples.append(numpy.array(random.sample(allIndices, numEvents)))
            
        allIndices = numpy.array(allIndices)
        for key in self.buffer:
            self.buffer[key] = numpy.array(self.buffer[key])

        for i, quickConvergenceSample in enumerate(quickConvergeSamples):
            if self.logDebug:
                self.logger.debug("KMeansClustering.produce: ===== quickConverge %d: preparing for k-means by clustering a random subset of %d events" % (i+1, len(quickConvergenceSample)))
            self.iterations(quickConvergenceSample)

        self.logger.debug("KMeansClustering.produce: ===== starting k-means clustering algorithm (whole dataset)")
        convergence.attrib["iterations"] = self.iterations()

        # find the best one
        best = None
        for trial in self.trials:
            if trial.hasConverged:
                if best is None or trial.updator.mean() < best.updator.mean():
                    best = trial

        convergence.attrib["converged"] = (best is not None)

        if best is None:
            self.logger.error("KMeansClustering.produce: no trial cluster-sets converged within the desired number of iterations (%s), using the best UNCONVERGED set instead." % str(self.maxIterations) if self.maxIterations is not None else "unset")
            for trial in self.trials:
                if best is None or trial.updator.mean() < best.updator.mean():
                    best = trial

        # write it to the PMML file
        for bestCluster, pmmlCluster in zip(best.clusters, self.model.matches(pmml.Cluster)):
            pmmlCluster.attrib["size"] = bestCluster.count()
            theArray = pmmlCluster.child(pmml.Array)
            theArray.value = bestCluster.initialPosition
            theArray.attrib["n"] = len(theArray.value)

    def iterations(self, indices=None):
        if indices is None:
            dataset = self.buffer
        else:
            dataset = {}
            for key, value in self.buffer.items():
                dataset[key] = value[indices]

        # loop over the data many times until a subset of trials converge
        iteration = 0
        while True:
            iteration += 1

            # set "initialPosition" to the mean within each cluster
            for trial in self.trials:
                trial.reset()

            if self.logDebug:
                self.logger.debug("KMeansClustering.produce: iteration %d" % iteration)

            # loop over data (pre-calculated, including all derived fields)
            for i in xrange(len(dataset[self.SYNCNUMBER])):
                if self.logDebug and i % 10000 == 0:
                    self.logger.debug("    event %d/%d = %g%%" % (i, len(dataset[self.SYNCNUMBER]), 100.*i/len(dataset[self.SYNCNUMBER])))

                syncNumber = dataset[self.SYNCNUMBER][i]
                vector = [dataset[field][i] for field in self.model.fields]

                weight = None
                if self.model.weightField is not None:
                    weight = dataset[self.model.weightField][i]

                for trial in self.trials:
                    trial.update(syncNumber, vector, self.model, False, weight)

            if self.logDebug:
                self.logger.debug("    event %d/%d = 100%%" % (len(dataset[self.SYNCNUMBER]), len(dataset[self.SYNCNUMBER])))

            self.logger.debug("KMeansClustering.produce: about to sort the trials")

            self.trials.sort(lambda a, b: -cmp(a.updator.mean(), b.updator.mean()))

            self.logger.debug("KMeansClustering.produce: about to check convergence of the trials")

            numConverged = 0
            for trial in self.trials:
                if trial.converged(self.closeEnough):
                    trial.hasConverged = True

                    numConverged += 1

                else:
                    trial.hasConverged = False

            if self.logDebug:
                self.logger.debug("KMeansClustering.produce: iteration %d has %d converged cluster-set trials" % (iteration, numConverged))

                best = None
                for trial in self.trials:
                    if trial.hasConverged:
                        if best is None or trial.updator.mean() < best.updator.mean():
                            best = trial

                if best is not None:
                    self.logger.debug("    best CONVERGED so far: %s" % " ".join(map(repr, best.clusters)))
                else:
                    best = None
                    for trial in self.trials:
                        if best is None or trial.updator.mean() < best.updator.mean():
                            best = trial

                    if best is not None:
                        self.logger.debug("    best so far: %s" % " ".join(map(repr, best.clusters)))

            for trial in self.trials:
                # self.logger.debug("    show all: %s%s" % (" ".join(map(repr, trial.clusters)), " (converged)" if trial.hasConverged else ""))
                trial.rethrowInvalid(self.randomization, self.engine.producerUpdateScheme)

            if numConverged >= self.numberToConverge:
                return iteration

            if self.maxIterations is not None and iteration >= self.maxIterations:
                return iteration
