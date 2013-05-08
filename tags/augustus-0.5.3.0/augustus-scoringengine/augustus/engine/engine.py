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

"""Defines a class that manages segments and calls algorithms in the main event loop."""

from sys import version_info
if version_info < (2, 6): from sets import Set as set
import datetime
import logging

try:
    from functools import reduce
except ImportError:
    pass

import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
from augustus.core.defs import Atom, INVALID, MISSING, IMMATURE, MATURE, LOCKED, NameSpace
from augustus.engine.segmentrecord import SegmentRecord, SELECTFIRST, SELECTALL, SELECTONLY
from augustus.engine.outputwriter import OutputMiniEvent, OutputSegment, OutputRecord

import augustus.algorithms.baseline
import augustus.algorithms.clustering
import augustus.algorithms.trees
import augustus.algorithms.rules
import augustus.algorithms.regression
import augustus.algorithms.naivebayes

consumerAlgorithmMap = {
    pmml.BaselineModel:   augustus.algorithms.baseline.ConsumerBaselineModel,
    pmml.ClusteringModel: augustus.algorithms.clustering.ConsumerClusteringModel,
    pmml.TreeModel:       augustus.algorithms.trees.ConsumerTreeModel,
    pmml.RuleSetModel:    augustus.algorithms.rules.ConsumerRuleSetModel,
    pmml.RegressionModel: augustus.algorithms.regression.ConsumerRegressionModel,
    pmml.NaiveBayesModel: augustus.algorithms.naivebayes.ConsumerNaiveBayesModel,
    }

producerAlgorithmMap = {
    (pmml.BaselineModel, "streaming"):   augustus.algorithms.baseline.ProducerBaselineModel,
    (pmml.BaselineModel, "hold"):        augustus.algorithms.baseline.ProducerBaselineHold,
    (pmml.BaselineModel, "pass"):        augustus.algorithms.baseline.ProducerBaselinePass,

    (pmml.ClusteringModel, "streaming"): augustus.algorithms.clustering.ProducerClusteringModel,
    (pmml.ClusteringModel, "kmeans"):    augustus.algorithms.clustering.ProducerKMeans,

    (pmml.TreeModel, "streaming"):       augustus.algorithms.trees.ProducerTreeModel,
    (pmml.TreeModel, "iterative"):       augustus.algorithms.trees.ProducerIterative,
    (pmml.TreeModel, "c45"):             augustus.algorithms.trees.ProducerC45,
    (pmml.TreeModel, "cart"):            augustus.algorithms.trees.ProducerCART,
    (pmml.RuleSetModel, "streaming"):    augustus.algorithms.trees.ProducerTreeModel,
    (pmml.RuleSetModel, "iterative"):    augustus.algorithms.trees.ProducerIterative,
    (pmml.RuleSetModel, "c45"):          augustus.algorithms.trees.ProducerC45,
    (pmml.RuleSetModel, "cart"):         augustus.algorithms.trees.ProducerCART,

    (pmml.RegressionModel, "streaming"): augustus.algorithms.regression.ProducerRegressionModel,

    (pmml.NaiveBayesModel, "streaming"): augustus.algorithms.naivebayes.ProducerNaiveBayesModel,
    }

########################################################### for faster segment lookup

MATCHRANGES = Atom("MatchRanges")

def __matchesPartition(matcher, partition):
    for bound, comparator in partition:
        if bound is not None and not comparator(matcher, bound):
            return False
    return True

# FIXME: what should segment-finding do if x is INVALID or MISSING (isinstance(x, Atom) is True)?
# Predicates in TreeModels return UNKNOWN, which is handed in the MiningSchema
# Predicates in Segments might have to refer to the MiningModel's MiningSchema
# For now, maintain the current behavior.
# If you ever change this, verify that the random_predicates work first (including bigtests), make the change, and then replace all of the reference data.
_segmentHelpers = NameSpace(
    lessThan=lambda x, val: isinstance(x, Atom) or x < val,
    lessOrEqual=lambda x, val: isinstance(x, Atom) or x <= val,
    greaterThan=lambda x, val: not isinstance(x, Atom) and x > val,
    greaterOrEqual=lambda x, val: not isinstance(x, Atom) and x >= val,
    isCompoundAnd=lambda x:
        isinstance(x, pmml.CompoundPredicate) and
        x.attrib['booleanOperator'] == "and", 
    isSimpleEqual=lambda x:
        isinstance(x, pmml.SimplePredicate) and
        x.attrib['operator'] == "equal",
    isComparator=lambda x:
        isinstance(x, pmml.SimplePredicate) and \
        x.attrib['operator'][0] in ('l', 'g'),  # less|greater + Than|OrEqual
    matchesPartition=__matchesPartition)

########################################################### Engine

class Engine(object):
    """Object called by Augustus main event loop to process one event/pseudoevent."""

    def __init__(self, pmmlFile, dataStream, producerUpdateScheme, consumerUpdateScheme, segmentationScheme, producerAlgorithm, **settings):

        # everything that would be set by configuration
        self.pmmlFile = pmmlFile
        self.modelName = None
        self.dataStream = dataStream
        self.consumerUpdateScheme = consumerUpdateScheme
        self.producerUpdateScheme = producerUpdateScheme
        self.maturityThreshold = 10
        self.lockingThreshold = None
        self.lockAllSegments = False
        self.segmentationScheme = segmentationScheme
        self.producerAlgorithm = producerAlgorithm
        self.customProcessing = None

        self.logger = logging.getLogger()
        self.metadata = logging.getLogger("metadata")
        self.metadata.data["Time to look up existing segments"] = 0
        self.metadata.data["Score calculation, total"] = 0

        self.__dict__.update(settings)

    ########################################################## initialize

    def resetDataStream(self, dataStream):
        self.eventNumber = -1
        self.pseudoEventNumber = -1
        self.lastPseudoEventNumber = None
        self.lastPseudoEventField = None
        
        self.dataStream = dataStream
        self.pmmlModel.dataContext.parent = dataStream

    def initialize(self):
        """Interpret PMML file, set up SegmentRecords list, and
        initialize all algorithms."""

        self.firstSegment = True
        
        # set up the header, so that our models can be stamped with time and event number
        header = self.pmmlFile.child(pmml.Header)
        if header.exists(pmml.Extension):
            headerExtension = header.child(pmml.Extension)
        else:
            headerExtension = pmml.Extension()
            header.children.insert(0, headerExtension)

        if headerExtension.exists(pmml.X_ODG_RandomSeed):
            del headerExtension[headerExtension.index(pmml.X_ODG_RandomSeed)]
        augustusRandomSeed = pmml.X_ODG_RandomSeed(value=self.augustusRandomSeed)
        headerExtension.children.append(augustusRandomSeed)

        if headerExtension.exists(pmml.X_ODG_Eventstamp):
            del headerExtension[headerExtension.index(pmml.X_ODG_Eventstamp)]
        self.eventStamp = pmml.X_ODG_Eventstamp(number=0)
        headerExtension.children.append(self.eventStamp)
        
        if header.exists(pmml.Timestamp):
            del header[header.index(pmml.Timestamp)]
        self.timeStamp = pmml.Timestamp(xmlbase.XMLText(datetime.datetime.today().isoformat()))
        header.children.append(self.timeStamp)

        # select the first model or select a model by name
        if self.modelName is None:
            self.pmmlModel = self.pmmlFile.topModels[0]
        else:
            self.pmmlModel = None
            for model in self.pmmlFile.topModels:
                if "modelName" in model.attrib and model.attrib["modelName"] == self.modelName:
                    self.pmmlModel = model
                    break
            if self.pmmlModel is None:
                raise RuntimeError("No model named \"%s\" was found in the PMML file" % self.modelName)

        # connect the dataContext to the dataStream, so that events will flow from the input file into the transformations
        self.resetDataStream(self.dataStream)

        # clear the cache the model DataContexts (initializes some dictionaries)
        self.pmmlModel.dataContext.clear()
        if self.pmmlModel.dataContext.transformationDictionary:
            self.metadata.data["Transformation dictionary elements"] = len(self.pmmlModel.dataContext.transformationDictionary.cast)
        else:
            self.metadata.data["Transformation dictionary elements"] = 0

        self.segmentRecords = []
        self._lookup = NameSpace(tuples={}, fields={}, other=[])
        SegmentRecord.maturityThreshold = self.maturityThreshold
        SegmentRecord.lockingThreshold = self.lockingThreshold

        if self.pmmlFile.exists(pmml.TransformationDictionary):
            if self.pmmlFile.child(pmml.TransformationDictionary).exists(pmml.Aggregate, maxdepth=None):
                raise NotImplementedError("Aggregate transformations in the TransformationDictionary are not supported")
            if self.pmmlFile.child(pmml.TransformationDictionary).exists(pmml.X_ODG_AggregateReduce, maxdepth=None):
                raise NotImplementedError("X-ODG-AggregateReduce transformations in the TransformationDictionary are not supported")

        # MiningModels are special because we handle segmentation at the Engine level
        # Currently no support for MiningModels nested within MiningModels
        if isinstance(self.pmmlModel, pmml.MiningModel):
            self.pmmlOutput = self.pmmlModel.child(pmml.Output, exception=False)
            segmentation = self.pmmlModel.child(pmml.Segmentation, exception=False)
            # for now, assume a MiningModel without any segments will be populated through autosegmentation

            if self.pmmlModel.exists(pmml.LocalTransformations):
                if self.pmmlModel.child(pmml.LocalTransformations).exists(pmml.Aggregate, maxdepth=None):
                    raise NotImplementedError("Aggregate transformations in the MiningModel's LocalTransformations are not supported")
                if self.pmmlModel.child(pmml.LocalTransformations).exists(pmml.X_ODG_AggregateReduce, maxdepth=None):
                    raise NotImplementedError("X-ODG-AggregateReduce transformations in the MiningModel's LocalTransformations are not supported")

            if segmentation.attrib["multipleModelMethod"] == "selectFirst":
                self.multipleModelMethod = SELECTFIRST
            elif segmentation.attrib["multipleModelMethod"] == "selectAll":
                self.multipleModelMethod = SELECTALL
            else:
                raise NotImplementedError("Only 'selectFirst', 'selectAll', and no segmentation have been implemented.")
            self.metadata.data["Match all segments"] = self.multipleModelMethod != SELECTFIRST
            
            for pmmlSegment in segmentation.matches(pmml.Segment):
                self._makeSegmentRecord(pmmlSegment)
                
        else:
            self.multipleModelMethod = SELECTONLY

            segmentRecord = SegmentRecord(self.pmmlModel, None, None, self)

            modelClass = self.pmmlModel.__class__
            algoName = self.producerAlgorithm[modelClass.__name__].attrib["algorithm"]
            segmentRecord.consumerAlgorithm = consumerAlgorithmMap[modelClass](self, segmentRecord)
            segmentRecord.producerAlgorithm = producerAlgorithmMap[modelClass, algoName](self, segmentRecord)
            segmentRecord.producerParameters = self.producerAlgorithm[modelClass.__name__].parameters
            self.setProvenance(self.pmmlModel, algoName, segmentRecord.producerAlgorithm, segmentRecord.producerParameters)

            localTransformations = self.pmmlModel.child(pmml.LocalTransformations, exception=False)
            if localTransformations is not None:
                segmentRecord.aggregates = localTransformations.matches(pmml.Aggregate, maxdepth=None)
                segmentRecord.aggregates.extend(localTransformations.matches(pmml.X_ODG_AggregateReduce, maxdepth=None))
            else:
                segmentRecord.aggregates = []
            for aggregate in segmentRecord.aggregates:
                aggregate.initialize(self.consumerUpdateScheme)

            self.segmentRecords.append(segmentRecord)
            self.metadata.data["First segment model type"] = segmentRecord.pmmlModel.tag

        self.reinitialize()

    def reinitialize(self):
        for segmentRecord in self.segmentRecords:
            segmentRecord.initialize(existingSegment=True, customProcessing=self.customProcessing, setModelMaturity=(not self.hasProducer))

    def flushAggregates(self):
        for segmentRecord in self.segmentRecords:
            for aggregate in segmentRecord.aggregates:
                aggregate.flush()

    ########################################################## setProvenance

    def setProvenance(self, model, algoName, algorithm, userParameters):
        model.attrib["algorithmName"] = algoName

        parameters = dict(algorithm.defaultParams)
        parameters.update(userParameters)

        extension = model.child(pmml.Extension, exception=False)
        if extension is None:
            extension = pmml.newInstance("Extension")
        else:
            extension.children = [c for c in extension.children if not isinstance(c, pmml.X_ODG_AlgorithmParameter)]
        
        keys = parameters.keys()
        keys.sort()
        for key in keys:
            ap = pmml.newInstance("X-ODG-AlgorithmParameter", attrib={"name": key, "value": parameters[key]}, base=pmml.X_ODG_PMML)
            extension.children.append(ap)

    ########################################################## updateSegment
    def updateSegment(self, segment, pseudoEvent=False):
        """Attempt to update (produce) a model attached to the segment
        and update the segment's maturity counter."""

        syncNumberSource = getattr(self.producerUpdateScheme, "timeFieldName", None)
        if syncNumberSource is None:
            if pseudoEvent:
                syncNumber = self.pseudoEventNumber
            else:
                syncNumber = self.eventNumber
        else:
            syncNumber = segment.pmmlModel.dataContext.get(syncNumberSource)
    
        # only increment the SegmentRecord's maturity counter if the producer successfully updated
        if segment.producerAlgorithm.update(syncNumber, segment.pmmlModel.dataContext.get):
            segment.update(syncNumber)

    ########################################################## event
    def event(self, score=True, update=True, explicitSegments=None, predictedName=None):
        """Called once per event.

        If 'score' is True, run the consumer algorithm.
        If 'update' is True, run the producer algorithm.
        If 'explicitSegments' is a list of SegmentRecords, use these segments instead of finding the right ones.
        If 'predictedName' is a string, use this instead of what Output requires (for the case of no Output block).
        """

        logInfo = self.logger.getEffectiveLevel() <= logging.INFO
        logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG

        # increment the data stream
        self.eventNumber += 1

        if logDebug:
            self.logger.debug("========== Starting event %d" % self.eventNumber)
        elif self.eventNumber % 1000 == 0:
            self.logger.info("========== Starting event %d" % self.eventNumber)

        self.metadata.startTiming("Time to advance through data")
        self.dataStream.next()
        self.metadata.stopTiming("Time to advance through data")

        self.logger.debug("Loaded a record from the data stream.")

        # update the PMML's header
        self.eventStamp.attrib["number"] = self.eventNumber
        self.timeStamp.children[0].text = [datetime.datetime.today().isoformat()]

        # find the matching segments
        if explicitSegments is None:
            segmentMatches = []
            self.metadata.startTiming("Time searching for blacklisted items")
            blacklisted = self.segmentationScheme.blacklisted(self.pmmlModel.dataContext.get)
            self.metadata.stopTiming("Time searching for blacklisted items")

            if not blacklisted:
                self.metadata.startTiming("Time to look up existing segments")
                matchCandidates = self._getMatchCandidates()
                if self.multipleModelMethod is SELECTFIRST:

                    for index in matchCandidates:
                        segmentRecord = self.segmentRecords[index]
                        if segmentRecord.predicateMatches(self.pmmlModel.dataContext.get):
                            segmentMatches = [segmentRecord]
                            break

                elif self.multipleModelMethod is SELECTALL:
                    segmentMatches = [
                        self.segmentRecords[index] for index in matchCandidates if
                        self.segmentRecords[index].predicateMatches(self.pmmlModel.dataContext.get)]

                elif self.multipleModelMethod is SELECTONLY:
                    segmentMatches = [self.segmentRecords[0]]

                self.metadata.stopTiming("Time to look up existing segments")

                if len(segmentMatches) == 0:
                    self.metadata.startTiming("Time to find and create new PMML segments")
                    match = self.segmentationScheme.getMatch(self.pmmlModel.dataContext.get)
                    self.metadata.stopTiming("Time to find and create new PMML segments")

                    if not match:
                        if logInfo:
                            self.logger.info(" ".join([
                                "Event %d did not match any segment descriptions; discarding." % self.eventNumber,
                                "Data=%s" % self.pmmlModel.dataContext.contextString() ]))

                    else:
                        match = self._makeSegmentRecord(match, autoSegment=True)
                        segmentMatches.append(match)
            else:
                if logInfo:
                    self.logger.info(" ".join([
                        "Event %d matches a user blacklisted item, and will be ignored." % self.eventNumber,
                        "Data=%s" % self.pmmlModel.dataContext.contextString() ]))

        else:
            segmentMatches = explicitSegments

        syncNumberSource = getattr(self.consumerUpdateScheme, "timeFieldName", None)
        if syncNumberSource is None:
            syncNumber = self.eventNumber
        else:
            syncNumber = self.pmmlModel.dataContext.get(syncNumberSource)

        # possibly do custom processing
        if self.customProcessing is not None:
            self.metadata.startTiming("Score calculation, total")

            matchingSegments = [s.userFriendly for s in segmentMatches]
            if logDebug:
                self.logger.debug("About to enter customProcessing.doEvent() for event %d." % self.eventNumber)
            output = self.customProcessing.doEvent(syncNumber, self.eventNumber, self.pmmlModel.dataContext.get, matchingSegments)
            if logDebug:
                self.logger.debug("Finished customProcessing.doEvent().")

            for segmentMatch in segmentMatches:
                segmentMatch.pmmlModel.dataContext.clear()
            self.pmmlModel.dataContext.clear()

            self.metadata.stopTiming("Score calculation, total")
            self.dataStream.flush()
            return output

        # run the algorithms and create the output
        self.metadata.startTiming("Score calculation, total")
        outputRecord = OutputRecord(self.eventNumber, self.multipleModelMethod)

        ### big loop over segmentMatches
        for segmentMatch in segmentMatches:

            for l in self.logger, self.metadata:
                if "segment" in l.differentLevel:
                    seglevels = l.differentLevel["segment"]
                    segname = segmentMatch.name()
                    if segname in seglevels:
                        l.setLevel(seglevels[segname])
                    else:
                        
                        l.setLevel(l.eventLogLevel)
                else:
                    l.setLevel(l.eventLogLevel)
            logInfo = self.logger.getEffectiveLevel() <= logging.INFO
            logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG

            if logDebug:
                self.logger.debug("Segment %s (%s) matched in event %d." % (segmentMatch.name(), segmentMatch.expressionTree, self.eventNumber))

            if syncNumberSource is not None:
                syncNumber = segmentMatch.pmmlModel.dataContext.get(syncNumberSource)

            for aggregate in segmentMatch.aggregates:
                aggregate.increment(syncNumber, segmentMatch.pmmlModel.dataContext.get)

            if score and segmentMatch.pmmlModel.isScorable:
                if segmentMatch.state() is MATURE or segmentMatch.state() is LOCKED:
                    if logDebug:
                        self.logger.debug("About to enter consumerAlgorithm.score() for event %d, segment %s." % (self.eventNumber, segmentMatch.name()))

                    scores = segmentMatch.consumerAlgorithm.score(syncNumber, segmentMatch.pmmlModel.dataContext.get)
                    if logDebug:
                        self.logger.debug("Finished consumerAlgorithm.score().")
                else:
                    scores = IMMATURE
            else:
                scores = INVALID

            outputSegment = OutputSegment(segmentMatch)

            # override Output block
            if predictedName is not None:
                if scores is INVALID or scores is IMMATURE:
                    outputSegment.fields = [(predictedName, scores)]
                elif scores is None:
                    outputSegment.fields = [(predictedName, MISSING)]
                else:
                    outputSegment.fields = [(predictedName, scores.get(pmml.OutputField.predictedValue, MISSING))]

            # use Output block
            elif segmentMatch.pmmlOutput is not None:
                outputSegment.fields = segmentMatch.pmmlOutput.evaluate(segmentMatch.pmmlModel.dataContext.get, scores)

            outputRecord.segments.append(outputSegment)

            if update and segmentMatch.state() is not LOCKED:
                if logDebug:
                    self.logger.debug("About to enter producerAlgorithm.update() for event %d, segment %s." % (self.eventNumber, segmentMatch.name()))
                self.updateSegment(segmentMatch)
                if logDebug:
                    self.logger.debug("Finished producerAlgorithm.update().")

            # clear the cache of each of the segment DataContexts
            segmentMatch.pmmlModel.dataContext.clear()

        # END big loop over segmentMatches

        for l in self.logger, self.metadata:
            l.setLevel(l.eventLogLevel)

        self.pmmlModel.dataContext.clear()

        self.metadata.stopTiming("Score calculation, total")
        self.dataStream.flush()
        return outputRecord

    ########################################################## produce
    def produce(self):
        """Check to see if there are any non-serialized producers and run them."""

        logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG

        for segment in self.segmentRecords:
            if hasattr(segment.producerAlgorithm, "produce") and callable(segment.producerAlgorithm.produce):
                if logDebug:
                    self.logger.debug("About to enter producerAlgorithm.produce() for segment %s." % segment.name())
                segment.producerAlgorithm.produce()
                if logDebug:
                    self.logger.debug("Finished producerAlgorithm.produce().")

    ########################################################## pseudoevent
    def checkPseudoeventReadiness(self, pseudoEventConfig):
        """Return True if it is time to do a pseudoevent."""

        if pseudoEventConfig["eventNumberInterval"] >= 0:
            if self.lastPseudoEventNumber is None:
                ready = self.eventNumber >= pseudoEventConfig["eventNumberInterval"]
            else:
                ready = self.eventNumber - self.lastPseudoEventNumber > pseudoEventConfig["eventNumberInterval"]

            if ready:
                self.lastPseudoEventNumber = self.eventNumber
                return True

        if pseudoEventConfig["fieldValueInterval"] >= 0:
            value = self.pmmlModel.dataContext.get(pseudoEventConfig["field"])
            if value is INVALID or value is MISSING:
                return False

            try:
                value = int(value)
            except ValueError:
                raise RuntimeError("AggregateScore's field=\"%s\" (%s lines %s-%s) is not an integer: %s" % (pseudoEventConfig["field"], getattr(pseudoEventConfig, "fileName", "<Config>"), str(getattr(pseudoEventConfig, "lineStart", "?")), str(getattr(pseudoEventConfig, "lineEnd", "?")), value))

            if self.lastPseudoEventField is None:
                ready = value >= pseudoEventConfig["fieldValueInterval"]
            else:
                ready = value - self.lastPseudoEventField > pseudoEventConfig["fieldValueInterval"]

            if ready:
                self.lastPseudoEventField = value
                return True
                
        return False

    def pseudoevent(self, score=True, update=True):
        """Called once per pseudoevent.

        If 'score' is True, run the consumer algorithm.
        If 'update' is True, run the producer algorithm.
        """

        self.pseudoEventNumber += 1
        outputRecord = OutputRecord(self.pseudoEventNumber, SELECTALL, pseudo=True)

        for segment in self.segmentRecords:
            currentContext = segment.pmmlModel.dataContext
            while not isinstance(currentContext, pmml.DataContext):
                currentContext.clear()
                currentContext = currentContext.parentContext
            currentContext.clear()

            groupsToKeys = {}
            for aggregate in segment.aggregates:
                groupName = aggregate.groupField
                if groupName is not None and groupName not in groupsToKeys:
                    # find aggregates for this segment
                    groupsToKeys[groupName] = aggregate.updators.keys()

            outputSegment = OutputSegment(segment)
            if len(groupsToKeys) == 0:
                if score:
                    if segment.state() is MATURE or segment.state() is LOCKED:
                        scores = segment.consumerAlgorithm.score(self.pseudoEventNumber, segment.pmmlModel.dataContext.get)
                    else:
                        scores = IMMATURE
                else:
                    scores = INVALID

                if segment.pmmlOutput is not None:
                    outputSegment.fields = segment.pmmlOutput.evaluate(segment.pmmlModel.dataContext.get, scores)

                if update and segment.state() is not LOCKED:
                    self.updateSegment(segment, pseudoEvent=True)

            else:
                groupFields = groupsToKeys.keys()
                groupFields.sort()
                for groupField in groupFields:
                    groupValues = groupsToKeys[groupField]
                    for value in groupValues:
                        segment.pmmlModel.dataContext.setOverride({groupField: value}, False)
                        if score:
                            if segment.state() is MATURE or segment.state() is LOCKED:
                                scores = segment.consumerAlgorithm.score(self.pseudoEventNumber, segment.pmmlModel.dataContext.get)
                            else:
                                scores = IMMATURE
                        else:
                            scores = INVALID

                        outputMiniEvent = OutputMiniEvent()
                        if segment.pmmlOutput is not None:
                            outputMiniEvent.fields = segment.pmmlOutput.evaluate(segment.pmmlModel.dataContext.get, scores)

                        segment.pmmlModel.dataContext.releaseOverride()

                        if segment.consumerAlgorithm.pseudoOutputAll:
                            outputSegment.minievents.append(outputMiniEvent)

                    if not segment.consumerAlgorithm.pseudoOutputAll:
                        outputSegment.minievents.append(outputMiniEvent)

                    if update and segment.state() is not LOCKED:
                        self.updateSegment(segment, pseudoEvent=True)

            outputRecord.segments.append(outputSegment)

            # clear all of the aggregates
            for aggregate in segment.aggregates:
                aggregate.flush()

        return outputRecord

    ############################################# _getMatchCandidates
    def _getMatchCandidates(self):
        matchCandidates = set()

        get = self.pmmlModel.dataContext.get

        for group, lookup in self._lookup.tuples.iteritems():
            # group is a pair of tuples of field names: ((field_names_that_match_exactly), (field_names_that_match_a_range))
            # lookup is a dictionary; the keys are either tuples of the exact matches or MATCHRANGES
            if len(group[0]) == 0:
                matcher = ()
                intermediateCandidates = []
                keepGoing = True
            else:
                matcher = hash(tuple([get(field) for field in group[0]]))
                if matcher in lookup:
                    intermediateCandidates = lookup[matcher]
                    keepGoing = True
                else:
                    intermediateCandidates = []
                    keepGoing = False

            if keepGoing:
                if MATCHRANGES in lookup:
                    # Each partition in each field maps to a list of candidate indices.  Intersect the intermediate
                    # candidates with these; the intersection of all is the candidate.
                    rangeIterator = lookup[MATCHRANGES].iteritems()
                    field, partitions = rangeIterator.next()
                    matcher = get(field)
                    partitionedCandidates = set()

                    map(lambda x: partitionedCandidates.update(x),
                        [indices for partition, indices in partitions.iteritems() if _segmentHelpers.matchesPartition(matcher, partition)])
                    try:
                        rangeIterator.next()
                    except StopIteration:
                        pass

                    for field, partitions in rangeIterator:
                        matcher = get(field)
                        for partition, indices in partitions.iteritems():

                            didntMatch = True
                            if _segmentHelpers.matchesPartition(matcher, partition):
                                partitionedCandidates.intersection_update(indices)
                                didntMatch = False
                                break

                        if didntMatch:
                            partitionedCandidates = set()
                            break

                    if intermediateCandidates:
                        partitionedCandidates.intersection_update(intermediateCandidates)

                    intermediateCandidates = partitionedCandidates

                matchCandidates.update(intermediateCandidates)

        for field, lookup in self._lookup.fields.iteritems():
            matcher = hash(get(field))
            if matcher in lookup:
                matchCandidates.update(lookup[matcher])

        matchCandidates.update(self._lookup.other)

        matchCandidates = list(matchCandidates)
        matchCandidates.sort()

        return matchCandidates

    ################################################ _makeSegmentRecord
    def _makeSegmentRecord(self, pmmlSegment, autoSegment=False):
        pmmlPredicate, pmmlSubModel = pmmlSegment.matches(pmml.nonExtension)

        originalId = pmmlSegment.attrib.get("id", None)
        segmentRecord = SegmentRecord(pmmlSubModel, pmmlPredicate, self.pmmlOutput, self, originalId)
        if originalId is None:
            pmmlSegment.attrib["id"] = segmentRecord.name()

        modelClass = pmmlSubModel.__class__
        algoName = self.producerAlgorithm[modelClass.__name__].attrib["algorithm"]
        segmentRecord.consumerAlgorithm = consumerAlgorithmMap[modelClass](self, segmentRecord)
        segmentRecord.producerAlgorithm = producerAlgorithmMap[modelClass, algoName](self, segmentRecord)
        segmentRecord.producerParameters = self.producerAlgorithm[modelClass.__name__].parameters
        self.setProvenance(pmmlSubModel, algoName, segmentRecord.producerAlgorithm, segmentRecord.producerParameters)

        localTransformations = pmmlSubModel.child(pmml.LocalTransformations, exception=False)
        if localTransformations is not None:
            segmentRecord.aggregates = localTransformations.matches(pmml.Aggregate, maxdepth=None)
            segmentRecord.aggregates.extend(localTransformations.matches(pmml.X_ODG_AggregateReduce, maxdepth=None))
        else:
            segmentRecord.aggregates = []

        for aggregate in segmentRecord.aggregates:
            aggregate.initialize(self.consumerUpdateScheme)

        index = len(self.segmentRecords)
        added = False

        wantFastLookup = True
        if wantFastLookup:

            if _segmentHelpers.isSimpleEqual(pmmlPredicate):
                allSimpleEquals = set([pmmlPredicate])
                isOr = False
                compoundAnds = []
            elif _segmentHelpers.isCompoundAnd(pmmlPredicate):
                allSimpleEquals = set()
                compoundAnds = [pmmlPredicate]
                isOr = False
            elif isinstance(pmmlPredicate, pmml.pmmlFalse):
                allSimpleEquals = []
                compoundAnds = []
                # If the top level predicate is False, nothing will ever match.
                # Don't even put this in the lookup list (so set added=True).
                added = True
            else:
                allSimpleEquals = set(pmmlPredicate.matches(_segmentHelpers.isSimpleEqual))
                compoundAnds = pmmlPredicate.matches(_segmentHelpers.isCompoundAnd)
                isOr = True
                if len(allSimpleEquals) + len(compoundAnds) != len(pmmlPredicate.children):
                    allSimpleEquals = compoundAnds = []

            for element in compoundAnds:
                if isOr: added = False
                elif added: break
                addEq = {}
                addComp = {}

                if element.child(pmml.pmmlTrue, exception=False) or element.child(pmml.pmmlFalse, exception=False):
                    # True short-circuits all matches; put it in the slow-lookup list self._lookup.other
                    allSimpleEquals = []
                    added = False
                    break

                simpleEquals = element.matches(_segmentHelpers.isSimpleEqual)
                if simpleEquals:
                    addEq = dict([(x['field'], x['value']) for x in simpleEquals])

                def matchesAddEq(x, y):
                    return x and y in addEq

                simpleComparators = element.matches(_segmentHelpers.isComparator)
                if len(simpleComparators):
                    for s in simpleComparators:
                        field = s['field']
                        lowerBound = s['operator'].startswith("g")  # greater
                        func = _segmentHelpers[s['operator']]
                        val = s['value']
                        if field not in addComp:
                            if lowerBound:
                                addComp[field] = ((val, func), (None, None))
                            else:
                                addComp[field] = ((None, None), (val, func))
                        else:
                            if lowerBound:
                                addComp[field] = ((val, func), addComp[field][1])
                            else:
                                addComp[field] = (addComp[field][0], (val, func))
                elif not len(simpleEquals):
                    # If any of the compound ands have neither an equals nor a comparator
                    # the entire predicate has to be added to the slow-lookup list self._lookup.other
                    break

                def matchesAddComp(x, y):
                    return x and y in addComp

                for eqTuple, compTuple in self._lookup.tuples.keys():
                    if len(addEq) == len(eqTuple) and \
                        len(addComp) == len(compTuple) and \
                        reduce(matchesAddEq, eqTuple, True) and \
                        reduce(matchesAddComp, compTuple, True):

                        match = hash(tuple([addEq[key] for key in eqTuple]))
                        self._lookup.tuples[(eqTuple, compTuple)].setdefault(match,[]).append(index)

                        if len(compTuple):
                            d = self._lookup.tuples[(eqTuple, compTuple)].setdefault(MATCHRANGES,{})
                            for field, tup in addComp.iteritems():
                                if field not in d:
                                    d[field] = {tup:[index]}
                                else:
                                    d[field].setdefault(tup, []).append(index)
                        added = True
                        break

                if not added:
                    compTuple = tuple(addComp.keys())
                    if len(addEq):
                        eqTuple, match = zip(*[[k,v] for k,v in addEq.iteritems()])
                        match = hash(match)
                        self._lookup.tuples[(eqTuple, compTuple)] = {match:[index]}
                    else:
                        eqTuple = ()

                    d = self._lookup.tuples.setdefault((eqTuple, compTuple), {})

                    if len(compTuple):

                        d = d.setdefault(MATCHRANGES, {})
                        for field, tup in addComp.iteritems():
                            d[field] = {tup:[index]}
                    added = True

            for element in allSimpleEquals:
                field = element['field']
                value = hash(element['value'])
                lookup = self._lookup.fields.setdefault(field, {})
                lookup.setdefault(value, []).append(index)
                added = True
        
        if not added:
            self._lookup.other.append(index)

        if self.firstSegment:
            self.metadata.data["Total segments"] = 0
            self.metadata.data["New segments created"] = 0
            self.metadata.data["Average aggregations per segment"] = len(segmentRecord.aggregates)
            self.metadata.data["Average predicates per segment"] = (1.0 + len(pmmlPredicate.matches(lambda x: True, maxdepth=None)))
            self.metadata.data["Average local transformations per segment"] = \
                len(segmentRecord.pmmlModel.dataContext.cast) - len(self.pmmlModel.dataContext.cast)
            self.firstSegment = False
            self.metadata.data["First segment model type"] = segmentRecord.pmmlModel.tag

        self.segmentRecords.append(segmentRecord)
        if autoSegment:
            segmentRecord.initialize(existingSegment=False, customProcessing=self.customProcessing, setModelMaturity=(not self.hasProducer))
            self.pmmlFile.subModels.append(segmentRecord.pmmlModel)
            if self.customProcessing is not None:
                self.customProcessing.allSegments.append(segmentRecord.userFriendly)
            self.metadata.data["New segments created"] += 1
            self.metadata.info("New segment created: %s, ID=%s" % (segmentRecord.expressionTree, segmentRecord.name()))
        segmentRecord.pmmlModel.dataContext.clear()

        self.metadata.data["Total segments"] += 1
        increment = 1.0 / float(self.metadata.data["Total segments"])
        self.metadata.data["Average aggregations per segment"] *= (1.0 - increment)
        self.metadata.data["Average aggregations per segment"] += len(segmentRecord.aggregates) * increment
        self.metadata.data["Average predicates per segment"] *= (1.0 - increment)
        self.metadata.data["Average predicates per segment"] += (1.0 + len(pmmlPredicate.matches(lambda x: True, maxdepth=None))) * increment
        self.metadata.data["Average local transformations per segment"] *= (1.0 - increment)
        self.metadata.data["Average local transformations per segment"] += \
            (len(segmentRecord.pmmlModel.dataContext.cast) - len(self.pmmlModel.dataContext.cast)) * increment

        return segmentRecord
