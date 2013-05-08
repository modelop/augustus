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

"""Represents a segment, maintains the producer and consumer algorithms for this segment, and has pointers to everything relevant.  Maintained by Engine."""

from augustus.algorithms.eventweighting import COUNT
import augustus.core.pmml41 as pmml
from augustus.core.defs import Atom, IMMATURE, MATURE, LOCKED, UNINITIALIZED, NameSpace, NameSpaceReadOnly

SELECTFIRST = Atom("SelectFirst")
SELECTALL = Atom("SelectAll")
SELECTONLY = Atom("SelectOnly")

########################################################### SegmentRecord

class SegmentNameRegistry(object):
    """Give new segments IDs if they don't already have one and
    enforces uniqueness of IDs.

    Only one object of this class should exist.
    Maintains a double-referenced lookup table (dict <-> dict).
    """

    def __init__(self):
        """Called by SegmentRecord (the class, not an instance) when Python starts up."""

        self.nameToObject = {}
        self.objectToName = {}
        self.counter = 0

    def newName(self):
        """Create a new name; called if a segment isn't given a name by the PMML file."""

        self.counter += 1
        return "Untitled-%d" % self.counter

    def register(self, name, obj):
        """Attempt to give segment 'obj' a name; auto-generate one if 'name' is None."""

        if name is None:
            # if we are not given an explicit name, auto-generate a
            # name that doesn't already exist
            while True:
                name = self.newName()
                if name not in self.nameToObject:
                    break

        else:
            # if we are given an explicit name, that name must have
            # priority over auto-generated names (for users' sanity)
            if name in self.nameToObject:
                newName = self.newName()
                oldObj = self.nameToObject[name]

                self.nameToObject[newName] = oldObj
                self.objectToName[oldObj] = newName

        self.nameToObject[name] = obj
        self.objectToName[obj] = name

    def unregister(self, obj):
        """Remove obj from the lookup table.

        Probably only ever called when Python shuts down, if at all."""

        name = self.objectToName[obj]
        del self.nameToObject[name]
        del self.objectToName[obj]

class SegmentRecord(object):
    """Represent a segment from a PMML file (outside of pmml.py).

    Maintains the ProducerAlgorithm, the ConsumerAlgorithm, and a copy
    of the OutputFields (MiningModel's <Output> and the segment's
    <Output> are concatenated).

    Has pointers to just about everything: the PMML snippet, the
    predicate, the engine...

    The maturity counter (maintained by self.updator) represents the
    number of producer calls that were *successful*.
    """

    maturityThreshold = 10
    lockingThreshold = 10
    segmentNameRegistry = SegmentNameRegistry()

    def __init__(self, pmmlModel, pmmlPredicate, parentPmmlOutput, engine, name=None):
        """Called by Engine when a PMML file is loaded or when a new
        segment is observed."""

        self.pmmlModel = pmmlModel

        self.pmmlPredicate = pmmlPredicate
        if pmmlPredicate is not None:
            streamlined = True
            if pmmlPredicate.exists(lambda x: isinstance(x, pmml.CompoundPredicate) and x.attrib["booleanOperator"] == "surrogate", maxdepth=None):
                streamlined = False
            self.predicateMatches = pmmlPredicate.createTest(streamlined)
            self.expressionTree = pmmlPredicate.expressionTree()
        else:
            self.predicateMatches = None
            self.expressionTree = None

        # merge this <Output> section with the parent's
        thisOutput = pmmlModel.child(pmml.Output, exception=False)
        if thisOutput is None:
            self.pmmlOutput = parentPmmlOutput
        elif parentPmmlOutput is None:
            self.pmmlOutput = thisOutput
        else:
            self.pmmlOutput = parentPmmlOutput.copy()
            for outputField in thisOutput.matches(pmml.OutputField):
                self.pmmlOutput.children.append(outputField)
            self.pmmlOutput.validate()

        self.engine = engine
        self.segmentNameRegistry.register(name, id(self))

        # make an X-ODG-ModelMaturity object to keep track of how many updates this segment has seen
        pmmlExtension = self.pmmlModel.child(pmml.Extension, exception=False)
        if pmmlExtension is not None:
            self.pmmlModelMaturity = pmmlExtension.child(pmml.X_ODG_ModelMaturity, exception=False)
            if self.pmmlModelMaturity is None:
                self.pmmlModelMaturity = pmml.X_ODG_ModelMaturity(numUpdates=0, locked=self.engine.lockAllSegments)
                pmmlExtension.children.append(self.pmmlModelMaturity)
            elif self.engine.lockAllSegments:
                # Always lock if the user asked for it in the configuration file
                self.pmmlModelMaturity.attrib["locked"] = True
        else:
            pmmlExtension = pmml.Extension()
            self.pmmlModelMaturity = pmml.X_ODG_ModelMaturity(numUpdates=0, locked=self.engine.lockAllSegments)
            pmmlExtension.children.append(self.pmmlModelMaturity)
            self.pmmlModel.children.insert(0, pmmlExtension)

    def initialize(self, existingSegment=False, customProcessing=None, setModelMaturity=False):
        """Initialize the consumer, the producer, and start the
        maturity count."""

        self.updator = self.engine.producerUpdateScheme.updator(COUNT)   # use the producer's UpdateScheme
        if not existingSegment:
            self.lock = False
            self.pmmlModelMaturity.attrib["locked"] = False
        else:
            if setModelMaturity or ("updateExisting" in self.producerParameters and self.producerParameters["updateExisting"] is True):
                self.updator.initialize({COUNT: self.pmmlModelMaturity.attrib["numUpdates"]})
            self.lock = self.pmmlModelMaturity.attrib["locked"]

        self.consumerAlgorithm.initialize()
        self.producerAlgorithm.initialize(**self.producerParameters)

        self.constants = self.pmmlModel.child(pmml.Extension, exception=False)
        if self.constants is None:
            self.constants = NameSpaceReadOnly()
        else:
            self.constants = self.constants.child(pmml.X_ODG_CustomProcessingConstants, exception=False)
            if self.constants is None:
                self.constants = NameSpaceReadOnly()
            else:
                self.constants = self.constants.nameSpace

        self.userFriendly = getattr(self, "userFriendly", Segment.__new__(Segment))
        self.userFriendly.name = self.name()
        self.userFriendly.pmmlPredicate = self.pmmlPredicate
        self.userFriendly.expression = self.expressionTree
        self.userFriendly.evaluate = self.predicateMatches
        self.userFriendly.pmmlModel = self.pmmlModel
        self.userFriendly.consumer = self.consumerAlgorithm
        self.userFriendly.producer = self.producerAlgorithm
        self.userFriendly.const = self.constants
        self.userFriendly.state = self.state

        if customProcessing is not None:
            db = customProcessing.persistentStorage.db
            if self.userFriendly.name not in db:
                db[self.userFriendly.name] = NameSpace()
            self.userFriendly.db = db[self.userFriendly.name]

    def state(self):
        """Return the current state (UNINITIALIZED -> IMMATURE ->
        MATURE -> LOCKED)."""

        if not hasattr(self, "lock"):
            return UNINITIALIZED
        elif self.lock:
            return LOCKED
        elif self.updator.count() >= self.maturityThreshold:
            return MATURE
        else:
            return IMMATURE

    def update(self, syncNumber):
        """Update the maturity counter.

        Only called (by Engine) if the producer algorithm was
        *successful*.
        """

        self.updator.increment(syncNumber, 0.)
        if self.lockingThreshold and self.updator.count() >= self.lockingThreshold:
            self.lock = True

        self.pmmlModelMaturity.attrib["numUpdates"] = self.updator.count()
        if self.lock:
            self.pmmlModelMaturity.attrib["locked"] = True

    def name(self):
        """Return the segment id (or auto-assigned segment id)."""

        return self.segmentNameRegistry.objectToName[id(self)]

    def __repr__(self):
        return "<SegmentRecord id=\"%s\" (%s) at 0x%02x>" % (self.name(), str(self.state()).lower(), id(self))

    def __del__(self):
        self.segmentNameRegistry.unregister(id(self))

class Segment(object):
    """A user-friendly version of the SegmentRecord, presenting only what is needed to access the data."""

    def __repr__(self):
        return "<Segment \"%s\">" % self.name
