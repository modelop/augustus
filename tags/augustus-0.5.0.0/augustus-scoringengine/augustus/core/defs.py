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

import datetime
import logging
import random

########################################################### Atoms

class Atom:
    """Singleton objects for name-based lookup without the overhead of strings.

    Only one copy of each Atom exists, all references are pointers
    (hence the `__copy__` and `__deepcopy__` methods.  Like Python's
    `None` object, they should be checked with the `is` operator
    (object-based equality, rather than value-based equality).

    Example::

       SCORE = Atom("Score")
       SEGMENT = Atom("Segment")

       returnValue = {SCORE: 3.2, SEGMENT: mySegment}

    The components of `returnValue` can be accessed in a
    human-readable way, e.g. `returnValue[SCORE]` and
    `returnValue[SEGMENT]`, but without having to allocate "score" and
    "segment" as strings, and without having to do string comparisons
    to extract the items from the dictionary.
    """

    def __init__(self, name):
        self.name = name
        self.hash = hash("Atom_%d" % id(self))

    def __repr__(self):
        return self.name.upper()

    def __str__(self):
        return self.name

    def __hash__(self):
        return self.hash

    def __copy__(self):
        return self

    def __deepcopy__(self):
        return self

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return not (self == other)

    def __nonzero__(self):
        return False

IMMATURE = Atom("Immature")
MATURE = Atom("Mature")
LOCKED = Atom("Locked")
UNINITIALIZED = Atom("Uninitialized")

INVALID = Atom("Invalid")
MISSING = Atom("Missing")
UNKNOWN = Atom("Unknown")
class InvalidDataError(ValueError): pass

########################################################### Namespace

class NameSpace:
    """A class to allow grouping of related variables.

    Dict-like and object-like interface:

        __init__(self, **init)
        __repr_(self)
        __len__(self)
        __getitem__(self, key)
        __setitem__(self, key, value)
        __delitem__(self, key)
        __iter__(self)
        __contains__(self, item)

    Data attributes:

        None

    Example:

        class SomeAlgorithm:
            def __init__(self):
                self.runOptions = NameSpace(doThingsQuickly=False,
                                            doThingsWell=True)
                self.statConfig = NameSpace(testStatistic='zScore',
                                            mu=3., sigma=1.)
                self.internal = NameSpace()
                self.results = NameSpace()

            def runAlgorithm(self, value):
                if self.runOptions.doThingsQuickly:
                    return 1.2

                if self.runOptions.doThingsWell:
                    self.internal.distFromMu = value - self.statConfig.mu
                    (self.results.score =
                        self.internal.distFromMu / self.statConfig.sigma)
                return self.results.score

        someAlgorithm = SomeAlgorithm()
        print someAlgorithm.runAlgorithm(5.)
        # 2.0

        someAlgorithm.runOptions.doThingsQuickly = True
        print someAlgorithm.runAlgorithm(5.)
        # 1.2

        print someAlgorithm.results
        # <NameSpace score=2.0>

        print dict(someAlgorithm.results)
        # {'score': 2.0}

        print someAlgorithm.results.score, someAlgorithm.results['score']
        # 2.0 2.0
    """
    def __init__(self, **init):
        """Construct a NameSpace with either a list or dictionary of keys.
        
        NameSpace(key1=value1, key2=value2) or
        NameSpace(**{"key1": value1, "key2": value2})."""
        self.__dict__.update(init)

    def __repr__(self):
        """Return a representation of the NameSpace for interactive work."""
        if len(self) == 0:
            return "<NameSpace (empty)>"
        else:
            return "<NameSpace %s>" % " ".join(
                [ "%s=%s" % (x, y) for x, y in self.__dict__.items()])
    
    def __len__(self):
        """Return the number of variables in the NameSpace with
        len(nameSpace)."""
        return len(self.__dict__)
    
    def __getitem__(self, key):
        """Get a value from the NameSpace with nameSpace["key1"] (rather
        than nameSpace.key1)."""
        return self.__dict__[key]
    
    def __setitem__(self, key, value):
        """Set a value in the NameSpace with nameSpace["key1"] = value1
        (rather than nameSpace.key1 = value1)."""
        self.__dict__[key] = value
    
    def __delitem__(self, key):
        """Delete a value in the NameSpace with del nameSpace["key1"]
        (rather than del nameSpace.key1)."""
        del self.__dict__[key]
    
    def __iter__(self):
        """Iterate through key, value pairs or cast a NameSpace as a dict
        with dict(nameSpace)."""
        return iter(self.__dict__.items())
    
    def __contains__(self, item):
        """Return True iff the NameSpace contains item as a key."""
        return item in self.__dict__

