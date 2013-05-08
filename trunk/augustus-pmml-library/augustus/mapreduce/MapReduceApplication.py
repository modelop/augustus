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

"""This module defines the MapReduceApplication base class for all
map-reduce applications.  See the source code."""

class MapReduceApplication(object):
    # These four attributes are updated by MapReduce
    metadata = {}
    iteration = 0
    def emit(self, key, record): pass
    performanceTable = None
    logger = None

    # The rest are to be overloaded by the user
    loggerName = "MapReduceApplication"
    gatherOutput = True
    chain = False
    imports = None
    files = None
    cmdenv = None

    def reportStatus(self, message):
        print "reporter:status:%s" % message

    def beginMapperTask(self):
        pass

    def mapper(self, record):
        pass

    def endMapperTask(self):
        pass
    
    def beginReducerTask(self):
        pass

    def beginReducerKey(self, key):
        pass

    def reducer(self, key, record):
        pass

    def endReducerKey(self, key):
        pass

    def endReducerTask(self):
        pass

    def endIteration(self, outputRecords, outputKeyValues):
        return False
