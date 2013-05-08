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

"""Basic definitions for all algorithms and associated code (e.g. eventweighting)."""

import logging

class ConsumerAlgorithm(object):
    """Base class for all consumer algorithms."""

    def __init__(self, engine, segmentRecord):
        self.engine = engine
        self.segmentRecord = segmentRecord
        self.lastScore = None
        
        self.logger = logging.getLogger()
        self.metadata = logging.getLogger("metadata")

    def resetLoggerLevels(self):
        self.logWarning = self.logger.getEffectiveLevel() <= logging.WARNING
        self.logInfo = self.logger.getEffectiveLevel() <= logging.INFO
        self.logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG

class ProducerAlgorithm(object):
    """Base class for all producer algorithms."""

    def __init__(self, engine, segmentRecord):
        self.engine = engine
        self.segmentRecord = segmentRecord

        self.logger = logging.getLogger()
        self.metadata = logging.getLogger("metadata")

    def resetLoggerLevels(self):
        self.logWarning = self.logger.getEffectiveLevel() <= logging.WARNING
        self.logInfo = self.logger.getEffectiveLevel() <= logging.INFO
        self.logDebug = self.logger.getEffectiveLevel() <= logging.DEBUG
