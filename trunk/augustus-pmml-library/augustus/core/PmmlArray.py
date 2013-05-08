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

"""This module defines the PmmlArray class."""

from augustus.core.PmmlBinding import PmmlBinding

class PmmlArray(PmmlBinding):
    """PmmlArray is an abstract base class for explicit and sparse arrays.
    """

    def values(self, convertType=False):
        """Extract values from the PMML and represent them in a
        Pythonic form.

        @type convertType: bool
        @param convertType: If False, return a list of strings; if True, convert the type of the values.
        @rtype: list
        @return: List of values.
        """
        raise NotImplementedError("Subclasses of PmmlArray must implement values()")

    def __repr__(self):
        if self["type"] == "string":
            return "<%s [%s] at 0x%x>" % (self.t, ", ".join("\"%s\"" % v.replace('"', r'\"') for v in self.values()), id(self))
        else:
            return "<%s [%s] at 0x%x>" % (self.t, ", ".join("%g" % v for v in self.values(convertType=True)), id(self))
