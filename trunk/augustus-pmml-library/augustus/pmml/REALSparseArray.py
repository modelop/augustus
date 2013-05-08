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

"""This module defines the REALSparseArray class."""

import re

from augustus.core.defs import defs
from augustus.core.PmmlArray import PmmlArray

class REALSparseArray(PmmlArray):
    """REALSparseArray implements a sparse array of real-valued constants.

    U{PMML specification<http://www.dmg.org/v4-1/GeneralStructure.html>}.
    """

    def values(self, convertType=False):
        """Extract values from the PMML and represent them in a
        Pythonic form.

        @type convertType: bool
        @param convertType: If False, return a list of strings; if True, convert the type of the values.
        @rtype: list
        @return: List of values.
        """

        n = self.get("n")
        defaultValue = self.get("defaultValue", defaultFromXsd=True, convertType=convertType)
        indices = self.childOfTag("Indices")
        entries = self.childOfTag("REAL-Entries")

        if indices is None:
            indices = []
        else:
            indices = map(int, indices.text.strip().split())

        if entries is None:
            entries = []
        else:
            if convertType:
                entries = map(float, entries.text.strip().split())
            else:
                entries = entries.text.strip().split()
        
        if n is None:
            n = max(indices)
        else:
            n = max(int(n), max(indices))
        
        output = [defaultValue] * n
        for index, entry in zip(indices, entries):
            if index - 1 < n:
                output[index - 1] = entry

        return output
