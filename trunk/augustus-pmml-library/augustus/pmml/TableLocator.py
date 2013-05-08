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

"""This module defines the TableLocator class."""

import glob
import itertools

from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.TableInterface import TableInterface

class TableLocator(PmmlBinding, TableInterface):
    """TableLocator implements an XML table outside the PMML
    document.

    U{PMML specification<http://www.dmg.org/v4-1/Taxonomy.html>}.
    """

    def iterate(self):
        """Iterate over the table.

        @rtype: iterator that yields dicts
        @return: Each item from the iterator is a dictionary mapping tagnames to string values.
        """

        generators = []

        for xmlFileNameGlob in self.xpath("pmml:Extension[@name='xmlFileName']/@value"):
            for xmlFileName in glob.glob(xmlFileNameGlob):
                generators.append(self.iterateOverFile(open(xmlFileName)))

        return itertools.chain(*generators)
