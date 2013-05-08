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

"""This module defines the Array class."""

import re

from augustus.core.defs import defs
from augustus.core.PmmlArray import PmmlArray

class Array(PmmlArray):
    """Array implements an explicit array of constants.

    U{PMML specification<http://www.dmg.org/v4-1/GeneralStructure.html>}
    """

    _re_word = re.compile(r'("(([^"]|\\")*[^\\])"|""|[^\s"]+)', (re.MULTILINE | re.UNICODE))

    def values(self, convertType=False):
        """Extract values from the PMML and represent them in a
        Pythonic form.

        @type convertType: bool
        @param convertType: If False, return a list of strings; if True, convert the type of the values.
        @rtype: list
        @return: List of values.
        """

        output = []

        if not convertType or self["type"] == "string":
            if self.text is not None:
                for word in re.finditer(self._re_word, self.text):
                    one, two, three = word.groups()
                    if two is not None:
                        output.append(two.replace(r'\"', '"'))
                    elif one == r'""':
                        output.append("")
                    else:
                        output.append(one)

        elif self["type"] == "int":
            if self.text is not None:
                try:
                    output = [int(x) for x in self.text.split()]
                except ValueError as err:
                    raise defs.PmmlValidationError("Array of type int has a badly formatted value: %s" % str(err))

        elif self["type"] == "real":
            if self.text is not None:
                try:
                    output = [float(x) for x in self.text.split()]
                except ValueError as err:
                    raise defs.PmmlValidationError("Array of type real has a badly formatted value: %s" % str(err))

        return output
