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

"""This module defines the DataTablePlots class."""

import sys

from augustus.core.OrderedDict import OrderedDict

class DataTablePlots(OrderedDict):
    """DataTablePlots is a dictionary mapping plot names to SvgBinding
    representations of the SVG images.
    """

    @property
    def name(self):
        return self._name

    def __init__(self, *args, **kwds):
        self._name = "plots"
        super(DataTablePlots, self).__init__(*args, **kwds)

    def __repr__(self):
        return "<DataTable.%s %d records at 0x%x>" % (self.name, len(self), id(self))

    def look(self, stream=None):
        """An informative representation of the DataTablePlots,
        intended for interactive use.

        @type stream: file-like object or None
        @param stream: If None, print to C{sys.stdout}; otherwise, write to the specified stream.

        @todo: This has not been implemented.
        """

        if stream is None:
            stream = sys.stdout

        # TODO
        pass
