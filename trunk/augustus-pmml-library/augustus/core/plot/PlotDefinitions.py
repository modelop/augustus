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

"""This module defines the PlotDefinitions class."""

import random

from augustus.core.OrderedDict import OrderedDict

class PlotDefinitions(OrderedDict):
    """PlotDefinitions collects SVG elements that appear in the <defs>
    section of the SVG document, using the same "id" attribute for
    each element as the keys in the dictionary.
    """

    def __init__(self, *args, **kwds):
        """Initialize a PlotDefinitions and set the SVG ids of the
        initial contents to their key names.

        @param *args, **kwds: Arguments passed to OrderedDict.
        """

        super(PlotDefinitions, self).__init__(*args, **kwds)
        for name, value in self.iteritems():
            value.set("id", name)

    def update(self, dictionary):
        """Add key-value pairs to this dictionary (PlotDefinitions is
        a dictionary) and set the SVG ids of the items to the key
        names.

        @type dictionary: dict
        @param dictionary: Dictionary to merge into this one.
        """

        output = super(PlotDefinitions, self).update(dictionary)
        for name, value in dictionary.items():
            value.set("id", name)
        return output

    def uniqueName(self):
        """Create a unique name for a new element in the <defs> section.

        @rtype: string
        @return: A new name to use as a unique SVG id.
        """

        bigNumber = random.randint(0, 1000000)
        name = "Untitled_%06d" % bigNumber
        while name in self:
            bigNumber = random.randint(0, 1000000)
            name = "Untitled_%06d" % bigNumber
        return name
