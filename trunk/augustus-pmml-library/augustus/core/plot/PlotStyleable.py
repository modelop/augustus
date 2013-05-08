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

"""This module defines the PlotStylable class."""

from augustus.core.defs import defs
from augustus.core.plot.PlotStyle import PlotStyle

class PlotStyleable(object):
    """Base class for all plotting elements that have a CSS style
    attribute.

    The style of the plotting element can be manipulated in the same
    way that Javascript manipulates the CSS of style elements in web
    browsers::

        plot.style["stroke-width"] = "3.0"

    is equivalent to::

        plot.style.strokeWidth = 3.0

    Non-strings are automatically converted to strings, but strings
    are not automatically converted to values.  Camel-case in
    attribute access is converted to hyphens, following the
    Javascript/CSS convention.

    @type styleProperties: list of strings
    @param styleProperties: Exhaustive list of legal style properties.
    @type styleDefaults: dict
    @param styleDefaults: Map from style property names to their default values (as strings).
    """

    styleProperties = []
    styleDefaults = {}

    @property
    def style(self):
        s = self.get("style")
        if s is None:
            s = self.get("style", defaultFromXsd=True)
            self.set("style", s)
        return PlotStyle(self)

    @style.setter
    def style(self, value):
        self.set("style", PlotStyle.toString(value))

    @style.deleter
    def style(self):
        self.set("style", "")

    def checkStyleProperties(self):
        """Verify that all properties currently requested in the
        C{style} attribute are in the legal C{styleProperties} list.

        @raise PmmlValidationError: If the list contains an unrecognized style property name, raise an error.  Otherwise, silently pass.
        """

        style = self.get("style")
        if style is not None:
            for name in PlotStyle.toDict(style).keys():
                if name not in self.styleProperties:
                    raise defs.PmmlValidationError("Unrecognized style property: \"%s\"" % name)

    def getStyleState(self):
        """Get the current state of the style (including any
        unmodified defaults) as a dictionary.

        @rtype: dict
        @return: Dictionary mapping style property names to their values (as strings).
        """

        style = dict(self.styleDefaults)
        currentStyle = self.get("style")
        if currentStyle is not None:
            style.update(PlotStyle.toDict(currentStyle))
        return style
