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

"""This module defines the PlotStyle class."""

import re

from augustus.core.defs import defs

class PlotStyle(object):
    """PlotStyle provides Javascript-style access to plot CSS attributes.

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

    A PlotStyle instance is connected to the PlotStyleable that
    created it; changing style properties in the PlotStyle affects the
    PlotStyleable (so that the code example above works).
    """

    @staticmethod
    def toDict(value):
        """Convert a CSS style string into a dictionary.

        @type value: string
        @param value: A string with the form "name1: value1; name2: value2".
        @rtype: dict
        @return: A dictionary that maps style property names to their values.
        """

        if isinstance(value, PlotStyle):
            value = value._parent.get("style")
        if isinstance(value, basestring):
            try:
                return dict([y.strip() for y in x.split(":")] for x in value.split(";") if x.strip() != "")
            except ValueError:
                raise defs.PmmlValidationError("Improperly formatted style string: \"%s\"" % value)
        return value

    @staticmethod
    def toString(value):
        """Convert a style dictionary into a CSS style string.

        @type value: dict
        @param value: A dictionary that maps style property names to their values.
        @rtype: string
        @return: A string with the form "name1: value1; name2: value2".
        """

        if isinstance(value, PlotStyle):
            value = value._parent.get("style")
        if isinstance(value, basestring):
            return value
        return "; ".join(": ".join(map(str, (x, y))) for x, y in value.items())

    def __init__(self, parent):
        self.__dict__["_parent"] = parent

    def __repr__(self):
        return self.toString(self)

    def _hypenToCamelCase(self, name):
        if "-" in name:
            words = name.split("-")
            return words[0] + "".join(x.capitalize() for x in words[1:])
        else:
            return name

    _re_camel_case = re.compile("((^.[^A-Z]+)|([A-Z][^A-Z]+))")
    def _camelCaseToHyphen(self, name):
        return "-".join(x.group(1).lower() if x.group(2) is None else x.group(1) for x in re.finditer(self._re_camel_case, name))

    def __getitem__(self, name):
        return self.toDict(self._parent.get("style")).get(name)

    def __getattr__(self, name):
        return self.__getitem__(self._camelCaseToHyphen(name))

    def __setitem__(self, name, value):
        d = self.toDict(self._parent.get("style"))
        d[name] = str(value)
        self._parent.set("style", self.toString(d))

    def __setattr__(self, name, value):
        self.__setitem__(self._camelCaseToHyphen(name), value)

    def __delitem__(self, name):
        d = self.toDict(self._parent.get("style"))
        del d[name]
        self._parent.set("style", self.toString(d))

    def __delattr__(self, name):
        self.__delitem__(self._camelCaseToHyphen(name))

    def __iter__(self):
        for item in PlotStyle.toDict(self).items():
            yield item
