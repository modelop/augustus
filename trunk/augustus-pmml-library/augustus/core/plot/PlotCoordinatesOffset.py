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

"""This module defines the PlotCoordinatesOffset class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.plot.PlotCoordinates import PlotCoordinates

class PlotCoordinatesOffset(PlotCoordinates):
    """PlotCoordinatesOffset is a plot coordinate system that consists
    of a simple offset with respect to the parent's coordinates.
    """

    def __init__(self, parent, xoffset, yoffset):
        """Initialize a PlotCoordinatesOffset.

        @type parent: PlotCoordinates or None
        @param parent: The enclosing coordinate system.
        @type xoffset: number
        @param xoffset: The horizontal translation.
        @type yoffset: number
        @param yoffset: The vertical translation.
        """

        self.xoffset = xoffset
        self.yoffset = yoffset
        super(PlotCoordinatesOffset, self).__init__(parent)

    def __repr__(self):
        return "<PlotCoordinatesOffset xoffset=%g yoffset=%g at 0x%x>" % (self.xoffset, self.yoffset, id(self))

    def __call__(self, x, y):
        """Transform the point x, y from this inner coordinate system
        all the way out to the outermost global coordinates, the
        coordinates of the SVG file.

        @type x: number
        @param x: The horizontal position in this coordinate system.
        @type y: number
        @param y: The vertical position in this coordinate system.
        @rtype: 2-tuple of numbers
        @return: The X, Y position in the outermost global coordinates.
        """

        if not isinstance(x, (NP.ndarray, NP.double)):
            x = NP.double(x)
        if not isinstance(y, (NP.ndarray, NP.double)):
            y = NP.double(y)

        x, y = self.xoffset + x, self.yoffset + y
        x, y = super(PlotCoordinatesOffset, self).__call__(x, y)
        return x, y
