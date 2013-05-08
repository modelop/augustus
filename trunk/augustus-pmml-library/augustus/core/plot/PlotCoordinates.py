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

"""This module defines the PlotCoordinates class."""

class PlotCoordinates(object):
    """PlotCoordinates is the abstract base class for plot coordinate
    systems.

    Transformations are applied by calling an instance::

        X, Y = plotCoordinates(x, y): transforms the point (x, y)

    Coordinate systems can be nested.  Calling a nested chain of
    PlotCoordinates A-B-C (with C being the innermost) automatically
    executes::

        X, Y = A(*B(*C(x, y)))

    so that C takes you from x, y to B's internal system, B takes you
    to A's internal system, and A takes you to global coordinates.
    Most often, these are used in nested boxes, so that a box and its
    contents act as a connected unit as you change one of the
    outermost coordinate systems.  Only values from the global
    coordinate system are actually written to the SVG file.
    """

    def __init__(self, parent=None):
        """Initialize a PlotCoordinates.

        @type parent: PlotCoordinates or None
        @param parent: The enclosing coordinate system.
        """

        self._parent = parent

    def __repr__(self):
        return "<PlotCoordinates at 0x%x>" % id(self)

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

        if self._parent is not None:
            x, y = self._parent(x, y)
        return x, y
