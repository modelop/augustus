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

"""This module defines the PlotCoordinatesWindow class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.plot.PlotCoordinates import PlotCoordinates

class PlotCoordinatesWindow(PlotCoordinates):
    """PlotCoordinatesWindow is a plot coordinate system that maps an
    (xmin, ymin), (xmax, ymax) box to an (x, y), (x + width, y +
    height) box, possibly with logarithmic axes, and possibly flipping
    the x or y direction.

    When used in a PlotWindow, C{flipy} must usually be True, so that
    positive y is up in the plot's coordinates but down in the SVG
    file.

    This is often used in conjunction with PlotRange, as
    part of the "prepare" and "draw" stages of plotting.  PlotRange
    accumulates requested ranges from a set of overlapping plot
    graphics (the "prepare" stage) and reports a bounding box slightly
    wider than the union of all of the requested intervals (ignoring
    negative values if a logarithmic axis is requested).  This
    bounding box is used to construct a PlotCoordinatesWindow, and the
    same plots are then drawn in the common window (the "draw" stage).
    """

    def __init__(self, parent, xmin, ymin, xmax, ymax, x, y, width, height, flipx=False, flipy=False, xlog=False, ylog=False, xfieldType=None, yfieldType=None, xstrings=None, ystrings=None):
        """Initialize a PlotCoordinatesWindow.

        @type parent: PlotCoordinates or None
        @param parent: The enclosing coordinate system.
        @type xmin: number
        @param xmin: The minimum x (left edge) within the coordinate system.
        @type ymin: number
        @param ymin: The minimum y (bottom edge, if C{flipy} is True) within the coordinate system.
        @type xmax: number
        @param xmax: The maximum x (right edge) within the coordinate system.
        @type ymax: number
        @param ymax: The maximum y (top edge, if C{flipy} is True) within the coordinate system.
        @type x: number
        @param x: The left edge of the box in the enclosing coordinate system.
        @type y: number
        @param y: The top edge of the box in the enclosing coordinate system.
        @type width: number
        @param width: The width of the box in the enclosing coordinate system.
        @type height: number
        @param height: The height of the box in the enclosing coordinate system.
        @type flipx: bool
        @param flipx: If True, flip the box horizontally (I don't foresee a reason to do this).
        @type flipy: bool
        @param flipy: If True, flip the box vertically (this usually must be done once in the stack of coordinate systems from the plot coordinates, where positive y is up, to the global SVG coordinates, where positive y is down).
        @type xlog: bool
        @param xlog: If True, distort the horizontal coordinates logarithmically; if False, use a linear interpolation.  Note that displaying different log bases (2 and 10 being the most common) are configured in the PlotTickMarks, not here.
        @type ylog: bool
        @param ylog: If True, distort the vertical coordinates logarithmically; if False, use a linear interpolation.  Note that displaying different log bases (2 and 10 being the most common) are configured in the PlotTickMarks, not here.
        @type xfieldType: FieldType or None
        @param xfieldType: Sets the data type of the x axis (e.g. number, date, categorical), which is used as a hint when drawing the tick marks.
        @type yfieldType: FieldType or None
        @param yfieldType: Sets the data type of the y axis (e.g. number, date, categorical), which is used as a hint when drawing the tick marks.
        @type xstrings: list of strings or None
        @param xstrings: Sets the values and order of a set of categorical labels, which is used as a hint when drawing tick marks.
        @type ystrings: list of strings or None
        @param ystrings: Sets the values and order of a set of categorical labels, which is used as a hint when drawing tick marks.
        """

        if xmin >= xmax:
            raise defs.PmmlValidationError("Plot xmin (%g) is greater than xmax (%g)" % (xmin, xmax))
        if ymin >= ymax:
            raise defs.PmmlValidationError("Plot ymin (%g) is greater than ymax (%g)" % (ymin, ymax))

        if xlog and xmin <= 0.0:
            raise defs.PmmlValidationError("Plot xmin cannot be non-positive with xlog")
        if ylog and ymin <= 0.0:
            raise defs.PmmlValidationError("Plot ymin cannot be non-positive when ylog")

        if width <= 0.0 or height <= 0.0:
            raise defs.PmmlValidationError("Plot width or height is less than zero: width=%g height=%g" % (width, height))

        self.innerX1 = xmin
        self.innerY1 = ymin
        self.innerX2 = xmax
        self.innerY2 = ymax

        if flipx:
            self.outerX1 = x + width
            self.outerX2 = x
        else:
            self.outerX1 = x
            self.outerX2 = x + width

        if flipy:
            self.outerY1 = y + height
            self.outerY2 = y
        else:
            self.outerY1 = y
            self.outerY2 = y + height

        self.xlog = xlog
        self.ylog = ylog

        self.outerXPlusInfinity = x + (-1.0 if flipx else 1.0) * defs.INFINITY * width
        self.outerXMinusInfinity = x - (-1.0 if flipx else 1.0) * defs.INFINITY * width
        self.outerYPlusInfinity = y - (-1.0 if flipy else 1.0) * defs.INFINITY * height
        self.outerYMinusInfinity = y + (-1.0 if flipy else 1.0) * defs.INFINITY * height

        if xlog:
            self._fx = self._logX
        else:
            self._fx = self._linearX

        if ylog:
            self._fy = self._logY
        else:
            self._fy = self._linearY

        self.xfieldType = xfieldType
        self.yfieldType = yfieldType
        self.xstrings = xstrings
        self.ystrings = ystrings

        super(PlotCoordinatesWindow, self).__init__(parent)

    def _linearX(self, x):
        """Used by __call__."""

        return NP(self.outerX1 + NP(NP(NP(x - self.innerX1)/NP(self.innerX2 - self.innerX1)) * NP(self.outerX2 - self.outerX1)))

    def _logX(self, x):
        """Used by __call__."""

        x = NP(self.outerX1 + NP(NP(NP("log10", x) - NP("log10", self.innerX1))/NP(NP("log10", self.innerX2) - NP("log10", self.innerX1)) * NP(self.outerX2 - self.outerX1)))
        if isinstance(x, NP.ndarray):
            x[NP("isnan", x)] = self.outerXMinusInfinity
        else:
            if NP("isnan", x):
                x = self.outerXMinusInfinity
        return x

    def _linearY(self, y):
        """Used by __call__."""

        return NP(self.outerY1 + NP(NP(NP(y - self.innerY1)/NP(self.innerY2 - self.innerY1)) * NP(self.outerY2 - self.outerY1)))

    def _logY(self, y):
        """Used by __call__."""

        y = NP(self.outerY1 + NP(NP(NP("log10", y) - NP("log10", self.innerY1))/NP(NP("log10", self.innerY2) - NP("log10", self.innerY1)) * NP(self.outerY2 - self.outerY1)))
        if isinstance(y, NP.ndarray):
            y[NP("isnan", y)] = self.outerYMinusInfinity
        else:
            if NP("isnan", y):
                y = self.outerYMinusInfinity
        return y

    def __repr__(self):
        xlog = " xlog" if self.xlog else ""
        ylog = " ylog" if self.ylog else ""
        return "<PlotCoordinatesWindow ((%g %g), (%g %g) -> ((%g %g), (%g %g))%s%s at 0x%x>" % (self.innerX1, self.innerY1, self.innerX2, self.innerY2, self.outerX1, self.outerY1, self.outerX2, self.outerY2, xlog, ylog, id(self))

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

        x, y = self._fx(x), self._fy(y)

        if isinstance(x, NP.ndarray):
            infinite = NP("isinf", x)
            minusInfinity = NP("logical_and", infinite, NP(x < 0.0))
            x[infinite] = self.outerYPlusInfinity
            x[minusInfinity] = self.outerYMinusInfinity
        else:
            if x == float("inf"):
                x = self.outerYPlusInfinity
            elif x == float("-inf"):
                x = self.outerYMinusInfinity

        if isinstance(y, NP.ndarray):
            infinite = NP("isinf", y)
            minusInfinity = NP("logical_and", infinite, NP(y < 0.0))
            y[infinite] = self.outerYPlusInfinity
            y[minusInfinity] = self.outerYMinusInfinity
        else:
            if y == float("inf"):
                y = self.outerYPlusInfinity
            elif y == float("-inf"):
                y = self.outerYMinusInfinity

        x, y = super(PlotCoordinatesWindow, self).__call__(x, y)
        return x, y
    
