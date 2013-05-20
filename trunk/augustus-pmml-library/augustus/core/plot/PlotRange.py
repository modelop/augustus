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

"""This module defines the PlotRange class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP

class PlotRange(object):
    """PlotRange provides a bounding box for plot elements so that
    overlays can zoom to fit all content by default.

    This is often used in conjunction with PlotCoordinatesWindow, as
    part of the "prepare" and "draw" stages of plotting.  PlotRange
    accumulates requested ranges from a set of overlapping plot
    graphics (the "prepare" stage) and reports a bounding box slightly
    wider than the union of all of the requested intervals (ignoring
    negative values if a logarithmic axis is requested).  This
    bounding box is used to construct a PlotCoordinatesWindow, and the
    same plots are then drawn in the common window (the "draw" stage).
    """

    def __init__(self, xStrictlyPositive=False, yStrictlyPositive=False, zStrictlyPositive=False):
        """Initialize the PlotRange, specifying which axes must be
        strictly positive (because they will be drawn on log scales).

        @type xStrictlyPositive: bool
        @param xStrictlyPositive: If True, only accept positive x values.
        @type yStrictlyPositive: bool
        @param yStrictlyPositive: If True, only accept positive y values.
        @type zStrictlyPositive: bool
        @param zStrictlyPositive: If True, only accept positive z values.  The z axis is the color scale used in heat maps.
        """

        self.xmin = None
        self.ymin = None
        self.zmin = None
        self.xmax = None
        self.ymax = None
        self.zmax = None

        self.xminSticky = None
        self.yminSticky = None
        self.zminSticky = None
        self.xmaxSticky = None
        self.ymaxSticky = None
        self.zmaxSticky = None

        self._xStrictlyPositive = xStrictlyPositive
        self._yStrictlyPositive = yStrictlyPositive
        self._zStrictlyPositive = zStrictlyPositive

        self.xstrings = []
        self.xstringsSeen = set()
        self.ystrings = []
        self.ystringsSeen = set()

        self.xfieldType = None
        self.yfieldType = None
        self.zfieldType = None

    @property
    def xStrictlyPositive(self):
        return self._xStrictlyPositive

    @property
    def yStrictlyPositive(self):
        return self._yStrictlyPositive

    @property
    def zStrictlyPositive(self):
        return self._zStrictlyPositive

    def _checkFieldTypeX(self, xfieldType):
        if self.xfieldType is None:
            self.xfieldType = xfieldType
        elif self.xfieldType.isstring() and not xfieldType.isstring():
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))
        elif self.xfieldType.isstring() and self.xfieldType.optype == "ordinal" and self.xfieldType != xfieldType:
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))
        elif self.xfieldType.isboolean() and not xfieldType.isboolean():
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))
        elif self.xfieldType.isnumeric() and not xfieldType.isnumeric():
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))
        elif self.xfieldType.istime() and not xfieldType.istime():
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))
        elif (self.xfieldType.isdate() or self.xfieldType.isdatetime()) and not (xfieldType.isdate() or xfieldType.isdatetime()):
            raise defs.PmmlValidationError("Overlaid x plot axis has conflicting types: %r and %r" % (self.xfieldType, xfieldType))

    def _checkFieldTypeY(self, yfieldType):
        if self.yfieldType is None:
            self.yfieldType = yfieldType
        elif self.yfieldType.isstring() and not yfieldType.isstring():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))
        elif self.yfieldType.isstring() and self.yfieldType.optype == "ordinal" and self.yfieldType != yfieldType:
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))
        elif self.yfieldType.isboolean() and not yfieldType.isboolean():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))
        elif self.yfieldType.isnumeric() and not yfieldType.isnumeric():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))
        elif self.yfieldType.istime() and not yfieldType.istime():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))
        elif (self.yfieldType.isdate() or self.yfieldType.isdatetime()) and not (yfieldType.isdate() or yfieldType.isdatetime()):
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.yfieldType, yfieldType))

    def _checkFieldTypeZ(self, zfieldType):
        if self.zfieldType is None:
            self.zfieldType = zfieldType
        elif self.zfieldType.isstring() and not zfieldType.isstring():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))
        elif self.zfieldType.isstring() and self.zfieldType.optype == "ordinal" and self.zfieldType != zfieldType:
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))
        elif self.zfieldType.isboolean() and not zfieldType.isboolean():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))
        elif self.zfieldType.isnumeric() and not zfieldType.isnumeric():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))
        elif self.zfieldType.istime() and not zfieldType.istime():
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))
        elif (self.zfieldType.isdate() or self.zfieldType.isdatetime()) and not (zfieldType.isdate() or zfieldType.isdatetime()):
            raise defs.PmmlValidationError("Overlaid y plot axis has conflicting types: %r and %r" % (self.zfieldType, zfieldType))

    def xminPush(self, xmin, fieldType, sticky=False):
        """Make the x range of the bounding box larger by (possibly)
        pushing the x minimum lower.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmin and xmax of the plot window
        should align with the xmin and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{xStrictlyPositive} is True, negative C{xmin} values are
        ignored.

        @type xmin: number
        @param xmin: The new C{xmin}, if this C{xmin} is smaller than the currently smallest C{xmin}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of x.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this xmin as a "sticky" xmin.
        @raise PmmlValidationError: If any x FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeX(fieldType)
        if NP("isfinite", xmin) and (not self.xStrictlyPositive or xmin > 0.0) and (self.xmin is None or xmin < self.xmin):
            self.xmin = xmin
            if sticky: self.xminSticky = xmin

    def yminPush(self, ymin, fieldType, sticky=False):
        """Make the y range of the bounding box larger by (possibly)
        pushing the y minimum lower.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmin and xmax of the plot window
        should align with the xmin and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{yStrictlyPositive} is True, negative C{ymin} values are
        ignored.

        @type ymin: number
        @param ymin: The new C{ymin}, if this C{ymin} is smaller than the currently smallest C{ymin}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of y.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this ymin as a "sticky" ymin.
        @raise PmmlValidationError: If any y FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeY(fieldType)
        if NP("isfinite", ymin) and (not self.yStrictlyPositive or ymin > 0.0) and (self.ymin is None or ymin < self.ymin):
            self.ymin = ymin
            if sticky: self.yminSticky = ymin

    def zminPush(self, zmin, fieldType, sticky=False):
        """Make the z range of the bounding box larger by (possibly)
        pushing the z minimum lower.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmin and xmax of the plot window
        should align with the xmin and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{zStrictlyPositive} is True, negative C{zmin} values are
        ignored.

        @type zmin: number
        @param zmin: The new C{zmin}, if this C{zmin} is smaller than the currently smallest C{zmin}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of z.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this zmin as a "sticky" zmin.
        @raise PmmlValidationError: If any z FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeZ(fieldType)
        if NP("isfinite", zmin) and (not self.zStrictlyPositive or zmin > 0.0) and (self.zmin is None or zmin < self.zmin):
            self.zmin = zmin
            if sticky: self.zminSticky = zmin

    def xmaxPush(self, xmax, fieldType, sticky=False):
        """Make the x range of the bounding box larger by (possibly)
        pushing the x maximum higher.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmax and xmax of the plot window
        should align with the xmax and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{xStrictlyPositive} is True, negative C{xmax} values are
        ignored.

        @type xmax: number
        @param xmax: The new C{xmax}, if this C{xmax} is larger than the currently largest C{xmax}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of x.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this xmax as a "sticky" xmax.
        @raise PmmlValidationError: If any x FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeX(fieldType)
        if NP("isfinite", xmax) and (not self.xStrictlyPositive or xmax > 0.0) and (self.xmax is None or xmax > self.xmax):
            self.xmax = xmax
            if sticky: self.xmaxSticky = xmax

    def ymaxPush(self, ymax, fieldType, sticky=False):
        """Make the y range of the bounding box larger by (possibly)
        pushing the y maximum higher.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmin and xmax of the plot window
        should align with the xmin and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{yStrictlyPositive} is True, negative C{ymax} values are
        ignored.

        @type ymax: number
        @param ymax: The new C{ymax}, if this C{ymax} is larger than the currently largest C{ymax}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of y.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this ymax as a "sticky" ymax.
        @raise PmmlValidationError: If any y FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeY(fieldType)
        if NP("isfinite", ymax) and (not self.yStrictlyPositive or ymax > 0.0) and (self.ymax is None or ymax > self.ymax):
            self.ymax = ymax
            if sticky: self.ymaxSticky = ymax

    def zmaxPush(self, zmax, fieldType, sticky=False):
        """Make the z range of the bounding box larger by (possibly)
        pushing the z maximum higher.

        "Sticky" means that the final bounding box will not be
        expanded beyond this value, if it turns out to be the most
        extreme.  This feature is used, for example, in the layout of
        a vertical histogram: the xmin and xmax of the plot window
        should align with the xmin and xmax of a histogram unless an
        overlaying graphic pushes the boundary farther.  The ymax of
        the histogram should be inflated beyond the tallest bin so
        that it can be clearly seen.

        If C{zStrictlyPositive} is True, negative C{zmax} values are
        ignored.

        @type zmax: number
        @param zmax: The new C{zmax}, if this C{zmax} is larger than the currently largest C{zmax}.
        @type fieldType: FieldType
        @param fieldType: The FieldType of z.  Only homogeneous FieldTypes are allowed.
        @type sticky: bool
        @param sticky: Label this zmax as a "sticky" zmax.
        @raise PmmlValidationError: If any z FieldTypes differ, this function will raise an error.
        """

        self._checkFieldTypeZ(fieldType)
        if NP("isfinite", zmax) and (not self.zStrictlyPositive or zmax > 0.0) and (self.zmax is None or zmax > self.zmax):
            self.zmax = zmax
            if sticky: self.zmaxSticky = zmax

    def expand(self, xarray, yarray, xfieldType, yfieldType):
        """Expand the C{xmin}, C{ymin}, C{xmax}, C{ymax} bounds using
        a dataset of x, y points.

        None of these points are considered "sticky."

        The arrays come from typed DataColumns; they might represent a
        categorical dataset, dates, or other not-strictly-numeric
        types.

        If C{xStrictlyPositve} is True, then x, y pairs with a
        negative x are ignored.  If C{yStrictlyPositive} is True, then
        x, y pairs with a negative y are ignored.

        @type xarray: 1d Numpy array
        @param xarray: Array of x values.
        @type yarray: 1d Numpy array
        @param yarray: Array of y values.
        @type xfieldType: FieldType
        @param xfieldType: Data type of x values.
        @type yfieldType: FieldType
        @param yfieldType: Data type of y values.
        @raise PmmlValidationError: If this x FieldType differs from previous x FieldTypes, or this y FieldType differs from previous y FieldTypes, this function with raise an error.
        """

        self._checkFieldTypeX(xfieldType)
        self._checkFieldTypeY(yfieldType)

        if self.xStrictlyPositive and not xfieldType.isnumeric():
            raise defs.PmmlValidationError("Logarithmic x scale can only be used with numeric types, not %r" % xfieldType)
        if xfieldType.dataType == "object":
            raise defs.PmmlValidationError("The x axis is an arbitrary object; only numbers, times, and strings can be plotted")

        if self.yStrictlyPositive and not yfieldType.isnumeric():
            raise defs.PmmlValidationError("Logarithmic y scale can only be used with numeric types, not %r" % xfieldType)
        if yfieldType.dataType == "object":
            raise defs.PmmlValidationError("The y axis is an arbitrary object; only numbers, times, and strings can be plotted")

        selection = None

        if xfieldType.isnumeric() or xfieldType.istemporal():
            if selection is None:
                selection = NP("isfinite", xarray)
            if self.xStrictlyPositive:
                NP("logical_and", selection, NP(xarray > 0.0), selection)

        if yfieldType.isnumeric() or yfieldType.istemporal():
            if selection is None:
                selection = NP("isfinite", yarray)
            if self.yStrictlyPositive:
                NP("logical_and", selection, NP(yarray > 0.0), selection)

        if selection is not None:
            xarray = xarray[selection]
            yarray = yarray[selection]

        if xfieldType.isnumeric() or xfieldType.istemporal():
            if len(xarray) != 0:
                xmin = xarray.min()
                xmax = xarray.max()
            else:
                xmin, xmax = None, None

            if self.xmin is None or (xmin is not None and xmin < self.xmin):
                self.xmin = xmin

            if self.xmax is None or (xmax is not None and xmax > self.xmax):
                self.xmax = xmax

        elif xfieldType.isstring():
            for item in xarray:
                if item not in self.xstringsSeen:
                    self.xstrings.append(item)
                    self.xstringsSeen.add(item)
            if xfieldType.optype == "ordinal":
                self.xstrings.sort(lambda a, b: cmp(xfieldType.stringToValue(a), xfieldType.stringToValue(b)))

        if yfieldType.isnumeric() or yfieldType.istemporal():
            if len(yarray) != 0:
                ymin = yarray.min()
                ymax = yarray.max()
            else:
                ymin, ymax = None, None

            if self.ymin is None or (ymin is not None and ymin < self.ymin):
                self.ymin = ymin

            if self.ymax is None or (ymax is not None and ymax > self.ymax):
                self.ymax = ymax

        elif yfieldType.isstring():
            for item in yarray:
                if item not in self.ystringsSeen:
                    self.ystrings.append(item)
                    self.ystringsSeen.add(item)
            if yfieldType.optype == "ordinal":
                self.ystrings.sort(lambda a, b: cmp(yfieldType.stringToValue(a), yfieldType.stringToValue(b)))

    def zranges(self, zfactor=0.0):
        """Report the bounding interval in the z direction.

        @type zfactor: number
        @param zfactor: The amount to inflate the interval, if the edges are non-sticky, as a fraction of the entire uninflated z interval.
        @rtype: 2-tuple of numbers
        @return: The possibly-inflated C{zmin} and C{zmax}.
        """

        zmin, zmax = self.zmin, self.zmax

        if zfactor > 0.0 and zmin is not None and zmax is not None:
            zmargin = zfactor * (zmax - zmin)
            if zmin != self.zminSticky:
                zmin -= zmargin
            if zmax != self.zmaxSticky:
                zmax += zmargin

        if zmin == zmax and zmin is not None:
            if self.zStrictlyPositive:
                zmin, zmax = zmin/10.0, zmax*10.0
            else:
                zmin, zmax = zmin - 1.0, zmax + 1.0

        return zmin, zmax

    def ranges(self, xfactor=0.2, yfactor=0.2):
        """Report the bounding box in the x, y directions.

        @type xfactor: number
        @param xfactor: The amount to inflate the interval, if the edges are non-sticky, as a fraction of the entire uninflated x interval.
        @type yfactor: number
        @param yfactor: The amount to inflate the interval, if the edges are non-sticky, as a fraction of the entire uninflated y interval.
        @rtype: 4-tuple of numbers
        @return: The possibly-inflated C{xmin}, C{ymin}, C{xmax}, C{ymax} (in that order).
        """

        if self.xfieldType is not None and self.xfieldType.isstring():
            if len(self.xstrings) > 0:
                xmin, xmax = -0.5, len(self.xstrings) - 0.5
            else:
                xmin, xmax = 0.0, 1.0

        elif self.xfieldType is not None and self.xfieldType.isboolean():
            xmin, xmax = -0.5, 1.5

        else:
            if self.xmin is None and self.xmax is None:
                if self.xStrictlyPositive:
                    xmin, xmax = 0.1, 10.0
                else:
                    xmin, xmax = 0.0, 1.0

            elif self.xmin is None:
                xmax = self.xmax
                if self.xStrictlyPositive:
                    xmin = xmax / 100.0
                else:
                    xmin = xmax - 1.0

            elif self.xmax is None:
                xmin = self.xmin
                if self.xStrictlyPositive:
                    xmax = xmin * 100.0
                else:
                    xmax = xmin

            else:
                xmin, xmax = self.xmin, self.xmax

            if self.xStrictlyPositive:
                xmargin = (xmax / xmin)**xfactor
                if xmin != self.xminSticky:
                    xmin /= xmargin
                if xmax != self.xmaxSticky:
                    xmax *= xmargin
            else:
                xmargin = xfactor * (xmax - xmin)
                if xmin != self.xminSticky:
                    xmin -= xmargin
                if xmax != self.xmaxSticky:
                    xmax += xmargin

            if xmin == xmax:
                if self.xStrictlyPositive:
                    xmin, xmax = xmin/10.0, xmax*10.0
                else:
                    xmin, xmax = xmin - 1.0, xmax + 1.0

        if self.yfieldType is not None and self.yfieldType.isstring():
            if len(self.ystrings) > 0:
                ymin, ymax = -0.5, len(self.ystrings) - 0.5
            else:
                ymin, ymax = 0.0, 1.0

        elif self.yfieldType is not None and self.yfieldType.isboolean():
            ymin, ymax = -0.5, 1.5

        else:
            if self.ymin is None and self.ymax is None:
                if self.yStrictlyPositive:
                    ymin, ymax = 0.1, 10.0
                else:
                    ymin, ymax = 0.0, 1.0

            elif self.ymin is None:
                ymax = self.ymax
                if self.yStrictlyPositive:
                    ymin = ymax / 100.0
                else:
                    ymin = ymax - 1.0

            elif self.ymax is None:
                ymin = self.ymin
                if self.yStrictlyPositive:
                    ymax = ymin * 100.0
                else:
                    ymax = ymin

            else:
                ymin, ymax = self.ymin, self.ymax

            if self.yStrictlyPositive:
                ymargin = (ymax / ymin)**yfactor
                if ymin != self.yminSticky:
                    ymin /= ymargin
                if ymax != self.ymaxSticky:
                    ymax *= ymargin
            else:
                ymargin = yfactor * (ymax - ymin)
                if ymin != self.yminSticky:
                    ymin -= ymargin
                if ymax != self.ymaxSticky:
                    ymax += ymargin

            if ymin == ymax:
                if self.yStrictlyPositive:
                    ymin, ymax = ymin/10.0, ymax*10.0
                else:
                    ymin, ymax = ymin - 1.0, ymax + 1.0

        return xmin, ymin, xmax, ymax
