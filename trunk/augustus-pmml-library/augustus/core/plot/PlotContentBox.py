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

"""This module defines the PlotContentBox class."""

class PlotContentBox(object):
    """PlotContentBox is a base class for all plotting elements that
    partition the screen into margin, border, padding, and content
    (the U{CSS box model<http://www.w3.org/TR/CSS2/box.html>})."""

    def __init__(self, x, y, width, height):
        """Initialize a PlotContentBox.

        @type x: number
        @param x: The left edge of the box.
        @type y: number
        @param y: The top edge of the box.
        @type width: number
        @param width: The width of the box.
        @type height: number
        @param height: The height of the box.
        """

        self.x = x
        self.y = y
        self.width = width
        self.height = height

    def __repr__(self):
        return "<PlotContentBox x=%g y=%g width=%g height=%g at 0x%x>" % (self.x, self.y, self.width, self.height, id(self))

    def subContent(self, styleState):
        """Determine the location of inner content using a
        U{CSS box model<http://www.w3.org/TR/CSS2/box.html>}.

        @type styleState: dict
        @param styleState: Dictionary of style properties.
        @rtype: PlotContentBox or None
        @return: The x, y, width, height as a PlotContentBox.
        """

        margin = float(styleState.get("margin", 0.0))
        TM = float(styleState.get("margin-top", margin))
        RM = float(styleState.get("margin-right", margin))
        BM = float(styleState.get("margin-bottom", margin))
        LM = float(styleState.get("margin-left", margin))

        borderWidth = float(styleState.get("border-width", 0.0))
        TB = float(styleState.get("border-top-width", borderWidth))
        RB = float(styleState.get("border-right-width", borderWidth))
        BB = float(styleState.get("border-bottom-width", borderWidth))
        LB = float(styleState.get("border-left-width", borderWidth))

        padding = float(styleState.get("padding", 0.0))
        TP = float(styleState.get("padding-top", padding))
        RP = float(styleState.get("padding-right", padding))
        BP = float(styleState.get("padding-bottom", padding))
        LP = float(styleState.get("padding-left", padding))

        x = self.x + LM + LB + LP
        y = self.y + TM + TB + TP
        width = self.width - LM - LB - LP - RP - RB - RM
        height = self.height - TM - TB - TP - BP - BB - BM

        if width <= 0.0 or height <= 0.0:
            return None
        else:
            return PlotContentBox(x, y, width, height)

    def border(self, styleState):
        """Determine the location of the border using a
        U{CSS box model<http://www.w3.org/TR/CSS2/box.html>}.

        @type styleState: dict
        @param styleState: Dictionary of style properties.
        @rtype: PlotContentBox or None
        @return: The x, y, width, height as a PlotContentBox.
        """

        margin = float(styleState.get("margin", 0.0))
        TM = float(styleState.get("margin-top", margin))
        RM = float(styleState.get("margin-right", margin))
        BM = float(styleState.get("margin-bottom", margin))
        LM = float(styleState.get("margin-left", margin))

        x = self.x + LM
        y = self.y + TM
        width = self.width - LM - RM
        height = self.height - TM - BM

        if width <= 0.0 or height <= 0.0:
            return None
        else:
            return PlotContentBox(x, y, width, height)
