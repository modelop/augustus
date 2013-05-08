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

"""This module defines the PmmlPlotContentAnnotation class."""

import codecs

from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.plot.PlotStyleable import PlotStyleable

class PmmlPlotContentAnnotation(PmmlBinding, PlotStyleable):
    """PmmlPlotContentAnnotation is the base class for all
    PLOT-CONTENT-ANNOTATION elements, which are graphics that overlay
    a plot frame but are not confined to the plot's coordinate system,
    such as a plot legend.

    Plot content differs from plot annotations in that content lives
    in a frame's coordinate system, as denoted by the tick marks on
    the plot axes.  Overlaid plot elements are geometrically arranged
    according to the values of their datasets.  Plot annotations, on
    the other hand, are in a coordinate system that is tied to the
    page.
    """

    def postValidate(self):
        """After XSD validation, verify that the style properties are
        well-formed and no unexpected style property names are
        present.

        @raise PmmlValidationError: If the PMML is not valid, this method raises an error; otherwise, it silently passes.
        """

        self.checkStyleProperties()

    def draw(self, dataTable, functionTable, performanceTable, plotCoordinates, plotContentBox, plotDefinitions):
        """Draw the plot annotation.

        @type dataTable: DataTable
        @param dataTable: Contains the data to plot, if any.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @type plotCoordinates: PlotCoordinates
        @param plotCoordinates: The coordinate system in which this plot will be placed.
        @type plotContentBox: PlotContentBox
        @param plotContentBox: A bounding box in which this plot will be placed.
        @type plotDefinitions: PlotDefinitions
        @type plotDefinitions: The dictionary of key-value pairs that forms the <defs> section of the SVG document.
        @rtype: SvgBinding
        @return: An SVG fragment representing the fully drawn plot.
        """

        raise NotImplementedError("Subclasses of PmmlPlotContentAnnotation must implement draw(dataTable, functionTable, performanceTable, plotCoordinates, plotContentBox, plotDefinitions)")
