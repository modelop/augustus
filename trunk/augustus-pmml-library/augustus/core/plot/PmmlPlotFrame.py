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

"""This module defines the PmmlPlotFrame class."""

from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.plot.PlotStyleable import PlotStyleable

class PmmlPlotFrame(PmmlBinding, PlotStyleable):
    """PmmlPlotFrame is a base class for all PLOT-FRAME elements,
    """

    def postValidate(self):
        self.checkStyleProperties()

    def frame(self, dataTable, functionTable, performanceTable, plotCoordinates, plotContentBox, plotDefinitions):
        """Draw a plot frame and the plot elements it contains.

        @type dataTable: DataTable
        @param dataTable: Contains the data to plot.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @type plotCoordinates: PlotCoordinates
        @param plotCoordinates: The coordinate system in which this plot will be placed (not the coordinate system defined by the plot).
        @type plotContentBox: PlotContentBox
        @param plotContentBox: A bounding box in which this plot will be placed.
        @type plotDefinitions: PlotDefinitions
        @type plotDefinitions: The dictionary of key-value pairs that forms the <defs> section of the SVG document.
        @rtype: SvgBinding
        @return: An SVG fragment representing the fully drawn plot.
        """

        raise NotImplementedError("Subclasses of PmmlPlotFrame must implement frame(dataTable, functionTable, performanceTable, plotCoordinates, plotContentBox, plotDefinitions)")
