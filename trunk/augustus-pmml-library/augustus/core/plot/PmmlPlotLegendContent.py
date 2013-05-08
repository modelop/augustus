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

from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.plot.PlotStyleable import PlotStyleable

class PmmlPlotLegendContent(PmmlBinding, PlotStyleable):
    """PmmlPlotLegendContent is a base class for PLOT-LEGEND-CONTENT,
    any element that can be included in a legend table.
    """

    def postValidate(self):
        """After XSD validation, verify that the style properties are
        well-formed and no unexpected style property names are
        present.

        @raise PmmlValidationError: If the PMML is not valid, this method raises an error; otherwise, it silently passes.
        """

        self.checkStyleProperties()

    def draw(self, dataTable, functionTable, performanceTable, rowIndex, colIndex, cellContents, labelAttributes, plotDefinitions):
        """Draw the plot legend content, which is more often text than graphics.

        @type dataTable: DataTable
        @param dataTable: Contains the data to describe, if any.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @type rowIndex: int
        @param rowIndex: Row number of the C{cellContents} to fill.
        @type colIndex: int
        @param colIndex: Column number of the C{cellContents} to fill.
        @type cellContents: dict
        @param cellContents: Dictionary that maps pairs of integers to SVG graphics to draw.
        @type labelAttributes: CSS style dict
        @param labelAttributes: Style properties that are defined at the level of the legend and must percolate down to all drawables within the legend.
        @type plotDefinitions: PlotDefinitions
        @type plotDefinitions: The dictionary of key-value pairs that forms the <defs> section of the SVG document.
        @rtype: 2-tuple
        @return: The next C{rowIndex} and C{colIndex} in the sequence.
        """

        raise NotImplementedError("Subclasses of PmmlPlotLegendContent must implement draw(dataTable, functionTable, performanceTable, rowIndex, colIndex, cellContents, labelAttributes, plotDefinitions)")
