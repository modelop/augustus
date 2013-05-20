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

"""This module defines the PmmlPlotContent class."""

from augustus.core.defs import defs
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.plot.PlotStyleable import PlotStyleable

class PmmlPlotContent(PmmlBinding, PlotStyleable):
    """PmmlPlotContent is the base class of PLOT-CONTENT items, such
    as scatter plots and histograms.

    Plot content differs from plot annotations in that content lives
    in a frame's coordinate system, as denoted by the tick marks on
    the plot axes.  Overlaid plot elements are geometrically arranged
    according to the values of their datasets.  Plot annotations, on
    the other hand, are in a coordinate system that is tied to the
    page.

    Visualizing a plot element is split into two stages: C{prepare}
    and C{draw}.  The prepare stage computes all expressions and
    determines the bounds of the data, and the draw stage actually
    draws the pre-computed data.  The reason for this split is that a
    plot may have many overlaid plot elements that all need to be
    drawn in the same coordinate system, but (unless xmin, ymin, xmax,
    ymax are always specified by the user), the coordinate system
    cannot be determined until all plot elements have computed and
    unioned their bounding boxes.
    """

    def postValidate(self):
        """After XSD validation, verify that the style properties are
        well-formed and no unexpected style property names are
        present.

        @raise PmmlValidationError: If the PMML is not valid, this method raises an error; otherwise, it silently passes.
        """

        self.checkStyleProperties()

    def _saveContext(self, dataTable):
        stateId = self.get("stateId")
        if stateId is not None:
            context = {}
            for fieldName, dataColumn in dataTable.fields.items():
                context[fieldName] = (dataColumn.fieldType.dataType, dataColumn.fieldType.optype)
            dataTable.state[stateId + ".context"] = context

    def prepare(self, state, dataTable, functionTable, performanceTable, plotRange):
        """Prepare a plot element for drawing.

        This stage consists of calculating all quantities and
        determing the bounds of the data.  These bounds may be unioned
        with bounds from other plot elements that overlay this plot
        element, so the drawing (which requires a finalized coordinate
        system) cannot begin yet.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to use quantities computed in C{prepare} in the C{draw} stage.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
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

        raise NotImplementedError("Subclasses of PmmlPlotContent must implement prepare(state, dataTable, functionTable, performanceTable, plotRange)")

    def draw(self, state, plotCoordinates, plotDefinitions, performanceTable):
        """Draw the plot element.

        This stage consists of creating an SVG image of the
        pre-computed data.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to use quantities computed in C{prepare} in the C{draw} stage.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
        @type plotCoordinates: PlotCoordinates
        @param plotCoordinates: The coordinate system in which this plot element will be placed.
        @type plotDefinitions: PlotDefinitions
        @type plotDefinitions: The dictionary of key-value pairs that forms the <defs> section of the SVG document.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @rtype: SvgBinding
        @return: An SVG fragment representing the fully drawn plot element.
        """

        raise NotImplementedError("Subclasses of PmmlPlotContent must implement draw(state, plotCoordinates, plotDefinitions, performanceTable)")

    def checkRoles(self, expected):
        """Helper method to verify that all expected roles are
        present.

        (Some plot types use
        PlotFormula/PlotExpression/PlotNumericExpression elements
        with predefined "roles" to specify the contents of the axes.)

        @type expected: list of strings
        @param expected: The names of the roles that are required.
        @raise PmmlValidationError: If a role is unrecognized, this method raises an error; otherwise, it silently passes.
        """

        for role in self.xpath("pmml:PlotFormula/@role | pmml:PlotExpression/@role | pmml:PlotNumericExpression/@role"):
            if role not in expected:
                raise defs.PmmlValidationError("Unrecognized role: \"%s\" (expected one of \"%s\")" % (role, "\" \"".join(expected)))
