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

"""This module defines the PlotStatic class."""

from augustus.core.DataTable import DataTable
from augustus.core.SvgBinding import SvgBinding
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent

class PlotStatic(PmmlPlotContent):
    """PlotStatic encloses one or more plots and their preserved
    state, which can represent a reference or plots produced using
    other datasets.

    Plots contained within a PlotStatic are not affected by new data.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotStatic">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element ref="SerializedState" minOccurs="1" maxOccurs="1" />
                <xs:group ref="PLOT-CONTENT" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    class _State(object):
        pass

    def prepare(self, state, dataTable, functionTable, performanceTable, plotRange):
        """Prepare a plot element for drawing.

        This stage consists of calculating all quantities and
        determing the bounds of the data.  These bounds may be unioned
        with bounds from other plot elements that overlay this plot
        element, so the drawing (which requires a finalized coordinate
        system) cannot begin yet.

        This method modifies C{plotRange}.

        @type state: ad-hoc Python object
        @param state: State information that persists long enough to use quantities computed in C{prepare} in the C{draw} stage.  This is a work-around of lxml's refusal to let its Python instances maintain C{self} and it is unrelated to DataTableState.
        @type dataTable: DataTable
        @param dataTable: Contains the data to plot.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @type plotRange: PlotRange
        @param plotRange: The bounding box of plot coordinates that this function will expand.
        """

        performanceTable.begin("PlotStatic prepare")
        self._saveContext(dataTable)

        serializedState = self.childOfTag("SerializedState")
        emptyDataTable = serializedState.emptyDataTable()

        state.subStates = []
        for plotContent in self.childrenOfClass(PmmlPlotContent):
            # intentionally include all PerformanceTable entries below this one
            # to segregate the the measurements of real drawing times from fake ones
            subState = self._State()
            plotContent.prepare(subState, emptyDataTable, functionTable, performanceTable, plotRange)
            state.subStates.append(subState)

        performanceTable.end("PlotStatic prepare")

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

        svg = SvgBinding.elementMaker
        performanceTable.begin("PlotStatic draw")

        svgId = self.get("svgId")
        if svgId is None:
            output = svg.g()
        else:
            output = svg.g(id=svgId)

        for subState, plotContent in zip(state.subStates, self.childrenOfClass(PmmlPlotContent)):
            # intentionally include all PerformanceTable entries below this one
            # to segregate the the measurements of real drawing times from fake ones
            output.append(plotContent.draw(subState, plotCoordinates, plotDefinitions, performanceTable))

        performanceTable.end("PlotStatic draw")
        return output
