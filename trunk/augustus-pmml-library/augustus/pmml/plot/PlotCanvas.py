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

"""This module defines the PlotCanvas class."""

from lxml.etree import tostring, ElementTree

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.plot.PmmlPlotFrame import PmmlPlotFrame
from augustus.core.plot.PlotStyleable import PlotStyleable
from augustus.core.plot.PlotCoordinates import PlotCoordinates
from augustus.core.plot.PlotContentBox import PlotContentBox
from augustus.core.plot.PlotDefinitions import PlotDefinitions
from augustus.core.DataTable import DataTable
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakePerformanceTable import FakePerformanceTable

class PlotCanvas(PmmlCalculable, PlotStyleable):
    """PlotCanvas represents an SVG image to be built from
    PLOT-FRAMES.

    PMML subelements:

      - Any PLOT-FRAMEs (PmmlPlotFrames)

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - width: width of the SVG image's viewBox.
      - height: height of the SVG image's viewBox.
      - style: global CSS style properties.
      - font-family: global font choices.
      - font-weight: global font weights.
      - plotName: name of the plot in the DataTablePlots.
      - fileName: if present, evaluating the PlotCanvas causes
        an SVG file to be written out with this name.
      - isPlotable: if "true", draw the plot; if "false", don't.
    
    See the source code for the full XSD.

    @type globalAttrib: dict
    @param globalAttrib: Default attributes of the top-level SVG object.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotCanvas">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:group ref="PLOT-FRAME" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="width" type="INT-NUMBER" use="optional" default="1200" />
            <xs:attribute name="height" type="INT-NUMBER" use="optional" default="750" />
            <xs:attribute name="style" type="xs:string" use="optional" default="fill: none; stroke: black; stroke-linejoin: miter; stroke-width: 2; text-anchor: middle;" />
            <xs:attribute name="font-family" type="xs:string" use="optional" default="DejaVu Sans Condensed, DejaVu Sans, Lucida Sans Unicode, Lucida Sans, Helvetica, Sans, sans-serif" />
            <xs:attribute name="font-weight" type="xs:string" use="optional" default="normal" />
            <xs:attribute name="plotName" type="xs:string" use="optional" />
            <xs:attribute name="fileName" type="xs:string" use="optional" />
            <xs:attribute name="isPlotable" type="xs:boolean" use="optional" default="true" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    globalAttrib = {"version": "1.1", "width": "100%", "height": "100%", "preserveAspectRatio": "xMidYMin meet"}
    emptyPlot = SvgBinding.elementMaker.svg(**globalAttrib)

    @property
    def name(self):
        return self.get("plotName")

    def calculate(self, dataTable, functionTable=None, performanceTable=None):
        """Perform a calculation directly, without constructing a
        DataTable first.

        This method is intended for performance-critical cases where
        the DataTable would be built without having to analyze the
        PMML for field type context.

        This method modifies the input DataTable and FunctionTable.

        Note that PmmlCalculables return a DataTable from
        C{calculate}, wheras PlotCanvas returns an SvgBinding.  When
        computing an entire PMML document, Augustus ignores the return
        value of both PmmlCalculable and PlotCanvas C{calculate}
        methods, using the fact that they both modify the input
        DataTable.  The return values are just for user convenience.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: SvgBinding
        @return: A complete SVG image representing the fully drawn plot.        
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        plot = self.emptyPlot
        if self.get("isPlotable", defaultFromXsd=True, convertType=True):
            plot = self.makePlot(dataTable, functionTable, performanceTable)

            if self.get("plotName") is not None:
                dataTable.plots[self.get("plotName")] = plot

            if self.get("fileName") is not None:
                plot.xmlFile(self.get("fileName"))

        return plot

    def calc(self, inputData, inputMask=None, inputState=None, functionTable=None, performanceTable=None):
        """User interface to quickly make and return a plot.

        This method is intended for interactive use, since it is more
        laborious to construct a DataTable by hand.

        This method modifies the input FunctionTable.

        Note that PmmlCalculables return a DataTable from C{calc},
        wheras PlotCanvas returns an SvgBinding.

        @type inputData: dict
        @param inputData: Dictionary from field names to data, as required by the DataTable constructor.
        @type inputMask: dict or None
        @param inputMask: Dictionary from field names to missing value masks, as required by the DataTable constructor.
        @type inputState: DataTableState or None
        @param inputState: Calculation state, used to continue a calculation over many C{calc} calls.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: SvgBinding
        @return: A complete SVG image representing the fully drawn plot.        
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        performanceTable.begin("make DataTable")
        dataTable = DataTable(self, inputData, inputMask, inputState)
        performanceTable.end("make DataTable")

        return self.makePlot(dataTable, functionTable, performanceTable)

    def makePlot(self, dataTable, functionTable=None, performanceTable=None):
        """Construct a plot from the data and return a complete SVG
        image.
        
        @type dataTable: DataTable
        @param dataTable: Contains the data to plot.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @rtype: SvgBinding
        @return: A complete SVG image representing the fully drawn plot.
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        svg = SvgBinding.elementMaker
        performanceTable.begin("PlotCanvas")
        
        width = self.get("width", defaultFromXsd=True, convertType=True)
        height = self.get("height", defaultFromXsd=True, convertType=True)
        style = self.get("style", defaultFromXsd=True)

        attrib = self.globalAttrib.copy()
        svgId = self.get("svgId")
        if svgId is not None:
            attrib["id"] = svgId

        attrib["viewBox"] = "0 0 %d %d" % (width, height)
        attrib["style"] = style
        attrib["font-family"] = self.get("font-family", defaultFromXsd=True)
        attrib["font-weight"] = self.get("font-weight", defaultFromXsd=True)

        plotCoordinates = PlotCoordinates()
        plotContentBox = PlotContentBox(0, 0, width, height)
        plotDefinitions = PlotDefinitions()

        performanceTable.pause("PlotCanvas")
        content = [x.frame(dataTable, functionTable, performanceTable, plotCoordinates, plotContentBox, plotDefinitions) for x in self.childrenOfClass(PmmlPlotFrame)]
        performanceTable.unpause("PlotCanvas")

        content = [svg.defs(*plotDefinitions.values())] + content

        performanceTable.end("PlotCanvas")
        return svg.svg(*content, **attrib)
