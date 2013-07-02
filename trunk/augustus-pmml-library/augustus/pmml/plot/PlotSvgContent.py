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

"""This module defines the PlotSvgContent class."""

import copy

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotCoordinatesWindow import PlotCoordinatesWindow
from augustus.pmml.plot.PlotSvgAnnotation import PlotSvgAnnotation

class PlotSvgContent(PmmlPlotContent):
    """PlotSvgContent represents an SVG image embedded in a coordinate
    system.

    PMML subelements:

      - SvgBinding for inline SVG.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - fileName: for external SVG.
      - x1: left edge.
      - y1: bottom edge.
      - x2: right edge.
      - y2: top edge.

    Inline and external SVG are mutually exclusive.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotSvgContent">
        <xs:complexType>
            <xs:complexContent>
                <xs:restriction base="xs:anyType">
                    <xs:sequence>
                        <xs:any minOccurs="0" maxOccurs="1" processContents="skip" />
                    </xs:sequence>
                    <xs:attribute name="svgId" type="xs:string" use="optional" />
                    <xs:attribute name="fileName" type="xs:string" use="optional" />
                    <xs:attribute name="x1" type="xs:double" use="required" />
                    <xs:attribute name="y1" type="xs:double" use="required" />
                    <xs:attribute name="x2" type="xs:double" use="required" />
                    <xs:attribute name="y2" type="xs:double" use="required" />
                </xs:restriction>
            </xs:complexContent>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    fieldTypeNumeric = FakeFieldType("double", "continuous")

    def prepare(self, state, dataTable, functionName, performanceTable, plotRange):
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

        self._saveContext(dataTable)

        x1 = float(self["x1"])
        y1 = float(self["y1"])
        x2 = float(self["x2"])
        y2 = float(self["y2"])

        if x1 >= x2 or y1 >= y2:
            raise defs.PmmlValidationError("x1 must be less than x2 and y1 must be less than y2")

        if plotRange.xStrictlyPositive or plotRange.yStrictlyPositive:
            raise defs.PmmlValidationError("PlotSvgContent can only be properly displayed in linear coordinates")

        plotRange.xminPush(x1, self.fieldTypeNumeric, sticky=True)
        plotRange.yminPush(y1, self.fieldTypeNumeric, sticky=True)
        plotRange.xmaxPush(x2, self.fieldTypeNumeric, sticky=True)
        plotRange.ymaxPush(y2, self.fieldTypeNumeric, sticky=True)

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

        x1 = float(self["x1"])
        y1 = float(self["y1"])
        x2 = float(self["x2"])
        y2 = float(self["y2"])

        inlineSvg = self.getchildren()
        fileName = self.get("fileName")
        if len(inlineSvg) == 1 and fileName is None:
            svgBinding = inlineSvg[0]
        elif len(inlineSvg) == 0 and fileName is not None:
            svgBinding = SvgBinding.loadXml(fileName)
        else:
            raise defs.PmmlValidationError("PlotSvgContent should specify an inline SVG or a fileName but not both or neither")
        
        sx1, sy1, sx2, sy2 = PlotSvgAnnotation.findSize(svgBinding)
        subCoordinates = PlotCoordinatesWindow(plotCoordinates, sx1, sy1, sx2, sy2, x1, y1, x2 - x1, y2 - y1)

        tx0, ty0 = subCoordinates(0.0, 0.0)
        tx1, ty1 = subCoordinates(1.0, 1.0)
        transform = "translate(%r, %r) scale(%r, %r)" % (tx0, ty0, tx1 - tx0, ty1 - ty0)

        attribs = {"transform": transform}
        svgId = self.get("svgId")
        if svgId is not None:
            attribs["id"] = svgId
        if "style" in svgBinding.attrib:
            attribs["style"] = svgBinding.attrib["style"]

        return svg.g(*(copy.deepcopy(svgBinding).getchildren()), **attribs)
