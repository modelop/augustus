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

"""This module defines the PlotGuideLines class."""

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.NumpyInterface import NP
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle

class PlotGuideLines(PmmlPlotContent):
    """Represents a set of guide lines to help interpret the plot.

    PMML subelements:

      - PlotVerticalLines: infinite set of vertical lines to draw,
        usually used as part of a background grid.
      - PlotHorizontalLines: infinite set of horizontal lines to
        draw, usually used as part of a background grid.
      - PlotLine: arbitrary line used to call out a feature on a
        plot.  One of its endpoints may be at infinity.

    PMML attributes:

      - svgId: id for the resulting SVG element.

    CSS properties:
      - stroke, stroke-dasharray, stroke-dashoffset, stroke-linecap,
        stroke-linejoin, stroke-miterlimit, stroke-opacity,
        stroke-width: properties of the line drawing.

    See the source code for the full XSD.
    """

    styleProperties = ["stroke", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke-width",
                       ]

    styleDefaults = {"stroke": "black"}
    
    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotGuideLines">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:choice minOccurs="1" maxOccurs="unbounded">
                    <xs:element ref="PlotVerticalLines" minOccurs="1" maxOccurs="1" />
                    <xs:element ref="PlotHorizontalLines" minOccurs="1" maxOccurs="1" />
                    <xs:element ref="PlotLine" minOccurs="1" maxOccurs="1" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    xsdRemove = ["PlotVerticalLines", "PlotHorizontalLines", "PlotLine"]

    xsdAppend = ["""<xs:element name="PlotVerticalLines" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="x0" type="xs:string" use="required" />
            <xs:attribute name="spacing" type="xs:double" use="required" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
""" % PlotStyle.toString(styleDefaults),
"""<xs:element name="PlotHorizontalLines" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="y0" type="xs:string" use="required" />
            <xs:attribute name="spacing" type="xs:double" use="required" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
""" % PlotStyle.toString(styleDefaults),
"""<xs:element name="PlotLine" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="x1" type="xs:string" use="required" />
            <xs:attribute name="y1" type="xs:string" use="required" />
            <xs:attribute name="x2" type="xs:string" use="required" />
            <xs:attribute name="y2" type="xs:string" use="required" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>""" % PlotStyle.toString(styleDefaults)]

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

        self._saveContext(dataTable)

        for directive in self.xpath("pmml:PlotLine"):
            try:
                x1 = float(directive["x1"])
                y1 = float(directive["y1"])
                x2 = float(directive["x2"])
                y2 = float(directive["y2"])
            except ValueError:
                pass
            else:
                fieldType = FakeFieldType("double", "continuous")
                plotRange.xminPush(x1, fieldType, sticky=False)
                plotRange.yminPush(y1, fieldType, sticky=False)
                plotRange.xmaxPush(x2, fieldType, sticky=False)
                plotRange.ymaxPush(y2, fieldType, sticky=False)

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
        performanceTable.begin("PlotGuideLines draw")

        output = svg.g()

        for directive in self.xpath("pmml:PlotVerticalLines | pmml:PlotHorizontalLines | pmml:PlotLine"):
            style = dict(self.styleDefaults)
            currentStyle = directive.get("style")
            if currentStyle is not None:
                style.update(PlotStyle.toDict(currentStyle))
            style["fill"] = "none"
            style = PlotStyle.toString(style)

            if directive.hasTag("PlotVerticalLines"):
                try:
                    x0 = plotCoordinates.xfieldType.stringToValue(directive["x0"])
                except ValueError:
                    raise defs.PmmlValidationError("Invalid x0: %r" % directive["x0"])

                spacing = float(directive["spacing"])
                low = plotCoordinates.innerX1
                high = plotCoordinates.innerX2

                up = list(NP("arange", x0, high, spacing, dtype=NP.dtype(float)))
                down = list(NP("arange", x0 - spacing, low, -spacing, dtype=NP.dtype(float)))

                for x in up + down:
                    x1, y1 = x, float("-inf")
                    X1, Y1 = plotCoordinates(x1, y1)
                    x2, y2 = x, float("inf")
                    X2, Y2 = plotCoordinates(x2, y2)

                    output.append(svg.path(d="M %r %r L %r %r" % (X1, Y1, X2, Y2), style=style))

            elif directive.hasTag("PlotHorizontalLines"):
                try:
                    y0 = plotCoordinates.xfieldType.stringToValue(directive["y0"])
                except ValueError:
                    raise defs.PmmlValidationError("Invalid y0: %r" % directive["y0"])

                spacing = float(directive["spacing"])
                low = plotCoordinates.innerY1
                high = plotCoordinates.innerY2

                up = list(NP("arange", y0, high, spacing, dtype=NP.dtype(float)))
                down = list(NP("arange", y0 - spacing, low, -spacing, dtype=NP.dtype(float)))

                for y in up + down:
                    x1, y1 = float("-inf"), y
                    X1, Y1 = plotCoordinates(x1, y1)
                    x2, y2 = float("inf"), y
                    X2, Y2 = plotCoordinates(x2, y2)

                    output.append(svg.path(d="M %r %r L %r %r" % (X1, Y1, X2, Y2), style=style))

            elif directive.hasTag("PlotLine"):
                try:
                    x1 = plotCoordinates.xfieldType.stringToValue(directive["x1"])
                    y1 = plotCoordinates.xfieldType.stringToValue(directive["y1"])
                    x2 = plotCoordinates.xfieldType.stringToValue(directive["x2"])
                    y2 = plotCoordinates.xfieldType.stringToValue(directive["y2"])
                except ValueError:
                    raise defs.PmmlValidationError("Invalid x1, y1, x2, or y2: %r %r %r %r" % (directive["x1"], directive["y1"], directive["x2"], directive["y2"]))

                X1, Y1 = plotCoordinates(x1, y1)
                X2, Y2 = plotCoordinates(x2, y2)

                output.append(svg.path(d="M %r %r L %r %r" % (X1, Y1, X2, Y2), style=style))

        svgId = self.get("svgId")
        if svgId is not None:
            output["id"] = svgId

        performanceTable.end("PlotGuideLines draw")

        return output
