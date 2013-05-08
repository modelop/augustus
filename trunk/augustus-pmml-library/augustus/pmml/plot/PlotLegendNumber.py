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

"""This module defines the PlotLegendNumber class."""

from augustus.core.SvgBinding import SvgBinding
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.core.plot.PlotNumberFormat import PlotNumberFormat
from augustus.core.plot.PmmlPlotLegendContent import PmmlPlotLegendContent

class PlotLegendNumber(PmmlPlotLegendContent):
    """PlotLegendNumber is an element that can be placed in a
    PlotLegend to present a mutable number.
    
    PMML content:

      - Text representation of the initial value.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - digits: number of significant digits to present.
      - style: CSS style properties.

    CSS properties:

      - font, font-family, font-size, font-size-adjust, font-stretch,
        font-style, font-variant, font-weight: font properties.
      - text-color: text color.
                       
    @type value: number
    @param value: Programmatic access to the value.
    """

    styleProperties = ["font", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight",
                       "text-color",
                       ]

    styleDefaults = {"font-size": "25.0", "text-color": "black"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotLegendNumber">
        <xs:complexType>
            <xs:simpleContent>
                <xs:extension base="xs:double">
                    <xs:attribute name="svgId" type="xs:string" use="optional" />
                    <xs:attribute name="digits" type="xs:nonNegativeInteger" use="optional" />
                    <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
                </xs:extension>
            </xs:simpleContent>
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    @property
    def value(self):
        try:
            float(self.text)
        except (ValueError, TypeError):
            self.text = "0"
        return float(self.text)

    @value.setter
    def value(self, value):
        self.text = repr(value)

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

        svg = SvgBinding.elementMaker
        performanceTable.begin("PlotLegendNumber")

        myLabelAttributes = dict(labelAttributes)
        style = PlotStyle.toDict(myLabelAttributes["style"])
        style.update(self.getStyleState())
        myLabelAttributes["style"] = PlotStyle.toString(style)
        myLabelAttributes["font-size"] = style["font-size"]

        svgId = self.get("svgId")
        if svgId is not None:
            myLabelAttributes["id"] = svgId

        try:
            float(self.text)
        except (ValueError, TypeError):
            self.text = "0"

        digits = self.get("digits")
        if digits is not None:
            astext = PlotNumberFormat.roundDigits(float(self.text), int(digits))
        else:
            astext = PlotNumberFormat.toUnicode(self.text)

        cellContents[rowIndex, colIndex] = svg.text(astext, **myLabelAttributes)
        colIndex += 1

        performanceTable.end("PlotLegendNumber")
        return rowIndex, colIndex
