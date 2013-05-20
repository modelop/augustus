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

"""This module defines the PlotLegendSvg class."""

import copy

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.plot.PmmlPlotLegendContent import PmmlPlotLegendContent
from augustus.pmml.plot.PlotSvgAnnotation import PlotSvgAnnotation

class PlotLegendSvg(PmmlPlotLegendContent):
    """PlotLegendSvg represents an arbitrary SVG image as a glyph that
    can be put into a PlotLegend.

    PMML subelements:

      - SvgBinding for inline SVG.

    PMML attributes:

      - fileName: for external SVG.

    Inline and external SVG are mutually exclusive.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotLegendSvg">
        <xs:complexType>
            <xs:complexContent>
                <xs:restriction base="xs:anyType">
                    <xs:sequence>
                        <xs:any minOccurs="0" maxOccurs="1" processContents="skip" />
                    </xs:sequence>
                    <xs:attribute name="svgId" type="xs:string" use="optional" />
                    <xs:attribute name="fileName" type="xs:string" use="optional" />
                </xs:restriction>
            </xs:complexContent>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

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

        svgId = self.get("svgId")
        if svgId is None:
            output = svg.g()
        else:
            output = svg.g(**{"id": svgId})

        inlineSvg = self.getchildren()
        fileName = self.get("fileName")
        if len(inlineSvg) == 1 and fileName is None:
            svgBinding = copy.deepcopy(inlineSvg[0])
        elif len(inlineSvg) == 0 and fileName is not None:
            svgBinding = SvgBinding.loadXml(fileName)
        else:
            raise defs.PmmlValidationError("PlotLegendSvg should specify an inline SVG or a fileName but not both or neither")

        sx1, sy1, sx2, sy2 = PlotSvgAnnotation.findSize(svgBinding)
        nominalHeight = sy2 - sy1
        nominalWidth = sx2 - sx1

        # TODO: set this correctly from the text height
        rowHeight = 30.0

        # output["transform"] = "translate(%r, %r) scale(%r, %r)" % (-sx1, -sy1, rowHeight/float(sx2 - sx1), rowHeight/float(sy2 - sy1))
        output["transform"] = "translate(%r, %r) scale(%r, %r)" % (-sx1 - 0.5*nominalWidth*rowHeight/nominalHeight, -sy1 - 0.75*rowHeight, rowHeight/nominalHeight, rowHeight/nominalHeight)
        output.append(svgBinding)
        
        cellContents[rowIndex, colIndex] = svg.g(output)
        cellContents[rowIndex, colIndex].text = "  "   # TODO: set the width correctly, too
        colIndex += 1

        return rowIndex, colIndex
