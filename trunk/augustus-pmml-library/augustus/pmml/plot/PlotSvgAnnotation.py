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

"""This module defines the PlotSvgAnnotation class."""

import re
import copy

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.plot.PmmlPlotContentAnnotation import PmmlPlotContentAnnotation
from augustus.core.plot.PlotStyle import PlotStyle

class PlotSvgAnnotation(PmmlPlotContentAnnotation):
    """PlotSvgAnnotation represents an arbitrary SVG image as an
    annotation (a graphic that is not embedded in a PlotWindow's
    coordinate system).

    To center the PlotSvgAnnotation, set all margins to "auto".  To put
    it in a corner, set all margins to "auto" except for the desired corner
    (default is margin-right: -10; margin-top: -10; margin-left: auto;
    margin-bottom: auto).  To fill the area (hiding anything below
    it), set all margins to a specific value.

    PMML subelements:

      - SvgBinding for inline SVG.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - fileName: for external SVG.
      - style: CSS style properties.

    Inline and external SVG are mutually exclusive.

    CSS properties:

      - margin-top, margin-right, margin-bottom, margin-left,
        margin: space between the enclosure and the border.
      - border-top-width, border-right-width, border-bottom-width,
        border-left-width, border-width: thickness of the border.
      - padding-top, padding-right, padding-bottom, padding-left,
        padding: space between the border and the inner content.
      - background, background-opacity: color of the background.
      - border-color, border-dasharray, border-dashoffset,
        border-linecap, border-linejoin, border-miterlimit,
        border-opacity, border-width: properties of the border line.

    See the source code for the full XSD.
    """

    styleProperties = ["margin-top", "margin-right", "margin-bottom", "margin-left",
                       "border-top-width", "border-right-width", "border-bottom-width", "border-left-width", "border-width",
                       "padding-top", "padding-right", "padding-bottom", "padding-left", "padding",
                       "border-color", "border-dasharray", "border-dashoffset", "border-linecap", "border-linejoin", "border-miterlimit", "border-opacity", "border-width",
                       ]

    styleDefaults = {"border-color": "none", "margin-right": "10", "margin-top": "10", "padding": "0"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotSvgAnnotation">
        <xs:complexType>
            <xs:complexContent>
                <xs:restriction base="xs:anyType">
                    <xs:sequence>
                        <xs:any minOccurs="0" maxOccurs="1" processContents="skip" />
                    </xs:sequence>
                    <xs:attribute name="svgId" type="xs:string" use="optional" />
                    <xs:attribute name="fileName" type="xs:string" use="optional" />
                    <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
                </xs:restriction>
            </xs:complexContent>
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    @staticmethod
    def findSize(svgBinding):
        """Determine the bounding box of an SVG image.

        @type svgBinding: SvgBinding
        @param svgBinding: The SVG image.
        @rtype: 4-tuple of numbers
        @return: C{xmin}, C{ymin}, C{xmax}, C{ymax}
        """

        viewBox = svgBinding.get("viewBox")

        if viewBox is not None:
            return map(float, viewBox.split())
        
        else:
            xmax, ymax = None, None
            for item in svgBinding.iterdescendants():
                try:
                    x = float(item.get("x"))
                except (ValueError, TypeError):
                    pass
                else:
                    if xmax is None or x > xmax: xmax = x

                try:
                    x = float(item.get("x")) + float(item.get("width"))
                except (ValueError, TypeError):
                    pass
                else:
                    if xmax is None or x > xmax: xmax = x

                try:
                    x = float(item.get("x1"))
                except (ValueError, TypeError):
                    pass
                else:
                    if xmax is None or x > xmax: xmax = x

                try:
                    x = float(item.get("x2"))
                except (ValueError, TypeError):
                    pass
                else:
                    if xmax is None or x > xmax: xmax = x

                try:
                    y = float(item.get("y"))
                except (ValueError, TypeError):
                    pass
                else:
                    if ymax is None or y > ymax: ymax = y

                try:
                    y = float(item.get("y")) + float(item.get("height"))
                except (ValueError, TypeError):
                    pass
                else:
                    if ymax is None or y > ymax: ymax = y

                try:
                    y = float(item.get("y1"))
                except (ValueError, TypeError):
                    pass
                else:
                    if ymax is None or y > ymax: ymax = y

                try:
                    y = float(item.get("y2"))
                except (ValueError, TypeError):
                    pass
                else:
                    if ymax is None or y > ymax: ymax = y

                d = item.get("d")
                if d is not None:
                    for m in re.finditer("[A-Za-z]\s*([0-9\.\-+eE]+)[\s+,]([0-9\.\-+eE]+)", d):
                        x, y = float(m.group(1)), float(m.group(2))

                        if xmax is None or x > xmax: xmax = x
                        if ymax is None or y > ymax: ymax = y
                
            if xmax is None: xmax = 1
            if ymax is None: ymax = 1

            return 0, 0, xmax, ymax

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

        svg = SvgBinding.elementMaker

        svgId = self.get("svgId")
        if svgId is None:
            output = svg.g()
        else:
            output = svg.g(**{"id": svgId})
        content = [output]

        inlineSvg = self.getchildren()
        fileName = self.get("fileName")
        if len(inlineSvg) == 1 and fileName is None:
            svgBinding = inlineSvg[0]
        elif len(inlineSvg) == 0 and fileName is not None:
            svgBinding = SvgBinding.loadXml(fileName)
        else:
            raise defs.PmmlValidationError("PlotSvgAnnotation should specify an inline SVG or a fileName but not both or neither")

        style = self.getStyleState()

        if style.get("margin-bottom") == "auto": del style["margin-bottom"]
        if style.get("margin-top") == "auto": del style["margin-top"]
        if style.get("margin-left") == "auto": del style["margin-left"]
        if style.get("margin-right") == "auto": del style["margin-right"]

        subContentBox = plotContentBox.subContent(style)
        sx1, sy1, sx2, sy2 = PlotSvgAnnotation.findSize(svgBinding)
        nominalHeight = sy2 - sy1
        nominalWidth = sx2 - sx1

        if nominalHeight < subContentBox.height:
            if "margin-bottom" in style and "margin-top" in style:
                pass
            elif "margin-bottom" in style:
                style["margin-top"] = subContentBox.height - nominalHeight
            elif "margin-top" in style:
                style["margin-bottom"] = subContentBox.height - nominalHeight
            else:
                style["margin-bottom"] = style["margin-top"] = (subContentBox.height - nominalHeight) / 2.0

        if nominalWidth < subContentBox.width:
            if "margin-left" in style and "margin-right" in style:
                pass
            elif "margin-left" in style:
                style["margin-right"] = subContentBox.width - nominalWidth
            elif "margin-right" in style:
                style["margin-left"] = subContentBox.width - nominalWidth
            else:
                style["margin-left"] = style["margin-right"] = (subContentBox.width - nominalWidth) / 2.0

        subContentBox = plotContentBox.subContent(style)
        borderRect = plotContentBox.border(style)

        if subContentBox is not None:
            tx1, ty1 = plotCoordinates(subContentBox.x, subContentBox.y)
            tx2, ty2 = plotCoordinates(subContentBox.x + subContentBox.width, subContentBox.y + subContentBox.height)

            output.extend([copy.deepcopy(x) for x in svgBinding.getchildren()])

            output["transform"] = "translate(%r, %r) scale(%r, %r)" % (tx1 - sx1, ty1 - sy1, (tx2 - tx1)/float(sx2 - sx1), (ty2 - ty1)/float(sy2 - sy1))

        if borderRect is not None:
            rectStyle = {"stroke": style["border-color"]}
            if rectStyle["stroke"] != "none":
                for styleProperty in "border-dasharray", "border-dashoffset", "border-linecap", "border-linejoin", "border-miterlimit", "border-opacity", "border-width":
                    if styleProperty in style:
                        rectStyle[styleProperty.replace("border-", "stroke-")] = style[styleProperty]

                x1 = borderRect.x
                y1 = borderRect.y
                x2 = borderRect.x + borderRect.width
                y2 = borderRect.y + borderRect.height
                x1, y1 = plotCoordinates(x1, y1)
                x2, y2 = plotCoordinates(x2, y2)

                subAttrib = {"x": repr(x1), "y": repr(y1), "width": repr(x2 - x1), "height": repr(y2 - y1), "style": PlotStyle.toString(rectStyle)}

                subAttrib["style"] = PlotStyle.toString(rectStyle)
                if svgId is not None:
                    subAttrib["id"] = svgId + ".border"
                content.append(svg.rect(**subAttrib))

        return svg.g(*content)
