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

"""This module defines the PlotLayout class."""

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.plot.PmmlPlotFrame import PmmlPlotFrame
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.core.plot.PlotCoordinatesOffset import PlotCoordinatesOffset
from augustus.core.plot.PlotContentBox import PlotContentBox

class PlotLayout(PmmlPlotFrame):
    """PlotLayout arranges plots (or nested PlotLayouts) on a page.
    
    It has CSS properties to stylize the space between plots,
    including a margin, border, and padding following the CSS box
    model.

    PMML subelements:

      - Any PLOT-FRAMEs (PmmlPlotFrames)

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - rows: number of rows in the layout grid.
      - cols: number of columns in the layout grid.
      - title: global title above the layout grid.
      - style: CSS style properties.

    CSS properties:

      - margin-top, margin-right, margin-bottom, margin-left,
        margin: space between the enclosure and the border.
      - border-top-width, border-right-width, border-bottom-width,
        border-left-width, border-width: thickness of the border.
      - padding-top, padding-right, padding-bottom, padding-left,
        padding: space between the border and the inner content.
      - row-heights, col-widths: space-delimited array of relative
        heights and widths of each row and column, respectively;
        use C{"auto"} for equal divisions (the default); raises an
        error if the number of elements in the array is not equal
        to C{rows} or C{cols}, respectively.
      - title-height, title-gap: height of and gap below the global
        title.
      - background, background-opacity: color of the background.
      - border-color, border-dasharray, border-dashoffset,
        border-linecap, border-linejoin, border-miterlimit,
        border-opacity, border-width: properties of the border line.
      - font, font-family, font-size, font-size-adjust, font-stretch,
        font-style, font-variant, font-weight: properties of the
        title font.

    See the source code for the full XSD.
    """

    styleProperties = ["margin-top", "margin-right", "margin-bottom", "margin-left", "margin",
                       "border-top-width", "border-right-width", "border-bottom-width", "border-left-width", "border-width",
                       "padding-top", "padding-right", "padding-bottom", "padding-left", "padding",
                       "row-heights", "col-widths",
                       "title-height", "title-gap",
                       "background", "background-opacity", 
                       "border-color", "border-dasharray", "border-dashoffset", "border-linecap", "border-linejoin", "border-miterlimit", "border-opacity", "border-width",
                       "font", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight",
                       ]

    styleDefaults = {"background": "none", "border-color": "none", "margin": "2", "padding": "2", "border-width": "0", "row-heights": "auto", "col-widths": "auto", "title-height": "30", "title-gap": "5", "title-color": "black", "font-size": "30.0"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotLayout">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:group ref="PLOT-FRAME" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="rows" type="xs:positiveInteger" use="required" />
            <xs:attribute name="cols" type="xs:positiveInteger" use="required" />
            <xs:attribute name="title" type="xs:string" use="optional" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

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

        svg = SvgBinding.elementMaker
        performanceTable.begin("PlotLayout")

        svgId = self.get("svgId")
        content = []
        if svgId is None: attrib = {}
        else: attrib = {"id": svgId}

        style = self.getStyleState()

        title = self.get("title")
        if title is not None:
            textStyle = {"stroke": "none", "fill": style["title-color"]}
            for styleProperty in "font", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight":
                if styleProperty in style:
                    textStyle[styleProperty] = style[styleProperty]

            plotContentBox = plotContentBox.subContent({"margin-top": repr(float(style.get("margin-top", style["margin"])) + float(style["title-height"]) + float(style["title-gap"]))})
            content.append(svg.text(title, **{"transform": "translate(%r,%r)" % (plotContentBox.x + plotContentBox.width / 2.0, plotContentBox.y - float(style["title-gap"])), "text-anchor": "middle", defs.XML_SPACE: "preserve", "style": PlotStyle.toString(textStyle)}))

        subContentBox = plotContentBox.subContent(style)
        borderRect = plotContentBox.border(style)

        ### background rectangle
        if borderRect is not None:
            rectStyle = {"fill": style["background"], "stroke": "none"}
            x1 = borderRect.x
            y1 = borderRect.y
            x2 = borderRect.x + borderRect.width
            y2 = borderRect.y + borderRect.height
            x1, y1 = plotCoordinates(x1, y1)
            x2, y2 = plotCoordinates(x2, y2)

            subAttrib = {"x": repr(x1), "y": repr(y1), "width": repr(x2 - x1), "height": repr(y2 - y1), "style": PlotStyle.toString(rectStyle)}

            if rectStyle["fill"] != "none":
                if "background-opacity" in style:
                    rectStyle["fill-opacity"] = style["background-opacity"]
                if svgId is not None:
                    subAttrib["id"] = svgId + ".background"
                content.append(svg.rect(**subAttrib))

        ### sub-content
        if subContentBox is not None:
            plotFrames = self.childrenOfClass(PmmlPlotFrame)
            rows = self.get("rows", defaultFromXsd=True, convertType=True)
            cols = self.get("cols", defaultFromXsd=True, convertType=True)
            
            rowHeights = style["row-heights"]
            if rowHeights == "auto":
                rowHeights = [subContentBox.height / float(rows)] * rows
            else:
                try:
                    rowHeights = map(float, rowHeights.split())
                    if any(x <= 0.0 for x in rowHeights): raise ValueError
                except ValueError:
                    raise defs.PmmlValidationError("If not \"auto\", all items in row-heights must be positive numbers")
                if len(rowHeights) != rows:
                    raise defs.PmmlValidationError("Number of elements in row-heights (%d) must be equal to rows (%d)" % (len(rowHeights), rows))

                norm = sum(rowHeights) / subContentBox.height
                rowHeights = [x/norm for x in rowHeights]

            colWidths = style["col-widths"]
            if colWidths == "auto":
                colWidths = [subContentBox.width / float(cols)] * cols
            else:
                try:
                    colWidths = map(float, colWidths.split())
                    if any(x <= 0.0 for x in colWidths): raise ValueError
                except ValueError:
                    raise defs.PmmlValidationError("If not \"auto\", all items in col-widths must be positive numbers")
                if len(colWidths) != cols:
                    raise defs.PmmlValidationError("Number of elements in col-widths (%d) must be equal to cols (%d)" % (len(colWidths), cols))

                norm = sum(colWidths) / subContentBox.width
                colWidths = [x/norm for x in colWidths]

            plotFramesIndex = 0
            cellY = subContentBox.y
            for vertCell in xrange(rows):
                cellX = subContentBox.x
                for horizCell in xrange(cols):
                    if plotFramesIndex < len(plotFrames):
                        plotFrame = plotFrames[plotFramesIndex]

                        cellCoordinates = PlotCoordinatesOffset(plotCoordinates, cellX, cellY)
                        cellContentBox = PlotContentBox(0, 0, colWidths[horizCell], rowHeights[vertCell])

                        performanceTable.pause("PlotLayout")
                        content.append(plotFrame.frame(dataTable, functionTable, performanceTable, cellCoordinates, cellContentBox, plotDefinitions))
                        performanceTable.unpause("PlotLayout")

                    plotFramesIndex += 1
                    cellX += colWidths[horizCell]
                cellY += rowHeights[vertCell]

        ### border rectangle (reuses subAttrib, replaces subAttrib["style"])
        if borderRect is not None:
            rectStyle = {"stroke": style["border-color"]}
            if rectStyle["stroke"] != "none":
                for styleProperty in "border-dasharray", "border-dashoffset", "border-linecap", "border-linejoin", "border-miterlimit", "border-opacity", "border-width":
                    if styleProperty in style:
                        rectStyle[styleProperty.replace("border-", "stroke-")] = style[styleProperty]

                subAttrib["style"] = PlotStyle.toString(rectStyle)
                if svgId is not None:
                    subAttrib["id"] = svgId + ".border"
                content.append(svg.rect(**subAttrib))
                       
        performanceTable.end("PlotLayout")
        return svg.g(*content, **attrib)
