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

"""This module defines the PlotLegend class."""

import os
import re
import math

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.plot.PmmlPlotContentAnnotation import PmmlPlotContentAnnotation
from augustus.core.plot.PmmlPlotLegendContent import PmmlPlotLegendContent
from augustus.core.plot.PlotStyle import PlotStyle

class PlotLegend(PmmlPlotContentAnnotation):
    """PlotLegend represents a plot legend that may overlay a plot or
    stand alone in an empty PlotLayout.

    The content of a PlotLegend is entered as the PMML element's text
    (like Array).  Newlines delimit rows and spaces delimit columns,
    though spaces may be included in an item by quoting the item (also
    like Array).  In addition to text, a PlotLegend can contain any
    PLOT-LEGEND-CONTENT (PmmlPlotLegendContent).
    
    To center the PlotLegend, set all margins to "auto".  To put it in
    a corner, set all margins to "auto" except for the desired corner
    (default is margin-right: -10; margin-top: -10; margin-left: auto;
    margin-bottom: auto).  To fill the area (hiding anything below
    it), set all margins to a specific value.

    PMML contents:

      - Newline and space-delimited text, as well as PLOT-LEGEND-CONTENT
        (PmmlPlotLegendContent) elements.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - style: CSS style properties.

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
      - font, font-family, font-size, font-size-adjust, font-stretch,
        font-style, font-variant, font-weight: properties of the
        title font.

    See the source code for the full XSD.
    """

    styleProperties = ["margin-top", "margin-right", "margin-bottom", "margin-left",
                       "border-top-width", "border-right-width", "border-bottom-width", "border-left-width", "border-width",
                       "padding-top", "padding-right", "padding-bottom", "padding-left", "padding",
                       "background", "background-opacity",
                       "border-color", "border-dasharray", "border-dashoffset", "border-linecap", "border-linejoin", "border-miterlimit", "border-opacity", "border-width",
                       "font", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight",
                       "text-color", "column-align", "column-padding",
                       ]

    styleDefaults = {"background": "white", "border-color": "black", "margin-right": "-10", "margin-top": "-10", "padding": "10", "border-width": "2", "font-size": "25.0", "text-color": "black", "column-align": "m", "column-padding": "30"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotLegend">
        <xs:complexType mixed="true">
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:group ref="PLOT-LEGEND-CONTENT" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    _re_word = re.compile(r'("(([^"]|\\")*[^\\])"|""|[^ \t"]+)', (re.MULTILINE | re.UNICODE))

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
        performanceTable.begin("PlotLegend")

        # figure out how to format text
        style = self.getStyleState()
        textStyle = {"fill": style["text-color"], "stroke": "none"}
        for styleProperty in "font", "font-family", "font-size", "font-size-adjust", "font-stretch", "font-style", "font-variant", "font-weight":
            if styleProperty in style:
                textStyle[styleProperty] = style[styleProperty]
        labelAttributes = {"font-size": style["font-size"], defs.XML_SPACE: "preserve", "style": PlotStyle.toString(textStyle)}

        columnAlign = style["column-align"]
        if not set(columnAlign.lower()).issubset(set(["l", "m", "r", "."])):
            raise defs.PmmlValidationError("PlotLegend's column-align style property may only contain the following characters: \"l\", \"m\", \"r\", \".\"")

        columnPadding = float(style["column-padding"])

        ### get an <svg:text> object for each cell

        # content follows the same delimiter logic as Array, except that lineseps outside of quotes signify new table rows
        rowIndex = 0
        colIndex = 0
        cellContents = {}
        for item in sum([[x, x.tail] for x in self.childrenOfClass(PmmlPlotLegendContent)], [self.text]):
            if item is None: pass

            elif isinstance(item, basestring):
                for word in re.finditer(self._re_word, item):
                    one, two, three = word.groups()

                    # quoted text; take it all as-is, without the outermost quotes and unquoting quoted quotes
                    if two is not None:
                        cellContents[rowIndex, colIndex] = svg.text(two.replace(r'\"', '"'), **labelAttributes)
                        colIndex += 1

                    elif one == r'""':
                        colIndex += 1

                    else:
                        newlineIndex = one.find(os.linesep)
                        if newlineIndex == 0 and not (rowIndex == 0 and colIndex == 0):
                            rowIndex += 1
                            colIndex = 0
                        while newlineIndex != -1:
                            if one[:newlineIndex] != "":
                                cellContents[rowIndex, colIndex] = svg.text(one[:newlineIndex], **labelAttributes)
                                rowIndex += 1
                                colIndex = 0

                            one = one[(newlineIndex + len(os.linesep)):]
                            newlineIndex = one.find(os.linesep)

                        if one != "":
                            cellContents[rowIndex, colIndex] = svg.text(one, **labelAttributes)
                            colIndex += 1

            else:
                performanceTable.pause("PlotLegend")
                rowIndex, colIndex = item.draw(dataTable, functionTable, performanceTable, rowIndex, colIndex, cellContents, labelAttributes, plotDefinitions)
                performanceTable.unpause("PlotLegend")

        maxRows = 0
        maxCols = 0
        maxChars = {}
        beforeDot = {}
        afterDot = {}
        for row, col in cellContents:
            if row > maxRows:
                maxRows = row
            if col > maxCols:
                maxCols = col

            if col >= len(columnAlign):
                alignment = columnAlign[-1]
            else:
                alignment = columnAlign[col]

            if col not in maxChars:
                maxChars[col] = 0
                beforeDot[col] = 0
                afterDot[col] = 0

            textContent = cellContents[row, col].text
            if textContent is not None:
                if len(textContent) > maxChars[col]:
                    maxChars[col] = len(textContent)

                if alignment == ".":
                    dotPosition = textContent.find(".")
                    if dotPosition == -1:
                        dotPosition = textContent.find("e")
                        if dotPosition == -1:
                            dotPosition = textContent.find("E")
                            if dotPosition == -1:
                                dotPosition = textContent.find(u"\u00d710")
                                if dotPosition == -1:
                                    dotPosition = len(textContent)
                    if dotPosition > beforeDot[col]:
                        beforeDot[col] = dotPosition
                    if len(textContent) - dotPosition > afterDot[col]:
                        afterDot[col] = len(textContent) - dotPosition
        
        maxRows += 1
        maxCols += 1
        for col in xrange(maxCols):
            if beforeDot[col] + afterDot[col] > maxChars[col]:
                maxChars[col] = beforeDot[col] + afterDot[col]
        cellWidthDenom = float(sum(maxChars.values()))

        ### create a subContentBox and fill the table cells

        svgId = self.get("svgId")
        content = []
        if svgId is None: attrib = {}
        else: attrib = {"id": svgId}

        # change some of the margins based on text, unless overridden by explicit styleProperties

        if style.get("margin-bottom") == "auto": del style["margin-bottom"]
        if style.get("margin-top") == "auto": del style["margin-top"]
        if style.get("margin-left") == "auto": del style["margin-left"]
        if style.get("margin-right") == "auto": del style["margin-right"]

        subContentBox = plotContentBox.subContent(style)
        nominalHeight = maxRows * float(style["font-size"])
        nominalWidth = cellWidthDenom * 0.5*float(style["font-size"]) + columnPadding * (maxCols - 1)

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

        ### create a border rectangle
        if borderRect is not None:
            rectStyle = {"fill": style["background"], "stroke": "none"}
            if "background-opacity" in style:
                rectStyle["fill-opacity"] = style["background-opacity"]

            x1 = borderRect.x
            y1 = borderRect.y
            x2 = borderRect.x + borderRect.width
            y2 = borderRect.y + borderRect.height
            x1, y1 = plotCoordinates(x1, y1)
            x2, y2 = plotCoordinates(x2, y2)

            subAttrib = {"x": repr(x1), "y": repr(y1), "width": repr(x2 - x1), "height": repr(y2 - y1), "style": PlotStyle.toString(rectStyle)}
            if svgId is not None:
                subAttrib["id"] = svgId + ".background"

            if rectStyle["fill"] != "none":
                content.append(svg.rect(**subAttrib))

        ### put the cell content in the table
        if subContentBox is not None:
            cellHeight = subContentBox.height / float(maxRows)
            colStart = [subContentBox.x]
            for col in xrange(maxCols):
                colStart.append(colStart[col] + subContentBox.width * maxChars[col] / cellWidthDenom)

            for row in xrange(maxRows):
                for col in xrange(maxCols):
                    cellContent = cellContents.get((row, col))
                    if cellContent is not None:
                        if col >= len(columnAlign):
                            alignment = columnAlign[-1]
                        else:
                            alignment = columnAlign[col]

                        textContent = None
                        if cellContent.tag == "text" or cellContent.tag[-5:] == "}text":
                            if alignment.lower() == "l":
                                cellContent.set("text-anchor", "start")
                            elif alignment.lower() == "m":
                                cellContent.set("text-anchor", "middle")
                            elif alignment.lower() == "r":
                                cellContent.set("text-anchor", "end")
                            elif alignment.lower() == ".":
                                cellContent.set("text-anchor", "middle")
                            textContent = cellContent.text

                        if alignment.lower() == ".":
                            if textContent is None:
                                alignment = "m"
                            else:
                                dotPosition = textContent.find(".")
                                if dotPosition == -1:
                                    dotPosition = textContent.find("e")
                                    if dotPosition == -1:
                                        dotPosition = textContent.find("E")
                                        if dotPosition == -1:
                                            dotPosition = textContent.find(u"\u00d710")
                                            if dotPosition == -1:
                                                dotPosition = len(textContent) - 0.3
                                dotPosition += 0.2*textContent[:int(math.ceil(dotPosition))].count(u"\u2212")

                                x = (colStart[col] + colStart[col + 1]) / 2.0
                                x -= (dotPosition - 0.5*len(textContent) + 0.5) * nominalWidth/cellWidthDenom

                        if alignment.lower() == "l":
                            x = colStart[col]
                        elif alignment.lower() == "m":
                            x = (colStart[col] + colStart[col + 1]) / 2.0
                        elif alignment.lower() == "r":
                            x = colStart[col + 1]

                        y = subContentBox.y + cellHeight * (row + 0.75)
                        x, y = plotCoordinates(x, y)

                        cellContent.set("transform", "translate(%r,%r)" % (x, y))
                        content.append(cellContent)
        
        ### create a border rectangle (reuses subAttrib, replaces subAttrib["style"])
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

        performanceTable.end("PlotLegend")
        return svg.g(*content, **attrib)
