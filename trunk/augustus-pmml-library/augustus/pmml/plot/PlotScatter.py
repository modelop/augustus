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

"""This module defines the PlotScatter class."""

import math
import random
import itertools
import copy

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.NumpyInterface import NP
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.pmml.plot.PlotSvgAnnotation import PlotSvgAnnotation

class PlotScatter(PmmlPlotContent):
    """PlotScatter represents a scatter plot of x-y values defined by
    two expressions.

    PMML subelements:

      - PlotNumericExpression role="x"
      - PlotNumericExpression role="y"
      - PlotNumericExpression role="x-errorbar"
      - PlotNumericExpression role="x-errorbar-up"
      - PlotNumericExpression role="x-errorbar-down"
      - PlotNumericExpression role="y-errorbar"
      - PlotNumericExpression role="y-errorbar-up"
      - PlotNumericExpression role="y-errorbar-down"
      - PlotNumericExpression role="weight"
      - PlotSelection: expression or predicate to filter the data before plotting.

    Errorbars do not need to be specified, but asymmetric and
    symmetric error bars are mututally exclusive.

    The optional C{weight} scales the opacity according to values
    observed in data.  These must be scaled by the user to lie in the
    range 0 to 1.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - stateId: key for persistent storage in a DataTableState.
      - marker: type of marker, must be one of PLOT-MARKER-TYPE.
      - limit: optional number specifying the maximum number of data
        points to generate.  If the true number of data points exceeds
        this limit, points will be randomly chosen.
      - style: CSS style properties.

    CSS properties:

      - fill, fill-opacity: color of the markers.
      - stroke, stroke-dasharray, stroke-dashoffset, stroke-linecap,
        stroke-linejoin, stroke-miterlimit, stroke-opacity,
        stroke-width: properties of the marker lines and error bar
        lines.
      - marker-size: size of the marker.
      - marker-outline: optional outline for the marker.

    See the source code for the full XSD.
    """

    styleProperties = ["fill", "fill-opacity", 
                       "stroke", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke-width",
                       "marker-size", "marker-outline",
                       ]

    styleDefaults = {"fill": "black", "stroke": "black", "marker-size": "5", "marker-outline": "none"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotScatter">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element ref="PlotNumericExpression" minOccurs="2" maxOccurs="9" />
                <xs:element ref="PlotSelection" minOccurs="0" maxOccurs="1" />
                <xs:element ref="PlotSvgMarker" minOccurs="0" maxOccurs="1" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />
            <xs:attribute name="marker" type="PLOT-MARKER-TYPE" use="optional" default="circle" />
            <xs:attribute name="limit" type="INT-NUMBER" use="optional" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    xsdRemove = ["PLOT-MARKER-TYPE", "PlotSvgMarker"]

    xsdAppend = ["""<xs:simpleType name="PLOT-MARKER-TYPE" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:restriction base="xs:string">
        <xs:enumeration value="circle" />
        <xs:enumeration value="square" />
        <xs:enumeration value="diamond" />
        <xs:enumeration value="plus" />
        <xs:enumeration value="times" />
        <xs:enumeration value="svg" />
    </xs:restriction>
</xs:simpleType>
""",
                 """<xs:element name="PlotSvgMarker" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType>
        <xs:complexContent>
            <xs:restriction base="xs:anyType">
                <xs:sequence>
                    <xs:any minOccurs="0" maxOccurs="1" processContents="skip" />
                </xs:sequence>
                <xs:attribute name="fileName" type="xs:string" use="optional" />
            </xs:restriction>
        </xs:complexContent>
    </xs:complexType>
</xs:element>
"""]

    @staticmethod
    def makeMarker(svgIdMarker, marker, style, plotSvgMarker):
        """Construct a marker from a set of known shapes or an SVG
        pictogram.

        @type svgIdMarker: string
        @param svgIdMarker: SVG id for the new marker.
        @type marker: string
        @param marker: Name of the marker shape; must be one of PLOT-MARKER-TYPE.
        @type style: dict
        @param style: CSS style for the marker in dictionary form.
        @type plotSvgMarker: PmmlBinding or None
        @param plotSvgMarker: A PlotSvgMarker element, which either contains an inline SvgBinding or a fileName pointing to an external image.
        @rtype: SvgBinding
        @return: The marker image, appropriate for adding to a PlotDefinitions.
        """

        svg = SvgBinding.elementMaker

        style["stroke"] = style["marker-outline"]
        del style["marker-outline"]
        markerSize = float(style["marker-size"])
        del style["marker-size"]

        if marker == "circle":
            return svg.circle(id=svgIdMarker, cx="0", cy="0", r=repr(markerSize), style=PlotStyle.toString(style))

        elif marker == "square":
            p =  markerSize
            m = -markerSize
            return svg.path(id=svgIdMarker, d="M %r,%r L %r,%r L %r,%r L %r,%r z" % (m,m, p,m, p,p, m,p), style=PlotStyle.toString(style))

        elif marker == "diamond":
            p =  math.sqrt(2.0) * markerSize
            m = -math.sqrt(2.0) * markerSize
            return svg.path(id=svgIdMarker, d="M %r,0 L 0,%r L %r,0 L 0,%r z" % (m, m, p, p), style=PlotStyle.toString(style))

        elif marker == "plus":
            p =  markerSize
            m = -markerSize
            if style["stroke"] == "none":
                style["stroke"] = style["fill"]
            style["fill"] = "none"
            return svg.path(id=svgIdMarker, d="M %r,0 L %r,0 M 0,%r L 0,%r" % (m, p, m, p), style=PlotStyle.toString(style))

        elif marker == "times":
            p =  math.sqrt(2.0) * markerSize
            m = -math.sqrt(2.0) * markerSize
            if style["stroke"] == "none":
                style["stroke"] = style["fill"]
            style["fill"] = "none"
            return svg.path(id=svgIdMarker, d="M %r,%r L %r,%r M %r,%r L %r,%r" % (m,m, p,p, p,m, m,p), style=PlotStyle.toString(style))

        elif marker == "svg":
            if plotSvgMarker is None:
                raise defs.PmmlValidationError("When marker is \"svg\", a PlotSvgMarker must be provided")

            inlineSvg = plotSvgMarker.getchildren()
            fileName = plotSvgMarker.get("fileName")
            if len(inlineSvg) == 1 and fileName is None:
                svgBinding = inlineSvg[0]
            elif len(inlineSvg) == 0 and fileName is not None:
                svgBinding = SvgBinding.loadXml(fileName)
            else:
                raise defs.PmmlValidationError("PlotSvgMarker should specify an inline SVG or a fileName but not both or neither")

            sx1, sy1, sx2, sy2 = PlotSvgAnnotation.findSize(svgBinding)
            tx1, ty1 = -markerSize, -markerSize
            tx2, ty2 = markerSize, markerSize

            transform = "translate(%r, %r) scale(%r, %r)" % (tx1 - sx1, ty1 - sy1, (tx2 - tx1)/float(sx2 - sx1), (ty2 - ty1)/float(sy2 - sy1))
            return svg.g(copy.deepcopy(svgBinding), id=svgIdMarker, transform=transform)

    @staticmethod
    def drawErrorbars(xarray, yarray, exup, exdown, eyup, eydown, markerSize, strokeStyle, weight=None):
        """Draw a set of error bars, given values in global SVG
        coordinates.

        @type xarray: 1d Numpy array
        @param xarray: The X positions in global SVG coordinates.
        @type yarray: 1d Numpy array
        @param yarray: The Y positions in global SVG coordinates.
        @type exup: 1d Numpy array or None
        @param exup: The upper ends of the X error bars in global SVG coordinates (already added to the X positions).
        @type exdown: 1d Numpy array or None
        @param exdown: The lower ends of the X error bars in global SVG coordinates (already added to the X positions).
        @type eyup: 1d Numpy array or None
        @param eyup: The upper ends of the Y error bars in global SVG coordinates (already added to the Y positions).
        @type eydown: 1d Numpy array or None
        @param eydown: The lower ends of the Y error bars in global SVG coordinates (already added to the Y positions).
        @type markerSize: number
        @param markerSize: Size of the marker in SVG coordinates.
        @type strokeStyle: dict
        @param strokeStyle: CSS style attributes appropriate for stroking (not filling) in dictionary form.
        @type weight: 1d Numpy array or None
        @param weight: The opacity of each point (if None, the opacity is not specified and is therefore fully opaque).
        """

        svg = SvgBinding.elementMaker
        output = []
        
        strokeStyle = copy.copy(strokeStyle)
        strokeStyle["fill"] = "none"
        if weight is not None:
            strokeStyle["opacity"] = "1"

        for i in xrange(len(xarray)):
            x = xarray[i]
            y = yarray[i]

            pathdata = []

            if exup is not None:
                pathdata.append("M %r %r L %r %r" % (exdown[i], y             ,   exup[i], y             ))
                pathdata.append("M %r %r L %r %r" % (exdown[i], y - markerSize, exdown[i], y + markerSize))
                pathdata.append("M %r %r L %r %r" % (  exup[i], y - markerSize,   exup[i], y + markerSize))

            if eyup is not None:
                pathdata.append("M %r %r L %r %r" % (x             , eydown[i], x             ,   eyup[i]))
                pathdata.append("M %r %r L %r %r" % (x - markerSize, eydown[i], x + markerSize, eydown[i]))
                pathdata.append("M %r %r L %r %r" % (x - markerSize, eyup[i],   x + markerSize,   eyup[i]))

            if len(pathdata) > 0:
                if weight is not None:
                    strokeStyle["opacity"] = repr(weight[i])
                output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(strokeStyle)))

        return output

    def _makeMarker(self, plotDefinitions):
        """Used by C{draw}."""

        style = self.getStyleState()

        svgId = self.get("svgId")
        if svgId is None:
            svgIdMarker = plotDefinitions.uniqueName()
        else:
            svgIdMarker = svgId + ".marker"

        marker = self.get("marker", defaultFromXsd=True)

        return self.makeMarker(svgIdMarker, marker, style, self.childOfTag("PlotSvgMarker"))

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

        self.checkRoles(["x", "y", "x-errorbar", "x-errorbar-up", "x-errorbar-down", "y-errorbar", "y-errorbar-up", "y-errorbar-down", "weight"])

        xExpression = self.xpath("pmml:PlotNumericExpression[@role='x']")
        yExpression = self.xpath("pmml:PlotNumericExpression[@role='y']")

        cutExpression = self.xpath("pmml:PlotSelection")

        exExpression = self.xpath("pmml:PlotNumericExpression[@role='x-errorbar']")
        exupExpression = self.xpath("pmml:PlotNumericExpression[@role='x-errorbar-up']")
        exdownExpression = self.xpath("pmml:PlotNumericExpression[@role='x-errorbar-down']")

        eyExpression = self.xpath("pmml:PlotNumericExpression[@role='y-errorbar']")
        eyupExpression = self.xpath("pmml:PlotNumericExpression[@role='y-errorbar-up']")
        eydownExpression = self.xpath("pmml:PlotNumericExpression[@role='y-errorbar-down']")

        weightExpression = self.xpath("pmml:PlotNumericExpression[@role='weight']")

        if len(xExpression) != 1 or len(yExpression) != 1:
            raise defs.PmmlValidationError("PlotScatter requires two PlotNumericExpressions, one with role \"x\", the other with role \"y\"")

        xValues = xExpression[0].evaluate(dataTable, functionTable, performanceTable)
        yValues = yExpression[0].evaluate(dataTable, functionTable, performanceTable)

        if len(cutExpression) == 1:
            selection = cutExpression[0].select(dataTable, functionTable, performanceTable)
        else:
            selection = NP("ones", len(dataTable), NP.dtype(bool))

        if len(exExpression) == 0 and len(exupExpression) == 0 and len(exdownExpression) == 0:
            exup, exdown = None, None
        elif len(exExpression) == 1 and len(exupExpression) == 0 and len(exdownExpression) == 0:
            exup = exExpression[0].evaluate(dataTable, functionTable, performanceTable)
            exdown = None
        elif len(exExpression) == 0 and len(exupExpression) == 1 and len(exdownExpression) == 1:
            exup = exupExpression[0].evaluate(dataTable, functionTable, performanceTable)
            exdown = exdownExpression[0].evaluate(dataTable, functionTable, performanceTable)
        else:
            raise defs.PmmlValidationError("Use \"x-errorbar\" for symmetric error bars or \"x-errorbar-up\" and \"x-errorbar-down\" for asymmetric errorbars, but no other combinations")

        if len(eyExpression) == 0 and len(eyupExpression) == 0 and len(eydownExpression) == 0:
            eyup, eydown = None, None
        elif len(eyExpression) == 1 and len(eyupExpression) == 0 and len(eydownExpression) == 0:
            eyup = eyExpression[0].evaluate(dataTable, functionTable, performanceTable)
            eydown = None
        elif len(eyExpression) == 0 and len(eyupExpression) == 1 and len(eydownExpression) == 1:
            eyup = eyupExpression[0].evaluate(dataTable, functionTable, performanceTable)
            eydown = eydownExpression[0].evaluate(dataTable, functionTable, performanceTable)
        else:
            raise defs.PmmlValidationError("Use \"y-errorbar\" for symmetric error bars or \"y-errorbar-up\" and \"y-errorbar-down\" for asymmetric errorbars, but no other combinations")

        if len(weightExpression) == 1:
            weight = weightExpression[0].evaluate(dataTable, functionTable, performanceTable)
        else:
            weight = None

        performanceTable.begin("PlotScatter prepare")

        if xValues.mask is not None:
            NP("logical_and", selection, NP(xValues.mask == defs.VALID), selection)
        if yValues.mask is not None:
            NP("logical_and", selection, NP(yValues.mask == defs.VALID), selection)

        if exup is not None and exup.mask is not None:
            NP("logical_and", selection, NP(exup.mask == defs.VALID), selection)
        if exdown is not None and exdown.mask is not None:
            NP("logical_and", selection, NP(exdown.mask == defs.VALID), selection)
        if eyup is not None and eyup.mask is not None:
            NP("logical_and", selection, NP(eyup.mask == defs.VALID), selection)
        if eydown is not None and eydown.mask is not None:
            NP("logical_and", selection, NP(eydown.mask == defs.VALID), selection)

        state.x = xValues.data[selection]
        state.y = yValues.data[selection]

        state.exup, state.exdown, state.eyup, state.eydown = None, None, None, None
        if exup is not None:
            state.exup = exup.data[selection]
        if exdown is not None:
            state.exdown = exdown.data[selection]
        if eyup is not None:
            state.eyup = eyup.data[selection]
        if eydown is not None:
            state.eydown = eydown.data[selection]

        state.weight = None
        if weight is not None:
            state.weight = weight.data[selection]

        stateId = self.get("stateId")
        if stateId is not None:
            persistentState = dataTable.state.get(stateId)
            if persistentState is None:
                persistentState = {}
                dataTable.state[stateId] = persistentState
            else:
                state.x = NP("concatenate", (persistentState["x"], state.x))
                state.y = NP("concatenate", (persistentState["y"], state.y))

                if exup is not None:
                    state.exup = NP("concatenate", (persistentState["exup"], state.exup))
                if exdown is not None:
                    state.exdown = NP("concatenate", (persistentState["exdown"], state.exdown))
                if eyup is not None:
                    state.eyup = NP("concatenate", (persistentState["eyup"], state.eyup))
                if eydown is not None:
                    state.eydown = NP("concatenate", (persistentState["eydown"], state.eydown))

                if weight is not None:
                    state.weight = NP("concatenate", (persistentState["weight"], state.weight))

            persistentState["x"] = state.x
            persistentState["y"] = state.y

            if exup is not None:
                persistentState["exup"] = state.exup
            if exdown is not None:
                persistentState["exdown"] = state.exdown
            if eyup is not None:
                persistentState["eyup"] = state.eyup
            if eydown is not None:
                persistentState["eydown"] = state.eydown

            if weight is not None:
                persistentState["weight"] = state.weight

        plotRange.expand(state.x, state.y, xValues.fieldType, yValues.fieldType)
        performanceTable.end("PlotScatter prepare")

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
        performanceTable.begin("PlotScatter draw")

        output = svg.g()

        marker = self._makeMarker(plotDefinitions)
        plotDefinitions[marker.get("id")] = marker

        # selection = NP("isfinite", state.x)
        # NP("logical_and", selection, NP("isfinite", state.y))
        # state.x = state.x[selection]
        # state.y = state.y[selection]

        plotx, ploty, plotexup, plotexdown, ploteyup, ploteydown, plotweight = None, None, None, None, None, None, None

        if self.get("limit") is not None and int(self.get("limit")) < len(state.x):
            indexes = random.sample(xrange(len(state.x)), int(self.get("limit")))
            plotx = state.x[indexes]
            ploty = state.y[indexes]

            if state.exup is not None:
                if state.exdown is not None:
                    plotexup = NP(plotx + NP("absolute", state.exup[indexes]))
                    plotexdown = NP(plotx - NP("absolute", state.exdown[indexes]))
                else:
                    plotexup = NP(plotx + NP("absolute", state.exup[indexes]))
                    plotexdown = NP(plotx - NP("absolute", state.exup[indexes]))

            if state.eyup is not None:
                if state.eydown is not None:
                    ploteyup = NP(ploty + NP("absolute", state.eyup[indexes]))
                    ploteydown = NP(ploty - NP("absolute", state.eydown[indexes]))
                else:
                    ploteyup = NP(ploty + NP("absolute", state.eyup[indexes]))
                    ploteydown = NP(ploty - NP("absolute", state.eyup[indexes]))

            if state.weight is not None:
                plotweight = state.weight[indexes]

        else:
            plotx = state.x
            ploty = state.y

            if state.exup is not None:
                if state.exdown is not None:
                    plotexup = NP(plotx + NP("absolute", state.exup))
                    plotexdown = NP(plotx - NP("absolute", state.exdown))
                else:
                    plotexup = NP(plotx + NP("absolute", state.exup))
                    plotexdown = NP(plotx - NP("absolute", state.exup))

            if state.eyup is not None:
                if state.eydown is not None:
                    ploteyup = NP(ploty + NP("absolute", state.eyup))
                    ploteydown = NP(ploty - NP("absolute", state.eydown))
                else:
                    ploteyup = NP(ploty + NP("absolute", state.eyup))
                    ploteydown = NP(ploty - NP("absolute", state.eyup))

            if state.weight is not None:
                plotweight = state.weight

        if plotexup is not None:
            plotexup, dummy = plotCoordinates(plotexup, ploty)
        if plotexdown is not None:
            plotexdown, dummy = plotCoordinates(plotexdown, ploty)
        if ploteyup is not None:
            dummy, ploteyup = plotCoordinates(plotx, ploteyup)
        if ploteydown is not None:
            dummy, ploteydown = plotCoordinates(plotx, ploteydown)

        plotx, ploty = plotCoordinates(plotx, ploty)

        style = self.getStyleState()
        strokeStyle = dict((x, style[x]) for x in style if x.startswith("stroke"))
        output.extend(self.drawErrorbars(plotx, ploty, plotexup, plotexdown, ploteyup, ploteydown, float(style["marker-size"]), strokeStyle, weight=plotweight))

        markerReference = "#" + marker.get("id")
        if plotweight is None:
            output.extend(svg.use(**{"x": repr(x), "y": repr(y), defs.XLINK_HREF: markerReference}) for x, y in itertools.izip(plotx, ploty))
        else:
            output.extend(svg.use(**{"x": repr(x), "y": repr(y), "style": "opacity: %r;" % w, defs.XLINK_HREF: markerReference}) for x, y, w in itertools.izip(plotx, ploty, plotweight))

        svgId = self.get("svgId")
        if svgId is not None:
            output["id"] = svgId

        performanceTable.end("PlotScatter draw")
        return output
