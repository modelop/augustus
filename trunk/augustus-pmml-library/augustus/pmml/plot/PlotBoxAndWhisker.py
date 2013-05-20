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

"""This module defines the PlotBoxAndWhisker class."""

import itertools

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.NumpyInterface import NP
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.pmml.plot.PlotHistogram import PlotHistogram

class PlotBoxAndWhisker(PmmlPlotContent):
    """Represents a "box-and-whiskers" plot or a "profile histogram."

    PMML subelements:

      - PlotExpression role="sliced": expression to be sliced like a
        histogram.
      - PlotNumericExpression role="profiled": expression to be
        profiled in each slice.
      - PlotSelection: expression or predicate to filter the data
        before plotting.
      - Intervals: non-uniform (numerical) histogram bins.
      - Values: explicit (categorical) histogram values.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - stateId: key for persistent storage in a DataTableState.
      - numBins: number of histogram bins.
      - low: histogram low edge.
      - high: histogram high edge.
      - levels: "percentage" for quartile-like box-and-whiskers,
        "standardDeviation" for mean and standard deviation, as in
        a profile histogram.
      - lowWhisker: bottom of the lower whisker, usually the 0th
        percentile (absolute minimum).
      - lowBox: bottom of the box, usually the 25th percentile.
      - midLine: middle line of the box, usually the median.
      - highBox: top of the box, usually the 75th percentile.
      - highWhisker: top of the upper whisker, usually the 100th
        percentile (absolute maximum).
      - vertical: if "true", plot the "sliced" expression on the
        x axis and the "profiled" expression on the y axis.
      - gap: size of the space between boxes in SVG coordinates.
      - style: CSS style properties.

    CSS properties:

      - fill, fill-opacity: color of the box.
      - stroke, stroke-dasharray, stroke-dashoffset, stroke-linecap,
        stroke-linejoin, stroke-miterlimit, stroke-opacity,
        stroke-width: properties of the line drawing the box and
        the whiskers.

    See the source code for the full XSD.
    """

    styleProperties = ["fill", "fill-opacity", 
                       "stroke", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke-width",
                       ]

    styleDefaults = {"fill": "none", "stroke": "black"}

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotBoxAndWhisker">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element ref="PlotExpression" minOccurs="1" maxOccurs="1" />
                <xs:element ref="PlotNumericExpression" minOccurs="1" maxOccurs="1" />
                <xs:element ref="PlotSelection" minOccurs="0" maxOccurs="1" />
                <xs:choice minOccurs="0" maxOccurs="1">
                    <xs:element ref="Interval" minOccurs="1" maxOccurs="unbounded" />
                    <xs:element ref="Value" minOccurs="1" maxOccurs="unbounded" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />
            <xs:attribute name="numBins" type="xs:positiveInteger" use="optional" />
            <xs:attribute name="low" type="xs:double" use="optional" />
            <xs:attribute name="high" type="xs:double" use="optional" />
            <xs:attribute name="levels" use="optional" default="percentage">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="percentage" />
                        <xs:enumeration value="standardDeviation" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="lowWhisker" type="xs:double" use="optional" default="0" />
            <xs:attribute name="lowBox" type="xs:double" use="optional" default="25" />
            <xs:attribute name="midLine" type="xs:double" use="optional" default="50" />
            <xs:attribute name="highBox" type="xs:double" use="optional" default="75" />
            <xs:attribute name="highWhisker" type="xs:double" use="optional" default="100" />
            <xs:attribute name="vertical" type="xs:boolean" use="optional" default="true" />
            <xs:attribute name="gap" type="xs:double" use="optional" default="10" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    fieldTypeNumeric = FakeFieldType("double", "continuous")

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

        self.checkRoles(["sliced", "profiled"])

        slicedExpression = self.xpath("pmml:PlotExpression[@role='sliced']")
        profiledExpression = self.xpath("pmml:PlotNumericExpression[@role='profiled']")
        cutExpression = self.xpath("pmml:PlotSelection")
        if len(slicedExpression) != 1:
            raise defs.PmmlValidationError("PlotHistogram requires a PlotExpression with role \"sliced\"")
        if len(profiledExpression) != 1:
            raise defs.PmmlValidationError("PlotHistogram requires a PlotNumericExpression with role \"profiled\"")

        slicedDataColumn = slicedExpression[0].evaluate(dataTable, functionTable, performanceTable)
        profiledDataColumn = profiledExpression[0].evaluate(dataTable, functionTable, performanceTable)

        if len(cutExpression) == 1:
            selection = cutExpression[0].select(dataTable, functionTable, performanceTable)
        else:
            selection = NP("ones", len(dataTable), NP.dtype(bool))

        performanceTable.begin("PlotBoxAndWhisker prepare")
        self._saveContext(dataTable)

        if slicedDataColumn.mask is not None:
            NP("logical_and", selection, NP(slicedDataColumn.mask == defs.VALID), selection)
        if profiledDataColumn.mask is not None:
            NP("logical_and", selection, NP(profiledDataColumn.mask == defs.VALID), selection)

        slicedArray = slicedDataColumn.data[selection]
        profiledArray = profiledDataColumn.data[selection]
        
        persistentState = {}
        stateId = self.get("stateId")
        if stateId is not None:
            if stateId in dataTable.state:
                persistentState = dataTable.state[stateId]
            else:
                dataTable.state[stateId] = persistentState

        intervals = self.xpath("pmml:Interval")
        values = self.xpath("pmml:Value")

        if "binType" not in persistentState:
            performanceTable.begin("establish binType")

            binType = PlotHistogram.establishBinType(slicedDataColumn.fieldType, intervals, values)
            persistentState["binType"] = binType

            if binType == "nonuniform":
                persistentState["distributions"] = [NP("empty", 0, dtype=profiledDataColumn.fieldType.dtype) for x in xrange(len(intervals))]

            elif binType == "explicit":
                persistentState["distributions"] = [NP("empty", 0, dtype=profiledDataColumn.fieldType.dtype) for x in xrange(len(values))]

            elif binType == "unique":
                persistentState["distributions"] = {}

            elif binType == "scale":
                numBins = self.get("numBins", convertType=True)
                low = self.get("low", convertType=True)
                high = self.get("high", convertType=True)

                numBins, low, high = PlotHistogram.determineScaleBins(numBins, low, high, slicedArray)

                persistentState["low"] = low
                persistentState["high"] = high
                persistentState["numBins"] = numBins
                persistentState["distributions"] = [NP("empty", 0, dtype=profiledDataColumn.fieldType.dtype) for x in xrange(numBins)]

            performanceTable.end("establish binType")

        if persistentState["binType"] == "nonuniform":
            performanceTable.begin("binType nonuniform")

            distributions = [None] * len(intervals)
            state.edges = []
            lastLimitPoint = None
            lastClosed = None
            lastInterval = None

            for index, interval in enumerate(intervals):
                selection, lastLimitPoint, lastClosed, lastInterval = PlotHistogram.selectInterval(slicedDataColumn.fieldType, slicedArray, index, len(intervals) - 1, interval, state.edges, lastLimitPoint, lastClosed, lastInterval)

                if selection is None:
                    distributions[index] = profiledArray
                else:
                    distributions[index] = profiledArray[selection]

            persistentState["distributions"] = [NP("concatenate", [x, y]) for x, y in itertools.izip(persistentState["distributions"], distributions)]
            distributions = persistentState["distributions"]
            lowEdge = min(low for low, high in state.edges if low is not None)
            highEdge = max(high for low, high in state.edges if high is not None)
            state.slicedFieldType = self.fieldTypeNumeric

            performanceTable.end("binType nonuniform")

        elif persistentState["binType"] == "explicit":
            performanceTable.begin("binType explicit")

            distributions = [None] * len(values)
            displayValues = []

            for index, value in enumerate(values):
                internalValue = slicedDataColumn.fieldType.stringToValue(value["value"])
                displayValues.append(value.get("displayValue", slicedDataColumn.fieldType.valueToString(internalValue, displayValue=True)))

                selection = NP(slicedArray == internalValue)
                distributions[index] = profiledArray[selection]
                
            persistentState["distributions"] = [NP("concatenate", [x, y]) for x, y in itertools.izip(persistentState["distributions"], distributions)]
            distributions = persistentState["distributions"]
            state.edges = displayValues
            state.slicedFieldType = slicedDataColumn.fieldType

            performanceTable.end("binType explicit")

        elif persistentState["binType"] == "unique":
            performanceTable.begin("binType unique")

            uniques, inverse = NP("unique", slicedArray, return_inverse=True)

            persistentDistributions = persistentState["distributions"]
            for i, u in enumerate(uniques):
                string = slicedDataColumn.fieldType.valueToString(u, displayValue=False)
                selection = NP(inverse == i)

                if string in persistentDistributions:
                    persistentDistributions[string] = NP("concatenate", [persistentDistributions[string], profiledArray[selection]])
                else:
                    persistentDistributions[string] = profiledArray[selection]

            tosort = [(len(distribution), string) for string, distribution in persistentDistributions.items()]
            tosort.sort(reverse=True)

            numBins = self.get("numBins", convertType=True)
            if numBins is not None:
                tosort = tosort[:numBins]

            distributions = [persistentDistributions[string] for count, string in tosort]
            state.edges = [slicedDataColumn.fieldType.valueToString(slicedDataColumn.fieldType.stringToValue(string), displayValue=True) for count, string in tosort]
            state.slicedFieldType = slicedDataColumn.fieldType
            
            performanceTable.end("binType unique")

        elif persistentState["binType"] == "scale":
            performanceTable.begin("binType scale")

            numBins = persistentState["numBins"]
            low = persistentState["low"]
            high = persistentState["high"]
            binWidth = (high - low) / float(numBins)

            binAssignments = NP("array", NP("floor", NP(NP(slicedArray - low)/binWidth)), dtype=NP.dtype(int))
            distributions = [None] * numBins

            for index in xrange(numBins):
                selection = NP(binAssignments == index)
                distributions[index] = profiledArray[selection]
                
            persistentState["distributions"] = [NP("concatenate", [x, y]) for x, y in itertools.izip(persistentState["distributions"], distributions)]
            distributions = persistentState["distributions"]
            state.edges = [(low + i*binWidth, low + (i + 1)*binWidth) for i in xrange(numBins)]
            lowEdge = low
            highEdge = high
            state.slicedFieldType = self.fieldTypeNumeric
        
            performanceTable.end("binType scale")

        levels = self.get("levels", defaultFromXsd=True)
        lowWhisker = self.get("lowWhisker", defaultFromXsd=True, convertType=True)
        lowBox = self.get("lowBox", defaultFromXsd=True, convertType=True)
        midLine = self.get("midLine", defaultFromXsd=True, convertType=True)
        highBox = self.get("highBox", defaultFromXsd=True, convertType=True)
        highWhisker = self.get("highWhisker", defaultFromXsd=True, convertType=True)

        state.ranges = []
        minProfiled = None
        maxProfiled = None
        for distribution in distributions:
            if levels == "percentage":
                if len(distribution) > 0:
                    state.ranges.append(NP("percentile", distribution, [lowWhisker, lowBox, midLine, highBox, highWhisker]))
                else:
                    state.ranges.append(None)

            elif levels == "standardDeviation":
                mu = NP("mean", distribution)
                sigma = NP("std", distribution, ddof=1)

                if NP("isfinite", sigma) and sigma > 0.0:
                    state.ranges.append([(lowWhisker - mu)/sigma, (lowBox - mu)/sigma, (midLine - mu)/sigma, (highBox - mu)/sigma, (highWhisker - mu)/sigma])
                else:
                    state.ranges.append(None)

            if state.ranges[-1] is not None:
                if minProfiled is None:
                    minProfiled = min(state.ranges[-1])
                    maxProfiled = max(state.ranges[-1])
                else:
                    minProfiled = min(minProfiled, min(state.ranges[-1]))
                    maxProfiled = max(maxProfiled, max(state.ranges[-1]))

        state.profiledFieldType = profiledDataColumn.fieldType

        if self.get("vertical", defaultFromXsd=True, convertType=True):
            if state.slicedFieldType is self.fieldTypeNumeric:
                plotRange.xminPush(lowEdge, state.slicedFieldType, sticky=False)
                plotRange.xmaxPush(highEdge, state.slicedFieldType, sticky=False)
                if minProfiled is not None:
                    plotRange.yminPush(minProfiled, state.profiledFieldType, sticky=False)
                    plotRange.ymaxPush(maxProfiled, state.profiledFieldType, sticky=False)

            else:
                strings = NP("array", state.edges, dtype=NP.dtype(object))
                if minProfiled is not None:
                    values = NP("ones", len(state.edges), dtype=state.profiledFieldType.dtype) * maxProfiled
                    values[0] = minProfiled
                else:
                    values = NP("zeros", len(state.edges), dtype=state.profiledFieldType.dtype)

                plotRange.expand(strings, values, state.slicedFieldType, state.profiledFieldType)

        else:
            if state.slicedFieldType is self.fieldTypeNumeric:
                plotRange.yminPush(lowEdge, state.slicedFieldType, sticky=False)
                plotRange.ymaxPush(highEdge, state.slicedFieldType, sticky=False)
                if minProfiled is not None:
                    plotRange.xminPush(minProfiled, state.profiledFieldType, sticky=False)
                    plotRange.xmaxPush(maxProfiled, state.profiledFieldType, sticky=False)

            else:
                strings = NP("array", state.edges, dtype=NP.dtype(object))
                if minProfiled is not None:
                    values = NP("ones", len(state.edges), dtype=state.profiledFieldType.dtype) * maxProfiled
                    values[0] = minProfiled
                else:
                    values = NP("zeros", len(state.edges), dtype=state.profiledFieldType.dtype)
                
                plotRange.expand(values, strings, state.profiledFieldType, state.slicedFieldType)

        performanceTable.end("PlotBoxAndWhisker prepare")

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
        performanceTable.begin("PlotBoxAndWhisker draw")

        vertical = self.get("vertical", defaultFromXsd=True, convertType=True)
        gap = self.get("gap", defaultFromXsd=True, convertType=True)

        if state.slicedFieldType is not self.fieldTypeNumeric:
            if vertical:
                strings = plotCoordinates.xstrings
            else:
                strings = plotCoordinates.ystrings

            newRanges = []
            for string in strings:
                try:
                    index = state.edges.index(string)
                except ValueError:
                    newRanges.append(None)
                else:
                    newRanges.append(state.ranges[index])

            state.ranges = newRanges
            state.edges = [(i - 0.5, i + 0.5) for i in xrange(len(strings))]

        lowEdge = NP("array", [low if low is not None else float("-inf") for low, high in state.edges], dtype=NP.dtype(float))
        highEdge = NP("array", [high if high is not None else float("inf") for low, high in state.edges], dtype=NP.dtype(float))

        selection = NP("array", [levels is not None for levels in state.ranges], dtype=NP.dtype(bool))
        lowEdge = lowEdge[selection]
        highEdge = highEdge[selection]

        lowWhisker  = NP("array", [levels[0] for levels in state.ranges if levels is not None], dtype=state.profiledFieldType.dtype)
        lowBox      = NP("array", [levels[1] for levels in state.ranges if levels is not None], dtype=state.profiledFieldType.dtype)
        midLine     = NP("array", [levels[2] for levels in state.ranges if levels is not None], dtype=state.profiledFieldType.dtype)
        highBox     = NP("array", [levels[3] for levels in state.ranges if levels is not None], dtype=state.profiledFieldType.dtype)
        highWhisker = NP("array", [levels[4] for levels in state.ranges if levels is not None], dtype=state.profiledFieldType.dtype)
        
        output = svg.g()
        if len(lowEdge) > 0:
            if vertical:
                Ax = lowEdge
                Bx = lowEdge
                Cx = lowEdge
                Dx = highEdge
                Ex = highEdge
                Fx = highEdge
                Gx = NP(NP(lowEdge + highEdge) / 2.0)
                Hx = Gx
                Ix = Gx
                Jx = Gx

                Ay = lowBox
                By = midLine
                Cy = highBox
                Dy = lowBox
                Ey = midLine
                Fy = highBox
                Gy = lowWhisker
                Hy = lowBox
                Iy = highBox
                Jy = highWhisker

            else:
                Ax = lowBox
                Bx = midLine
                Cx = highBox
                Dx = lowBox
                Ex = midLine
                Fx = highBox
                Gx = lowWhisker
                Hx = lowBox
                Ix = highBox
                Jx = highWhisker

                Ay = lowEdge
                By = lowEdge
                Cy = lowEdge
                Dy = highEdge
                Ey = highEdge
                Fy = highEdge
                Gy = NP(NP(lowEdge + highEdge) / 2.0)
                Hy = Gy
                Iy = Gy
                Jy = Gy

            AX, AY = plotCoordinates(Ax, Ay)
            BX, BY = plotCoordinates(Bx, By)
            CX, CY = plotCoordinates(Cx, Cy)
            DX, DY = plotCoordinates(Dx, Dy)
            EX, EY = plotCoordinates(Ex, Ey)
            FX, FY = plotCoordinates(Fx, Fy)
            GX, GY = plotCoordinates(Gx, Gy)
            HX, HY = plotCoordinates(Hx, Hy)
            IX, IY = plotCoordinates(Ix, Iy)
            JX, JY = plotCoordinates(Jx, Jy)

            if vertical:
                if gap > 0.0 and NP(NP(DX - gap/2.0) - NP(AX + gap/2.0)).min() > 0.0:
                    AX += gap/2.0
                    BX += gap/2.0
                    CX += gap/2.0
                    DX -= gap/2.0
                    EX -= gap/2.0
                    FX -= gap/2.0
            else:
                if gap > 0.0 and NP(NP(DY - gap/2.0) - NP(AY + gap/2.0)).min() > 0.0:
                    AY += gap/2.0
                    BY += gap/2.0
                    CY += gap/2.0
                    DY -= gap/2.0
                    EY -= gap/2.0
                    FY -= gap/2.0

            style = self.getStyleState()
            strokeStyle = dict((x, style[x]) for x in style if x.startswith("stroke"))
            strokeStyle["fill"] = "none"
            style = PlotStyle.toString(style)
            strokeStyle = PlotStyle.toString(strokeStyle)

            for i in xrange(len(lowEdge)):
                pathdata = ["M %r %r" % (HX[i], HY[i]),
                            "L %r %r" % (AX[i], AY[i]),
                            "L %r %r" % (BX[i], BY[i]),
                            "L %r %r" % (CX[i], CY[i]),
                            "L %r %r" % (IX[i], IY[i]),
                            "L %r %r" % (FX[i], FY[i]),
                            "L %r %r" % (EX[i], EY[i]),
                            "L %r %r" % (DX[i], DY[i]),
                            "L %r %r" % (HX[i], HY[i]),
                            "Z"]
                output.append(svg.path(d=" ".join(pathdata), style=style))
                output.append(svg.path(d="M %r %r L %r %r" % (BX[i], BY[i], EX[i], EY[i]), style=strokeStyle))
                output.append(svg.path(d="M %r %r L %r %r" % (HX[i], HY[i], GX[i], GY[i]), style=strokeStyle))
                output.append(svg.path(d="M %r %r L %r %r" % (IX[i], IY[i], JX[i], JY[i]), style=strokeStyle))

                if vertical:
                    width = (DX[i] - AX[i]) / 4.0
                    output.append(svg.path(d="M %r %r L %r %r" % (GX[i] - width, GY[i], GX[i] + width, GY[i]), style=strokeStyle))
                    output.append(svg.path(d="M %r %r L %r %r" % (JX[i] - width, JY[i], JX[i] + width, JY[i]), style=strokeStyle))
                else:
                    width = (DY[i] - AY[i]) / 4.0
                    output.append(svg.path(d="M %r %r L %r %r" % (GX[i], GY[i] - width, GX[i], GY[i] + width), style=strokeStyle))
                    output.append(svg.path(d="M %r %r L %r %r" % (JX[i], JY[i] - width, JX[i], JY[i] + width), style=strokeStyle))

        performanceTable.end("PlotBoxAndWhisker draw")

        svgId = self.get("svgId")
        if svgId is not None:
            output["id"] = svgId

        return output
