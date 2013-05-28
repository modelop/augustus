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

"""This module defines the PlotHistogram class."""

import math
import itertools

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.NumpyInterface import NP
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.core.plot.PlotCoordinates import PlotCoordinates
from augustus.pmml.plot.PlotCurve import PlotCurve
from augustus.pmml.plot.PlotScatter import PlotScatter

class PlotHistogram(PmmlPlotContent):
    """Represents a 1d histogram of the data.

    PMML subelements:

      - PlotExpression role="data": the numeric or categorical data.
      - PlotNumericExpression role="weight": histogram weights.
      - PlotSelection: expression or predicate to filter the data
        before plotting.
      - Intervals: non-uniform (numerical) histogram bins.
      - Values: explicit (categorical) histogram values.
      - PlotSvgMarker: inline SVG for histograms drawn with markers,
        where the markers are SVG pictograms.

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - stateId: key for persistent storage in a DataTableState.
      - numBins: number of histogram bins.
      - low: histogram low edge.
      - high: histogram high edge.
      - normalized: if "false", the histogram represents the number
        of counts in each bin; if "true", the histogram represents
        density, with a total integral (taking into account bin
        widths) of 1.0.
      - cumulative: if "false", the histogram approximates a
        probability density function (PDF) with flat-top bins;
        if "true", the histogram approximates a cumulative
        distribution function (CDF) with linear-top bins.
      - vertical: if "true", plot the "data" expression on the x
        axis and the counts/density/cumulative values on the y
        axis.
      - visualization: one of "skyline", "polyline", "smooth",
        "points", "errorbars".
      - gap: size of the space between histogram bars in SVG
        coordinates.
      - marker: marker to use for "points" visualization (see
        PlotScatter).
      - style: CSS style properties.
        
    CSS properties:
      - fill, fill-opacity: color of the histogram bars.
      - stroke, stroke-dasharray, stroke-dashoffset, stroke-linecap,
        stroke-linejoin, stroke-miterlimit, stroke-opacity,
        stroke-width: properties of the line drawing.
      - marker-size, marker-outline: marker style for "points"
        visualization.

    See the source code for the full XSD.
    """

    styleProperties = ["fill", "fill-opacity", 
                       "stroke", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke-width",
                       "marker-size", "marker-outline",
                       ]

    styleDefaults = {"fill": "none", "stroke": "black", "marker-size": "5", "marker-outline": "none"}
    
    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotHistogram">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element ref="PlotExpression" minOccurs="1" maxOccurs="1" />
                <xs:element ref="PlotNumericExpression" minOccurs="0" maxOccurs="1" />
                <xs:element ref="PlotSelection" minOccurs="0" maxOccurs="1" />
                <xs:choice minOccurs="0" maxOccurs="1">
                    <xs:element ref="Interval" minOccurs="1" maxOccurs="unbounded" />
                    <xs:element ref="Value" minOccurs="1" maxOccurs="unbounded" />
                </xs:choice>
                <xs:element ref="PlotSvgMarker" minOccurs="0" maxOccurs="1" />
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />
            <xs:attribute name="numBins" type="xs:positiveInteger" use="optional" />
            <xs:attribute name="low" type="xs:double" use="optional" />
            <xs:attribute name="high" type="xs:double" use="optional" />
            <xs:attribute name="normalized" type="xs:boolean" use="optional" default="false" />
            <xs:attribute name="cumulative" type="xs:boolean" use="optional" default="false" />
            <xs:attribute name="vertical" type="xs:boolean" use="optional" default="true" />
            <xs:attribute name="visualization" use="optional" default="skyline">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="skyline" />
                        <xs:enumeration value="polyline" />
                        <xs:enumeration value="smooth" />
                        <xs:enumeration value="points" />
                        <xs:enumeration value="errorbars" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="gap" type="xs:double" use="optional" default="0.0" />
            <xs:attribute name="marker" type="PLOT-MARKER-TYPE" use="optional" default="circle" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    fieldType = FakeFieldType("double", "continuous")
    fieldTypeNumeric = FakeFieldType("double", "continuous")

    @staticmethod
    def establishBinType(fieldType, intervals, values):
        """Determine the type of binning to use for a histogram with
        the given FieldType, Intervals, and Values.

        @type fieldType: FieldType
        @param fieldType: The FieldType of the plot expression.
        @type intervals: list of PmmlBinding
        @param intervals: The <Interval> elements; may be empty.
        @type values: list of PmmlBinding
        @param values: The <Value> elements; may be empty.
        @rtype: string
        @return: One of "nonuniform", "explicit", "unique", "scale".
        """

        if len(intervals) > 0:
            if not fieldType.isnumeric() and not fieldType.istemporal():
                raise defs.PmmlValidationError("Explicit Intervals are intended for numerical data, not %r" % fieldType)
            return "nonuniform"

        elif len(values) > 0:
            if not fieldType.isstring():
                raise defs.PmmlValidationError("Explicit Values are intended for string data, not %r" % fieldType)
            return "explicit"

        elif fieldType.isstring():
            return "unique"

        else:
            if not fieldType.isnumeric() and not fieldType.istemporal():
                raise defs.PmmlValidationError("PlotHistogram requires numerical or string data, not %r" % fieldType)
            return "scale"

    @staticmethod
    def determineScaleBins(numBins, low, high, array):
        """Determine the C{numBins}, C{low}, and C{high} of the
        histogram from explicitly set values where available and
        implicitly derived values where necessary.

        Explicitly set values always override implicit values derived
        from the dataset.
          - C{low}, C{high} implicit values are the extrema of the
            dataset.
          - C{numBins} implicit value is the Freedman-Diaconis
            heuristic for number of histogram bins.

        @type numBins: int or None
        @param numBins: Input number of bins.
        @type low: number or None
        @param low: Low edge.
        @type high: number or None
        @param high: High edge.
        @type array: 1d Numpy array of numbers
        @param array: Dataset to use to implicitly derive values.
        @rtype: 3-tuple
        @return: C{numBins}, C{low}, C{high}
        """

        generateLow = (low is None)
        generateHigh = (high is None)

        if generateLow: low = float(array.min())
        if generateHigh: high = float(array.max())

        if low == high:
            low, high = low - 1.0, high + 1.0
        elif high < low:
            if generateLow:
                low = high - 1.0
            elif generateHigh:
                high = low + 1.0
            else:
                raise defs.PmmlValidationError("PlotHistogram attributes low and high must be in the right order: low = %g, high = %g" % (low, high))
        else:
            if generateLow and generateHigh:
                low, high = low - 0.2*(high - low), high + 0.2*(high - low)
            elif generateLow:
                low = low - 0.2*(high - low)
            elif generateHigh:
                high = high + 0.2*(high - low)

        if numBins is None:
            # the Freedman-Diaconis rule
            q1, q3 = NP("percentile", array, [25.0, 75.0])
            binWidth = 2.0 * (q3 - q1) / math.pow(len(array), 1.0/3.0)
            if binWidth > 0.0:
                numBins = max(10, int(math.ceil((high - low)/binWidth)))
            else:
                numBins = 10

        return numBins, low, high

    @staticmethod
    def selectInterval(fieldType, array, index, lastIndex, interval, edges, lastLimitPoint, lastClosed, lastInterval):
        """Select rows of an array within an interval as part of
        filling a non-uniform histogram.

        @type fieldType: FieldType
        @param fieldType: FieldType used to interpret the bounds of the interval.
        @type array: 1d Numpy array
        @param array: Values to select.
        @type index: int
        @param index: Current bin index.
        @type lastIndex: int
        @param lastIndex: Previous bin index.
        @type interval: PmmlBinding
        @param interval: PMML <Interval> element defining the interval.
        @type edges: list of 2-tuples
        @param edges: Pairs of interpreted C{leftMargin}, C{rightMargin} for the histogram.
        @type lastLimitPoint: number
        @param lastLimitPoint: Larger of the two last edges.  ("Limit point" because it may have been open or closed.)
        @type lastClosed: bool
        @param lastClosed: If True, the last limit point was closed.
        @type lastInterval: PmmlBinding
        @param lastInterval: PMML <Interval> for the last bin.
        @rtype: 4-tuple
        @return: C{selection} (1d Numpy array of bool), C{lastLimitPoint}, C{lastClosed}, C{lastInterval}
        """

        closure = interval["closure"]
        leftMargin = interval.get("leftMargin")
        rightMargin = interval.get("rightMargin")

        selection = None

        if leftMargin is None and rightMargin is None and len(intervals) != 1:
            raise defs.PmmlValidationError("If a histogram bin is unbounded on both ends, it must be the only bin")

        if leftMargin is not None:
            try:
                leftMargin = fieldType.stringToValue(leftMargin)
            except ValueError:
                raise defs.PmmlValidationError("Improper value in Interval leftMargin specification: \"%s\"" % leftMargin)

            if closure in ("openClosed", "openOpen"):
                if selection is None:
                    selection = NP(leftMargin < array)
                else:
                    NP("logical_and", selection, NP(leftMargin < array), selection)

            elif closure in ("closedOpen", "closedClosed"):
                if selection is None:
                    selection = NP(leftMargin <= array)
                else:
                    NP("logical_and", selection, NP(leftMargin <= array), selection)

            if lastLimitPoint is not None:
                if leftMargin < lastLimitPoint or (leftMargin == lastLimitPoint and (closure in ("closedOpen", "closedClosed")) and lastClosed):
                    raise defs.PmmlValidationError("Intervals are out of order or overlap: %r and %r" % (lastInterval, interval))

        elif index != 0:
            raise defs.PmmlValidationError("Only the first Interval can have an open-ended leftMargin: %r" % interval)

        if rightMargin is not None:
            try:
                rightMargin = fieldType.stringToValue(rightMargin)
            except ValueError:
                raise defs.PmmlValidationError("Improper value in Interval rightMargin specification: \"%s\"" % rightMargin)

            if closure in ("openOpen", "closedOpen"):
                if selection is None:
                    selection = NP(array < rightMargin)
                else:
                    NP("logical_and", selection, NP(array < rightMargin), selection)

            elif closure in ("openClosed", "closedClosed"):
                if selection is None:
                    selection = NP(array <= rightMargin)
                else:
                    NP("logical_and", selection, NP(array <= rightMargin), selection)

            lastLimitPoint = rightMargin
            lastClosed = (closure in ("openClosed", "closedClosed"))
            lastInterval = interval

        elif index != lastIndex:
            raise defs.PmmlValidationError("Only the last Interval can have an open-ended rightMargin: %r" % interval)

        edges.append((leftMargin, rightMargin))

        return selection, lastLimitPoint, lastClosed, lastInterval

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

        self.checkRoles(["data", "weight"])

        dataExpression = self.xpath("pmml:PlotExpression[@role='data']")
        weightExpression = self.xpath("pmml:PlotNumericExpression[@role='weight']")
        cutExpression = self.xpath("pmml:PlotSelection")
        if len(dataExpression) != 1:
            raise defs.PmmlValidationError("PlotHistogram requires a PlotNumericExpression with role \"data\"")

        dataColumn = dataExpression[0].evaluate(dataTable, functionTable, performanceTable)

        if len(weightExpression) == 0:
            weight = None
        elif len(weightExpression) == 1:
            weight = weightExpression[0].evaluate(dataTable, functionTable, performanceTable)
        else:
            raise defs.PmmlValidationError("PlotHistogram may not have more than one PlotNumericExpression with role \"data\"")

        if len(cutExpression) == 1:
            selection = cutExpression[0].select(dataTable, functionTable, performanceTable)
        else:
            selection = NP("ones", len(dataTable), NP.dtype(bool))

        performanceTable.begin("PlotHistogram prepare")
        self._saveContext(dataTable)

        if dataColumn.mask is not None:
            NP("logical_and", selection, NP(dataColumn.mask == defs.VALID), selection)

        if weight is not None and weight.mask is not None:
            NP("logical_and", selection, NP(weight.mask == defs.VALID), selection)

        array = dataColumn.data[selection]
        if weight is not None:
            weight = weight.data[selection]

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

            binType = self.establishBinType(dataColumn.fieldType, intervals, values)
            persistentState["binType"] = binType

            if binType == "nonuniform":
                persistentState["count"] = [0.0] * len(intervals)

            elif binType == "explicit":
                persistentState["count"] = [0.0] * len(values)

            elif binType == "unique":
                persistentState["count"] = {}

            elif binType == "scale":
                numBins = self.get("numBins", convertType=True)
                low = self.get("low", convertType=True)
                high = self.get("high", convertType=True)

                numBins, low, high = self.determineScaleBins(numBins, low, high, array)

                persistentState["low"] = low
                persistentState["high"] = high
                persistentState["numBins"] = numBins
                persistentState["count"] = [0.0] * numBins

            performanceTable.end("establish binType")

        missingSum = 0.0
        if persistentState["binType"] == "nonuniform":
            performanceTable.begin("binType nonuniform")

            count = [0.0] * len(intervals)
            edges = []
            lastLimitPoint = None
            lastClosed = None
            lastInterval = None

            for index, interval in enumerate(intervals):
                selection, lastLimitPoint, lastClosed, lastInterval = self.selectInterval(dataColumn.fieldType, array, index, len(intervals) - 1, interval, edges, lastLimitPoint, lastClosed, lastInterval)

                if selection is not None:
                    if weight is None:
                        count[index] += NP("count_nonzero", selection)
                    else:
                        count[index] += weight[selection].sum()

            persistentState["count"] = [x + y for x, y in itertools.izip(count, persistentState["count"])]

            state.fieldType = self.fieldTypeNumeric
            state.count = persistentState["count"]
            state.edges = edges
            lowEdge = min(low for low, high in edges if low is not None)
            highEdge = max(high for low, high in edges if high is not None)

            performanceTable.end("binType nonuniform")

        elif persistentState["binType"] == "explicit":
            performanceTable.begin("binType explicit")

            count = [0.0] * len(values)
            displayValues = []

            for index, value in enumerate(values):
                internalValue = dataColumn.fieldType.stringToValue(value["value"])
                displayValues.append(value.get("displayValue", dataColumn.fieldType.valueToString(internalValue, displayValue=True)))

                selection = NP(array == internalValue)

                if weight is None:
                    count[index] += NP("count_nonzero", selection)
                else:
                    count[index] += weight[selection].sum()

            persistentState["count"] = [x + y for x, y in itertools.izip(count, persistentState["count"])]

            state.fieldType = dataColumn.fieldType
            state.count = persistentState["count"]
            state.edges = displayValues

            performanceTable.end("binType explicit")

        elif persistentState["binType"] == "unique":
            performanceTable.begin("binType unique")

            uniques, inverse = NP("unique", array, return_inverse=True)
            if weight is None:
                counts = NP("bincount", inverse)
            else:
                counts = NP("bincount", inverse, weights=weight)

            persistentCount = persistentState["count"]
            for i, u in enumerate(uniques):
                string = dataColumn.fieldType.valueToString(u, displayValue=False)

                if string in persistentCount:
                    persistentCount[string] += counts[i]
                else:
                    persistentCount[string] = counts[i]

            tosort = [(count, string) for string, count in persistentCount.items()]
            tosort.sort(reverse=True)

            numBins = self.get("numBins", convertType=True)
            if numBins is not None:
                missingSum = sum(count for count, string in tosort[numBins:])
                tosort = tosort[:numBins]

            state.fieldType = dataColumn.fieldType
            state.count = [count for count, string in tosort]
            state.edges = [dataColumn.fieldType.valueToString(dataColumn.fieldType.stringToValue(string), displayValue=True) for count, string in tosort]

            performanceTable.end("binType unique")

        elif persistentState["binType"] == "scale":
            performanceTable.begin("binType scale")

            numBins = persistentState["numBins"]
            low = persistentState["low"]
            high = persistentState["high"]
            binWidth = (high - low) / float(numBins)

            binAssignments = NP("array", NP("floor", NP(NP(array - low)/binWidth)), dtype=NP.dtype(int))
            binAssignments[NP(binAssignments > numBins)] = numBins
            binAssignments[NP(binAssignments < 0)] = numBins
            
            if len(binAssignments) == 0:
                count = NP("empty", 0, dtype=NP.dtype(float))
            else:
                if weight is None:
                    count = NP("bincount", binAssignments)
                else:
                    count = NP("bincount", binAssignments, weights=weight)

            if len(count) < numBins:
                padded = NP("zeros", numBins, dtype=NP.dtype(float))
                padded[:len(count)] = count
            else:
                padded = count

            persistentState["count"] = [x + y for x, y in itertools.izip(padded, persistentState["count"])]

            state.fieldType = self.fieldTypeNumeric
            state.count = persistentState["count"]
            state.edges = [(low + i*binWidth, low + (i + 1)*binWidth) for i in xrange(numBins)]
            lowEdge = low
            highEdge = high

            performanceTable.end("binType scale")

        if self.get("normalized", defaultFromXsd=True, convertType=True):
            if state.fieldType is self.fieldTypeNumeric:
                weightedValues = 0.0
                for (low, high), value in itertools.izip(state.edges, state.count):
                    if low is not None and high is not None:
                        weightedValues += value / (high - low)

                newCount = []
                for (low, high), value in zip(state.edges, state.count):
                    if low is None or high is None:
                        newCount.append(0.0)
                    else:
                        newCount.append(value / (high - low) / weightedValues)
                    
                state.count = newCount

            else:
                totalCount = sum(state.count) + missingSum
                state.count = [float(x)/totalCount for x in state.count]

        if self.get("cumulative", defaultFromXsd=True, convertType=True):
            maximum = sum(state.count)
        else:
            maximum = max(state.count)

        if self.get("vertical", defaultFromXsd=True, convertType=True):
            plotRange.yminPush(0.0, self.fieldType, sticky=True)

            if state.fieldType is self.fieldTypeNumeric:
                plotRange.xminPush(lowEdge, state.fieldType, sticky=True)
                plotRange.xmaxPush(highEdge, state.fieldType, sticky=True)
                plotRange.ymaxPush(maximum, state.fieldType, sticky=False)
            else:
                plotRange.expand(NP("array", state.edges, dtype=NP.dtype(object)), NP("ones", len(state.edges), dtype=NP.dtype(float)) * maximum, state.fieldType, self.fieldType)

        else:
            plotRange.xminPush(0.0, self.fieldType, sticky=True)

            if state.fieldType is self.fieldTypeNumeric:
                plotRange.yminPush(lowEdge, state.fieldType, sticky=True)
                plotRange.ymaxPush(highEdge, state.fieldType, sticky=True)
                plotRange.xmaxPush(maximum, state.fieldType, sticky=False)
            else:
                plotRange.expand(NP("ones", len(state.edges), dtype=NP.dtype(float)) * maximum, NP("array", state.edges, dtype=NP.dtype(object)), self.fieldType, state.fieldType)

        performanceTable.end("PlotHistogram prepare")
        
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
        performanceTable.begin("PlotHistogram draw")

        cumulative = self.get("cumulative", defaultFromXsd=True, convertType=True)
        vertical = self.get("vertical", defaultFromXsd=True, convertType=True)
        visualization = self.get("visualization", defaultFromXsd=True)

        output = svg.g()
        if len(state.count) > 0:
            if state.fieldType is not self.fieldTypeNumeric:
                if vertical:
                    strings = plotCoordinates.xstrings
                else:
                    strings = plotCoordinates.ystrings

                newCount = []
                for string in strings:
                    try:
                        index = state.edges.index(string)
                    except ValueError:
                        newCount.append(0.0)
                    else:
                        newCount.append(state.count[index])

                state.count = newCount
                state.edges = [(i - 0.5, i + 0.5) for i in xrange(len(strings))]

            if vertical:
                Ax = NP("array", [low if low is not None else float("-inf") for low, high in state.edges], dtype=NP.dtype(float))
                Bx = NP(Ax.copy())
                Cx = NP("array", [high if high is not None else float("inf") for low, high in state.edges], dtype=NP.dtype(float))
                Dx = NP(Cx.copy())
                Ay = NP("zeros", len(state.count), dtype=NP.dtype(float))
                if cumulative:
                    Cy = NP("cumsum", NP("array", state.count, dtype=NP.dtype(float)))
                    By = NP("roll", Cy, 1)
                    By[0] = 0.0
                else:
                    By = NP("array", state.count, dtype=NP.dtype(float))
                    Cy = NP(By.copy())
                Dy = NP(Ay.copy())

            else:
                if cumulative:
                    Cx = NP("cumsum", NP("array", state.count, dtype=NP.dtype(float)))
                    Bx = NP("roll", Cx, 1)
                    Bx[0] = 0.0
                else:
                    Bx = NP("array", state.count, dtype=NP.dtype(float))
                    Cx = NP(Bx.copy())
                Ax = NP("zeros", len(state.count), dtype=NP.dtype(float))
                Dx = NP(Ax.copy())
                Ay = NP("array", [low if low is not None else float("-inf") for low, high in state.edges], dtype=NP.dtype(float))
                By = NP(Ay.copy())
                Cy = NP("array", [high if high is not None else float("inf") for low, high in state.edges], dtype=NP.dtype(float))
                Dy = NP(Cy.copy())

            AX, AY = plotCoordinates(Ax, Ay)
            BX, BY = plotCoordinates(Bx, By)
            CX, CY = plotCoordinates(Cx, Cy)
            DX, DY = plotCoordinates(Dx, Dy)

            if visualization == "skyline":
                gap = self.get("gap", defaultFromXsd=True, convertType=True)

                if vertical:
                    if gap > 0.0 and NP(NP(DX - gap/2.0) - NP(AX + gap/2.0)).min() > 0.0:
                        AX += gap/2.0
                        BX += gap/2.0
                        CX -= gap/2.0
                        DX -= gap/2.0
                else:
                    if gap > 0.0 and NP(NP(AY + gap/2.0) - NP(DY - gap/2.0)).min() > 0.0:
                        AY -= gap/2.0
                        BY -= gap/2.0
                        CY += gap/2.0
                        DY += gap/2.0

                pathdata = []
                nextIsMoveto = True
                for i in xrange(len(state.count)):
                    iprev = i - 1
                    inext = i + 1

                    if vertical and By[i] == 0.0 and Cy[i] == 0.0:
                        if i > 0 and not nextIsMoveto:
                            pathdata.append("L %r %r" % (DX[iprev], DY[iprev]))
                        nextIsMoveto = True

                    elif not vertical and Bx[i] == 0.0 and Cx[i] == 0.0:
                        if i > 0 and not nextIsMoveto:
                            pathdata.append("L %r %r" % (DX[iprev], DY[iprev]))
                        nextIsMoveto = True

                    else:
                        if nextIsMoveto or gap > 0.0 or (vertical and DX[iprev] != AX[i]) or (not vertical and DY[iprev] != AY[i]):
                            pathdata.append("M %r %r" % (AX[i], AY[i]))
                            nextIsMoveto = False

                        pathdata.append("L %r %r" % (BX[i], BY[i]))
                        pathdata.append("L %r %r" % (CX[i], CY[i]))

                        if i == len(state.count) - 1 or gap > 0.0 or (vertical and DX[i] != AX[inext]) or (not vertical and DY[i] != AY[inext]):
                            pathdata.append("L %r %r" % (DX[i], DY[i]))

                style = self.getStyleState()
                del style["marker-size"]
                del style["marker-outline"]
                output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(style)))

            elif visualization == "polyline":
                pathdata = []
                for i in xrange(len(state.count)):
                    if i == 0:
                        pathdata.append("M %r %r" % (AX[i], AY[i]))

                    pathdata.append("L %r %r" % ((BX[i] + CX[i])/2.0, (BY[i] + CY[i])/2.0))

                    if i == len(state.count) - 1:
                        pathdata.append("L %r %r" % (DX[i], DY[i]))

                style = self.getStyleState()
                del style["marker-size"]
                del style["marker-outline"]
                output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(style)))

            elif visualization == "smooth":
                smoothingSamples = math.ceil(len(state.count) / 2.0)

                BCX = NP(NP(BX + CX) / 2.0)
                BCY = NP(NP(BY + CY) / 2.0)

                xarray = NP("array", [AX[0]] + list(BCX) + [DX[-1]], dtype=NP.dtype(float))
                yarray = NP("array", [AY[0]] + list(BCY) + [DY[-1]], dtype=NP.dtype(float))
                samples = NP("linspace", AX[0], DX[-1], int(smoothingSamples), endpoint=True)
                smoothingScale = abs(DX[-1] - AX[0]) / smoothingSamples

                xlist, ylist, dxlist, dylist = PlotCurve.pointsToSmoothCurve(xarray, yarray, samples, smoothingScale, False)

                pathdata = PlotCurve.formatPathdata(xlist, ylist, dxlist, dylist, PlotCoordinates(), False, True)

                style = self.getStyleState()
                fillStyle = dict((x, style[x]) for x in style if x.startswith("fill"))
                fillStyle["stroke"] = "none"
                strokeStyle = dict((x, style[x]) for x in style if x.startswith("stroke"))

                if style["fill"] != "none" and len(pathdata) > 0:
                    if vertical:
                        firstPoint = plotCoordinates(Ax[0], 0.0)
                        lastPoint = plotCoordinates(Dx[-1], 0.0)
                    else:
                        firstPoint = plotCoordinates(0.0, Ay[0])
                        lastPoint = plotCoordinates(0.0, Dy[-1])
                        
                    pathdata2 = ["M %r %r" % firstPoint, pathdata[0].replace("M", "L")]
                    pathdata2.extend(pathdata[1:])
                    pathdata2.append(pathdata[-1])
                    pathdata2.append("L %r %r" % lastPoint)

                    output.append(svg.path(d=" ".join(pathdata2), style=PlotStyle.toString(fillStyle)))

                output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(strokeStyle)))

            elif visualization == "points":
                currentStyle = PlotStyle.toDict(self.get("style") or {})
                style = self.getStyleState()
                if "fill" not in currentStyle:
                    style["fill"] = "black"

                BCX = NP(NP(BX + CX) / 2.0)
                BCY = NP(NP(BY + CY) / 2.0)

                svgId = self.get("svgId")
                if svgId is None:
                    svgIdMarker = plotDefinitions.uniqueName()
                else:
                    svgIdMarker = svgId + ".marker"

                marker = PlotScatter.makeMarker(svgIdMarker, self.get("marker", defaultFromXsd=True), style, self.childOfTag("PlotSvgMarker"))
                plotDefinitions[marker.get("id")] = marker

                markerReference = "#" + marker.get("id")
                output.extend(svg.use(**{"x": repr(x), "y": repr(y), defs.XLINK_HREF: markerReference}) for x, y in itertools.izip(BCX, BCY))
                
            else:
                raise NotImplementedError("TODO: add 'errorbars'")

        svgId = self.get("svgId")
        if svgId is not None:
            output["id"] = svgId

        performanceTable.end("PlotHistogram draw")
        return output
