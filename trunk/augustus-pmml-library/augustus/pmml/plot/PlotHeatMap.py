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

"""This module defines the PlotHeatMap class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.SvgBinding import SvgBinding
from augustus.core.DataTable import DataTable
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.ArrayToPng import ArrayToPng
from augustus.pmml.odg.Formula import Formula

class PlotHeatMap(PmmlPlotContent):
    """Represents a 2d heat map of a mathematical formula or a 2d
    histogram of data.

    PMML subelements for mathematical function plotting:

      - PlotFormula role="z(x,y)"

    PMML subelements for 2d histograms:

      - PlotNumericExpression role="x"
      - PlotNumericExpression role="y"
      - PlotNumericExpression role="zweight" (optional)
      - PlotSelection: expression or predicate to filter the data
        before plotting.

    PMML subelements for plotting the mean of a third coordinate z:

      - PlotNumericExpression role="x"
      - PlotNumericExpression role="y"
      - PlotNumericExpression role="zmean"
      - PlotSelection: expression or predicate to filter the data
        before plotting.

    PMML attribute:

      - svgId: id for the resulting SVG element.
      - stateId: key for persistent storage in a DataTableState.
      - xbins: number of histogram bins in the x direction.
      - ybins: number of histogram bins in the y direction.
      - xlow: low edge of the x range of the histogram.
      - ylow: low edge of the y range of the histogram.
      - xhigh: high edge of the x range of the histogram.
      - yhigh: high edge of the y range of the histogram.
      - imageRendering: "optimizeQuality", "optimizeSpeed"
      - onePixelBeyondBorder: if "true", extend the image beyond
        the border by one pixel.  This is to work around a feature
        of many SVG viewers that blend the borders of a raster
        image into the background.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotHeatMap">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:element ref="PlotFormula" minOccurs="1" maxOccurs="1" />
                    <xs:sequence>
                        <xs:element ref="PlotNumericExpression" minOccurs="2" maxOccurs="3" />
                        <xs:element ref="PlotSelection" minOccurs="0" maxOccurs="1" />
                    </xs:sequence>
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />
            <xs:attribute name="xbins" type="xs:positiveInteger" use="optional" />
            <xs:attribute name="ybins" type="xs:positiveInteger" use="optional" />
            <xs:attribute name="xlow" type="xs:double" use="optional" />
            <xs:attribute name="ylow" type="xs:double" use="optional" />
            <xs:attribute name="xhigh" type="xs:double" use="optional" />
            <xs:attribute name="yhigh" type="xs:double" use="optional" />
            <xs:attribute name="imageRendering" use="optional" default="optimizeQuality">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="optimizeQuality" />
                        <xs:enumeration value="optimizeSpeed" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="onePixelBeyondBorder" type="xs:boolean" use="optional" default="true" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    xyfieldType = FakeFieldType("double", "continuous")
    zfieldType = FakeFieldType("double", "continuous")

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

        self.checkRoles(["z(x,y)", "x", "y", "zmean", "zweight"])

        performanceTable.begin("PlotHeatMap prepare")
        self._saveContext(dataTable)
        
        zofxy = self.xpath("pmml:PlotFormula[@role='z(x,y)']")
        xexpr = self.xpath("pmml:PlotNumericExpression[@role='x']")
        yexpr = self.xpath("pmml:PlotNumericExpression[@role='y']")
        zmean = self.xpath("pmml:PlotNumericExpression[@role='zmean']")
        zweight = self.xpath("pmml:PlotNumericExpression[@role='zweight']")
        cutExpression = self.xpath("pmml:PlotSelection")

        if len(zofxy) == 1 and len(xexpr) == 0 and len(yexpr) == 0 and len(zmean) == 0 and len(zweight) == 0:
            xbins = self.get("xbins", convertType=True)
            xlow = self.get("xlow", convertType=True)
            xhigh = self.get("xhigh", convertType=True)
            ybins = self.get("ybins", convertType=True)
            ylow = self.get("ylow", convertType=True)
            yhigh = self.get("yhigh", convertType=True)

            if xbins is None or xlow is None or xhigh is None or ybins is None or ylow is None or yhigh is None:
                raise defs.PmmlValidationError("xbins, xlow, xhigh, ybins, ylow, and yhigh are required for HeatMaps of a mathematical formula")

            if xlow >= xhigh or ylow >= yhigh:
                raise defs.PmmlValidationError("xlow must be less than xhigh and ylow must be less than yhigh")

            if plotRange.xStrictlyPositive or plotRange.yStrictlyPositive:
                raise defs.PmmlValidationError("PlotHeatMap can only be properly displayed in linear x, y coordinates")

            xbinWidth = (xhigh - xlow) / float(xbins)
            ybinWidth = (yhigh - ylow) / float(ybins)

            xarray = NP("tile", NP("linspace", xlow, xhigh, xbins, endpoint=True), ybins)
            yarray = NP("repeat", NP("linspace", ylow, yhigh, ybins, endpoint=True), xbins)

            sampleTable = DataTable({"x": "double", "y": "double"}, {"x": xarray, "y": yarray})
            parsed = Formula.parse(zofxy[0].text)

            performanceTable.pause("PlotHeatMap prepare")
            zdataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
            performanceTable.unpause("PlotHeatMap prepare")
            if not zdataColumn.fieldType.isnumeric():
                raise defs.PmmlValidationError("PlotFormula z(x,y) must return a numeric expression, not %r" % zdataColumn.fieldType)

            selection = NP("isfinite", zdataColumn.data)
            if zdataColumn.mask is not None:
                NP("logical_and", selection, NP(zdataColumn.mask == defs.VALID), selection)
            if plotRange.zStrictlyPositive:
                NP("logical_and", selection, NP(zdataColumn.data > 0.0), selection)

            gooddata = zdataColumn.data[selection]
            plotRange.zminPush(gooddata.min(), zdataColumn.fieldType, sticky=False)
            plotRange.zmaxPush(gooddata.max(), zdataColumn.fieldType, sticky=False)

            state.zdata = zdataColumn.data
            state.zmask = NP("logical_not", selection) * defs.INVALID

        elif len(zofxy) == 0 and len(xexpr) == 1 and len(yexpr) == 1:
            performanceTable.pause("PlotHeatMap prepare")
            xdataColumn = xexpr[0].evaluate(dataTable, functionTable, performanceTable)
            ydataColumn = yexpr[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("PlotHeatMap prepare")

            xbins = self.get("xbins", convertType=True)
            xlow = self.get("xlow", convertType=True)
            xhigh = self.get("xhigh", convertType=True)
            ybins = self.get("ybins", convertType=True)
            ylow = self.get("ylow", convertType=True)
            yhigh = self.get("yhigh", convertType=True)

            if len(xdataColumn) > 0:
                if xlow is None: xlow = NP("nanmin", xdataColumn.data)
                if xhigh is None: xhigh = NP("nanmax", xdataColumn.data)
                if ylow is None: ylow = NP("nanmin", ydataColumn.data)
                if yhigh is None: yhigh = NP("nanmax", ydataColumn.data)
            else:
                if xlow is None: xlow = 0.0
                if xhigh is None: xhigh = 1.0
                if ylow is None: ylow = 0.0
                if yhigh is None: yhigh = 1.0

            if xbins is None:
                q1, q3 = NP("percentile", xdataColumn.data, [25.0, 75.0])
                binWidth = 2.0 * (q3 - q1) / math.pow(len(xdataColumn.data), 1.0/3.0)
                if binWidth > 0.0:
                    xbins = max(10, int(math.ceil((xhigh - xlow)/binWidth)))
                else:
                    xbins = 10

            if ybins is None:
                q1, q3 = NP("percentile", ydataColumn.data, [25.0, 75.0])
                binWidth = 2.0 * (q3 - q1) / math.pow(len(ydataColumn.data), 1.0/3.0)
                if binWidth > 0.0:
                    ybins = max(10, int(math.ceil((yhigh - ylow)/binWidth)))
                else:
                    ybins = 10

            if xlow >= xhigh or ylow >= yhigh:
                raise defs.PmmlValidationError("xlow must be less than xhigh and ylow must be less than yhigh")

            if plotRange.xStrictlyPositive or plotRange.yStrictlyPositive:
                raise defs.PmmlValidationError("PlotHeatMap can only be properly displayed in linear x, y coordinates")

            persistentState = {}
            stateId = self.get("stateId")
            if stateId is not None:
                if stateId in dataTable.state:
                    persistentState = dataTable.state[stateId]
                else:
                    dataTable.state[stateId] = persistentState

            if len(zmean) == 0:
                if "xbins" in persistentState: xbins = persistentState["xbins"]
                if "xlow" in persistentState: xlow = persistentState["xlow"]
                if "xhigh" in persistentState: xhigh = persistentState["xhigh"]
                if "ybins" in persistentState: ybins = persistentState["ybins"]
                if "ylow" in persistentState: ylow = persistentState["ylow"]
                if "yhigh" in persistentState: yhigh = persistentState["yhigh"]

                persistentState["xbins"] = xbins
                persistentState["xlow"] = xlow
                persistentState["xhigh"] = xhigh
                persistentState["ybins"] = ybins
                persistentState["ylow"] = ylow
                persistentState["yhigh"] = yhigh
                
            xbinWidth = (xhigh - xlow) / float(xbins)
            ybinWidth = (yhigh - ylow) / float(ybins)

            mask = NP("ones", len(dataTable), dtype=NP.dtype(float))
            if xdataColumn.mask is not None:
                NP("multiply", mask, (xdataColumn.mask == defs.VALID), mask)
            if ydataColumn.mask is not None:
                NP("multiply", mask, (ydataColumn.mask == defs.VALID), mask)

            if len(cutExpression) == 1:
                performanceTable.pause("PlotHeatMap prepare")
                NP("multiply", mask, cutExpression[0].select(dataTable, functionTable, performanceTable), mask)
                performanceTable.unpause("PlotHeatMap prepare")

            if len(zmean) == 0 and len(zweight) == 0:
                histogram, xedges, yedges = NP("histogram2d", ydataColumn.data, xdataColumn.data, bins=(ybins, xbins), range=[[ylow, yhigh], [xlow, xhigh]], weights=mask)
                if len(dataTable) == 0:
                    # work around Numpy <= 1.6.1 bug
                    histogram = NP("zeros", (ybins, xbins), dtype=NP.dtype(float))

                if "histogram" in persistentState:
                    persistentState["histogram"] = NP(persistentState["histogram"] + histogram)
                else:
                    persistentState["histogram"] = histogram

                histogram = persistentState["histogram"]

                if plotRange.zStrictlyPositive:
                    zmin = 0.1
                else:
                    zmin = 0.0
                zmax = NP("nanmax", histogram)

                plotRange.zminPush(zmin, self.zfieldType, sticky=True)
                if zmax > zmin:
                    plotRange.zmaxPush(zmax, self.zfieldType, sticky=False)

            elif len(zmean) == 0 and len(zweight) == 1:
                performanceTable.pause("PlotHeatMap prepare")
                weightsDataColumn = zweight[0].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("PlotHeatMap prepare")

                if weightsDataColumn.mask is not None:
                    NP("multiply", mask, (weightsDataColumn.mask == defs.VALID), mask)
                weights = NP(weightsDataColumn.data * mask)

                histogram, xedges, yedges = NP("histogram2d", ydataColumn.data, xdataColumn.data, bins=(ybins, xbins), range=[[ylow, yhigh], [xlow, xhigh]], weights=weights)

                if "histogram" in persistentState:
                    persistentState["histogram"] = NP(persistentState["histogram"] + histogram)
                else:
                    persistentState["histogram"] = histogram

                histogram = persistentState["histogram"]

                if plotRange.zStrictlyPositive:
                    w = weights[NP(weights > 0.0)]
                    if len(w) > 0:
                        zmin = 0.1 * NP("nanmin", w)
                    else:
                        zmin = 0.1
                else:
                    zmin = 0.0
                zmax = NP("nanmax", histogram)

                plotRange.zminPush(zmin, self.zfieldType, sticky=True)
                if zmax > zmin:
                    plotRange.zmaxPush(zmax, self.zfieldType, sticky=False)

            elif len(zmean) == 1 and len(zweight) == 0:
                performanceTable.pause("PlotHeatMap prepare")
                zdataColumn = zmean[0].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("PlotHeatMap prepare")

                if zdataColumn.mask is not None:
                    NP("multiply", mask, (zdataColumn.mask == defs.VALID), mask)
                weights = NP(zdataColumn.data * mask)

                numer, xedges, yedges = NP("histogram2d", ydataColumn.data, xdataColumn.data, bins=(ybins, xbins), range=[[ylow, yhigh], [xlow, xhigh]], weights=weights)
                denom, xedges, yedges = NP("histogram2d", ydataColumn.data, xdataColumn.data, bins=(ybins, xbins), range=[[ylow, yhigh], [xlow, xhigh]], weights=mask)

                if "numer" in persistentState:
                    persistentState["numer"] = NP(persistentState["numer"] + numer)
                    persistentState["denom"] = NP(persistentState["denom"] + denom)
                else:
                    persistentState["numer"] = numer
                    persistentState["denom"] = denom

                numer = persistentState["numer"]
                denom = persistentState["denom"]
                histogram = numer / denom

                selection = NP("isfinite", histogram)
                if plotRange.zStrictlyPositive:
                    NP("logical_and", selection, NP(histogram > 0.0), selection)

                if NP("count_nonzero", selection) > 0:
                    gooddata = histogram[selection]
                    plotRange.zminPush(gooddata.min(), self.zfieldType, sticky=False)
                    plotRange.zmaxPush(gooddata.max(), self.zfieldType, sticky=False)

            else:
                raise defs.PmmlValidationError("The only allowed combinations of PlotFormula/PlotNumericExpressions are: \"z(x,y)\" (function), \"x y\" (histogram), \"x y zmean\" (mean of z in x y bins), \"x y zweight\" (weighted x y histogram)")

            state.zdata = NP("reshape", histogram, xbins*ybins)
            state.zmask = None
                        
        else:
            raise defs.PmmlValidationError("The only allowed combinations of PlotFormula/PlotNumericExpressions are: \"z(x,y)\" (function), \"x y\" (histogram), \"x y zmean\" (mean of z in x y bins), \"x y zweight\" (weighted x y histogram)")

        plotRange.xminPush(xlow, self.xyfieldType, sticky=True)
        plotRange.yminPush(ylow, self.xyfieldType, sticky=True)
        plotRange.xmaxPush(xhigh, self.xyfieldType, sticky=True)
        plotRange.ymaxPush(yhigh, self.xyfieldType, sticky=True)

        state.xbins = xbins
        state.xlow = xlow
        state.xhigh = xhigh
        state.ybins = ybins
        state.ylow = ylow
        state.yhigh = yhigh

        performanceTable.end("PlotHeatMap prepare")

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

        svgId = self.get("svgId")
        if svgId is None:
            output = svg.g()
        else:
            output = svg.g(id=svgId)

        if not hasattr(plotCoordinates, "zmin"):
            return output

        performanceTable.begin("PlotHeatMap draw")

        xbins = state.xbins
        xlow = state.xlow
        xhigh = state.xhigh
        ybins = state.ybins
        ylow = state.ylow
        yhigh = state.yhigh

        reddata = NP("empty", len(state.zdata), dtype=NP.uint8)
        greendata = NP("empty", len(state.zdata), dtype=NP.uint8)
        bluedata = NP("empty", len(state.zdata), dtype=NP.uint8)
        alphadata = NP("empty", len(state.zdata), dtype=NP.uint8)

        if len(plotCoordinates.gradient) == 0:
            offsets = [0.0, 1.0]
            reds = [255, 0]
            greens = [255, 0]
            blues = [255, 255]
            alphas = [255, 255]
        else:
            offsets = [float(g["offset"]) for g in plotCoordinates.gradient]
            reds = [min(int(math.floor(256*float(g["red"]))), 255) for g in plotCoordinates.gradient]
            greens = [min(int(math.floor(256*float(g["green"]))), 255) for g in plotCoordinates.gradient]
            blues = [min(int(math.floor(256*float(g["blue"]))), 255) for g in plotCoordinates.gradient]
            alphas = [min(int(math.floor(256*float(g.get("opacity", 1.0)))), 255) for g in plotCoordinates.gradient]

        if not plotCoordinates.zlog:
            normalized = NP(NP(state.zdata - plotCoordinates.zmin) / (plotCoordinates.zmax - plotCoordinates.zmin))
        else:
            normalized = NP(NP(NP("log10", state.zdata) - NP("log10", plotCoordinates.zmin))/NP(NP("log10", plotCoordinates.zmax) - NP("log10", plotCoordinates.zmin)))

        for index in xrange(len(offsets) - 1):
            if index == 0:
                under = NP(normalized < offsets[index])
                reddata[under] = reds[index]
                greendata[under] = greens[index]
                bluedata[under] = blues[index]
                alphadata[under] = alphas[index]

            if index == len(offsets) - 2:
                over = NP(normalized >= offsets[index + 1])
                reddata[over] = reds[index + 1]
                greendata[over] = greens[index + 1]
                bluedata[over] = blues[index + 1]
                alphadata[over] = alphas[index + 1]

            selection = NP(normalized >= offsets[index])
            NP("logical_and", selection, NP(normalized < offsets[index + 1]), selection)

            subset = NP(NP(normalized[selection]) - offsets[index])
            norm = 1. / (offsets[index + 1] - offsets[index])

            reddata[selection] = NP("array", NP(NP(subset * ((reds[index + 1] - reds[index]) * norm)) + reds[index]), dtype=NP.uint8)
            greendata[selection] = NP("array", NP(NP(subset * ((greens[index + 1] - greens[index]) * norm)) + greens[index]), dtype=NP.uint8)
            bluedata[selection] = NP("array", NP(NP(subset * ((blues[index + 1] - blues[index]) * norm)) + blues[index]), dtype=NP.uint8)
            alphadata[selection] = NP("array", NP(NP(subset * ((alphas[index + 1] - alphas[index]) * norm)) + alphas[index]), dtype=NP.uint8)

        badpixels = NP("isnan", normalized)
        NP("logical_or", badpixels, NP("isinf", normalized), badpixels)
        if state.zmask is not None:
            NP("logical_or", badpixels, NP(state.zmask != defs.VALID), badpixels)

        alphadata[badpixels] = 0

        X1, Y1 = plotCoordinates(xlow, ylow)
        X2, Y2 = plotCoordinates(xhigh, yhigh)

        onePixelBeyondBorder = self.get("onePixelBeyondBorder", defaultFromXsd=True, convertType=True)
        if onePixelBeyondBorder:
            Xwidth = (X2 - X1) / xbins
            Yheight = (Y1 - Y2) / ybins
            X1 -= Xwidth
            X2 += Xwidth
            Y1 += Yheight
            Y2 -= Yheight

        arrayToPng = ArrayToPng()
        arrayToPng.putdata(xbins, ybins, reddata, greendata, bluedata, alphadata, flipy=True, onePixelBeyondBorder=onePixelBeyondBorder)

        output.append(svg.image(**{defs.XLINK_HREF: "data:image/png;base64," + arrayToPng.b64encode(), "x": repr(X1), "y": repr(Y2), "width": repr(X2 - X1), "height": repr(Y1 - Y2), "image-rendering": self.get("imageRendering", defaultFromXsd=True), "preserveAspectRatio": "none"}))

        performanceTable.end("PlotHeatMap draw")
        return output
