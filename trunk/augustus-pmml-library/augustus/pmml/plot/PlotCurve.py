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

"""This module defines the PlotCurve class."""

import math
import itertools

from augustus.core.defs import defs
from augustus.core.SvgBinding import SvgBinding
from augustus.core.NumpyInterface import NP
from augustus.core.DataTable import DataTable
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.core.FakeFieldType import FakeFieldType
from augustus.pmml.odg.Formula import Formula

class PlotCurve(PmmlPlotContent):
    """Represents a curve defined by mathematical formulae or a jagged
    line/smooth curve through a set of data points.

    PMML subelements for a 1d formula:

      - PlotFormula role="y(x)"
      - PlotFormula role="dy/dx" (optional)

    PMML subelements for a parametric formula:

      - PlotFormula role="x(t)"
      - PlotFormula role="y(t)"
      - PlotFormula role="dx/dt" (optional)
      - PlotFormula role="dy/dt" (optional)

    PMML subelements for a fit to data points:

      - PlotNumericExpression role="x"
      - PlotNumericExpression role="y"
      - PlotNumericExpression role="dx" (optional)
      - PlotNumericExpression role="dy" (optional)
      - PlotSelection (optional)

    PMML attributes:

      - svgId: id for the resulting SVG element.
      - stateId: key for persistent storage in a DataTableState.
      - low: low edge of domain (in x or t) for mathematical
        formulae.
      - high: high edge of domain (in x or t) for mathematical
        formulae.
      - numSamples: number of locations to sample for mathematical
        formulae.
      - samplingMethod: "uniform", "random", or "adaptive".
      - loop: if "true", draw a closed loop that connects the first
        and last points.
      - smooth: if "false", draw a jagged line between each data
        point; if "true", fit a smooth curve.
      - smoothingScale: size of the smoothing scale in units of the
        domain (in x or t).
      - style: CSS style properties.

    CSS properties:
      - fill, fill-opacity: color under the curve.
      - stroke, stroke-dasharray, stroke-dashoffset, stroke-linecap,
        stroke-linejoin, stroke-miterlimit, stroke-opacity,
        stroke-width: properties of the line drawing.

    See the source code for the full XSD.
    """

    styleProperties = ["fill", "fill-opacity", 
                       "stroke", "stroke-dasharray", "stroke-dashoffset", "stroke-linecap", "stroke-linejoin", "stroke-miterlimit", "stroke-opacity", "stroke-width",
                       ]

    styleDefaults = {"fill": "none", "stroke": "black"}
    
    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotCurve">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:element ref="PlotFormula" minOccurs="1" maxOccurs="4" />
                    <xs:sequence>
                        <xs:element ref="PlotNumericExpression" minOccurs="1" maxOccurs="4" />
                        <xs:element ref="PlotSelection" minOccurs="0" maxOccurs="1" />
                    </xs:sequence>
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="svgId" type="xs:string" use="optional" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />
            <xs:attribute name="low" type="xs:double" use="optional" />
            <xs:attribute name="high" type="xs:double" use="optional" />
            <xs:attribute name="numSamples" type="xs:positiveInteger" use="optional" default="100" />
            <xs:attribute name="samplingMethod" use="optional" default="uniform">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="uniform" />
                        <xs:enumeration value="random" />
                        <xs:enumeration value="adaptive" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="loop" type="xs:boolean" use="optional" default="false" />
            <xs:attribute name="smooth" type="xs:boolean" use="optional" default="true" />
            <xs:attribute name="smoothingScale" type="xs:double" use="optional" default="1.0" />
            <xs:attribute name="style" type="xs:string" use="optional" default="%s" />
        </xs:complexType>
    </xs:element>
</xs:schema>
""" % PlotStyle.toString(styleDefaults)

    xfieldType = FakeFieldType("double", "continuous")

    @classmethod
    def expressionsToPoints(cls, expression, derivative, samples, loop, functionTable, performanceTable):
        """Evaluate a set of given string-based formulae to generate
        numeric points.

        This is used to plot mathematical curves.

        @type expression: 1- or 2-tuple of strings
        @param expression: If a 1-tuple, the string is passed to Formula and interpreted as y(x); if a 2-tuple, the strings are passed to Formula and interpreted as x(t), y(t).
        @type derivative: 1- or 2-tuple of strings (same length as C{expression})
        @param derivative: Strings are passed to Formua and interpreted as dy/dx (if a 1-tuple) or dx/dt, dy/dt (if a 2-tuple).
        @type samples: 1d Numpy array
        @param samples: Values of x or t at which to evaluate the expression or expressions.
        @type loop: bool
        @param loop: If False, disconnect the end of the set of points from the beginning.
        @type functionTable: FunctionTable
        @param functionTable: Functions that may be used to perform the calculation.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the process.
        @rtype: 6-tuple
        @return: C{xlist}, C{ylist}, C{dxlist}, C{dylist} (1d Numpy arrays), xfieldType, yfieldType (FieldTypes).
        """

        if len(expression) == 1:
            sampleTable = DataTable({"x": "double"}, {"x": samples})

            parsed = Formula.parse(expression[0])
            ydataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
            if not ydataColumn.fieldType.isnumeric() and not ydataColumn.fieldType.istemporal():
                raise defs.PmmlValidationError("PlotFormula y(x) must return a numeric expression, not %r" % ydataColumn.fieldType)

            xfieldType = cls.xfieldType
            yfieldType = ydataColumn.fieldType

            selection = None
            if ydataColumn.mask is not None:
                selection = NP(ydataColumn.mask == defs.VALID)

            if derivative[0] is None:
                if selection is None:
                    xlist = samples
                    ylist = ydataColumn.data
                else:
                    xlist = samples[selection]
                    ylist = ydataColumn.data[selection]

                dxlist = NP((NP("roll", xlist, -1) - NP("roll", xlist, 1)) / 2.0)
                dylist = NP((NP("roll", ylist, -1) - NP("roll", ylist, 1)) / 2.0)
                if not loop:
                    dxlist[0] = 0.0
                    dxlist[-1] = 0.0
                    dylist[0] = 0.0
                    dylist[-1] = 0.0
                
            else:
                parsed = Formula.parse(derivative[0])
                dydataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
                if not dydataColumn.fieldType.isnumeric() and not dydataColumn.fieldType.istemporal():
                    raise defs.PmmlValidationError("PlotFormula dy/dx must return a numeric expression, not %r" % dydataColumn.fieldType)
                
                if dydataColumn.mask is not None:
                    if selection is None:
                        selection = NP(dydataColumn.mask == defs.VALID)
                    else:
                        NP("logical_and", selection, NP(dydataColumn.mask == defs.VALID), selection)

                if selection is None:
                    xlist = samples
                    ylist = ydataColumn.data
                    dxlist = NP((NP("roll", xlist, -1) - NP("roll", xlist, 1)) / 2.0)
                    dylist = dydataColumn.data
                else:
                    xlist = samples[selection]
                    ylist = ydataColumn.data[selection]
                    dxlist = NP((NP("roll", xlist, -1) - NP("roll", xlist, 1)) / 2.0)
                    dylist = NP(dydataColumn.data[selection] * dxlist)

                if not loop:
                    dxlist[0] = 0.0
                    dxlist[-1] = 0.0
                    dylist[0] = 0.0
                    dylist[-1] = 0.0

        elif len(expression) == 2:
            sampleTable = DataTable({"t": "double"}, {"t": samples})

            parsed = Formula.parse(expression[0])
            xdataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
            if not xdataColumn.fieldType.isnumeric() and not xdataColumn.fieldType.istemporal():
                raise defs.PmmlValidationError("PlotFormula x(t) must return a numeric expression, not %r" % xdataColumn.fieldType)

            parsed = Formula.parse(expression[1])
            ydataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
            if not ydataColumn.fieldType.isnumeric() and not ydataColumn.fieldType.istemporal():
                raise defs.PmmlValidationError("PlotFormula y(t) must return a numeric expression, not %r" % ydataColumn.fieldType)
            
            xfieldType = xdataColumn.fieldType
            yfieldType = ydataColumn.fieldType

            selection = None
            if xdataColumn.mask is not None:
                selection = NP(xdataColumn.mask == defs.VALID)
            if ydataColumn.mask is not None:
                if selection is None:
                    selection = NP(ydataColumn.mask == defs.VALID)
                else:
                    NP("logical_and", selection, NP(ydataColumn.mask == defs.VALID), selection)

            if derivative[0] is None:
                if selection is None:
                    xlist = xdataColumn.data
                    ylist = ydataColumn.data
                else:
                    xlist = xdataColumn.data[selection]
                    ylist = ydataColumn.data[selection]

                dxlist = NP((NP("roll", xlist, -1) - NP("roll", xlist, 1)) / 2.0)
                dylist = NP((NP("roll", ylist, -1) - NP("roll", ylist, 1)) / 2.0)
                if not loop:
                    dxlist[0] = 0.0
                    dxlist[-1] = 0.0
                    dylist[0] = 0.0
                    dylist[-1] = 0.0

            else:
                parsed = Formula.parse(derivative[0])
                dxdataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
                if not dxdataColumn.fieldType.isnumeric() and not dxdataColumn.fieldType.istemporal():
                    raise defs.PmmlValidationError("PlotFormula dx/dt must return a numeric expression, not %r" % dxdataColumn.fieldType)

                parsed = Formula.parse(derivative[1])
                dydataColumn = parsed.evaluate(sampleTable, functionTable, performanceTable)
                if not dydataColumn.fieldType.isnumeric() and not dydataColumn.fieldType.istemporal():
                    raise defs.PmmlValidationError("PlotFormula dy/dt must return a numeric expression, not %r" % dydataColumn.fieldType)
                
                if dxdataColumn.mask is not None:
                    if selection is None:
                        selection = NP(dxdataColumn.mask == defs.VALID)
                    else:
                        NP("logical_and", selection, NP(dxdataColumn.mask == defs.VALID), selection)
                
                if dydataColumn.mask is not None:
                    if selection is None:
                        selection = NP(dydataColumn.mask == defs.VALID)
                    else:
                        NP("logical_and", selection, NP(dydataColumn.mask == defs.VALID), selection)

                if selection is None:
                    dt = NP((NP("roll", samples, -1) - NP("roll", samples, 1)) / 2.0)

                    xlist = xdataColumn.data
                    ylist = ydataColumn.data
                    dxlist = NP(dxdataColumn.data * dt)
                    dylist = NP(dydataColumn.data * dt)
                else:
                    dt = NP((NP("roll", samples[selection], -1) - NP("roll", samples[selection], 1)) / 2.0)

                    xlist = xdataColumn.data[selection]
                    ylist = ydataColumn.data[selection]
                    dxlist = NP(dxdataColumn.data[selection] * dt)
                    dylist = NP(dydataColumn.data[selection] * dt)

                if not loop:
                    dxlist[0] = 0.0
                    dxlist[-1] = 0.0
                    dylist[0] = 0.0
                    dylist[-1] = 0.0

        return xlist, ylist, dxlist, dylist, xfieldType, yfieldType

    @staticmethod
    def pointsToSmoothCurve(xarray, yarray, samples, smoothingScale, loop):
        """Fit a smooth line through a set of given numeric points
        with a characteristic smoothing scale.

        This is a non-parametric locally linear fit, used to plot data
        as a smooth line.

        @type xarray: 1d Numpy array of numbers
        @param xarray: Array of x values.
        @type yarray: 1d Numpy array of numbers
        @param yarray: Array of y values.
        @type samples: 1d Numpy array of numbers
        @param samples: Locations at which to fit the C{xarray} and C{yarray} with best-fit positions and derivatives.
        @type smoothingScale: number
        @param smoothingScale: Standard deviation of the Gaussian kernel used to smooth the locally linear fit.
        @type loop: bool
        @param loop: If False, disconnect the end of the fitted curve from the beginning.
        @rtype: 4-tuple of 1d Numpy arrays
        @return: C{xlist}, C{ylist}, C{dxlist}, C{dylist} appropriate for C{formatPathdata}.
        """

        ylist = []
        dylist = []

        for sample in samples:
            weights = NP(NP(NP("exp", NP(NP(-0.5 * NP("power", NP(xarray - sample), 2)) / NP(smoothingScale * smoothingScale))) / smoothingScale) / (math.sqrt(2.0*math.pi)))
            sum1 = weights.sum()
            sumx = NP(weights * xarray).sum()
            sumxx = NP(weights * NP(xarray * xarray)).sum()
            sumy = NP(weights * yarray).sum()
            sumxy = NP(weights * NP(xarray * yarray)).sum()

            delta = (sum1 * sumxx) - (sumx * sumx)
            intercept = ((sumxx * sumy) - (sumx * sumxy)) / delta
            slope = ((sum1 * sumxy) - (sumx * sumy)) / delta

            ylist.append(intercept + (sample * slope))
            dylist.append(slope)

        xlist = samples
        ylist = NP("array", ylist, dtype=NP.dtype(float))
        dxlist = NP((NP("roll", xlist, -1) - NP("roll", xlist, 1)) / 2.0)
        dylist = NP("array", dylist, dtype=NP.dtype(float)) * dxlist
        if not loop:
            dxlist[0] = 0.0
            dxlist[-1] = 0.0
            dylist[0] = 0.0
            dylist[-1] = 0.0

        return xlist, ylist, dxlist, dylist

    @staticmethod
    def formatPathdata(xlist, ylist, dxlist, dylist, plotCoordinates, loop, smooth):
        """Compute SVG path data from position and derivatives lists.

        @type xlist: 1d Numpy array of numbers
        @param xlist: Array of x values at each point t.
        @type ylist: 1d Numpy array of numbers
        @param ylist: Array of y values at each point t.
        @type dxlist: 1d Numpy array of numbers
        @param dxlist: Array of dx/dt derivatives at each point t.
        @type dylist: 1d Numpy array of numbers
        @param dylist: Array of dy/dt derivatives at each point t.
        @type plotCoordinates: PlotCoordinates
        @param plotCoordinates: Coordinate system to convert the points.
        @type loop: bool
        @param loop: If True, the last point should be connected to the first point.
        @type smooth: bool
        @param smooth: If True, use the derivatives (C{dxlist} and C{dylist}) to define Bezier curves between the points; otherwise, draw straight lines.
        @rtype: list of strings
        @return: When concatenated with spaces, the return type is appropriate for an SVG path's C{d} attribute.
        """

        pathdata = []
        if not smooth:
            X, Y = plotCoordinates(xlist, ylist)

            nextIsMoveto = True
            for x, y in itertools.izip(X, Y):
                if nextIsMoveto:
                    pathdata.append("M %r %r" % (x, y))
                    nextIsMoveto = False
                else:
                    pathdata.append("L %r %r" % (x, y))

            if loop:
                pathdata.append("Z")

        else:
            C1x = NP("roll", xlist, 1) + NP("roll", dxlist, 1) / 3.0
            C1y = NP("roll", ylist, 1) + NP("roll", dylist, 1) / 3.0
            C2x = xlist - dxlist / 3.0
            C2y = ylist - dylist / 3.0

            X, Y = plotCoordinates(xlist, ylist)
            C1X, C1Y = plotCoordinates(C1x, C1y)
            C2X, C2Y = plotCoordinates(C2x, C2y)
            
            nextIsMoveto = True
            for x, y, c1x, c1y, c2x, c2y in itertools.izip(X, Y, C1X, C1Y, C2X, C2Y):
                if nextIsMoveto:
                    pathdata.append("M %r %r" % (x, y))
                    nextIsMoveto = False
                else:
                    pathdata.append("C %r %r %r %r %r %r" % (c1x, c1y, c2x, c2y, x, y))

            if loop:
                pathdata.append("Z")

        return pathdata

    def generateSamples(self, low, high):
        """Used by C{prepare} to generate an array of samples.

        @type low: number
        @param low: Minimum value to sample.
        @type high: number
        @param high: Maximum value to sample.
        @rtype: 1d Numpy array
        @return: An array of uniform, random, or adaptive samples of an interval.
        """

        numSamples = self.get("numSamples", defaultFromXsd=True, convertType=True)
        samplingMethod = self.get("samplingMethod", defaultFromXsd=True)

        if samplingMethod == "uniform":
            samples = NP("linspace", low, high, numSamples, endpoint=True)

        elif samplingMethod == "random":
            samples = NP(NP(NP(NP.random.rand(numSamples)) * (high - low)) + low)
            samples.sort()

        else:
            raise NotImplementedError("TODO: add 'adaptive'")

        return samples

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

        self.checkRoles(["y(x)", "dy/dx", "x(t)", "y(t)", "dx/dt", "dy/dt", "x", "y", "dx", "dy"])

        performanceTable.begin("PlotCurve prepare")
        self._saveContext(dataTable)
        
        yofx = self.xpath("pmml:PlotFormula[@role='y(x)']")
        dydx = self.xpath("pmml:PlotFormula[@role='dy/dx']")

        xoft = self.xpath("pmml:PlotFormula[@role='x(t)']")
        yoft = self.xpath("pmml:PlotFormula[@role='y(t)']")
        dxdt = self.xpath("pmml:PlotFormula[@role='dx/dt']")
        dydt = self.xpath("pmml:PlotFormula[@role='dy/dt']")

        nx = self.xpath("pmml:PlotNumericExpression[@role='x']")
        ny = self.xpath("pmml:PlotNumericExpression[@role='y']")
        ndx = self.xpath("pmml:PlotNumericExpression[@role='dx']")
        ndy = self.xpath("pmml:PlotNumericExpression[@role='dy']")
        cutExpression = self.xpath("pmml:PlotSelection")

        if len(yofx) + len(dydx) + len(xoft) + len(yoft) + len(dxdt) + len(dydt) > 0:
            if len(yofx) == 1 and len(dydx) == 0 and len(xoft) == 0 and len(yoft) == 0 and len(dxdt) == 0 and len(dydt) == 0:
                expression = (yofx[0].text,)
                derivative = (None,)

            elif len(yofx) == 1 and len(dydx) == 1 and len(xoft) == 0 and len(yoft) == 0 and len(dxdt) == 0 and len(dydt) == 0:
                expression = (yofx[0].text,)
                derivative = (dydx[0].text,)

            elif len(yofx) == 0 and len(dydx) == 0 and len(xoft) == 1 and len(yoft) == 1 and len(dxdt) == 0 and len(dydt) == 0:
                expression = xoft[0].text, yoft[0].text
                derivative = None, None

            elif len(yofx) == 0 and len(dydx) == 0 and len(xoft) == 1 and len(yoft) == 1 and len(dxdt) == 1 and len(dydt) == 1:
                expression = xoft[0].text, yoft[0].text
                derivative = dxdt[0].text, dydt[0].text

            else:
                raise defs.PmmlValidationError("The only allowed combinations of PlotFormulae are: \"y(x)\", \"y(x) dy/dx\", \"x(t) y(t)\", and \"x(t) y(t) dx/dt dy/dt\"")

            low = self.get("low", convertType=True)
            high = self.get("high", convertType=True)
            if low is None or high is None:
                raise defs.PmmlValidationError("The \"low\" and \"high\" attributes are required for PlotCurves defined by formulae")

            samples = self.generateSamples(low, high)

            loop = self.get("loop", defaultFromXsd=True, convertType=True)
            state.x, state.y, state.dx, state.dy, xfieldType, yfieldType = self.expressionsToPoints(expression, derivative, samples, loop, functionTable, performanceTable)

        else:
            performanceTable.pause("PlotCurve prepare")
            if len(ndx) == 1:
                dxdataColumn = ndx[0].evaluate(dataTable, functionTable, performanceTable)
            else:
                dxdataColumn = None
            if len(ndy) == 1:
                dydataColumn = ndy[0].evaluate(dataTable, functionTable, performanceTable)
            else:
                dydataColumn = None
            performanceTable.unpause("PlotCurve prepare")

            if len(nx) == 0 and len(ny) == 1:
                performanceTable.pause("PlotCurve prepare")
                ydataColumn = ny[0].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("PlotCurve prepare")

                if len(cutExpression) == 1:
                    performanceTable.pause("PlotCurve prepare")
                    selection = cutExpression[0].select(dataTable, functionTable, performanceTable)
                    performanceTable.unpause("PlotCurve prepare")
                else:
                    selection = NP("ones", len(ydataColumn.data), NP.dtype(bool))

                if ydataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(ydataColumn.mask == defs.VALID), selection)
                if dxdataColumn is not None and dxdataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(dxdataColumn.mask == defs.VALID), selection)
                if dydataColumn is not None and dydataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(dydataColumn.mask == defs.VALID), selection)
                    
                yarray = ydataColumn.data[selection]

                xarray = NP("ones", len(yarray), dtype=NP.dtype(float))
                xarray[0] = 0.0
                xarray = NP("cumsum", xarray)

                dxarray, dyarray = None, None
                if dxdataColumn is not None:
                    dxarray = dxdataColumn.data[selection]
                if dydataColumn is not None:
                    dyarray = dydataColumn.data[selection]

                xfieldType = self.xfieldType
                yfieldType = ydataColumn.fieldType

            elif len(nx) == 1 and len(ny) == 1:
                performanceTable.pause("PlotCurve prepare")
                xdataColumn = nx[0].evaluate(dataTable, functionTable, performanceTable)
                ydataColumn = ny[0].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("PlotCurve prepare")

                if len(cutExpression) == 1:
                    performanceTable.pause("PlotCurve prepare")
                    selection = cutExpression[0].select(dataTable, functionTable, performanceTable)
                    performanceTable.unpause("PlotCurve prepare")
                else:
                    selection = NP("ones", len(ydataColumn.data), NP.dtype(bool))

                if xdataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(xdataColumn.mask == defs.VALID), selection)
                if ydataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(ydataColumn.mask == defs.VALID), selection)
                if dxdataColumn is not None and dxdataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(dxdataColumn.mask == defs.VALID), selection)
                if dydataColumn is not None and dydataColumn.mask is not None:
                    selection = NP("logical_and", selection, NP(dydataColumn.mask == defs.VALID), selection)

                xarray = xdataColumn.data[selection]
                yarray = ydataColumn.data[selection]

                dxarray, dyarray = None, None
                if dxdataColumn is not None:
                    dxarray = dxdataColumn.data[selection]
                if dydataColumn is not None:
                    dyarray = dydataColumn.data[selection]

                xfieldType = xdataColumn.fieldType
                yfieldType = ydataColumn.fieldType

            else:
                raise defs.PmmlValidationError("The only allowed combinations of PlotNumericExpressions are: \"y(x)\" and \"x(t) y(t)\"")

            persistentState = {}
            stateId = self.get("stateId")
            if stateId is not None:
                if stateId in dataTable.state:
                    persistentState = dataTable.state[stateId]
                    xarray = NP("concatenate", [xarray, persistentState["x"]])
                    yarray = NP("concatenate", [yarray, persistentState["y"]])
                    if dxarray is not None:
                        dxarray = NP("concatenate", [dxarray, persistentState["dx"]])
                    if dyarray is not None:
                        dyarray = NP("concatenate", [dyarray, persistentState["dy"]])
                else:
                    dataTable.state[stateId] = persistentState

            persistentState["x"] = xarray
            persistentState["y"] = yarray
            if dxarray is not None:
                persistentState["dx"] = dxarray
            if dyarray is not None:
                persistentState["dy"] = dyarray

            smooth = self.get("smooth", defaultFromXsd=True, convertType=True)
            if not smooth:
                if dyarray is not None and dxarray is None:
                    dxarray = NP((NP("roll", xarray, -1) - NP("roll", xarray, 1)) / 2.0)
                    dyarray = dyarray * dxarray

                loop = self.get("loop", defaultFromXsd=True, convertType=True)
                if dxarray is not None and not loop:
                    dxarray[0] = 0.0
                    dxarray[-1] = 0.0
                if dyarray is not None and not loop:
                    dyarray[0] = 0.0
                    dyarray[-1] = 0.0

                state.x = xarray
                state.y = yarray
                state.dx = dxarray
                state.dy = dyarray

            else:
                smoothingScale = self.get("smoothingScale", defaultFromXsd=True, convertType=True)
                loop = self.get("loop", defaultFromXsd=True, convertType=True)

                samples = self.generateSamples(xarray.min(), xarray.max())
                state.x, state.y, state.dx, state.dy = self.pointsToSmoothCurve(xarray, yarray, samples, smoothingScale, loop)

        if plotRange is not None:
            plotRange.expand(state.x, state.y, xfieldType, yfieldType)

        performanceTable.end("PlotCurve prepare")

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
        performanceTable.begin("PlotCurve draw")

        loop = self.get("loop", defaultFromXsd=True, convertType=True)        
        pathdata = self.formatPathdata(state.x, state.y, state.dx, state.dy, plotCoordinates, loop, (state.dx is not None and state.dy is not None))
        output = svg.g()

        style = self.getStyleState()
        strokeStyle = dict((x, style[x]) for x in style if x.startswith("stroke"))
        fillStyle = dict((x, style[x]) for x in style if x.startswith("fill"))
        fillStyle["stroke"] = "none"

        if style["fill"] != "none":
            if len(self.xpath("pmml:PlotFormula[@role='y(x)']")) > 0 and len(pathdata) > 1:
                firstPoint = plotCoordinates(state.x[0], 0.0)
                lastPoint = plotCoordinates(state.x[-1], 0.0)

                X0, Y0 = plotCoordinates(state.x[0], state.y[0])

                pathdata2 = ["M %r %r" % firstPoint]
                pathdata2.append("L %r %r" % (X0, Y0))
                pathdata2.extend(pathdata[1:])
                pathdata2.append("L %r %r" % lastPoint)

                output.append(svg.path(d=" ".join(pathdata2), style=PlotStyle.toString(fillStyle)))

            else:
                output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(fillStyle)))

        output.append(svg.path(d=" ".join(pathdata), style=PlotStyle.toString(strokeStyle)))

        svgId = self.get("svgId")
        if svgId is not None:
            output["id"] = svgId

        performanceTable.end("PlotCurve draw")
        return output
