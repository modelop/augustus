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

"""This module defines the PlotToolkit class."""

import re
import random
import copy
import math

from lxml.etree import tostring, ElementTree

from augustus.core.defs import defs
from augustus.core.Toolkit import Toolkit
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.SvgBinding import SvgBinding
from augustus.core.PmmlPredicate import PmmlPredicate
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.plot.PmmlPlotFrame import PmmlPlotFrame
from augustus.core.plot.PmmlPlotContentAnnotation import PmmlPlotContentAnnotation
from augustus.core.plot.PmmlPlotContent import PmmlPlotContent
from augustus.core.plot.PlotStyle import PlotStyle
from augustus.pmml.odg.Formula import Formula
from augustus.pmml.odg.SerializedState import SerializedState

class PlotToolkit(Toolkit):
    """A Toolkit interface to the plotting elements, intended for
    interactive analysis and building workflows via templates.

    All functions in this toolkit share a ModelLoader for convenience.

    Calling a function for a particular plotting element builds all
    hierarchical structures needed to make a complete plot.  That is,
    if we ask for a PlotScatter, we'll get a PlotScatter embedded in a
    PlotOverlay in a PlotWindow in a PlotCanvas.  It can be
    immediately viewed with the C{view} method (or
    C{SvgBinding.toXmlFile} if you don't have the C{svgviewer}
    installed).

    The resulting plot elements may be embedded in other PMML trees,
    including ones craeted with these template functions.  For instance::

        overlay(scatter(x1, y1), scatter(x2, y2))
    
    creates two PlotScatters, each of which is complete with the full chain
    of PlotOverlay, PlotWindow, and PlotCanvas, and then discards their
    superstructures to embed them both in a single PlotOverlay, PlotWindow,
    PlotCanvas created by the C{overlay} function.
    
    Another feature of the template functions is that they propagate options
    to the first plotting element that will take them.  For instance, C{ylog}
    is a PlotOverlay option, but you can say::

        scatter(x1, y1, ylog=True)

    to get a PlotScatter-PlotOverlay-PlotWindow-PlotCanvas with C{ylog}.

    However, the combination of the two features described above can cause
    some surprises.  If an option propagates up to a plotting element that
    is discarded, its effect isn't seen.  For instance::

        overlay(scatter(x1, y1, ylog=True), scatter(x2, y2))

    produces a C{ylog=False} PlotOverlay because the the PlotOverlay that was
    built for the first C{scatter} was discarded in favor of the one that was
    built by the C{overlay}.  Just be aware of this as a potential source of
    bugs.
    """

    importables = ["canvas", "layout", "window", "overlay", "scatter", "histogram", "curve", "parametricCurve", "pointsCurve", "parametricPointsCurve", "boxAndWhiskers", "grid", "line", "svgAnnotation", "svgContent", "heatmap", "heatmapHistogram", "makeStatic",
                   "makeGradient"]

    def __init__(self, modelLoader):
        """Initialize the PlotToolkit with a ModelLoader.

        This ModelLoader will be used to build all plotting elements
        on demand.

        @type modelLoader: ModelLoader
        @param modelLoader: The ModelLoader.
        """

        super(PlotToolkit, self).__init__(modelLoader=modelLoader)

    def _uniqueSvgId(self):
        bigNumber = random.randint(0, 1000000)
        while bigNumber in self.usedids:
            bigNumber = random.randint(0, 1000000)
        output = "Untitled_%06d" % bigNumber
        return output

    def _uniqueStateId(self, tag):
        bigNumber = random.randint(0, 1000000)
        while bigNumber in self.usedids:
            bigNumber = random.randint(0, 1000000)
        output = "Untitled_%06d" % bigNumber
        return output

    def _camelCaseToHyphens(self, name):
        return "-".join(x.group(1).lower() if x.group(2) is None else x.group(1) for x in re.finditer("((^.[^A-Z]+)|([A-Z][^A-Z]+))", name))

    def _findOptions(self, cls):
        if cls.xsd is None:
            raise RuntimeError("%s object has no associated XSD" % cls.__name__)
        return set(cls.xsd.xpath(".//xs:attribute/@name", namespaces={"xs": defs.XSD_NAMESPACE}))
        
    def _splitOptions(self, elementName, options, addSvgId=False, addStateId=False):
        cls = self.modelLoader.tagToClass[elementName]
        expectedOptions = self._findOptions(cls)

        thisOptions = {}
        nextOptions = {}
        for name, value in options.items():
            strvalue = "true" if value is True else "false" if value is False else "none" if value is None else str(value)

            if name in expectedOptions:
                if value is not None:
                    thisOptions[name] = strvalue
            else:
                hyphenatedName = self._camelCaseToHyphens(name)
                if hyphenatedName in expectedOptions:
                    if value is not None:
                        thisOptions[hyphenatedName] = strvalue
                else:
                    if value is not None:
                        nextOptions[name] = strvalue

        if addSvgId and "svgId" not in thisOptions:
            thisOptions["svgId"] = self._uniqueSvgId()

        if addStateId and "stateId" not in thisOptions:
            thisOptions["stateId"] = self._uniqueStateId(elementName)

        return thisOptions, nextOptions

    def _splitFrames(self, contents, frames):
        for test in contents:
            if isinstance(test, PmmlPlotFrame):
                frames.append(test)
            elif isinstance(test, PmmlBinding):
                self._splitFrames(test.getchildren(), frames)
            else:
                raise TypeError("This function expects PmmlPlotFrames, not %r" % test)
        
    def canvas(self, *contents, **options):
        """Create a PlotCanvas.

        @param *contents: Plotting elements to embed in the PlotCanvas.
        @param **options: Options for a PlotCanvas.
        @rtype: PmmlBinding
        @return: The PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        """

        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", options, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        frames = []
        self._splitFrames(contents, frames)

        E = self.modelLoader.elementMaker()
        output = E.PlotCanvas(*copy.deepcopy(frames), **canvasOptions)
        self.modelLoader.validate(output)
        return output
    
    def layout(self, rows, cols, *contents, **options):
        """Create a PlotLayout.

        @type rows: int
        @param rows: The number of rows in the grid.
        @type cols: int
        @param cols: The number of columns in the grid.
        @param *contents: Plotting elements to embed in the PlotLayout.
        @param **options: Options for a PlotLayout or PlotCanvas.
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotLayout.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        """

        layoutOptions, nextOptions = self._splitOptions("PlotLayout", options, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        layoutOptions["rows"] = repr(rows)
        layoutOptions["cols"] = repr(cols)

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        frames = []
        self._splitFrames(contents, frames)
        
        E = self.modelLoader.elementMaker()
        output = E.PlotCanvas(E.PlotLayout(*copy.deepcopy(frames), **layoutOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def _splitCoordinatesAnnotations(self, contents, plotContents, overlayAnnotations):
        for test in contents:
            if isinstance(test, PmmlPlotContent):
                plotContents.append(plotContents)
            elif isinstance(test, (self.modelLoader.tagToClass["PlotOverlay"], PmmlPlotContentAnnotation)):
                overlayAnnotations.append(test)
            elif isinstance(test, PmmlBinding):
                self._splitCoordinatesAnnotations(test.getchildren(), plotContents, overlayAnnotations)
            else:
                raise TypeError("This function expects PlotOverlay and PmmlPlotContentAnnotation, not %r" % test)

    def makeGradient(self, name):
        """Create a gradient for a color scale in a PlotWindow.

        This function does not create a hierarchy of plotting elements
        that can be viewed; it only creates the gradient.

        The gradient C{name} can be one of

          * "greyscale", "antigreyscale"
          * "reds", "antireds"
          * "greens", "antigreens"
          * "blues", "antiblues"
          * "rainbow", "antirainbow"
          * "fire", "antifire"

        or it can be one of the above followed by ":opacity", where
        "opacity" can be one of

          * a number between 0.0 (transparent) and 1.0 (opaque)
          * "fadein": linearly increase from transparent at zmin to
            opaque at zmax
          * "fadeout": linearly decrease from opaque at zmin to
            transparent at zmax
          * "fadeinsqrt", "fadeoutsqrt": fade in/out more quickly
            with a square-root function
          * "fadeinsqr", "fadeoutsqr": fade in/out more slowly
            with a square function.

        @type name: string
        @param name: See above.
        @rtype: list of PmmlBindings
        @return: The PlotGradientStops that describe a gradient.
        """

        E = self.modelLoader.elementMaker()
        output = []

        if ":" in name:
            name, opacity = name.split(":")

            if opacity == "fadein":
                def getopacity(offset):
                    return repr(offset)

            elif opacity == "fadeout":
                def getopacity(offset):
                    return repr(1.0 - offset)

            elif opacity == "fadeinsqrt":
                def getopacity(offset):
                    return repr(math.sqrt(offset))

            elif opacity == "fadeoutsqrt":
                def getopacity(offset):
                    return repr(1.0 - math.sqrt(offset))

            elif opacity == "fadeinsqr":
                def getopacity(offset):
                    return repr(offset**2)

            elif opacity == "fadeoutsqr":
                def getopacity(offset):
                    return repr(1.0 - offset**2)

            else:
                float(opacity)

                def getopacity(offset):
                    return opacity

        else:
            def getopacity(offset):
                return "1"

        if name in ("grayscale", "greyscale"):
            output.append(E.PlotGradientStop(offset="0.00", red="1", green="1", blue="1", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="0", green="0", blue="0", opacity=getopacity(1.00)))

        elif name in ("antigrayscale", "antigreyscale"):
            output.append(E.PlotGradientStop(offset="0.00", red="0", green="0", blue="0", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="1", green="1", blue="1", opacity=getopacity(1.00)))

        elif name == "reds":
            output.append(E.PlotGradientStop(offset="0.00", red="1", green="1", blue="1", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="1", green="0", blue="0", opacity=getopacity(1.00)))

        elif name == "antireds":
            output.append(E.PlotGradientStop(offset="0.00", red="1", green="0", blue="0", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="1", green="1", blue="1", opacity=getopacity(1.00)))

        elif name == "greens":
            output.append(E.PlotGradientStop(offset="0.00", red="1", green="1", blue="1", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="0", green="1", blue="0", opacity=getopacity(1.00)))

        elif name == "antigreens":
            output.append(E.PlotGradientStop(offset="0.00", red="0", green="1", blue="0", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="1", green="1", blue="1", opacity=getopacity(1.00)))

        elif name == "blues":
            output.append(E.PlotGradientStop(offset="0.00", red="1", green="1", blue="1", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="0", green="0", blue="1", opacity=getopacity(1.00)))

        elif name == "antiblues":
            output.append(E.PlotGradientStop(offset="0.00", red="0", green="0", blue="1", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="1.00", red="1", green="1", blue="1", opacity=getopacity(1.00)))

        elif name == "rainbow":
            output.append(E.PlotGradientStop(offset="0.00", red="0.00", green="0.00", blue="0.51", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="0.34", red="0.00", green="0.81", blue="1.00", opacity=getopacity(0.34)))
            output.append(E.PlotGradientStop(offset="0.61", red="0.87", green="1.00", blue="0.12", opacity=getopacity(0.61)))
            output.append(E.PlotGradientStop(offset="0.84", red="1.00", green="0.20", blue="0.00", opacity=getopacity(0.84)))
            output.append(E.PlotGradientStop(offset="1.00", red="0.51", green="0.00", blue="0.00", opacity=getopacity(1.00)))

        elif name == "antirainbow":
            output.append(E.PlotGradientStop(offset="0.00", red="0.51", green="0.00", blue="0.00", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="0.34", red="1.00", green="0.20", blue="0.00", opacity=getopacity(0.34)))
            output.append(E.PlotGradientStop(offset="0.61", red="0.87", green="1.00", blue="0.12", opacity=getopacity(0.61)))
            output.append(E.PlotGradientStop(offset="0.84", red="0.00", green="0.81", blue="1.00", opacity=getopacity(0.84)))
            output.append(E.PlotGradientStop(offset="1.00", red="0.00", green="0.00", blue="0.51", opacity=getopacity(1.00)))

        elif name == "fire":
            output.append(E.PlotGradientStop(offset="0.00", red="0.00", green="0.00", blue="0.00", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="0.15", red="0.50", green="0.00", blue="0.00", opacity=getopacity(0.15)))
            output.append(E.PlotGradientStop(offset="0.30", red="1.00", green="0.00", blue="0.00", opacity=getopacity(0.30)))
            output.append(E.PlotGradientStop(offset="0.40", red="1.00", green="0.20", blue="0.00", opacity=getopacity(0.40)))
            output.append(E.PlotGradientStop(offset="0.60", red="1.00", green="1.00", blue="0.00", opacity=getopacity(0.60)))
            output.append(E.PlotGradientStop(offset="1.00", red="1.00", green="1.00", blue="1.00", opacity=getopacity(1.00)))

        elif name == "antifire":
            output.append(E.PlotGradientStop(offset="0.00", red="1.00", green="1.00", blue="1.00", opacity=getopacity(0.00)))
            output.append(E.PlotGradientStop(offset="0.15", red="1.00", green="1.00", blue="0.00", opacity=getopacity(0.15)))
            output.append(E.PlotGradientStop(offset="0.30", red="1.00", green="0.20", blue="0.00", opacity=getopacity(0.30)))
            output.append(E.PlotGradientStop(offset="0.40", red="1.00", green="0.00", blue="0.00", opacity=getopacity(0.40)))
            output.append(E.PlotGradientStop(offset="0.60", red="0.50", green="0.00", blue="0.00", opacity=getopacity(0.60)))
            output.append(E.PlotGradientStop(offset="1.00", red="0.00", green="0.00", blue="0.00", opacity=getopacity(1.00)))

        else:
            raise LookupError("Unrecognized gradient name: \"%s\"" % name)

        return output

    def window(self, *contents, **options):
        """Create a PlotWindow.

        @param *contents: Plotting elements to embed in the PlotWindow.
        @param **options: Options for a PlotWindow or PlotCanvas.
        @type gradient: string
        @param gradient: The desired gradient (keyword only).
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotLayout.
        """

        if "gradient" in options:
            gradient = self.makeGradient(options["gradient"])
            del options["gradient"]
        else:
            gradient = []

        windowOptions, nextOptions = self._splitOptions("PlotWindow", options, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        plotContents = []
        overlayAnnotations = []
        self._splitCoordinatesAnnotations(contents, plotContents, overlayAnnotations)

        E = self.modelLoader.elementMaker()

        if len(overlayAnnotations) == 0:
            overlayAnnotations = [E.PlotOverlay(*plotContents)]
        else:
            overlayAnnotations[0].extend(plotContents)

        output = E.PlotCanvas(E.PlotWindow(*(copy.deepcopy(overlayAnnotations) + gradient), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def _splitPlotContents(self, contents, plotContents):
        for test in contents:
            if isinstance(test, PmmlPlotContent):
                plotContents.append(test)
            elif isinstance(test, PmmlBinding):
                self._splitPlotContents(test.getchildren(), plotContents)
            else:
                raise TypeError("This function expects PmmlPlotContents, not %r" % test)

    def overlay(self, *contents, **options):
        """Create a PlotOverlay.

        @param *contents: Plotting elements to embed in the PlotOverlay.
        @param **options: Options for a PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotOverlay.
        """

        if "gradient" in options:
            gradient = self.makeGradient(options["gradient"])
            del options["gradient"]
        else:
            gradient = []

        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", options)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        plotContents = []
        self._splitPlotContents(contents, plotContents)

        E = self.modelLoader.elementMaker()
        output = E.PlotCanvas(E.PlotWindow(*([E.PlotOverlay(*copy.deepcopy(plotContents), **overlayOptions)] + gradient), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def _convertToPredicate(self, E, selection, newSelection):
        if selection.hasTag("Apply") and selection["function"] in ("isMissing", "isNotMissing") and len(selection) == 1 and selection[0].hasTag("FieldRef"):
            newSelection.append(E.SimplePredicate(field=selection[0, "field"], operator=selection["function"]))
            return True

        elif selection.hasTag("Apply") and selection["function"] in ("equal", "notEqual", "lessThan", "lessOrEqual", "greaterThan", "greaterOrEqual") and len(selection) == 2 and selection[0].hasTag("FieldRef") and selection[1].hasTag("Constant"):
            newSelection.append(E.SimplePredicate(field=selection[0, "field"], operator=selection["function"], value=selection[1].text))
            return True

        elif selection.hasTag("Apply") and selection["function"] in ("equal", "notEqual", "lessThan", "lessOrEqual", "greaterThan", "greaterOrEqual") and len(selection) == 2 and selection[1].hasTag("FieldRef") and selection[0].hasTag("Constant"):
            reversedFunction = {"lessThan": "greaterOrEqual",
                                "lessOrEqual": "greaterThan",
                                "greaterThan": "lessOrEqual",
                                "greaterOrEqual": "lessThan",
                                }[selection["function"]]
            newSelection.append(E.SimplePredicate(field=selection[1, "field"], operator=reversedFunction, value=selection[0].text))
            return True

        elif selection.hasTag("Apply") and selection["function"] in ("isIn", "isNotIn") and len(selection) >= 2 and selection[0].hasTag("FieldRef") and all(x.hasTag("Constant") for x in selection[1:]):
            newSelection.append(E.SimpleSetPredicate(E.Array(" ".join("\"%s\"" % x.text for x in selection[1:]), type="string"), field=selection[0, "field"], booleanOperator=selection["function"]))
            return True

        elif selection.hasTag("Apply") and selection["function"] in ("and", "or", "xor"):
            children = []
            for child in selection:
                if not self._convertToPredicate(E, child, children):
                    return False

            newSelection.append(E.CompoundPredicate(*children, booleanOperator=selection["function"]))
            return True

        return False

    def _transformSelection(self, E, selection):
        if selection is not None:
            if isinstance(selection, basestring):
                selection = Formula.expansion(self.modelLoader, selection)
            elif isinstance(selection, PmmlPredicate):
                pass
            elif isinstance(selection, PmmlExpression):
                pass
            else:
                raise TypeError("selection must be None, a string, a PmmlPredicate, or a PmmlExpression, not %r" % selection)

            if isinstance(selection, (PmmlPredicate, PmmlExpression)):
                newSelection = E.PlotSelection()
                if self._convertToPredicate(E, selection, newSelection):
                    selection = newSelection
                else:
                    selection = E.PlotSelection(selection)

        return selection

    def scatter(self, xexpr, yexpr, selection=None, xerr=None, xerrup=None, yerr=None, yerrup=None, weight=None, svg=None, **options):
        """Create a PlotScatter.

        @type xexpr: string or PmmlExpression
        @param xexpr: The expression to evaluate for x values in the plot.
        @type yexpr: string or PmmlExpression
        @param yexpr: The expression to evaluate for y values in the plot.
        @type yexpr: string, PmmlExpression, PmmlPredicate, or None
        @param yexpr: The expression to evaluate to filter data in the plot.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @type xerr: string, PmmlExpression, or None
        @param xerr: The expression to evaluate for symmetric x error bars or asymmetric low x error bars.
        @type xerrup: string, PmmlExpression, or None
        @param xerrup: The expression to evaluate for asymmetric high x error bars.
        @type yerr: string, PmmlExpression, or None
        @param yerr: The expression to evaluate for symmetric y error bars or asymmetric low y error bars.
        @type yerrup: string, PmmlExpression, or None
        @param yerrup: The expression to evaluate for asymmetric high y error bars.
        @type weight: string, PmmlExpression, or None
        @param weight: The expression to evaluate and use for the opacity of each point; the user must ensure that it ranges between 0 and 1.
        @type svg: SvgBinding or None
        @param svg: The SVG object for pictogram markers.
        @param **options: Options for a PlotScatter, PlotOverlay, PlotWindow, or PlotCanvas.
        @type limit: number or None
        @param limit: A limit on the number of points to view (keyword only).
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotScatter.
        """

        if "limit" not in options:
            options["limit"] = 1000

        if svg is not None and "marker" not in options:
            options["marker"] = "svg"

        scatterOptions, nextOptions = self._splitOptions("PlotScatter", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(xexpr, basestring):
            xexpr = Formula.expansion(self.modelLoader, xexpr)
        elif isinstance(xexpr, PmmlExpression):
            pass
        else:
            raise TypeError("xexpr must be a string or PmmlExpression, not %r" % xexpr)

        if isinstance(yexpr, basestring):
            yexpr = Formula.expansion(self.modelLoader, yexpr)
        elif isinstance(yexpr, PmmlExpression):
            pass
        else:
            raise TypeError("yexpr must be a string or PmmlExpression, not %r" % yexpr)

        if xerr is not None:
            if isinstance(xerr, basestring):
                xerr = Formula.expansion(self.modelLoader, xerr)
            elif isinstance(xerr, PmmlExpression):
                pass
            else:
                raise TypeError("xerr must be a string or PmmlExpression, not %r" % xerr)

        if xerrup is not None:
            if xerr is None:
                raise TypeError("If xerrup is provided, xerr (asymmetric downward error bars) must be provided as well")

            if isinstance(xerrup, basestring):
                xerrup = Formula.expansion(self.modelLoader, xerrup)
            elif isinstance(xerrup, PmmlExpression):
                pass
            else:
                raise TypeError("xerrup must be a string or PmmlExpression, not %r" % xerrup)

        if yerr is not None:
            if isinstance(yerr, basestring):
                yerr = Formula.expansion(self.modelLoader, yerr)
            elif isinstance(yerr, PmmlExpression):
                pass
            else:
                raise TypeError("yerr must be a string or PmmlExpression, not %r" % yerr)

        if yerrup is not None:
            if yerr is None:
                raise TypeError("If yerrup is provided, yerr (asymmetric downward error bars) must be provided as well")

            if isinstance(yerrup, basestring):
                yerrup = Formula.expansion(self.modelLoader, yerrup)
            elif isinstance(yerrup, PmmlExpression):
                pass
            else:
                raise TypeError("yerrup must be a string or PmmlExpression, not %r" % yerrup)

        if weight is not None:
            if isinstance(weight, basestring):
                weight = Formula.expansion(self.modelLoader, weight)
            elif isinstance(weight, PmmlExpression):
                pass
            else:
                raise TypeError("weight must be a string or PmmlExpression, not %r" % weight)

        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        scatterPlot = E.PlotScatter(E.PlotNumericExpression(xexpr, role="x"), E.PlotNumericExpression(yexpr, role="y"), **scatterOptions)
        if xerr is not None:
            if xerrup is not None:
                scatterPlot.append(E.PlotNumericExpression(xerrup, role="x-errorbar-up"))
                scatterPlot.append(E.PlotNumericExpression(xerr, role="x-errorbar-down"))
            else:
                scatterPlot.append(E.PlotNumericExpression(xerr, role="x-errorbar"))
        if yerr is not None:
            if yerrup is not None:
                scatterPlot.append(E.PlotNumericExpression(yerrup, role="y-errorbar-up"))
                scatterPlot.append(E.PlotNumericExpression(yerr, role="y-errorbar-down"))
            else:
                scatterPlot.append(E.PlotNumericExpression(yerr, role="y-errorbar"))
        if weight is not None:
            scatterPlot.append(E.PlotNumericExpression(weight, role="weight"))

        if selection is not None:
            scatterPlot.append(selection)
        if isinstance(svg, basestring):
            scatterPlot.append(E.PlotSvgMarker(fileName=svg))
        elif isinstance(svg, SvgBinding):
            scatterPlot.append(E.PlotSvgMarker(svg))
        elif svg is None:
            pass
        else:
            raise TypeError("svg must be an SVG fileName or an SvgBinding object")

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(scatterPlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def histogram(self, expr, weight=None, selection=None, **options):
        """Create a PlotHistogram.

        @type expr: string or PmmlExpression
        @param expr: The expression to evaluate as input to the histogram.
        @type weight: string, PmmlExpression, or None
        @param weight: The expression to evaluate for weights in the histogram.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @param **options: Options for a PlotHistogram, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotHistogram.
        """

        histogramOptions, nextOptions = self._splitOptions("PlotHistogram", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(expr, basestring):
            expr = Formula.expansion(self.modelLoader, expr)
        elif isinstance(expr, PmmlExpression):
            pass
        else:
            raise TypeError("expr must be a string or PmmlExpression, not %r" % expr)

        if weight is not None:
            if isinstance(weight, basestring):
                weight = Formula.expansion(self.modelLoader, weight)
            elif isinstance(weight, PmmlExpression):
                pass
            else:
                raise TypeError("weight must be a string or PmmlExpression, not %r" % weight)

        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        histogramPlot = E.PlotHistogram(E.PlotExpression(expr, role="data"), **histogramOptions)
        if weight is not None:
            histogramPlot.append(E.PlotNumericExpression(weight, role="weight"))
        if selection is not None:
            histogramPlot.append(selection)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(histogramPlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def curve(self, low, high, expr, deriv=None, **options):
        """Create a PlotCurve of a mathematical expression y(x).

        @type low: number
        @param low: The minimum x sampled.
        @type high: number
        @param high: The maximum x sampled.
        @type expr: string
        @param expr: The y(x) expression to evaluate and draw, with "x" being the independent variable.
        @type deriv: string or None
        @param deriv: The derivative of the expression, for smoother curves with fewer points.
        @param **options: Options for a PlotCurve, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotCurve.
        """

        curveOptions, nextOptions = self._splitOptions("PlotCurve", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        curvePlot = E.PlotCurve(E.PlotFormula(expr, role="y(x)"), low=repr(low), high=repr(high), **curveOptions)
        if deriv is not None:
            curvePlot.append(E.PlotFormula(deriv, role="dy/dx"))

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(curvePlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def parametricCurve(self, low, high, x, y, dx=None, dy=None, **options):
        """Create a parametric PlotCurve of mathematical expressions x(t), y(t).

        @type low: number
        @param low: The minimum t sampled.
        @type high: number
        @param high: The maximum t sampled.
        @type x: string
        @param x: The x(t) expression to evaluate and draw, with "t" being the independent variable.
        @type y: string
        @param y: The y(t) expression to evaluate and draw, with "t" being the independent variable.
        @type dx: string or None
        @param dx: The dx/dt derivative of the expression, for smoother curves with fewer points.
        @type dy: string or None
        @param dy: The dy/dt derivative of the expression, for smoother curves with fewer points.
        @param **options: Options for a PlotCurve, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotCurve.
        """

        curveOptions, nextOptions = self._splitOptions("PlotCurve", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        curvePlot = E.PlotCurve(E.PlotFormula(x, role="x(t)"), E.PlotFormula(y, role="y(t)"), low=repr(low), high=repr(high), **curveOptions)
        if dx is not None:
            curvePlot.append(E.PlotFormula(dx, role="dx/dt"))
        if dy is not None:
            curvePlot.append(E.PlotFormula(dy, role="dy/dt"))

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(curvePlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def pointsCurve(self, expr, dydx=None, selection=None, **options):
        """Create a PlotCurve of a dataset.

        @type expr: string or PmmlExpression
        @param expr: The expression to evaluate and plot.
        @type dydx: string, PmmlExpression, or None
        @param dydx: The expression to use as a derivative.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @param **options: Options for a PlotCurve, PlotOverlay, PlotWindow, or PlotCanvas.
        @type smooth: bool
        @param smooth: If True, draw a smooth curve near the points (with option "smoothingScale" to set the smear factor); if False, draw a zig-zag line through the points.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotCurve.
        """

        if "smooth" not in options:
            options["smooth"] = False

        if options.get("smooth") in ("true", "1", True, 1) and "numSamples" not in options:
            options["numSamples"] = 10

        curveOptions, nextOptions = self._splitOptions("PlotCurve", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(expr, basestring):
            expr = Formula.expansion(self.modelLoader, expr)
        elif isinstance(expr, PmmlExpression):
            pass
        else:
            raise TypeError("expr must be a string or PmmlExpression, not %r" % expr)

        if dydx is not None:
            if isinstance(dydx, basestring):
                dydx = Formula.expansion(self.modelLoader, dydx)
            elif isinstance(dydx, PmmlExpression):
                pass
            else:
                raise TypeError("dydx must be a string or PmmlExpression, not %r" % dydx)

        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        curvePlot = E.PlotCurve(E.PlotNumericExpression(expr, role="y"), **curveOptions)
        if dydx is not None:
            curvePlot.append(E.PlotNumericExpression(dydx, role="dy"))
        if selection is not None:
            curvePlot.append(selection)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(curvePlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def parametricPointsCurve(self, xexpr, yexpr, dxdt=None, dydt=None, selection=None, **options):
        """Create a parametric PlotCurve of a dataset.

        @type xexpr: string or PmmlExpression
        @param xexpr: The x expression to evaluate and plot.
        @type yexpr: string or PmmlExpression
        @param yexpr: The y expression to evaluate and plot.
        @type dxdt: string, PmmlExpression, or None
        @param dxdt: The expression to use as a dx/dt derivative.
        @type dydt: string, PmmlExpression, or None
        @param dydt: The expression to use as a dy/dt derivative.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @param **options: Options for a PlotCurve, PlotOverlay, PlotWindow, or PlotCanvas.
        @type smooth: bool
        @param smooth: If True, draw a smooth curve near the points (with option "smoothingScale" to set the smear factor); if False, draw a zig-zag line through the points.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotCurve.
        """

        if "smooth" not in options:
            options["smooth"] = False

        if options.get("smooth") in ("true", "1", True, 1) and "numSamples" not in options:
            options["numSamples"] = 10

        curveOptions, nextOptions = self._splitOptions("PlotCurve", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(xexpr, basestring):
            xexpr = Formula.expansion(self.modelLoader, xexpr)
        elif isinstance(xexpr, PmmlExpression):
            pass
        else:
            raise TypeError("xexpr must be a string or PmmlExpression, not %r" % xexpr)

        if isinstance(yexpr, basestring):
            yexpr = Formula.expansion(self.modelLoader, yexpr)
        elif isinstance(yexpr, PmmlExpression):
            pass
        else:
            raise TypeError("yexpr must be a string or PmmlExpression, not %r" % yexpr)

        if dxdt is not None:
            if isinstance(dxdt, basestring):
                dxdt = Formula.expansion(self.modelLoader, dxdt)
            elif isinstance(dxdt, PmmlExpression):
                pass
            else:
                raise TypeError("dxdt must be a string or PmmlExpression, not %r" % dxdt)

        if dydt is not None:
            if isinstance(dydt, basestring):
                dydt = Formula.expansion(self.modelLoader, dydt)
            elif isinstance(dydt, PmmlExpression):
                pass
            else:
                raise TypeError("dydt must be a string or PmmlExpression, not %r" % dydt)

        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        curvePlot = E.PlotCurve(E.PlotNumericExpression(xexpr, role="x"), E.PlotNumericExpression(yexpr, role="y"), **curveOptions)
        if dxdt is not None:
            curvePlot.append(E.PlotNumericExpression(dxdt, role="dx"))
        if dydt is not None:
            curvePlot.append(E.PlotNumericExpression(dydt, role="dy"))
        if selection is not None:
            curvePlot.append(selection)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(curvePlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def boxAndWhiskers(self, sliced, profiled, selection=None, **options):
        """Create a PlotBoxAndWhisker.

        @type sliced: string or PmmlExpression
        @param sliced: The expression to evaluate and slice into bins like a histogram.
        @type profiled: string or PmmlExpression
        @param profiled: The expression to evaluate and estimate within each slice.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @param **options: Options for a PlotBoxAndWhisker, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotBoxAndWhisker.
        """

        boxOptions, nextOptions = self._splitOptions("PlotBoxAndWhisker", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(sliced, basestring):
            sliced = Formula.expansion(self.modelLoader, sliced)
        elif isinstance(sliced, PmmlExpression):
            pass
        else:
            raise TypeError("sliced must be a string or PmmlExpression, not %r" % sliced)

        if isinstance(profiled, basestring):
            profiled = Formula.expansion(self.modelLoader, profiled)
        elif isinstance(profiled, PmmlExpression):
            pass
        else:
            raise TypeError("profiled must be a string or PmmlExpression, not %r" % profiled)
        
        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        boxPlot = E.PlotBoxAndWhisker(E.PlotExpression(sliced, role="sliced"), E.PlotNumericExpression(profiled, role="profiled"), **boxOptions)
        if selection is not None:
            boxPlot.append(selection)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(boxPlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def grid(self, xspacing, yspacing, x0=0.0, y0=0.0, **options):
        """Create a PlotGuideLines of an infinite grid.

        @type xspacing: number
        @param xspacing: The size of the gap between vertical grid lines.
        @type yspacing: number
        @param yspacing: The size of the gap between horizontal grid lines.
        @type x0: number
        @param x0: The "phase" of the grid: an x value that will have a line through it.
        @type y0: number
        @param y0: The "phase" of the grid: a y value that will have a line through it.
        @param **options: Options for a PlotGuideLines, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotGuideLines.
        """

        if "style" in options:
            style = PlotStyle.toDict(options["style"])
            if "stroke" not in style:
                style["stroke"] = "grey"
            if "stroke-dasharray" not in style:
                style["stroke-dasharray"] = "5, 5"
            lineOptions = {"style": PlotStyle.toString(style)}
            del options["style"]
        else:
            lineOptions = {"style": "stroke: grey; stroke-dasharray: 5, 5"}

        guideLinesOptions, nextOptions = self._splitOptions("PlotGuideLines", options, addSvgId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        gridPlot = E.PlotGuideLines(E.PlotVerticalLines(x0=repr(x0), spacing=repr(xspacing), **lineOptions), E.PlotHorizontalLines(y0=repr(y0), spacing=repr(yspacing), **lineOptions), **guideLinesOptions)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(gridPlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def line(self, x1, y1, x2, y2, **options):
        """Create a PlotGuideLines of a single arbirary line.

        @type x1: number
        @param x1: The first x coordinate of the line.
        @type y1: number
        @param y1: The first y coordinate of the line.
        @type x2: number
        @param x2: The second x coordinate of the line.
        @type y2: number
        @param y2: The second y coordinate of the line.
        @param **options: Options for a PlotGuideLines, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotGuideLines.
        """

        if "style" in options:
            lineOptions = {"style": options["style"]}
            del options["style"]
        else:
            lineOptions = {}

        guideLinesOptions, nextOptions = self._splitOptions("PlotGuideLines", options, addSvgId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        linePlot = E.PlotGuideLines(E.PlotLine(x1=repr(x1), y1=repr(y1), x2=repr(x2), y2=repr(y2), **lineOptions), **guideLinesOptions)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(linePlot, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def svgAnnotation(self, svg, **options):
        """Create a PlotSvgAnnotation.

        @type svg: SvgBinding
        @param svg: The SVG to add to the plot, outside of the data coordinate system.
        @param **options: Options for a PlotSvgAnnotation, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotSvgAnnotation.
        """

        svgOptions, nextOptions = self._splitOptions("PlotSvgAnnotation", options)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        if isinstance(svg, SvgBinding):
            svgWrapper = E.PlotSvgAnnotation(copy.deepcopy(svg), **svgOptions)
        elif isinstance(svg, basestring):
            svgWrapper = E.PlotSvgAnnotation(fileName=svg, **svgOptions)
        else:
            raise TypeError("Argument \"svg\" must either be an SvgBinding object or a fileName to reference an external SVG file")

        output = E.PlotCanvas(E.PlotWindow(svgWrapper, **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def svgContent(self, svg, x1, y1, x2, y2, **options):
        """Create a PlotSvgContent.

        @type svg: SvgBinding
        @param svg: The SVG to add to the plot, withing the data coordinate system.
        @type x1: number
        @param x1: The left edge of the SVG image.
        @type y1: number
        @param y1: The bottom edge of the SVG image.
        @type x2: number
        @param x2: The right edge of the SVG image.
        @type y2: number
        @param y2: The top edge of the SVG image.
        @param **options: Options for a PlotSvgContent, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotSvgContent.
        """

        svgOptions, nextOptions = self._splitOptions("PlotSvgAnnotation", options)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        if isinstance(svg, SvgBinding):
            svgWrapper = E.PlotSvgContent(copy.deepcopy(svg), x1=repr(x1), y1=repr(y1), x2=repr(x2), y2=repr(y2), **svgOptions)
        elif isinstance(svg, basestring):
            svgWrapper = E.PlotSvgContent(fileName=svg, x1=repr(x1), y1=repr(y1), x2=repr(x2), y2=repr(y2), **svgOptions)
        else:
            raise TypeError("Argument \"svg\" must either be an SvgBinding object or a fileName to reference an external SVG file")

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(svgWrapper, **overlayOptions), **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def heatmap(self, xbins, ybins, xlow, ylow, xhigh, yhigh, zexpr, **options):
        """Create a PlotHeatMap of a mathematical expression.

        @type xbins: int
        @param xbins: The number of horizontal bins.
        @type ybins: int
        @param ybins: The number of vertical bins.
        @type xlow: number
        @param xlow: The left edge of the heatmap.
        @type ylow: number
        @param ylow: The bottom edge of the heatmap.
        @type xhigh: number
        @param xhigh: The right edge of the heatmap.
        @type yhigh: number
        @param yhigh: The top edge of the heatmap.
        @type zexpr: string or PmmlExpression
        @param zexpr: The z(x,y) expression to evaluate and plot, with "x" and "y" being the independent variables.
        @param **options: Options for a PlotHeatMap, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotHeatMap.
        """

        if "gradient" in options:
            gradient = self.makeGradient(options["gradient"])
            del options["gradient"]
        else:
            gradient = []

        heatmapOptions, nextOptions = self._splitOptions("PlotHeatMap", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        E = self.modelLoader.elementMaker()

        heatmapPlot = E.PlotHeatMap(E.PlotFormula(zexpr, role="z(x,y)"), xbins=repr(xbins), xlow=repr(xlow), xhigh=repr(xhigh), ybins=repr(ybins), ylow=repr(ylow), yhigh=repr(yhigh), **heatmapOptions)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(heatmapPlot, **overlayOptions), *gradient, **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def heatmapHistogram(self, xexpr, yexpr, zmean=None, zweight=None, selection=None, **options):
        """Create a PlotHeatMap of a 2d histogram of data.

        @type xexpr: string or PmmlExpression
        @param xexpr: The x expression to evaluate and slice into horizontal bins.
        @type yexpr: string or PmmlExpression
        @param yexpr: The y expression to evaluate and slice into vertical bins.
        @type zweight: string, PmmlExpression, or None
        @param zweight: The expression to evaluate for weights in the histogram.  Incompatible with C{zmean}.
        @type zmean: string, PmmlExpression, or None
        @param zmean: The expression to evaluate and average in each bin.  Incompatible with C{zweight}.
        @type selection: string, PmmlExpression, PmmlPredicate, or None
        @param selection: The expression to evaluate to filter data in the plot.
        @param **options: Options for a PlotHeatMap, PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotHeatMap.
        """

        if "gradient" in options:
            gradient = self.makeGradient(options["gradient"])
            del options["gradient"]
        else:
            gradient = []

        heatmapOptions, nextOptions = self._splitOptions("PlotHeatMap", options, addSvgId=True, addStateId=True)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        if isinstance(xexpr, basestring):
            xexpr = Formula.expansion(self.modelLoader, xexpr)
        elif isinstance(xexpr, PmmlExpression):
            pass
        else:
            raise TypeError("xexpr must be a string or PmmlExpression, not %r" % xexpr)

        if isinstance(yexpr, basestring):
            yexpr = Formula.expansion(self.modelLoader, yexpr)
        elif isinstance(yexpr, PmmlExpression):
            pass
        else:
            raise TypeError("yexpr must be a string or PmmlExpression, not %r" % yexpr)

        if zmean is None and zweight is None:
            pass

        elif zmean is not None and zweight is None:
            if isinstance(zmean, basestring):
                zmean = Formula.expansion(self.modelLoader, zmean)
            elif isinstance(zmean, PmmlExpression):
                pass
            else:
                raise TypeError("zmean must be a string or PmmlExpression, not %r" % zmean)

        elif zmean is None and zweight is not None:
            if isinstance(zweight, basestring):
                zweight = Formula.expansion(self.modelLoader, zweight)
            elif isinstance(zweight, PmmlExpression):
                pass
            else:
                raise TypeError("zweight must be a string or PmmlExpression, not %r" % zweight)

        else:
            raise TypeError("zmean and zweight cannot both be specified")

        E = self.modelLoader.elementMaker()

        selection = self._transformSelection(E, selection)

        heatmapPlot = E.PlotHeatMap(E.PlotNumericExpression(xexpr, role="x"), E.PlotNumericExpression(yexpr, role="y"), **heatmapOptions)
        if zmean is not None:
            heatmapPlot.append(E.PlotNumericExpression(zmean, role="zmean"))
        if zweight is not None:
            heatmapPlot.append(E.PlotNumericExpression(zweight, role="zweight"))
        if selection is not None:
            heatmapPlot.append(selection)

        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(heatmapPlot, **overlayOptions), *gradient, **windowOptions), **canvasOptions)
        self.modelLoader.validate(output)
        return output

    def makeStatic(self, dataTableState, *contents, **options):
        """Save the current state of a set of plots as a PlotStatic.

        @type dataTableState: DataTableState
        @param dataTableState: The current state of execution to save for this plot.
        @param *contents: Plotting elements to save and embed in the PlotStatic.
        @param **options: Options for a PlotOverlay, PlotWindow, or PlotCanvas.
        @raise PmmlValidationError: If the resulting configuration is not valid PMML, this function raises an error.        
        @rtype: PmmlBinding
        @return: A PlotCanvas containing the PlotStatic.
        """

        staticOptions, nextOptions = self._splitOptions("PlotStatic", options, addSvgId=True, addStateId=False)
        overlayOptions, nextOptions = self._splitOptions("PlotOverlay", nextOptions)
        windowOptions, nextOptions = self._splitOptions("PlotWindow", nextOptions, addSvgId=True)
        canvasOptions, unrecognizedOptions = self._splitOptions("PlotCanvas", nextOptions, addSvgId=True)
        if len(unrecognizedOptions) > 0:
            raise TypeError("Unrecognized options: %s" % " ".join(unrecognizedOptions))

        if "plotName" not in canvasOptions:
            canvasOptions["plotName"] = canvasOptions["svgId"]

        plotContents = []
        self._splitPlotContents(contents, plotContents)

        restriction = []
        for plotContent in plotContents:
            stateId = plotContent.get("stateId")
            if stateId is not None:
                restriction.append(stateId)
                restriction.append(stateId + ".context")

        serializedState = SerializedState.serializeState(self.modelLoader, dataTableState, restriction=restriction)
        plotContents.insert(0, serializedState)

        E = self.modelLoader.elementMaker()
        output = E.PlotCanvas(E.PlotWindow(E.PlotOverlay(E.PlotStatic(*copy.deepcopy(plotContents), **staticOptions), **overlayOptions), **windowOptions), **canvasOptions)

        self.modelLoader.validate(output)
        return output
