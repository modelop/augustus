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

"""This module defines ODG plotting extensions that could be added to
any interpretation of PMML.  It is usually loaded as::

    from augustus.plot import addPlotting
    addPlotting(modelLoader)

which modifies an existing ModelLoader."""

from lxml.builder import ElementMaker

from augustus.core.defs import defs
from augustus.pmml.plot.PlotFormula import PlotFormula
from augustus.pmml.plot.PlotExpression import PlotExpression
from augustus.pmml.plot.PlotNumericExpression import PlotNumericExpression
from augustus.pmml.plot.PlotSelection import PlotSelection
from augustus.pmml.plot.PlotLegend import PlotLegend
from augustus.pmml.plot.PlotSvgAnnotation import PlotSvgAnnotation
from augustus.pmml.plot.PlotSvgContent import PlotSvgContent
from augustus.pmml.plot.PlotScatter import PlotScatter
from augustus.pmml.plot.PlotHistogram import PlotHistogram
from augustus.pmml.plot.PlotBoxAndWhisker import PlotBoxAndWhisker
from augustus.pmml.plot.PlotCurve import PlotCurve
from augustus.pmml.plot.PlotHeatMap import PlotHeatMap
from augustus.pmml.plot.PlotGuideLines import PlotGuideLines
from augustus.pmml.plot.PlotCanvas import PlotCanvas
from augustus.pmml.plot.PlotLayout import PlotLayout
from augustus.pmml.plot.PlotWindow import PlotWindow
from augustus.pmml.plot.PlotLegendNumber import PlotLegendNumber
from augustus.pmml.plot.PlotLegendSvg import PlotLegendSvg
from augustus.pmml.plot.PlotOverlay import PlotOverlay
from augustus.pmml.plot.PlotStatic import PlotStatic
from augustus.pmml.odg.SerializedState import SerializedState

def addPlotting(modelLoader):
    modelLoader.register("PlotFormula", PlotFormula)
    modelLoader.register("PlotExpression", PlotExpression)
    modelLoader.register("PlotNumericExpression", PlotNumericExpression)
    modelLoader.register("PlotSelection", PlotSelection)
    modelLoader.register("PlotLegendNumber", PlotLegendNumber)
    modelLoader.register("PlotLegendSvg", PlotLegendSvg)
    modelLoader.register("PlotLegend", PlotLegend)
    modelLoader.register("PlotSvgAnnotation", PlotSvgAnnotation)
    modelLoader.register("PlotSvgContent", PlotSvgContent)
    modelLoader.register("PlotScatter", PlotScatter)
    modelLoader.register("PlotHistogram", PlotHistogram)
    modelLoader.register("PlotBoxAndWhisker", PlotBoxAndWhisker)
    modelLoader.register("PlotCurve", PlotCurve)
    modelLoader.register("PlotHeatMap", PlotHeatMap)
    modelLoader.register("PlotGuideLines", PlotGuideLines)
    modelLoader.register("PlotCanvas", PlotCanvas)
    modelLoader.register("PlotLayout", PlotLayout)
    modelLoader.register("PlotWindow", PlotWindow)
    modelLoader.register("PlotOverlay", PlotOverlay)
    modelLoader.register("PlotStatic", PlotStatic)
    modelLoader.register("SerializedState", SerializedState)

    E = ElementMaker(namespace=defs.XSD_NAMESPACE, nsmap={"xs": defs.XSD_NAMESPACE})

    for derivedField in modelLoader.schema.xpath("//xs:element[@ref='DerivedField']", namespaces={"xs": defs.XSD_NAMESPACE}):
        isTransformation = False
        pointer = derivedField
        while pointer is not None:
            if pointer.get("name") in ("TransformationDictionary", "LocalTransformations"):
                isTransformation = True
                break
            pointer = pointer.getparent()

        parent = derivedField.getparent()
        alreadyDone = (parent.tag == "{%s}%s" % (defs.XSD_NAMESPACE, "choice") and len(parent.xpath("xs:element[@ref='PlotCanvas']", namespaces={"xs": defs.XSD_NAMESPACE})) > 0)

        if isTransformation and not alreadyDone:
            parent = derivedField.getparent()
            index = parent.index(derivedField)

            derivedField.attrib["minOccurs"] = "1"
            derivedField.attrib["maxOccurs"] = "1"
            replacement = E.choice(E.element(ref="PlotCanvas", minOccurs="1", maxOccurs="1"), minOccurs="0", maxOccurs="unbounded")
            parent[index] = replacement
            replacement.append(derivedField)

    for name in "PLOT-FRAME", "PLOT-CONTENT", "PLOT-CONTENT-ANNOTATION", "PLOT-LEGEND-CONTENT":
        modelLoader.xsdRemove(name)

    modelLoader.xsdAppend("""
<xs:group name="PLOT-FRAME" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:choice>
        <xs:element ref="PlotLayout" />
        <xs:element ref="PlotWindow" />
    </xs:choice>
</xs:group>
""")
    modelLoader.xsdAppend("""
<xs:group name="PLOT-CONTENT" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:choice>
        <xs:element ref="PlotScatter" />
        <xs:element ref="PlotHistogram" />
        <xs:element ref="PlotBoxAndWhisker" />
        <xs:element ref="PlotCurve" />
        <xs:element ref="PlotHeatMap" />
        <xs:element ref="PlotGuideLines" />
        <xs:element ref="PlotSvgContent" />
        <xs:element ref="PlotStatic" />
    </xs:choice>
</xs:group>
""")
    modelLoader.xsdAppend("""
<xs:group name="PLOT-CONTENT-ANNOTATION" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:choice>
        <xs:element ref="PlotLegend" />
        <xs:element ref="PlotSvgAnnotation" />
    </xs:choice>
</xs:group>
""")
    modelLoader.xsdAppend("""
<xs:group name="PLOT-LEGEND-CONTENT" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:choice>
        <xs:element ref="PlotLegendNumber" />
        <xs:element ref="PlotLegendSvg" />
    </xs:choice>
</xs:group>
""")
