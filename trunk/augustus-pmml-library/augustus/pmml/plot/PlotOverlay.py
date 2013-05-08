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

"""This module defines the PlotOverlay class."""

from augustus.core.PmmlBinding import PmmlBinding

class PlotOverlay(PmmlBinding):
    """PlotOverlay specifies a coordinate system for data in
    a PlotWindow.  If it contains multiple PLOT-CONTENTs, they
    will be overlaid.

    PMML subelements:

      - PLOT-CONTENT (PmmlPlotContent) elements.

    PMML attributes:

      - xmin: optional explicit left edge of the coordinate system.
      - ymin: optional explicit bottom edge of the coordinate system.
      - zmin: optional explicit minimal z color of the coordinate system.
      - xmax: optional explicit right edge of the coordinate system.
      - ymax: optional explicit top edge of the coordinate system.
      - zmax: optional explicit maximal z color of the coordinate system.
      - xlog: if "true", plot x logarithmically instead of linearly.
      - ylog: if "true", plot y logarithmically instead of linearly.
      - zlog: if "true", plot z colors logarithmically instead of linearly.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotOverlay">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:group ref="PLOT-CONTENT" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="xmin" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="ymin" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="zmin" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="xmax" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="ymax" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="zmax" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="xlog" type="xs:boolean" use="optional" default="false" />
            <xs:attribute name="ylog" type="xs:boolean" use="optional" default="false" />
            <xs:attribute name="zlog" type="xs:boolean" use="optional" default="false" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
