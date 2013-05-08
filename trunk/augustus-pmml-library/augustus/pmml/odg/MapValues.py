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

"""This module defines a custom ODG version of the the MapValues class."""

from augustus.pmml.expression.MapValues import MapValues as strict_MapValues

class MapValues(strict_MapValues):
    """This customized MapValues class adds dataType and optype
    attributes to cast the quantities described in an XML
    (string-based) table."""

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="MapValues">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
                <xs:element maxOccurs="unbounded" ref="FieldColumnPair"/>
                    <xs:choice minOccurs="0">
                        <xs:element ref="TableLocator"/>
                        <xs:element ref="InlineTable"/>
                    </xs:choice>
            </xs:sequence>
            <xs:attribute name="mapMissingTo" type="xs:string"/>
            <xs:attribute name="defaultValue" type="xs:string"/>
            <xs:attribute name="outputColumn" type="xs:string" use="required"/>
            <xs:attribute name="dataType" type="DATATYPE"/>                                    <!-- added dataType and optype -->
            <xs:attribute name="optype" type="OPTYPE" use="optional" default="continuous" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
