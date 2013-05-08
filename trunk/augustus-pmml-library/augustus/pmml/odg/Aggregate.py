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

"""This module defines a custom ODG version of the the Aggregate class."""

from augustus.pmml.expression.Aggregate import Aggregate as strict_Aggregate

class Aggregate(strict_Aggregate):
    """This customized Aggregate class adds a stateId attribute, so
    that users can track the quantity's state."""

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="Aggregate">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="function" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="count" />
                        <xs:enumeration value="sum" />
                        <xs:enumeration value="average" />
                        <xs:enumeration value="min"  />
                        <xs:enumeration value="max"  />
                        <xs:enumeration value="multiset" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="groupField" type="FIELD-NAME" />
            <xs:attribute name="sqlWhere" type="xs:string" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />   <!-- added stateId -->
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
