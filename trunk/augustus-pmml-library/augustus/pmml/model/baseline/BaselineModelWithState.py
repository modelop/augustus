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

"""This module defines the BaselineModel class."""

from augustus.pmml.model.baseline.BaselineModel import BaselineModel

class BaselineModelWithState(BaselineModel):
    """This customized BaselineModel class adds a stateId attribute,
    so that users can track the CUSUM, chiSquare*, or scalarProduct
    states."""

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="BaselineModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" ref="Extension" minOccurs="0" />
                <xs:element ref="MiningSchema" />
                <xs:element ref="Output" minOccurs="0" />
                <xs:element ref="ModelStats" minOccurs="0" />
                <xs:element ref="ModelExplanation" minOccurs="0" />
                <xs:element ref="Targets" minOccurs="0" />
                <xs:element ref="LocalTransformations" minOccurs="0" />
                <xs:element ref="TestDistributions" />
                <xs:element ref="ModelVerification" minOccurs="0" />
                <xs:element maxOccurs="unbounded" ref="Extension" minOccurs="0" />
            </xs:sequence>
            <xs:attribute use="optional" type="xs:string" name="modelName" />
            <xs:attribute use="required" type="MINING-FUNCTION" name="functionName" />
            <xs:attribute use="optional" type="xs:string" name="algorithmName" />
            <xs:attribute default="true" use="optional" type="xs:boolean" name="isScorable" />
            <xs:attribute name="stateId" type="xs:string" use="optional" />   <!-- added stateId -->
        </xs:complexType>
    </xs:element>
</xs:schema>
"""
