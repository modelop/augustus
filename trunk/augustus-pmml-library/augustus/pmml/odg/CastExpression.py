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

"""This module defines the CastExpression class."""

from augustus.core.defs import defs
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FieldType import FieldType
from augustus.core.FieldCastMethods import FieldCastMethods

class CastExpression(PmmlExpression):
    """CastExpression is a PMML extension that allows users to cast a
    value without naming it in a DerivedField, first."""

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="CastExpression">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element maxOccurs="unbounded" ref="Interval" minOccurs="0" />
                <xs:element maxOccurs="unbounded" ref="Value" minOccurs="0" />
                <xs:group ref="EXPRESSION" />
            </xs:sequence>
            <xs:attribute name="mapMissingTo" type="xs:string"/>
            <xs:attribute name="invalidValueTreatment" type="INVALID-VALUE-TREATMENT-METHOD" default="returnInvalid" />
            <xs:attribute use="required" type="OPTYPE" name="optype" />
            <xs:attribute use="required" type="DATATYPE" name="dataType" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    def evaluate(self, dataTable, functionTable, performanceTable):
        """Evaluate the expression, using a DataTable as input.

        @type dataTable: DataTable
        @param dataTable: The input DataTable, containing any fields that might be used to evaluate this expression.
        @type functionTable: FunctionTable
        @param functionTable: The FunctionTable, containing any functions that might be called in this expression.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataColumn
        @return: The result of the calculation as a DataColumn.
        """

        dataColumn = self.childOfClass(PmmlExpression).evaluate(dataTable, functionTable, performanceTable)
        performanceTable.begin("CastExpression")

        dataColumn = FieldCastMethods.cast(FieldType(self), dataColumn)
        mask = FieldCastMethods.applyInvalidValueTreatment(dataColumn.mask, self.get("invalidValueTreatment"))
        data, mask = FieldCastMethods.applyMapMissingTo(dataColumn.fieldType, dataColumn.data, mask, self.get("mapMissingTo"))

        performanceTable.end("CastExpression")
        return DataColumn(dataColumn.fieldType, data, mask)
