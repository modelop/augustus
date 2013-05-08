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

"""This module defines the PlotSelection class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.PmmlPredicate import PmmlPredicate

class PlotSelection(PmmlBinding):
    """A PREDICATE or EXPRESSION that results in a boolean, for use in
    filtering data for plots.

    PMML subelements:

      - Any PREDICATE (PmmlPredicate) or EXPRESSION (PmmlExpression)
        that results in a boolean.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotSelection">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:group ref="PREDICATE" minOccurs="1" maxOccurs="1" />
                    <xs:group ref="EXPRESSION" minOccurs="1" maxOccurs="1" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    def select(self, dataTable, functionTable, performanceTable):
        """Evaluate the expression or predicate, given input data and
        a function table.

        @type dataTable: DataTable
        @param dataTable: Contains the data to plot.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @rtype: 1d Numpy array of bool
        @return: The result of the expression or predicate as a Numpy mask.
        """

        predicate = self.childOfClass(PmmlPredicate)
        if predicate is not None:
            return predicate.evaluate(dataTable, functionTable, performanceTable)

        expression = self.childOfClass(PmmlExpression)
        dataColumn = expression.evaluate(dataTable, functionTable, performanceTable)

        if not dataColumn.fieldType.isboolean():
            raise defs.PmmlValidationError("PlotSelection must evaluate to boolean, not %r" % dataColumn.fieldType)

        dataColumn._unlock()
        if dataColumn.mask is not None:
            NP("logical_and", dataColumn.data, NP(dataColumn.mask == defs.VALID), dataColumn.data)

        return dataColumn.data
