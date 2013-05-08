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

"""This module defines the DerivedField class."""

from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FieldType import FieldType
from augustus.core.DataColumn import DataColumn
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakePerformanceTable import FakePerformanceTable

class DerivedField(PmmlCalculable):
    """DerivedField represents a named field that is calculated from
    other fields.

    It can be called like a function to evaluate all of the
    transformations and models contained within it.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    @property
    def name(self):
        return self.get("name")

    def calculate(self, dataTable, functionTable=None, performanceTable=None):
        """Calculate a DerivedField.

        This method modifies the input DataTable.

        If the data types between the DerivedField and its EXPRESSION
        are not matched, the DerivedField will need to cast the output.
        This is a potentially expensive and often unwanted operation.
        When a DerivedField casts, it reports the cast in the
        PerformanceTable with DerivedField name, to help the user
        debug their PMML.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataTable
        @return: A DataTable containing the result, usually a modified version of the input.
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        dataColumn = self.childOfClass(PmmlExpression).evaluate(dataTable, functionTable, performanceTable)
        performanceTable.begin("DerivedField")

        dataType = dataColumn.fieldType.dataType
        optype = dataColumn.fieldType.optype
        if self.get("dataType", dataType) == dataType and self.get("optype", optype) == optype and len(self.childrenOfTag("Value")) == 0:
            dataTable.fields[self.name] = dataColumn

        else:
            performanceTable.begin("cast (\"%s\")" % self.name)
            dataTable.fields[self.name] = FieldCastMethods.cast(FieldType(self), dataColumn)
            performanceTable.end("cast (\"%s\")" % self.name)

        performanceTable.end("DerivedField")

        return dataTable.fields[self.name]
