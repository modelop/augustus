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

"""This module defines the Constant class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataColumn import DataColumn

class Constant(PmmlExpression):
    """Constant implements an expression that returns a constant
    value.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    @property
    def fieldType(self):
        dataType = self.get("dataType")
        if dataType is None:
            return FakeFieldType("string", "continuous")
        else:
            return FakeFieldType(dataType, "continuous")
        
    def evaluateOne(self, convertType=True):
        """Evaluate the constant only once, not for every row of a
        DataColumn.

        @type convertType: bool
        @param convertType: If True, convert the type from a string into a Pythonic value.
        @rtype: string or object
        @return: Only one copy of the constant.
        """

        try:
            value = self.fieldType.stringToValue(self.text.strip())
        except ValueError as err:
            raise defs.PmmlValidationError("Constant \"%s\" cannot be cast as %r: %s" % (self.text.strip(), self.fieldType, str(err)))

        return value

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

        performanceTable.begin("Constant")

        data = NP("empty", len(dataTable), dtype=self.fieldType.dtype)
        data[:] = self.evaluateOne()
        dataColumn = self.fieldType.toDataColumn(data, None)

        performanceTable.end("Constant")
        return dataColumn
