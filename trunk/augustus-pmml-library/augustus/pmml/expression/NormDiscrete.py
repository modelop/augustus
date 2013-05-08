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

"""This module defines the NormDiscrete class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType

class NormDiscrete(PmmlExpression):
    """NormDiscrete implements an expression that acts as an indicator
    function on categorical fields, return 1 when a field is equal to
    a given value, 0 otherwise.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    _fieldType = FakeFieldType("integer", "continuous")

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

        performanceTable.begin("NormDiscrete")

        dataColumn = dataTable.fields[self["field"]]
        value = dataColumn.fieldType.stringToValue(self["value"])
        data = NP("array", NP(dataColumn.data == value), dtype=self._fieldType.dtype)
        data, mask = FieldCastMethods.applyMapMissingTo(self._fieldType, data, dataColumn.mask, self.get("mapMissingTo"))

        performanceTable.end("NormDiscrete")
        return DataColumn(self._fieldType, data, mask)

    @staticmethod
    def fanOutByValue(modelLoader, fieldName, dataColumn, prefix=None):
        """Create a suite of NormDiscrete transformations, one
        indicator function for each unique value in a categorical
        dataset.

        @type modelLoader: ModelLoader
        @param modelLoader: The ModelLoader used to create the new PMML nodes.
        @type fieldName: FieldName
        @param fieldName: The name of the categorical field to fan out, used in the names of the new fields.
        @type dataColumn: DataColumn
        @param dataColumn: The categorical dataset.
        @type prefix: string or None
        @param prefix: The PMML prefix, used to create an lxml.etree.ElementMaker.
        """

        if prefix is None:
            E = modelLoader.elementMaker()
        else:
            E = modelLoader.elementMaker(prefix)

        if dataColumn.mask is None:
            values = NP("unique", dataColumn.data)
        else:
            values = NP("unique", dataColumn.data[NP(dataColumn.mask == defs.VALID)])

        derivedFields = []
        for value in values:
            stringValue = dataColumn.fieldType.valueToString(value)
            normDiscrete = E.NormDiscrete(field=fieldName, value=stringValue)
            derivedField = E.DerivedField(normDiscrete, name=("%s.%s" % (fieldName, stringValue)), dataType="integer", optype="continuous")
            derivedFields.append(derivedField)

        return derivedFields
