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

"""This module defines the MapValues class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.TableInterface import TableInterface
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.DataColumn import DataColumn

class MapValues(PmmlExpression):
    """MapValues implements an expression that maps combinations of
    field values to an output value using a table.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """
    _optype = "continuous"

    @classmethod
    def setDefaultOptype(cls, optype):
        """Globally set the default C{optype} for MapValues operations.

        The default default is "categorical".

        @raise ValueError: If C{optype} is not "categorical", "ordinal", or "continuous", this function raises an error.
        """

        if optype in ("categorical", "ordinal", "continuous"):
            cls._optype = optype
        else:
            raise ValueError("optype must be one of \"categorical\", \"ordinal\", \"continuous\"")

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

        performanceTable.begin("MapValues")
        
        fieldType = FakeFieldType(self.get("dataType", "string"), self.get("optype", self._optype))
        fieldType._newValuesAllowed = True

        defaultValue = self.get("defaultValue")
        if defaultValue is not None:
            defaultValue = fieldType.stringToValue(defaultValue)

        data = NP("empty", len(dataTable), dtype=fieldType.dtype)
        if defaultValue is not None:
            data[:] = defaultValue

        outputColumn = self["outputColumn"]
        columnNameToField = {}
        for fieldColumnPair in self.childrenOfTag("FieldColumnPair"):
            dataColumn = dataTable.fields[fieldColumnPair["field"]]
            columnNameToField[fieldColumnPair["column"]] = dataColumn

        # cache partial selections because they'll be used over and over in intersecting sets
        dataSelections = {}
        missingSelections = {}
        coverage = NP("zeros", len(dataTable), dtype=NP.dtype(bool))

        for index, row in enumerate(self.childOfClass(TableInterface).iterate()):
            outputValue = row.get(outputColumn)
            if outputValue is None:
                raise defs.PmmlValidationError("MapValues has outputColumn \"%s\" but a column with that name does not appear in row %d of the table" % (outputColumn, index))
            del row[outputColumn]
            outputValue = fieldType.stringToValue(outputValue)

            # this is an intersection of all matching columns
            selection = NP("ones", len(dataTable), dtype=NP.dtype(bool))

            for columnName, columnValueString in row.items():
                dataColumn = columnNameToField.get(columnName)
                if dataColumn is not None:
                    columnValue = dataColumn.fieldType.stringToValue(columnValueString)

                    # one cached data array per column (name, value) pair
                    if (columnName, columnValueString) not in dataSelections:
                        selectData = NP(dataColumn.data == columnValue)
                        if dataColumn.mask is not None:
                            NP("logical_and", selectData, NP(dataColumn.mask == defs.VALID), selectData)
                        dataSelections[columnName, columnValueString] = selectData
                    NP("logical_and", selection, dataSelections[columnName, columnValueString], selection)

                    # one cached mask array per column name ("missing" has only one possible value, though I consider any non-VALID "missing")
                    if columnName not in missingSelections and dataColumn.mask is not None:
                        missingSelections[columnName] = NP(dataColumn.mask != defs.VALID)
                        
            # set the intersection to the output value
            data[selection] = outputValue
            NP("logical_or", coverage, selection, coverage)
        
        missing = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
        for missingSelection in missingSelections.values():
            NP("logical_or", missing, missingSelection, missing)
        coverage -= missing

        mask = missing * defs.MISSING

        data, mask = FieldCastMethods.applyMapMissingTo(fieldType, data, mask, self.get("mapMissingTo"))

        if defaultValue is None:
            NP("logical_not", coverage, coverage)
            if mask is None:
                mask = NP(coverage * defs.MISSING)
            else:
                mask[coverage] = defs.MISSING

        performanceTable.end("MapValues")
        return DataColumn(fieldType, data, mask)
