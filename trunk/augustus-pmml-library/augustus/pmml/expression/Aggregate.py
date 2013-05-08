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

"""This module defines the Aggregate class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType
from augustus.pmml.odg.Formula import Formula

class Aggregate(PmmlExpression):
    """Aggregate implements an expression that computes cumulative
    sums, averages, and other aggregate functions.

    In the custom interpretation of PMML, this element can take a
    stateId attribute to remember its last value so that aggregations
    can continue where they left off.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    def where(self, dataTable, functionTable, performanceTable):       
        """Approximate implementation of SQL where using the Formula class.

        It has a C{between} operator and various other SQL-like
        methods, but it is not syntactically identical to SQL.  See
        the Formula class for more.
        
        @type dataTable: DataTable
        @param dataTable: The input DataTable, containing any fields that might be used to evaluate this expression.
        @type functionTable: FunctionTable
        @param functionTable: The FunctionTable, containing any functions that might be called in this expression.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: 1d Numpy array of bool
        @return: The result as a Numpy selector.
        """

        formula = self.get("sqlWhere")
        if formula is None:
            return None

        performanceTable.begin("Aggregate sqlWhere")

        dataColumn = Formula().evaluate(dataTable, functionTable, performanceTable, formula)
        if dataColumn.fieldType.dataType != "boolean":
            raise defs.PmmlValidationError("Aggregate sqlWhere must evaluate to a boolean expression, not \"%s\"" % formula)
        dataColumn._unlock()

        if dataColumn.mask is not None:
            NP("logical_and", dataColumn.data, NP(dataColumn.mask == defs.VALID), dataColumn.data)

        performanceTable.end("Aggregate sqlWhere")
        return dataColumn.data

    def functionCount(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Counts rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn
        @return: A column of counted rows.
        """

        fieldType = FakeFieldType("integer", "continuous")

        ones = NP("ones", len(dataColumn), dtype=fieldType.dtype)
        if dataColumn.mask is not None:
            NP("logical_and", ones, NP(dataColumn.mask == defs.VALID), ones)

        if whereMask is not None:
            NP("logical_and", ones, whereMask, ones)

        if groupSelection is not None:
            NP("logical_and", ones, groupSelection, ones)

        if getstate is not None and len(dataColumn) > 0:
            startingState = getstate()
            if startingState is not None:
                ones[0] += startingState

        data = NP("cumsum", ones)

        if setstate is not None and len(dataColumn) > 0:
            setstate(data[-1])

        return DataColumn(fieldType, data, None)

    def functionSum(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Adds up rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn
        @return: A column of added rows.
        """

        fieldType = FakeFieldType("double", "continuous")

        if dataColumn.fieldType.dataType not in ("integer", "float", "double"):
            raise defs.PmmlValidationError("Aggregate function \"sum\" requires a numeric input field: \"integer\", \"float\", \"double\"")

        ones = NP("ones", len(dataColumn), dtype=fieldType.dtype)
        if dataColumn.mask is not None:
            NP("logical_and", ones, NP(dataColumn.mask == defs.VALID), ones)

        if whereMask is not None:
            NP("logical_and", ones, whereMask, ones)

        if groupSelection is not None:
            NP("logical_and", ones, groupSelection, ones)

        NP("multiply", ones, dataColumn.data, ones)

        if getstate is not None and len(dataColumn) > 0:
            startingState = getstate()
            if startingState is not None:
                ones[0] += startingState

        data = NP("cumsum", ones)

        if setstate is not None and len(dataColumn) > 0:
            setstate(data[-1])

        return DataColumn(fieldType, data, None)

    def functionAverage(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Averages rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn
        @return: A column of averaged rows.
        """

        fieldType = FakeFieldType("double", "continuous")

        if dataColumn.fieldType.dataType not in ("integer", "float", "double"):
            raise defs.PmmlValidationError("Aggregate function \"average\" requires a numeric input field: \"integer\", \"float\", \"double\"")

        denominator = NP("ones", len(dataColumn), dtype=fieldType.dtype)
        if dataColumn.mask is not None:
            NP("logical_and", denominator, NP(dataColumn.mask == defs.VALID), denominator)

        if whereMask is not None:
            NP("logical_and", denominator, whereMask, denominator)

        if groupSelection is not None:
            NP("logical_and", denominator, groupSelection, denominator)

        numerator = NP("multiply", denominator, dataColumn.data)

        if getstate is not None and len(dataColumn) > 0:
            startingState  = getstate()
            if startingState is not None:
                startingNumerator, startingDenominator = startingState
                numerator[0] += startingNumerator
                denominator[0] += startingDenominator

        numerator = NP("cumsum", numerator)
        denominator = NP("cumsum", denominator)

        data = NP(numerator / denominator)
        mask = NP(NP("logical_not", NP("isfinite", data)) * defs.INVALID)
        if not mask.any():
            mask = None

        if setstate is not None and len(dataColumn) > 0:
            setstate((numerator[-1], denominator[-1]))

        return DataColumn(fieldType, data, mask)

    def functionMin(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Finds the minimum of rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn
        @return: A column of minimized rows.
        """

        fieldType = dataColumn.fieldType

        if fieldType.optype not in ("continuous", "ordinal"):
            raise defs.PmmlValidationError("Aggregate function \"min\" requires a continuous or ordinal input field")

        if dataColumn.mask is None:
            selection = NP("ones", len(dataColumn), dtype=NP.dtype(bool))
        else:
            selection = NP(dataColumn.mask == defs.VALID)

        if whereMask is not None:
            NP("logical_and", selection, whereMask, selection)

        if groupSelection is not None:
            NP("logical_and", selection, groupSelection, selection)

        minimum = None
        if getstate is not None:
            startingState = getstate()
            if startingState is not None:
                minimum = startingState

        data = NP("empty", len(dataColumn), dtype=fieldType.dtype)
        mask = NP("zeros", len(dataColumn), dtype=defs.maskType)

        for i, x in enumerate(dataColumn.data):
            if selection[i]:
                if minimum is None or x < minimum:
                    minimum = x
            if minimum is None:
                mask[i] = defs.INVALID
            else:
                data[i] = minimum

        if not mask.any():
            mask = None

        if setstate is not None:
            setstate(minimum)

        return DataColumn(fieldType, data, mask)

    def functionMax(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Finds the maximum of rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn
        @return: A column of maximized rows.
        """

        fieldType = dataColumn.fieldType

        if fieldType.optype not in ("continuous", "ordinal"):
            raise defs.PmmlValidationError("Aggregate function \"min\" requires a continuous or ordinal input field")

        if dataColumn.mask is None:
            selection = NP("ones", len(dataColumn), dtype=NP.dtype(bool))
        else:
            selection = NP(dataColumn.mask == defs.VALID)

        if whereMask is not None:
            NP("logical_and", selection, whereMask, selection)

        if groupSelection is not None:
            NP("logical_and", selection, groupSelection, selection)

        maximum = None
        if getstate is not None:
            startingState = getstate()
            if startingState is not None:
                maximum = startingState

        data = NP("empty", len(dataColumn), dtype=fieldType.dtype)
        mask = NP("zeros", len(dataColumn), dtype=defs.maskType)

        for i, x in enumerate(dataColumn.data):
            if selection[i]:
                if maximum is None or x > maximum:
                    maximum = x
            if maximum is None:
                mask[i] = defs.INVALID
            else:
                data[i] = maximum

        if not mask.any():
            mask = None

        if setstate is not None:
            setstate(maximum)

        return DataColumn(fieldType, data, mask)

    def functionMultiset(self, dataColumn, whereMask, groupSelection, getstate, setstate):
        """Derives a multiset of rows in a DataColumn, possibly with an SQL where mask and groupField.

        @type dataColumn: DataColumn
        @param dataColumn: The input data column.
        @type whereMask: 1d Numpy array of bool, or None
        @param whereMask: The result of the SQL where selection.
        @type groupSelection: 1d Numpy array of bool, or None.
        @param groupSelection: Rows corresponding to a particular value of the groupField.
        @type getstate: callable function
        @param getstate: Retrieve staring values from the DataTableState.
        @type setstate: callable function
        @param setstate: Store ending values to the DataTableState.
        @rtype: DataColumn of dict objects
        @return: A column of multisetted rows.
        """

        fieldType = FakeFieldType("object", "any")

        selection = NP("ones", len(dataColumn), dtype=NP.dtype(bool))
        if dataColumn.mask is not None:
            selection = NP("logical_and", selection, NP(dataColumn.mask == defs.VALID))

        if whereMask is not None:
            NP("logical_and", selection, whereMask, selection)

        if groupSelection is not None:
            NP("logical_and", selection, groupSelection, selection)

        multiset = {}
        if getstate is not None:
            startingState = getstate()
            if startingState is not None:
                multiset = startingState
        current = dict(multiset)

        data = NP("empty", len(dataColumn), dtype=NP.dtype(object))

        toPython = dataColumn.fieldType.valueToPython
        for i, x in enumerate(dataColumn.data):
            if selection[i]:
                value = toPython(x)
                if value not in multiset:
                    multiset[value] = 0
                multiset[value] += 1
                current = dict(multiset)
            data[i] = current

        if setstate is not None:
            setstate(multiset)

        return DataColumn(fieldType, data, None)

    def functionCountFake(self, value, howmany, fieldType):
        """Counts rows in a DataColumn when it is known that there are no matches.

        @type value: number
        @param value: Initial and final value.
        @type howmany: int
        @param howmany: Number of rows.
        @type fieldType: FieldType
        @param fieldType: The type of field to emulate.
        @rtype: DataColumn
        @return: The faked results.
        """

        fieldType = FakeFieldType("integer", "continuous")
        data = NP("empty", howmany, dtype=fieldType.dtype)
        data[:] = value
        return DataColumn(fieldType, data, None)

    def functionSumFake(self, value, howmany, fieldType):
        """Adds up rows in a DataColumn when it is known that there are no matches.

        @type value: number
        @param value: Initial and final value.
        @type howmany: int
        @param howmany: Number of rows.
        @type fieldType: FieldType
        @param fieldType: The type of field to emulate.
        @rtype: DataColumn
        @return: The faked results.
        """

        fieldType = FakeFieldType("double", "continuous")
        data = NP("empty", howmany, dtype=fieldType.dtype)
        data[:] = value
        return DataColumn(fieldType, data, None)

    def functionAverageFake(self, value, howmany, fieldType):
        """Averages rows in a DataColumn when it is known that there are no matches.

        @type value: number
        @param value: Initial and final value.
        @type howmany: int
        @param howmany: Number of rows.
        @type fieldType: FieldType
        @param fieldType: The type of field to emulate.
        @rtype: DataColumn
        @return: The faked results.
        """

        fieldType = FakeFieldType("double", "continuous")
        numerator = NP("empty", howmany, dtype=fieldType.dtype)
        denominator = NP("empty", howmany, dtype=fieldType.dtype)
        numerator[:] = value[0]
        denominator[:] = value[1]
        data = NP(numerator / denominator)
        if value[1] == 0:
            mask = NP("empty", howmany, dtype=defs.maskType)
            mask[:] = defs.INVALID
        else:
            mask = None
        return DataColumn(fieldType, data, mask)

    def functionMinMaxFake(self, value, howmany, fieldType):
        """Minimizes or maximizes rows in a DataColumn when it is known that there are no matches.

        @type value: number
        @param value: Initial and final value.
        @type howmany: int
        @param howmany: Number of rows.
        @type fieldType: FieldType
        @param fieldType: The type of field to emulate.
        @rtype: DataColumn
        @return: The faked results.
        """

        data = NP("empty", howmany, dtype=fieldType.dtype)
        data[:] = value
        return DataColumn(fieldType, data, None)

    def functionMultisetFake(self, value, howmany, fieldType):
        """Derives a multiset of rows in a DataColumn when it is known that there are no matches.

        @type value: number
        @param value: Initial and final value.
        @type howmany: int
        @param howmany: Number of rows.
        @type fieldType: FieldType
        @param fieldType: The type of field to emulate.
        @rtype: DataColumn
        @return: The faked results.
        """

        fieldType = FakeFieldType("object", "any")
        data = NP("empty", howmany, dtype=fieldType.dtype)
        data[:] = value
        return DataColumn(fieldType, data, None)

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

        function = self["function"]
        groupField = self.get("groupField")

        if groupField is None:
            performanceTable.begin("Aggregate %s" % function)
        else:
            performanceTable.begin("Aggregate %s groupField" % function)

        dataColumn = dataTable.fields[self["field"]]
        whereMask = self.where(dataTable, functionTable, performanceTable)
        stateId = self.get("stateId")

        if groupField is None:
            if stateId is None:
                getstate = None
                setstate = None
            else:
                def getstate():
                    return dataTable.state.get(stateId)
                def setstate(value):
                    dataTable.state[stateId] = value
                
            if function == "count":
                dataColumn = self.functionCount(dataColumn, whereMask, None, getstate, setstate)

            elif function == "sum":
                dataColumn = self.functionSum(dataColumn, whereMask, None, getstate, setstate)

            elif function == "average":
                dataColumn = self.functionAverage(dataColumn, whereMask, None, getstate, setstate)

            elif function == "min":
                dataColumn = self.functionMin(dataColumn, whereMask, None, getstate, setstate)

            elif function == "max":
                dataColumn = self.functionMax(dataColumn, whereMask, None, getstate, setstate)

            elif function == "multiset":
                dataColumn = self.functionMultiset(dataColumn, whereMask, None, getstate, setstate)

            performanceTable.end("Aggregate %s" % function)
            return dataColumn

        else:
            groupColumn = dataTable.fields[groupField]
            if groupColumn.mask is None:
                validGroup = groupColumn.data
            else:
                validGroup = groupColumn.data[NP(groupColumn.mask == defs.VALID)]

            if stateId is not None:
                state = dataTable.state.get(stateId)
                if state is None:
                    record = {}
                else:
                    record = state

            valuesSeen = dict((stringValue, False) for stringValue in record)

            groupTables = {}
            groupColumnFieldType = None
            for groupValue in NP("unique", validGroup):
                groupSelection = NP(groupColumn.data == groupValue)
                if groupColumn.mask is not None:
                    NP("logical_and", groupSelection, NP(groupColumn.mask == defs.VALID), groupSelection)

                groupColumnFieldType = groupColumn.fieldType
                stringValue = groupColumnFieldType.valueToString(groupValue)

                if stringValue in record:
                    def getstate():
                        return record[stringValue]
                else:
                    getstate = None

                def setstate(value):
                    record[stringValue] = value

                valuesSeen[stringValue] = True
                value = groupColumnFieldType.valueToPython(groupValue)

                if function == "count":
                    groupTables[value] = self.functionCount(dataColumn, whereMask, groupSelection, getstate, setstate)

                elif function == "sum":
                    groupTables[value] = self.functionSum(dataColumn, whereMask, groupSelection, getstate, setstate)

                elif function == "average":
                    groupTables[value] = self.functionAverage(dataColumn, whereMask, groupSelection, getstate, setstate)

                elif function == "min":
                    groupTables[value] = self.functionMin(dataColumn, whereMask, groupSelection, getstate, setstate)

                elif function == "max":
                    groupTables[value] = self.functionMax(dataColumn, whereMask, groupSelection, getstate, setstate)

                elif function == "multiset":
                    groupTables[value] = self.functionMultiset(dataColumn, whereMask, groupSelection, getstate, setstate)

            if stateId is not None:
                dataTable.state[stateId] = record

            for stringValue in valuesSeen:
                if not valuesSeen[stringValue]:
                    value = groupColumnFieldType.valueToPython(groupColumnFieldType.stringToValue(stringValue))

                    if function == "count":
                        groupTables[value] = self.functionCountFake(record[stringValue], len(dataTable), dataColumn.fieldType)

                    elif function == "sum":
                        groupTables[value] = self.functionSumFake(record[stringValue], len(dataTable), dataColumn.fieldType)

                    elif function == "average":
                        groupTables[value] = self.functionAverageFake(record[stringValue], len(dataTable), dataColumn.fieldType)

                    elif function in ("min", "max"):
                        groupTables[value] = self.functionMinMaxFake(record[stringValue], len(dataTable), dataColumn.fieldType)

                    elif function == "multiset":
                        groupTables[value] = self.functionMultisetFake(record[stringValue], len(dataTable), dataColumn.fieldType)

            performanceTable.begin("Aggregate %s groupField collect" % function)

            fieldType = FakeFieldType("object", "any")
            data = NP("empty", len(dataTable), dtype=NP.dtype(object))

            if function == "count":
                for i in xrange(len(dataTable)):
                    data[i] = dict((value, table.data[i]) for value, table in groupTables.items() if table.data[i] != 0)

            elif function == "sum":
                for i in xrange(len(dataTable)):
                    data[i] = dict((value, table.data[i]) for value, table in groupTables.items() if table.data[i] != 0.0)

            elif function == "average":
                for i in xrange(len(dataTable)):
                    data[i] = dict((value, table.data[i]) for value, table in groupTables.items() if table.data[i] > 0.0 or table.data[i] <= 0.0)

            elif function in ("min", "max"):
                for table in groupTables.values():
                    if table.mask is None:
                        table._mask = NP("zeros", len(table), dtype=defs.maskType)
                for i in xrange(len(dataTable)):
                    data[i] = dict((value, table.data[i]) for value, table in groupTables.items() if table.mask[i] == defs.VALID)

            elif function == "multiset":
                for i in xrange(len(dataTable)):
                    data[i] = dict((value, table.data[i]) for value, table in groupTables.items() if len(table.data[i]) > 0)

            performanceTable.end("Aggregate %s groupField collect" % function)
            performanceTable.end("Aggregate %s groupField" % function)
            return DataColumn(fieldType, data, None)
