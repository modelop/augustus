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

"""This module defines the DefineFunction class."""

from augustus.core.defs import defs
from augustus.core.Function import Function
from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.FieldCastMethods import FieldCastMethods

class DefineFunction(PmmlCalculable, Function):
    """DefineFunction implements user-defined functions in PMML.

    The C{calculate} method is invoked when the function is declared
    and the C{evaluate} method is invoked when it is used in an Apply
    element.

    U{PMML specification<http://www.dmg.org/v4-1/Functions.html>}.
    """

    @property
    def name(self):
        return self.get("name")

    def calculate(self, dataTable, functionTable=None, performanceTable=None):
        """Define a new function.

        This method modifies the input FunctionTable.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataTable
        @return: A DataTable containing the result, usually a modified version of the input.
        """

        dataTable, functionTable, performanceTable = self._setupCalculate(dataTable, functionTable, performanceTable)

        if self.name in functionTable:
            raise defs.PmmlValidationError("DefineFunction \"%s\" overshadows previously defined function" % self.name)
        functionTable[self.name] = self

    def evaluate(self, dataTable, functionTable, performanceTable, arguments):
        """Evaluate the function, using a DataTable as input.

        @type dataTable: DataTable
        @param dataTable: The input DataTable, containing any fields that might be used to evaluate this expression.
        @type functionTable: FunctionTable
        @param functionTable: The FunctionTable, containing any functions that might be called in this expression.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataColumn
        @return: The result of the calculation as a DataColumn.
        """

        arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
        performanceTable.begin("user-defined \"%s\"" % self.name)

        parameters = self.childrenOfTag("ParameterField")

        if len(arguments) != len(parameters):
            raise defs.PmmlValidationError("Apply function=\"%s\" has %d arguments but the corresponding DefineFunction has %d parameters" % (self.name, len(arguments), len(parameters)))

        subTable = dataTable.subTable()

        for argument, parameter in zip(arguments, parameters):
            dataType = parameter.get("dataType", argument.fieldType.dataType)
            optype = parameter.get("optype", argument.fieldType.optype)
            if dataType != argument.fieldType.dataType or optype != argument.fieldType.optype:
                argument = FieldCastMethods.cast(FakeFieldType(dataType, optype), argument)

            subTable.fields[parameter["name"]] = argument

        performanceTable.pause("user-defined \"%s\"" % self.name)
        dataColumn = self.childOfClass(PmmlExpression).evaluate(subTable, functionTable, performanceTable)
        performanceTable.unpause("user-defined \"%s\"" % self.name)

        dataType = self.get("dataType", dataColumn.fieldType.dataType)
        optype = self.get("optype", dataColumn.fieldType.optype)
        if dataType != dataColumn.fieldType.dataType or optype != dataColumn.fieldType.optype:
            dataColumn = FieldCastMethods.cast(FakeFieldType(dataType, optype), dataColumn)

        performanceTable.end("user-defined \"%s\"" % self.name)
        return dataColumn
