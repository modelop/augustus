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

"""This module defines the Discretize class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.FakeFieldValue import FakeFieldValue

class Discretize(PmmlExpression):
    """Discretize implements the PMML transformation that creates a
    categorical field from a continuous one by partitioning it into
    subintervals.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    _optype = "categorical"

    @classmethod
    def setDefaultOptype(cls, optype):
        """Globally set the default C{optype} for Discretize operations.

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

        performanceTable.begin("Discretize")

        dataColumn = dataTable.fields[self["field"]]
        if dataColumn.fieldType.dataType in ("object", "string", "boolean"):
            raise defs.PmmlValidationError("Discretize requires a numeric input field, but \"%s\" is" % dataColumn.fieldType.dataType)

        fieldType = FakeFieldType(self.get("dataType", "string"), self.get("optype", self._optype))
        fieldType._newValuesAllowed = True

        defaultValue = self.get("defaultValue")
        if defaultValue is not None:
            defaultValue = fieldType.stringToValue(defaultValue)

        data = NP("empty", len(dataTable), dtype=fieldType.dtype)
        mask = NP("empty", len(dataTable), dtype=defs.maskType)
        if defaultValue is None:
            mask[:] = defs.MISSING
        else:
            data[:] = defaultValue
            mask[:] = defs.VALID

        for discretizeBin in self.childrenOfTag("DiscretizeBin"):
            try:
                binValue = fieldType.stringToValue(discretizeBin["binValue"])
            except ValueError:
                raise defs.PmmlValidationError("Cannot cast DiscretizeBin binValue \"%s\" as %s %s" % (discretizeBin["binValue"], fieldType.optype, fieldType.dataType))

            fieldType.values.append(FakeFieldValue(value=binValue))

            interval = discretizeBin.childOfTag("Interval")

            closure = interval["closure"]
            leftMargin = interval.get("leftMargin")
            rightMargin = interval.get("rightMargin")
            selection = None

            if leftMargin is not None:
                try:
                    leftMargin = dataColumn.fieldType.stringToValue(leftMargin)
                except ValueError:
                    raise defs.PmmlValidationError("Improper value in Interval leftMargin specification: \"%s\"" % leftMargin)

                if closure in ("openClosed", "openOpen"):
                    if selection is None:
                        selection = NP(leftMargin < dataColumn.data)
                    else:
                        NP("logical_and", selection, NP(leftMargin < dataColumn.data), selection)

                elif closure in ("closedOpen", "closedClosed"):
                    if selection is None:
                        selection = NP(leftMargin <= dataColumn.data)
                    else:
                        NP("logical_and", selection, NP(leftMargin <= dataColumn.data), selection)

            if rightMargin is not None:
                try:
                    rightMargin = dataColumn.fieldType.stringToValue(rightMargin)
                except ValueError:
                    raise defs.PmmlValidationError("Improper value in Interval rightMargin specification: \"%s\"" % rightMargin)

                if closure in ("openOpen", "closedOpen"):
                    if selection is None:
                        selection = NP(dataColumn.data < rightMargin)
                    else:
                        NP("logical_and", selection, NP(dataColumn.data < rightMargin), selection)

                elif closure in ("openClosed", "closedClosed"):
                    if selection is None:
                        selection = NP(dataColumn.data <= rightMargin)
                    else:
                        NP("logical_and", selection, NP(dataColumn.data <= rightMargin), selection)
                
            if selection is not None:
                NP("logical_and", selection, NP(dataColumn.mask == defs.VALID), selection)
                data[selection] = binValue
                mask[selection] = defs.VALID

        mask[NP(dataColumn.mask == defs.MISSING)] = defs.MISSING
        mask[NP(dataColumn.mask == defs.INVALID)] = defs.INVALID

        data, mask = FieldCastMethods.applyMapMissingTo(fieldType, data, mask, self.get("mapMissingTo"))
        
        performanceTable.end("Discretize")
        return DataColumn(fieldType, data, mask)
