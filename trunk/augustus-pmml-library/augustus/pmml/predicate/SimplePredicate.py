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

"""This module defines the SimplePredicate class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlPredicate import PmmlPredicate

class SimplePredicate(PmmlPredicate):
    """SimplePredicate implements predicates that compare a field
    value with a single-valued constant.

    U{PMML specification<http://www.dmg.org/v4-1/TreeModel.html>}.
    """

    def postValidate(self):
        """Verify that "value" is only used when the operator is not "isMissing" or "isNotMissing".

        @raise PmmlValidationError: If the PMML specifies an invalid combination, raise an error.
        """

        if self.get("operator") in ("isMissing", "isNotMissing"):
            if self.get("value") is not None:
                raise defs.PmmlValidationError("SimplePredicate with %s must not have a value" % self.get("operator"))
        else:
            if self.get("value") is None:
                raise defs.PmmlValidationError("SimplePredicate with %s must have a value" % self.get("operator"))

    def evaluate(self, dataTable, functionTable, performanceTable, returnUnknowns=False):
        """Evaluate the predicate, using a DataTable as input.

        @type dataTable: DataTable
        @param dataTable: The input DataTable, containing any fields that might be used to evaluate this predicate.
        @type functionTable: FunctionTable
        @param functionTable: The FunctionTable, containing any functions that might be called in this predicate.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type returnUnknowns: bool
        @param returnUnknowns: If True, return a "mask" for the selection that indicates which rows are unknown, rather than True or False.
        @rtype: 1d Numpy array of bool or 3-tuple of arrays
        @return: Either a simple selection array or selection, unknowns, encounteredUnknowns
        """

        performanceTable.begin("SimplePredicate")

        fieldName = self.get("field")

        dataColumn = dataTable.fields[fieldName]

        operator = self.get("operator")
        if operator == "isMissing":
            if dataColumn.mask is None:
                selection = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
            else:
                selection = NP(dataColumn.mask == defs.MISSING)

        elif operator == "isNotMissing":
            if dataColumn.mask is None:
                selection = NP("ones", len(dataTable), dtype=NP.dtype(bool))
            else:
                selection = NP(dataColumn.mask != defs.MISSING)

        else:
            try:
                value = dataColumn.fieldType.stringToValue(self.get("value"))
            except ValueError as err:
                raise defs.PmmlValidationError("SimplePredicate.value \"%s\" cannot be cast as %r: %s" % (self.get("value"), dataColumn.fieldType, str(err)))

            if operator == "equal":
                selection = NP(dataColumn.data == value)

            elif operator == "notEqual":
                selection = NP(dataColumn.data != value)

            elif operator == "lessThan":
                if dataColumn.fieldType.optype == "categorical":
                    raise TypeError("Categorical field \"%s\" cannot be compared using %s" % (fieldName, operator))
                selection = NP(dataColumn.data < value)

            elif operator == "lessOrEqual":
                if dataColumn.fieldType.optype == "categorical":
                    raise TypeError("Categorical field \"%s\" cannot be compared using %s" % (fieldName, operator))
                selection = NP(dataColumn.data <= value)

            elif operator == "greaterThan":
                if dataColumn.fieldType.optype == "categorical":
                    raise TypeError("Categorical field \"%s\" cannot be compared using %s" % (fieldName, operator))
                selection = NP(dataColumn.data > value)

            elif operator == "greaterOrEqual":
                if dataColumn.fieldType.optype == "categorical":
                    raise TypeError("Categorical field \"%s\" cannot be compared using %s" % (fieldName, operator))
                selection = NP(dataColumn.data >= value)

        if returnUnknowns:
            if dataColumn.mask is None:
                unknowns = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
            else:
                unknowns = NP(dataColumn.mask != defs.VALID)

            performanceTable.end("SimplePredicate")
            return selection, unknowns, unknowns

        else:
            if dataColumn.mask is not None:
                NP("logical_and", selection, NP(dataColumn.mask == defs.VALID), selection)

            performanceTable.end("SimplePredicate")
            return selection
