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

"""This module defines the OutputField class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.FieldCastMethods import FieldCastMethods

class OutputField(PmmlBinding):
    """OutputField implements PMML output formats.

    U{PMML specification<http://www.dmg.org/v4-1/Output.html>}.
    """

    def format(self, subTable, functionTable, performanceTable, score):
        """Extract or post-process output for the output field of a DataTable.

        @type subTable: DataTable
        @param subTable: The DataTable associated with this local lexical scope.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type score: dict
        @param score: Dictionary mapping PMML score "feature" strings to DataColumns.  This dictionary always contains a None key, which is the basic feature ("predictedValue").
        @rtype: DataColumn
        @return: The output that would go into an output field of a DataTable.
        """

        performanceTable.begin("OutputField")

        feature = self.get("feature")
        if feature is None:
            dataColumn = subTable.fields[self["name"]]

        elif feature == "predictedValue":
            dataColumn = score[None]

        elif feature == "predictedDisplayValue":
            original = score[None]
            toString = original.fieldType.valueToString
            data = NP("empty", len(subTable), dtype=NP.dtype(object))
            for i, x in enumerate(original.data):
                data[i] = toString(x)
            dataColumn = DataColumn(FakeFieldType("string", "continuous"), data, None)

        elif feature == "transformedValue":
            expression = self.childOfClass(PmmlExpression)
            if expression is None:
                raise defs.PmmlValidationError("OutputField with feature \"transformedValue\" requires an EXPRESSION")
            
            performanceTable.pause("OutputField")
            dataColumn = expression.evaluate(subTable, functionTable, performanceTable)
            performanceTable.unpause("OutputField")

        elif feature == "decision":
            decisions = self.childOfTag("Decisions")
            if decisions is None:
                raise defs.PmmlValidationError("OutputField with feature \"decision\" requires a Decisions block")

            performanceTable.pause("OutputField")
            dataColumn = self.childOfClass(PmmlExpression).evaluate(subTable, functionTable, performanceTable)
            performanceTable.unpause("OutputField")

            if dataColumn.mask is None:
                valid = None
            else:
                valid = NP(dataColumn.mask == defs.VALID)

            fieldType = FakeFieldType("object", "any")
            data = NP("empty", len(subTable), dtype=fieldType.dtype)
            mask = NP(NP("ones", len(subTable), dtype=defs.maskType) * defs.MISSING)

            for decision in decisions.childrenOfTag("Decision"):
                value = dataColumn.fieldType.stringToValue(decision["value"])

                selection = NP(dataColumn.data == value)
                if valid is not None:
                    NP("logical_and", selection, valid, selection)

                for i in xrange(len(data)):
                    if selection[i]:
                        data[i] = decision

                mask[selection] = defs.VALID
            
            if not mask.any():
                mask = None

            dataColumn = DataColumn(fieldType, data, mask)

        elif feature in score:
            dataColumn = score[feature]

        else:
            model = self.getparent()
            if model is not None: model = model.getparent()

            if model is None:
                model = "(orphaned OutputField; no parent model)"
            else:
                model = model.t

            raise defs.PmmlValidationError("Models of type %s do not produce \"%s\" features (or at least, it is not yet implemented by Augustus)" % (model, feature))

        dataType = self.get("dataType", dataColumn.fieldType.dataType)
        optype = self.get("optype", dataColumn.fieldType.optype)
        if (dataType != dataColumn.fieldType.dataType or optype != dataColumn.fieldType.optype) and feature not in ("predictedDisplayValue", "decision"):
            dataColumn = FieldCastMethods.cast(FakeFieldType(dataType, optype), dataColumn)

        if feature is not None:
            subTable.fields[self.get("displayName", self["name"])] = dataColumn

        performanceTable.end("OutputField")
        return dataColumn
