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

"""This module defines the NormContinuous class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlExpression import PmmlExpression
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType

class NormContinuous(PmmlExpression):
    """NormContinuous implements an expression that performs piecewise
    linear, everywhere continuous, transformations on a continuous
    field.

    U{PMML specification<http://www.dmg.org/v4-1/Transformations.html>}.
    """

    _fieldType = FakeFieldType("double", "continuous")

    def transformSelection(self, linearNorm1, linearNorm2, indata, outdata, selection):
        """Linearly transform a Subset of the dataset as part of an
        overall piecewise linear transformation.

        @type linearNorm1: PmmlBinding
        @param linearNorm1: The left-side <LinearNorm> object.
        @type linearNorm2: PmmlBinding
        @param linearNorm2: The right-side <LinearNorm> object.
        @type indata: 1d Numpy array
        @param indata: Unselected input data.
        @type outdata: 1d Numpy array
        @param outdata: Output data, modified by this function.
        @type selection: 1d Numpy array of bool
        @param selection: The Numpy selector for this piecewise region.
        """

        a1 = linearNorm1.orig
        b1 = linearNorm1.norm
        a2 = linearNorm2.orig
        b2 = linearNorm2.norm
        outdata[selection] = NP(b1 + NP(NP(NP(indata[selection] - a1)/NP(a2 - a1))*NP(b2 - b1)))

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

        performanceTable.begin("NormContinuous")
        
        dataColumn = dataTable.fields[self["field"]]
        if dataColumn.fieldType.dataType in ("object", "string", "boolean"):
            raise defs.PmmlValidationError("NormContinuous requires a numeric input field, but \"%s\" is" % dataColumn.fieldType.dataType)

        outliers = self.get("outliers")

        linearNorms = self.childrenOfTag("LinearNorm")
        for linearNorm in linearNorms:
            linearNorm.orig = float(linearNorm["orig"])
            linearNorm.norm = float(linearNorm["norm"])

        linearNorms.sort(lambda x, y: cmp(x.orig, y.orig))   # technically, it's invalid if not already sorted

        data = NP("empty", len(dataTable), self._fieldType.dtype)
        mask = dataColumn.mask

        # extrapolate before the first
        selection = NP(dataColumn.data <= linearNorms[0].orig)
        if outliers == "asMissingValues":
            mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, selection)
        elif outliers == "asExtremeValues":
            data[selection] = linearNorms[0].norm
        else:
            self.transformSelection(linearNorms[0], linearNorms[1], dataColumn.data, data, selection)

        for i in xrange(len(linearNorms) - 1):
            selection = NP(linearNorms[i].orig < dataColumn.data)
            NP("logical_and", selection, NP(dataColumn.data <= linearNorms[i+1].orig), selection)

            self.transformSelection(linearNorms[i], linearNorms[i+1], dataColumn.data, data, selection)

        selection = NP(linearNorms[-1].orig < dataColumn.data)
        if outliers == "asMissingValues":
            mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, selection)
        elif outliers == "asExtremeValues":
            data[selection] = linearNorms[-1].norm
        else:
            self.transformSelection(linearNorms[-2], linearNorms[-1], dataColumn.data, data, selection)

        data, mask = FieldCastMethods.applyMapMissingTo(self._fieldType, data, mask, self.get("mapMissingTo"))

        performanceTable.end("NormContinuous")
        return DataColumn(self._fieldType, data, mask)
