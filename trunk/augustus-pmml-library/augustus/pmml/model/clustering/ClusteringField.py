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

"""This module defines the ClusteringField class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding

class ClusteringField(PmmlBinding):
    """A ClusteringField is a field used in clustering.  It may be continuous or binary, but must be a number."""

    LOG2 = math.log(2.0)

    def addToAdjustM(self, dataTable, functionTable, performanceTable, sumNMqi, missingWeight):
        """Accumulate the C{adjustM} parameter, which adjusts for
        missing data.

        @type dataTable: DataTable
        @param dataTable: The input data.
        @type functionTable: FunctionTable
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type sumNMqi: 1d Numpy array of numbers
        @param sumNMqi: The running sum C{sumNMqi} (see the PMML specification).  This method modifies it.
        @type missingWeight: number
        @param missingWeight: The missing value weight for this field.
        """

        dataColumn = dataTable.fields[self["field"]]
        if dataColumn.mask is not None:
            NP("add", sumNMqi, NP(NP(dataColumn.mask == defs.VALID) * missingWeight), sumNMqi)

    def compare(self, dataTable, functionTable, performanceTable, centerString, defaultCompareFunction, anyInvalid):
        """Compare input data with a cluster centern along the
        direction of this field.

        Cluster distances are computed in two steps: this C{compare}
        function, which determines the distance in the direction of a
        field, and the metric, which combines results from each field.

        @type dataTable: DataTable
        @param dataTable: The input data.
        @type functionTable: FunctionTable
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @type centerString: string
        @param centerString: The center of the cluster in this field, represented as a string.
        @type defaultCompareFunction: string
        @param defaultCompareFunction: The C{compareFunction} defined at the model level, which may be overruled on a per-field basis.
        @type anyInvalid: 1d Numpy array of bool
        @param anyInvalid: Mask for invalid data, accumulated with each C{compare} call.  This method modifies it.
        @rtype: 1d Numpy array of numbers
        @return: The distances or similarities between the input data and the cluster center, along the distance of this field.
        """

        performanceTable.begin("ClusteringField")

        dataColumn = dataTable.fields[self["field"]]

        if dataColumn.mask is not None:
            # even though DataColumns are immutable, we're allowed to change the invalid values
            # because they're not defined; set them so that x - y = 0, and hence they'll be
            # effectively skipped in summations without any extra work
            dataColumn._unlock()
            dataColumn.data[NP(dataColumn.mask != defs.VALID)] = dataColumn.fieldType.stringToValue(centerString)
            dataColumn._lock()

        compareFunction = self.get("compareFunction", defaultCompareFunction)

        if compareFunction == "absDiff":
            result = NP("absolute", NP(dataColumn.data - dataColumn.fieldType.stringToValue(centerString)))

        elif compareFunction == "gaussSim":
            similarityScale = self.get("similarityScale")
            if similarityScale is None:
                raise defs.PmmlValidationError("If compareFunction is \"gaussSim\", a similarityScale must be provided")
            s = float(similarityScale)
            z = NP(dataColumn.data - dataColumn.fieldType.stringToValue(centerString))

            result = NP("exp", NP((-self.LOG2/s**2) * NP(z**2)))

        elif compareFunction == "delta":
            result = NP(dataColumn.data != dataColumn.fieldType.stringToValue(centerString))

        elif compareFunction == "equal":
            result = NP(dataColumn.data == dataColumn.fieldType.stringToValue(centerString))

        elif compareFunction == "table":
            if dataColumn.fieldType.dataType != "integer":
                raise defs.PmmlValidationError("If compareFunction is \"table\", the data must be integers")

            matrix = self.xpath("pmml:Comparisons/pmml:Matrix")
            if len(matrix) != 1:
                raise defs.PmmlValidationError("If compareFunction is \"table\", ClusteringFields needs a Comparisons/Matrix")
            values = matrix[0].values(convertType=False)

            centerValue = dataColumn.fieldType.stringToValue(centerString)
            try:
                row = values[centerValue]
            except IndexError:
                raise defs.PmmlValidationError("Cluster center component is %s, but this is an invalid row index for the Comparisons/Matrix (0-indexed)" % centerString)

            result = NP("empty", len(dataTable), dtype=NP.dtype(float))
            valid = NP("zeros", len(dataTable), dtype=NP.dtype(bool))
            for j, value in enumerate(row):
                selection = NP(dataColumn.data == j)
                result[selection] = dataColumn.fieldType.stringToValue(value)
                NP("logical_or", valid, selection, valid)
            NP("logical_or", anyInvalid, NP("logical_not", valid), anyInvalid)

        performanceTable.end("ClusteringField")
        return result
