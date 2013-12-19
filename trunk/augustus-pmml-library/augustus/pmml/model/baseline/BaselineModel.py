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

"""This module defines the BaselineModel class."""

import math

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlModel import PmmlModel
from augustus.core.DataTable import DataTable
from augustus.core.DataColumn import DataColumn
from augustus.core.FakeFieldType import FakeFieldType

class BaselineModel(PmmlModel):
    """BaselineModel implements the baseline model in PMML, which is a
    collection of change-detection routines.

    U{PMML specification<http://www.dmg.org/v4-1/BaselineModel.html>}.
    """

    scoreType = FakeFieldType("double", "continuous")

    def calculateScore(self, dataTable, functionTable, performanceTable):
        """Calculate the score of this model.

        This method is called by C{calculate} to separate operations
        that are performed by all models (in C{calculate}) from
        operations that are performed by specific models (in
        C{calculateScore}).

        @type subTable: DataTable
        @param subTable: The DataTable representing this model's lexical scope.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataColumn
        @return: A DataColumn containing the score.
        """

        testDistributions = self.childOfTag("TestDistributions")
        testStatistic = testDistributions.get("testStatistic")

        performanceTable.begin("BaselineModel %s" % testStatistic)

        fieldName = testDistributions.get("field")
        dataColumn = dataTable.fields[fieldName]

        if testStatistic == "zValue":
            score = self.zValue(testDistributions, fieldName, dataColumn, dataTable.state, performanceTable)

        elif testStatistic == "CUSUM":
            score = self.cusum(testDistributions, fieldName, dataColumn, dataTable.state, performanceTable)

        else:
            raise NotImplementedError("TODO: add more testStatistics")

        performanceTable.end("BaselineModel %s" % testStatistic)
        return score

    def zValue(self, testDistributions, fieldName, dataColumn, state, performanceTable):
        """Calculate the score of a zValue TestStatistic.

        @type testDistributions: PmmlBinding
        @param testDistributions: The <TestDistributions> element.
        @type fieldName: string
        @param fieldName: The field name (for error messages).
        @type dataColumn: DataColumn
        @param dataColumn: The field.
        @type state: DataTableState
        @param state: The persistent state object (not used).
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: dict
        @return: A dictionary mapping PMML "feature" strings to DataColumns; zValue only defines the None key ("predictedValue").
        """

        if dataColumn.fieldType.dataType in ("object", "string", "boolean", "date", "time", "dateTime"):
            raise TypeError("Field \"%s\" has dataType \"%s\", which is incompatible with BaselineModel.zValue" % (fieldName, dataColumn.fieldType.dataType))

        distributions = testDistributions.xpath("pmml:Baseline/*[@mean and @variance]")
        if len(distributions) == 0:
            raise defs.PmmlValidationError("BaselineModel zValue requires a distribution with a mean and a variance")

        distribution = distributions[0]
        mean = float(distribution.get("mean"))
        variance = float(distribution.get("variance"))
        if variance <= 0.0:
            raise defs.PmmlValidationError("Variance must be positive, not %g" % variance)

        return {None: DataColumn(self.scoreType, NP(NP(dataColumn.data - mean) / math.sqrt(variance)), dataColumn.mask)}

    def cusum(self, testDistributions, fieldName, dataColumn, state, performanceTable):
        """Calculate the score of a CUSUM TestStatistic.

        The CUSUM cumulative sum is a stateful calculation: each row
        depends on the result of the previous row.  To continue
        calculations through multiple calls to C{calc} or
        C{calculate}, pass a DataTableState object and give the
        BaselineModel a C{stateId} attribute.  The C{stateId} is not
        valid in strict PMML, but it can be inserted after validation
        or used in custom-ODG models (C{from augustus.odg import *}).

        @type testDistributions: PmmlBinding
        @param testDistributions: The <TestDistributions> element.
        @type fieldName: string
        @param fieldName: The field name (for error messages).
        @type dataColumn: DataColumn
        @param dataColumn: The field.
        @type state: DataTableState
        @param state: The persistent state object, which is used to initialize the start state and save the end state of the cumulative sum.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: dict
        @return: A dictionary mapping PMML "feature" strings to DataColumns; CUSUM only defines the None key ("predictedValue").
        """

        baseline = testDistributions.xpath("pmml:Baseline/pmml:GaussianDistribution | pmml:Baseline/pmml:PoissonDistribution")
        alternate = testDistributions.xpath("pmml:Alternate/pmml:GaussianDistribution | pmml:Alternate/pmml:PoissonDistribution")

        if len(baseline) == 0 or len(alternate) == 0:
            raise defs.PmmlValidationError("BaselineModel CUSUM requires a Baseline and an Alternate that are either GaussianDistribution or PoissonDistribution")

        ratios = alternate[0].logpdf(dataColumn.data) - baseline[0].logpdf(dataColumn.data)
        if dataColumn.mask is None:
            good = NP("ones", len(dataColumn), dtype=NP.dtype(bool))
        else:
            good = NP(dataColumn.mask == defs.VALID)

        stateId = self.get("stateId")
        last = None
        if stateId is not None:
            last = state.get(stateId)
        if last is None:
            last = 0.0

        resetValue = testDistributions.get("resetValue", defaultFromXsd=True, convertType=True)

        output = NP("empty", len(dataColumn), dtype=NP.dtype(float))

        performanceTable.begin("fill CUSUM")
        for index in xrange(len(dataColumn)):
            if good[index]:
                last = max(resetValue, last + ratios[index])
            output[index] = last
        performanceTable.end("fill CUSUM")

        if stateId is not None:
            state[stateId] = last

        return {None: DataColumn(self.scoreType, output, None)}
