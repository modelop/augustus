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

"""This module defines the PmmlPredicate class."""

from augustus.core.PmmlBinding import PmmlBinding

class PmmlPredicate(PmmlBinding):
    """PmmlPredicate is an abstract base class for all predicates
       (such as SimplePredicate).
    """

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

        raise NotImplementedError("Subclasses of PmmlPredicate must implement evaluate(dataTable, functionTable, performanceTable, returnUnknowns=False)")
