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

"""This module defines the CompoundPredicate class."""

from augustus.core.NumpyInterface import NP
from augustus.core.PmmlPredicate import PmmlPredicate

class CompoundPredicate(PmmlPredicate):
    """CompoundPredicate implements predicates joined by "and", "or",
    "xor", or "surrogate".

    U{PMML specification<http://www.dmg.org/v4-1/TreeModel.html>}.
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

        predicates = [x.evaluate(dataTable, functionTable, performanceTable, returnUnknowns=True) for x in self.childrenOfClass(PmmlPredicate)]

        performanceTable.begin("CompoundPredicate")

        booleanOperator = self.get("booleanOperator")
        handleUnknowns = any(unknowns.any() for selection, unknowns, encounteredUnknowns in predicates)

        if booleanOperator == "or":
            selection, unknowns, encounteredUnknowns = predicates[0]

            if handleUnknowns:
                performanceTable.begin("or: handle unknowns")
                knownToBeTrue = NP("logical_and", selection, NP("logical_not", unknowns))

                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    performanceTable.pause("or: handle unknowns")
                    NP("logical_or", selection, newSelection, selection)
                    performanceTable.unpause("or: handle unknowns")

                    NP("logical_or", unknowns, newUnknowns, unknowns)
                    NP("logical_or", knownToBeTrue, NP("logical_and", newSelection, NP("logical_not", newUnknowns)), knownToBeTrue)

                NP("logical_or", selection, knownToBeTrue, selection)
                NP("logical_and", unknowns, NP("logical_not", knownToBeTrue), unknowns)

                performanceTable.end("or: handle unknowns")

            else:
                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    NP("logical_or", selection, newSelection, selection)

        elif booleanOperator == "and":
            selection, unknowns, encounteredUnknowns = predicates[0]

            if handleUnknowns:
                performanceTable.begin("and: handle unknowns")
                knownToBeFalse = NP("logical_and", NP("logical_not", selection), NP("logical_not", unknowns))

                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    performanceTable.pause("and: handle unknowns")
                    NP("logical_and", selection, newSelection, selection)
                    performanceTable.unpause("and: handle unknowns")

                    NP("logical_or", unknowns, newUnknowns, unknowns)
                    NP("logical_or", knownToBeFalse, NP("logical_not", NP("logical_or", newSelection, newUnknowns)), knownToBeFalse)

                NP("logical_and", selection, NP("logical_not", knownToBeFalse), selection)
                NP("logical_and", unknowns, NP("logical_not", knownToBeFalse), unknowns)

                performanceTable.end("and: handle unknowns")

            else:
                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    NP("logical_and", selection, newSelection, selection)

        elif booleanOperator == "xor":
            selection, unknowns, encounteredUnknowns = predicates[0]

            if handleUnknowns:
                performanceTable.begin("xor: handle unknowns")
                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    performanceTable.pause("xor: handle unknowns")
                    NP("logical_xor", selection, newSelection, selection)
                    performanceTable.unpause("xor: handle unknowns")

                    NP("logical_or", unknowns, newUnknowns, unknowns)

                performanceTable.end("xor: handle unknowns")

            else:
                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    NP("logical_xor", selection, newSelection, selection)

        elif booleanOperator == "surrogate":
            selection, unknowns, encounteredUnknowns = predicates[0]
            encounteredUnknowns = NP("copy", encounteredUnknowns)

            if handleUnknowns:
                for newSelection, newUnknowns, newEncounteredUnknowns in predicates[1:]:
                    selection[unknowns] = newSelection[unknowns]
                    NP("logical_and", unknowns, newUnknowns, unknowns)

        if returnUnknowns:
            performanceTable.end("CompoundPredicate")
            return selection, unknowns, encounteredUnknowns
        else:
            NP("logical_and", selection, NP("logical_not", unknowns), selection)
            performanceTable.end("CompoundPredicate")
            return selection
