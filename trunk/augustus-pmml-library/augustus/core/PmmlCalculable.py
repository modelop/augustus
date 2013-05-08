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

"""This module defines the PmmlCalculable class."""

from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.FieldType import FieldType
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.DataTable import DataTable
from augustus.core.OrderedDict import OrderedDict
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakePerformanceTable import FakePerformanceTable

class PmmlCalculable(PmmlBinding):
    """PmmlCalculable is an abstract base class for all PmmlBinding
    elements that calculate quantities and possibly enter them into
    the DataTable.fields namespace (such as DerivedFields and
    models).
    """

    @property
    def name(self):
        raise NotImplementedError("Subclasses of PmmlCalculable must implement name")

    def calculate(self, dataTable, functionTable=None, performanceTable=None):
        """Perform a calculation directly, without constructing a
        DataTable first.

        This method is intended for performance-critical cases where
        the DataTable would be built without having to analyze the
        PMML for field type context.

        This method potentially modifies the input DataTable and FunctionTable.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataTable
        @return: A DataTable containing the result, usually a modified version of the input.
        """

        raise NotImplementedError("Subclasses of PmmlCalculable must implement calculate(dataTable, functionTable, performanceTable)")

    def calc(self, inputData, inputMask=None, inputState=None, functionTable=None, performanceTable=None):
        """Build a DataTable from the input data and then perform a
        calculation.

        This method is intended for interactive use, since it is more
        laborious to construct a DataTable by hand.

        This method modifies the input FunctionTable.

        @type inputData: dict
        @param inputData: Dictionary from field names to data, as required by the DataTable constructor.
        @type inputMask: dict or None
        @param inputMask: Dictionary from field names to missing value masks, as required by the DataTable constructor.
        @type inputState: DataTableState or None
        @param inputState: Calculation state, used to continue a calculation over many C{calc} calls.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataTable
        @return: A DataTable containing the result.        
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        performanceTable.begin("make DataTable")
        dataTable = DataTable(self, inputData, inputMask, inputState)
        performanceTable.end("make DataTable")

        self.calculate(dataTable, functionTable, performanceTable)
        return dataTable

    def calculableParent(self):
        """Return the first parent above this element that is also a
        PmmlCalculable.

        @rtype: PmmlCalculable
        @return: The enclosing PMML algorithm.
        """

        parent = self.getparent()
        while not isinstance(parent, PmmlCalculable):
            if parent is None:
                return None
            parent = parent.getparent()
        return parent

    def fieldContext(self):
        """Analyze the structure of the surrounding PMML to determine
        which named fields would be defined at this point in the
        calculation.

        @rtype: dict
        @return: Dictionary from field names (strings) to DataFields and DerivedFields (PmmlBinding).
        """

        namedFields = {}

        sibling = self.getprevious()
        while sibling is not None:
            if isinstance(sibling, PmmlCalculable):
                if sibling.name is not None:
                    namedFields[sibling.name] = sibling
            sibling = sibling.getprevious()

        ancestor = self.calculableParent()
        if ancestor is not None:
            namedFields.update(ancestor.fieldContext())

        return namedFields
