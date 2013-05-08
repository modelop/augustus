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

"""This module defines the PMML class."""

from augustus.core.defs import defs
from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.DataColumn import DataColumn
from augustus.core.OrderedDict import OrderedDict
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakePerformanceTable import FakePerformanceTable

class PMML(PmmlCalculable):
    """PMML represents the top-level element of a PMML document.

    U{PMML specification<http://www.dmg.org/v4-1/GeneralStructure.html>}.
    """

    version = "4.1"

    @property
    def name(self):
        return None

    def postValidate(self):
        """After XSD validation, check the version of the document.

        The custom ODG version of this class checks for "4.1-odg", to
        avoid confusion among serialized models.
        """

        if self.get("version") != self.version:
            raise defs.PmmlValidationError("PMML version is \"%s\" when \"%s\" is expected for this class" % (self.get("version"), self.version))

    def calculate(self, dataTable, functionTable=None, performanceTable=None):
        """Perform a calculation directly, without constructing a
        DataTable first.

        This method is intended for performance-critical cases where
        the DataTable would be built without having to analyze the
        PMML for field type context.

        This method modifies the input DataTable and FunctionTable.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable or None
        @param functionTable: A table of functions.  Initially, it contains only the built-in functions, but any user functions defined in PMML would be added to it.
        @type performanceTable: PerformanceTable or None
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: DataTable
        @return: A DataTable containing the result, usually a modified version of the input.
        """

        if functionTable is None:
            functionTable = FunctionTable()
        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        for calculable in self.calculableTrans():
            calculable.calculate(dataTable, functionTable, performanceTable)

        calculableModels = self.calculableModels()
        if len(calculableModels) == 0:
            # if there was a model among the transformations, it erroneously set the score; unset it
            dataTable.score = None

        elif len(calculableModels) == 1:
            # this implicitly sets the score
            calculableModels[0].calculate(dataTable, functionTable, performanceTable)

        else:
            # this explicitly sets the score
            score = []
            for calculableModel in calculableModels:
                subTable = dataTable.subTable()
                calculableModel.calculate(subTable, functionTable, performanceTable)
                score.append(subTable.score)
            dataTable.score = tuple(score)

    def calculableTrans(self):
        """Return a list of PmmlCalculable instances from the
        <TransformationDictionary>.

        @rtype: list of PmmlCalculable
        @return: A list of transformations.
        """

        transformationDictionary = self.childOfTag("TransformationDictionary")
        if transformationDictionary is None:
            return []
        else:
            return transformationDictionary.childrenOfClass(PmmlCalculable)

    def calculableModels(self):
        """Return a list of top-level models.

        Usually only the first model is considered valid in a PMML document.

        @rtype: list of PmmlCalculable
        @return: A list of calculables.
        """

        return self.childrenOfClass(PmmlCalculable)

    def fieldContext(self):
        """Analyze the structure of the surrounding PMML to determine
        which named fields would be defined at this point in the
        calculation.

        @rtype: dict
        @return: Dictionary from field names (strings) to DataFields and DerivedFields (PmmlBinding).
        """

        dataFields = OrderedDict()
        for dataField in self.xpath("pmml:DataDictionary/pmml:DataField"):
            dataFields[dataField.get("name")] = dataField
        return dataFields
