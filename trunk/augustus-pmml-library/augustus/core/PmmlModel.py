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

"""This module defines the PmmlModel class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlCalculable import PmmlCalculable
from augustus.core.DataColumn import DataColumn
from augustus.pmml.MiningField import MiningField
from augustus.pmml.OutputField import OutputField
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakePerformanceTable import FakePerformanceTable

class PmmlModel(PmmlCalculable):
    """PmmlModel is the base class for any PmmlBinding that represents
    a model in PMML.
    """

    @property
    def name(self):
        return self.get("modelName")

    def calculateScore(self, subTable, functionTable, performanceTable):
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

        raise NotImplementedError("Subclasses of PmmlModel must implement calculateScore")

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

        if not self.get("isScorable", defaultFromXsd=True, convertType=True):
            dataTable.score = DataColumn(self.scoreType,
                                         NP(NP("ones", len(dataTable), dtype=self.scoreType.dtype) * defs.PADDING),
                                         NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.INVALID))
            return dataTable

        subTable = dataTable.subTable()

        for miningField in self.xpath("pmml:MiningSchema/pmml:MiningField"):
            miningField.replaceField(subTable, functionTable, performanceTable)

        for calculable in self.calculableTrans():
            calculable.calculate(subTable, functionTable, performanceTable)

        score = self.calculateScore(subTable, functionTable, performanceTable)
        dataTable.score = score[None]
        if self.name is not None:
            for key, value in score.items():
                if key is None:
                    dataTable.fields[self.name] = value
                else:
                    dataTable.fields["%s.%s" % (self.name, key)] = value

        for outputField in self.xpath("pmml:Output/pmml:OutputField"):
            displayName = outputField.get("displayName", outputField["name"])
            dataTable.output[displayName] = outputField.format(subTable, functionTable, performanceTable, score)

        for fieldName in subTable.output:
            dataTable.output[fieldName] = subTable.output[fieldName]

        return dataTable.score

    def calculableTrans(self):
        """Return a list of PmmlCalculable instances from this model's
        <LocalTransformations>.

        @rtype: list of PmmlCalculable
        @return: A list of transformations.
        """

        localTransformations = self.childOfTag("LocalTransformations")
        if localTransformations is not None:
            return localTransformations.childrenOfClass(PmmlCalculable)
        else:
            return []

    def fieldContext(self):
        """Analyze the structure of the surrounding PMML to determine
        which named fields would be defined at this point in the
        calculation.

        @rtype: dict
        @return: Dictionary from field names (strings) to DataFields and DerivedFields (PmmlBinding).
        """

        namedFields = super(PmmlModel, self).fieldContext()

        parent = self.calculableParent()
        if parent is not None:
            transformationDictionary = parent.childOfTag("TransformationDictionary")
            if transformationDictionary is not None:
                for calculable in transformationDictionary.childrenOfClass(PmmlCalculable):
                    if calculable.name is not None:
                        namedFields[calculable.name] = calculable

        return namedFields
