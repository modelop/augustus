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

"""This module defines the MiningField class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.DataColumn import DataColumn
from augustus.core.FieldCastMethods import FieldCastMethods
from augustus.core.FakeFieldType import FakeFieldType

class MiningField(PmmlBinding):
    """MiningField implements PMML mining schemas, which cast fields,
    set extreme values, outlier treatment, and invalid/missing value
    treatment.

    Augustus does not require all field names to be present in a
    model's MiningSchema to be used in the model, though this is a
    requirement in PMML.  Its MiningSchemas therefore do not act as
    gatekeepers, limiting the scope of fields.  It therefore allows
    models to be invalid and interprets them in a reasonable way,
    and the interpretation of strictly valid models is unchanged.

    The custom PMML does not even require the existence of a
    MiningSchema block in most models.

    Modifications to a field (casting, outlier/invalid/missing
    treatment) are implemented as local field replacements, rather
    than definitions.

    U{PMML specification<http://www.dmg.org/v4-1/MiningSchema.html>}.
    """

    @property
    def name(self):
        return self["name"]

    def replaceField(self, dataTable, functionTable, performanceTable):
        """Replace a field in the DataTable for outlier removal,
        missing value handling, and invalid value treatment.

        @type dataTable: DataTable
        @param dataTable: The pre-built DataTable.
        @type functionTable: FunctionTable
        @param functionTable: A table of functions.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        """

        dataColumn = dataTable.fields.get(self.name)
        if dataColumn is None:
            return

        performanceTable.begin("MiningField")

        optype = self.get("optype", dataColumn.fieldType.optype)
        if optype != dataColumn.fieldType.optype:
            dataColumn = FieldCastMethods.cast(FakeFieldType(dataColumn.fieldType.dataType, optype), dataColumn)

        data = dataColumn.data
        mask = dataColumn.mask

        outliers = self.get("outliers")
        
        lowValue = self.get("lowValue")
        if lowValue is not None:
            lowValue = dataColumn.fieldType.stringToValue(lowValue)

            if outliers == "asMissingValues":
                selection = NP(dataColumn.data < lowValue)
                mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, selection)

            elif outliers == "asExtremeValues":
                selection = NP(dataColumn.data < lowValue)
                if data is dataColumn.data:
                    data = NP("copy", data)
                    data.setflags(write=True)
                    data[selection] = lowValue

        highValue = self.get("highValue")
        if highValue is not None:
            highValue = dataColumn.fieldType.stringToValue(highValue)

            if outliers == "asMissingValues":
                selection = NP(dataColumn.data > highValue)
                mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, selection)

            elif outliers == "asExtremeValues":
                selection = NP(dataColumn.data > highValue)
                if data is dataColumn.data:
                    data = NP("copy", data)
                    data.setflags(write=True)
                    data[selection] = highValue

        mask = FieldCastMethods.applyInvalidValueTreatment(mask, self.get("invalidValueTreatment"))
        data, mask = FieldCastMethods.applyMapMissingTo(dataColumn.fieldType, data, mask, self.get("missingValueReplacement"))

        dataTable.fields.replaceField(self.name, DataColumn(dataColumn.fieldType, data, mask))
        performanceTable.end("MiningField")
