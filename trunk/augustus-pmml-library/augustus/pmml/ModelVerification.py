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

"""This module defines the ModelVerification class."""

from augustus.core.defs import defs
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.TableInterface import TableInterface
from augustus.core.DataTable import DataTable
from augustus.core.FakePerformanceTable import FakePerformanceTable
from augustus.core.FunctionTable import FunctionTable
from augustus.core.FakeFieldType import FakeFieldType

class ModelVerification(PmmlBinding):
    """ModelVerification implements PMML model verification.

    It must be invoked manually with C{verify}: the output is a
    JSON-like list of expected/observed comparisons.

    U{PMML specification<http://www.dmg.org/v4-1/ModelVerification.html>}.
    """

    def verify(self, showSuccess=False, performanceTable=None):
        """Run the model verification tests defined by this element.

        The output is a list of results (all results or only failures,
        depending on C{showSuccess}), each of which is a dictionary of
        field names to values.  Fields are:

          - "success": was the comparison successful?
          - "expectedMissing", "observedMissing": is the
             expected/observed value missing?
          - "expectedValue", "observedValue": result as an internal
             value.
          - "expectedPythonValue", "observedPythonValue": result as a
             Python value.
          - "expectedDisplayValue", "observedDisplayValue": result as
             a string displayValue.

        Only "success", "expectedMissing", and "observedMissing" appear
        if the "is missing?" comparison was unsuccessful.

        @type showSuccess: bool
        @param showSuccess: If True, emit output even if the tests are successful.
        @type performanceTable: PerformanceTable
        @param performanceTable: A PerformanceTable for measuring the efficiency of the calculation.
        @rtype: JSON-like list of dicts
        @return: As described above.
        """

        verificationFields = {}
        for verificationField in self.xpath("pmml:VerificationFields/pmml:VerificationField"):
            verificationField.column = verificationField.get("column", verificationField["field"])
            verificationField.precision = verificationField.get("precision", defaultFromXsd=True, convertType=True)
            verificationField.zeroThreshold = verificationField.get("zeroThreshold", defaultFromXsd=True, convertType=True)

            verificationField.data = []
            verificationField.mask = []
            verificationFields[verificationField.column] = verificationField

        inputData = {}
        inputMask = {}
        for index, row in enumerate(self.childOfClass(TableInterface).iterate()):
            for columnName, columnValue in row.items():
                verificationField = verificationFields.get(columnName)

                if verificationField is not None:
                    while len(verificationField.data) < index:
                        verificationField.data.append(defs.PADDING)
                        verificationField.mask.append(True)
                    
                    verificationField.data.append(columnValue)
                    verificationField.mask.append(False)

                else:
                    inputDataField = inputData.get(columnName)
                    if inputDataField is None:
                        inputDataField = []
                        inputData[columnName] = inputDataField
                        inputMask[columnName] = []
                    inputMaskField = inputMask[columnName]

                    while len(inputDataField) < index:
                        inputDataField.append(defs.PADDING)
                        inputMaskField.append(True)

                    inputDataField.append(columnValue)
                    inputMaskField.append(False)

        for verificationField in verificationFields.values():
            while len(verificationField.data) < index:
                verificationField.data.append(defs.PADDING)
                verificationField.mask.append(True)

        for columnName in inputData:
            inputDataField = inputData[columnName]
            inputMaskField = inputMask[columnName]
            while len(inputDataField) < index:
                inputDataField.append(defs.PADDING)
                inputMaskField.append(True)

        for columnName, verificationField in verificationFields.items():
            inputData[columnName] = verificationField.data
            inputMask[columnName] = verificationField.mask

        model = self.getparent()

        if performanceTable is None:
            performanceTable = FakePerformanceTable()

        performanceTable.begin("make DataTable")
        dataTable = DataTable(model, inputData, inputMask, inputState=None)
        performanceTable.end("make DataTable")

        functionTable = FunctionTable()

        for miningField in model.xpath("pmml:MiningSchema/pmml:MiningField"):
            miningField.replaceField(dataTable, functionTable, performanceTable)

        for calculable in model.calculableTrans():
            calculable.calculate(dataTable, functionTable, performanceTable)

        score = model.calculateScore(dataTable, functionTable, performanceTable)
        dataTable.score = score[None]
        if model.name is not None:
            for key, value in score.items():
                if key is None:
                    dataTable.fields[model.name] = value
                else:
                    dataTable.fields["%s.%s" % (model.name, key)] = value

        for outputField in self.xpath("../pmml:Output/pmml:OutputField"):
            displayName = outputField.get("displayName", outputField["name"])
            outputField.format(dataTable, functionTable, performanceTable, score)

        output = []
        for verificationField in verificationFields.values():
            observedOutput = dataTable.fields.get(verificationField["field"])

            if observedOutput is None:
                raise defs.PmmlValidationError("VerificationField references field \"%s\" but it was not produced by the model")
            fieldType = observedOutput.fieldType

            if fieldType.dataType == "object":
                try:
                    newArray = [float(x) for x in observedOutput.data]
                except ValueError:
                    pass
                else:
                    fieldType = FakeFieldType("double", "continuous")
                    observedOutput._data = newArray
                        
            for index in xrange(len(dataTable)):
                record = {"field": verificationField["field"], "index": index}

                record["expectedMissing"] = verificationField.mask[index]
                record["observedMissing"] = (observedOutput.mask is not None and observedOutput.mask[index] != defs.VALID)

                if record["expectedMissing"] != record["observedMissing"]:
                    record["success"] = False
                    output.append(record)

                elif not record["expectedMissing"]:
                    record["expectedValue"] = fieldType.stringToValue(verificationField.data[index])
                    record["observedValue"] = observedOutput.data[index]
                    record["expectedPythonValue"] = fieldType.valueToPython(record["expectedValue"])
                    record["observedPythonValue"] = fieldType.valueToPython(record["observedValue"])
                    record["expectedDisplayValue"] = fieldType.valueToString(record["expectedValue"])
                    record["observedDisplayValue"] = fieldType.valueToString(record["observedValue"])

                    if fieldType.optype == "continuous":
                        if (abs(record["expectedValue"]) <= verificationField.zeroThreshold) and (abs(record["observedValue"]) <= verificationField.zeroThreshold):
                            record["success"] = True
                        else:
                            record["success"] = ((record["expectedValue"] * (1.0 - verificationField.precision)) <= record["observedValue"] <= (record["expectedValue"] * (1.0 + verificationField.precision)))

                        if not record["success"] or showSuccess:
                            output.append(record)
                            
                    else:
                        if record["expectedValue"] != record["observedValue"]:
                            record["success"] = False
                            output.append(record)
                        else:
                            record["success"] = True
                            if showSuccess:
                                output.append(record)

        return output
