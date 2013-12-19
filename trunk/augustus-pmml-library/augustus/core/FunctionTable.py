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

"""This module defines the FunctionTable class."""

import datetime

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.DataColumn import DataColumn
from augustus.core.FieldType import FieldType
from augustus.core.Function import Function, FLOAT, INTEGER, STRING, BOOL, OBJECT, DATE, TIME, DATETIME
from augustus.pmml.expression.Constant import Constant

class FunctionTable(dict):
    """FunctionTable is a namespace of built-in and user-defined
    functions, to be used in Apply elements.

    The U{PMML built-in functions<http://www.dmg.org/v4-1/BuiltinFunctions.html>}
    are implemented as nested classes that are added to a FunctionTable
    instance when it is created.

    Additional functions may be added to a FunctionTable instance or defined
    more generally in a FunctionTable subclass.  See FunctionTableExtra.
    """

    def __init__(self):
        super(FunctionTable, self).__init__()
        self._defineBuiltins()

    def _defineBuiltins(self):
        for fcnClass in self.Addition, self.Subtraction, self.Multiplication, self.TrueDivision, self.Minimum, self.Maximum, self.Sum, self.Average, self.Median, self.Product, self.LogBase10, self.LogBaseE, self.SquareRoot, self.Absolute, self.Exponential, self.Power, self.Threshold, self.Floor, self.Ceiling, self.Round, self.IsMissing, self.IsNotMissing, self.Equal, self.NotEqual, self.LessThan, self.LessOrEqual, self.GreaterThan, self.GreaterOrEqual, self.LogicalAnd, self.LogicalOr, self.LogicalNot, self.Contains, self.NotContains, self.IfThenElse, self.Uppercase, self.Lowercase, self.Substring, self.TrimBlanks, self.FormatNumber, self.FormatDatetime, self.DateDaysSinceYear, self.DateSecondsSinceYear, self.DateSecondsSinceMidnight, self.Negative:
            self[fcnClass.name] = fcnClass()

    class Addition(Function):
        name = "+"
        signatures = {(FLOAT,   FLOAT):   FLOAT,
                      (FLOAT,   INTEGER): FLOAT,
                      (INTEGER, FLOAT):   FLOAT,
                      (INTEGER, INTEGER): INTEGER,
                      }
        
        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments
            dataColumn = DataColumn(fieldType, left.data + right.data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Subtraction(Function):
        name = "-"
        signatures = {(FLOAT,   FLOAT):   FLOAT,
                      (FLOAT,   INTEGER): FLOAT,
                      (INTEGER, FLOAT):   FLOAT,
                      (INTEGER, INTEGER): INTEGER,
                      }
        
        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments
            dataColumn = DataColumn(fieldType, left.data - right.data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Multiplication(Function):
        name = "*"
        signatures = {(FLOAT,   FLOAT):   FLOAT,
                      (FLOAT,   INTEGER): FLOAT,
                      (INTEGER, FLOAT):   FLOAT,
                      (INTEGER, INTEGER): INTEGER,
                      }
        
        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)
            
            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments
            dataColumn = DataColumn(fieldType, left.data * right.data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class TrueDivision(Function):
        name = "/"
        signatures = {(FLOAT,   FLOAT):   FLOAT,
                      (FLOAT,   INTEGER): FLOAT,
                      (INTEGER, FLOAT):   FLOAT,
                      (INTEGER, INTEGER): FLOAT,   # N.B.  Use FloorDivision (in FunctionTableExtra) for int / int = int
                      }
        
        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.data.dtype == NP.dtype(int) and right.data.dtype == NP.dtype(int):
                data = NP(NP("array", left.data, dtype=NP.dtype(float)) / NP("array", right.data, dtype=NP.dtype(float)))
            else:
                data = NP(left.data / right.data)
            mask = self.maskInvalid(data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Minimum(Function):
        name = "min"

        def applyWithoutMask(self, data, mask, argument):
            NP("minimum", data, argument.data, data)
            return data, mask

        def applyWithMask(self, data, mask, argument, mask2):
            data[mask2] = NP("minimum", data[mask2], argument.data[mask2])
            return data, mask

        def simpleCopy(self, data, mask, argument, mask2):
            data[mask2] = argument.data[mask2]
            return data, mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.broadestNumberType(arguments, atleast=1)

            data = NP("copy", arguments[0].data)
            if arguments[0].mask is None:
                mask = None
            else:
                mask = NP(arguments[0].mask == defs.VALID)

            data, mask = self.applySkipMissing(data, mask, arguments[1:])

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Maximum(Function):
        name = "max"

        def applyWithoutMask(self, data, mask, argument):
            NP("maximum", data, argument.data, data)
            return data, mask

        def applyWithMask(self, data, mask, argument, mask2):
            data[mask2] = NP("maximum", data[mask2], argument.data[mask2])
            return data, mask

        def simpleCopy(self, data, mask, argument, mask2):
            data[mask2] = argument.data[mask2]
            return data, mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.broadestNumberType(arguments, atleast=1)

            data = NP("copy", arguments[0].data)
            if arguments[0].mask is None:
                mask = None
            else:
                mask = NP(arguments[0].mask == defs.VALID)

            data, mask = self.applySkipMissing(data, mask, arguments[1:])

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Sum(Function):
        name = "sum"

        def applyWithoutMask(self, data, mask, argument):
            NP("add", data, argument.data, data)
            return data, mask

        def applyWithMask(self, data, mask, argument, mask2):
            data[mask2] = NP("add", data[mask2], argument.data[mask2])
            return data, mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.broadestNumberType(arguments, only=[FLOAT, INTEGER])

            data = NP("zeros", len(dataTable), dtype=fieldType.dtype)
            mask = None

            data, mask = self.applySkipMissing(data, mask, arguments)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Average(Function):
        name = "avg"

        def applyWithoutMask(self, data, mask, argument):
            data, denom = data
            NP("add", data, argument.data, data)
            NP("add", denom, NP("ones", len(argument.data), dtype=NP.dtype(float)), denom)
            return (data, denom), mask

        def applyWithMask(self, data, mask, argument, mask2):
            data, denom = data
            data[mask2] = NP("add", data[mask2], argument.data[mask2])
            denom[mask2] = NP("add", denom[mask2], NP("ones", len(NP("nonzero", mask2)[0]), dtype=NP.dtype(float)))
            return (data, denom), mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.broadestNumberType(arguments, only=[FLOAT, INTEGER])

            data = NP("zeros", len(dataTable), dtype=fieldType.dtype)
            denom = NP("zeros", len(dataTable), dtype=NP.dtype(float))
            mask = None

            (data, denom), mask = self.applySkipMissing((data, denom), mask, arguments)

            zeroDenominators = NP(denom == 0.0)
            if mask is None:
                mask = zeroDenominators * defs.INVALID
            else:
                NP("logical_and", zeroDenominators, NP(mask == defs.VALID), zeroDenominators)
                mask[zeroDenominators] = defs.INVALID

            if mask is not None and not mask.any():
                mask = None

            dataColumn = DataColumn(fieldType, data / denom, mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Median(Function):
        name = "median"

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            self.broadestNumberType(arguments, only=[FLOAT, INTEGER])
            fieldType = self._typeReverseMap[FLOAT]

            allArguments = NP("vstack", [NP("array", x.data, dtype=NP.dtype(float)) for x in arguments])

            for index, argument in enumerate(arguments):
                if argument.mask is not None:
                    allArguments[index][NP(argument.mask != defs.VALID)] = defs.NAN

            allArguments.sort(axis=0)
            middleOfIndex = (NP("sum", NP("isfinite", allArguments), axis=0) + 1) / 2.0
            allArguments = NP("vstack", (NP(NP("empty", len(dataTable)) * defs.NAN), allArguments))

            lowIndex = NP("array", NP("floor", middleOfIndex), dtype=NP.dtype(int))
            highIndex = NP("array", NP("ceil", middleOfIndex), dtype=NP.dtype(int))

            lowValues = NP("choose", lowIndex, allArguments)
            highValues = NP("choose", highIndex, allArguments)

            lowBad = NP("isnan", lowValues)
            highBad = NP("isnan", highValues)
            bothGood = NP(NP(lowBad + highBad) == 0)
            onlyHigh = NP(highBad - lowBad)

            lowValues[bothGood] = NP(NP(lowValues[bothGood] + highValues[bothGood]) / 2.0)
            lowValues[onlyHigh] = highValues[onlyHigh]

            dataColumn = DataColumn(fieldType, lowValues, NP(NP("logical_and", lowBad, highBad) * defs.INVALID))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Product(Function):
        name = "product"

        def applyWithoutMask(self, data, mask, argument):
            NP("multiply", data, argument.data, data)
            return data, mask

        def applyWithMask(self, data, mask, argument, mask2):
            data[mask2] = NP("multiply", data[mask2], argument.data[mask2])
            return data, mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.broadestNumberType(arguments, only=[FLOAT, INTEGER])

            data = NP("ones", len(dataTable), dtype=fieldType.dtype)
            mask = None

            data, mask = self.applySkipMissing(data, mask, arguments)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class LogBase10(Function):
        name = "log10"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("log10", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class LogBaseE(Function):
        name = "ln"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("log", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class SquareRoot(Function):
        name = "sqrt"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("sqrt", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Absolute(Function):
        name = "abs"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): INTEGER,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("absolute", arguments[0].data)
            mask = arguments[0].mask

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Exponential(Function):
        name = "exp"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("exp", arguments[0].data)
            mask = arguments[0].mask

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Power(Function):
        name = "pow"
        signatures = {(FLOAT,   FLOAT):   FLOAT,
                      (FLOAT,   INTEGER): FLOAT,
                      (INTEGER, FLOAT):   FLOAT,
                      (INTEGER, INTEGER): INTEGER,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            data = NP("power", left.data, right.data)
            mask = self.maskInvalid(data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Threshold(Function):
        name = "threshold"
        signatures = {(FLOAT,    FLOAT):    INTEGER,
                      (FLOAT,    INTEGER):  INTEGER,
                      (INTEGER,  FLOAT):    INTEGER,
                      (INTEGER,  INTEGER):  INTEGER,
                      (STRING,   STRING):   INTEGER,
                      (OBJECT,   OBJECT):   INTEGER,
                      (DATE,     DATE):     INTEGER,
                      (DATETIME, DATETIME): INTEGER,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (1 if ld[i] > rd[i] else 0 for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (1 if l2s(ld[i]) > r2s(rd[i]) else 0 for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("array", NP("greater", left.data, right.data), dtype=fieldType.dtype)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Floor(Function):
        name = "floor"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)
            
            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("floor", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Ceiling(Function):
        name = "ceil"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("ceil", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Round(Function):
        name = "round"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("rint", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class IsMissing(Function):
        name = "isMissing"
        signatures = {(FLOAT,): BOOL,
                      (INTEGER,): BOOL,
                      (STRING,): BOOL,
                      (BOOL,): BOOL,
                      (OBJECT,): BOOL,
                      (DATE,): BOOL,
                      (TIME,): BOOL,
                      (DATETIME,): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            if arguments[0].mask is None:
                return DataColumn(fieldType, NP("zeros", len(dataTable), dtype=fieldType.dtype), None)
            else:
                data = NP(arguments[0].mask == defs.MISSING)
                mask = NP("copy", arguments[0].mask)

                mask[data] = defs.VALID
                if not mask.any():
                    mask = None
                
            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class IsNotMissing(Function):
        name = "isNotMissing"
        signatures = {(FLOAT,): BOOL,
                      (INTEGER,): BOOL,
                      (STRING,): BOOL,
                      (BOOL,): BOOL,
                      (OBJECT,): BOOL,
                      (DATE,): BOOL,
                      (TIME,): BOOL,
                      (DATETIME,): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            if arguments[0].mask is None:
                return DataColumn(fieldType, NP("zeros", len(dataTable), dtype=fieldType.dtype), None)
            else:
                data = NP(arguments[0].mask == defs.MISSING)
                mask = NP("copy", arguments[0].mask)

                mask[data] = defs.VALID
                if not mask.any():
                    mask = None
                
            dataColumn = DataColumn(fieldType, NP("logical_not", data), mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Equal(Function):
        name = "equal"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (BOOL,     BOOL):     BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (TIME,     TIME):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] == rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) == r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("equal", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class NotEqual(Function):
        name = "notEqual"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (BOOL,     BOOL):     BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (TIME,     TIME):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] != rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) != r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("not_equal", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class LessThan(Function):
        name = "lessThan"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] < rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) < r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("less", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class LessOrEqual(Function):
        name = "lessOrEqual"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] <= rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) <= r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("less_equal", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class GreaterThan(Function):
        name = "greaterThan"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] > rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) > r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("greater", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class GreaterOrEqual(Function):
        name = "greaterOrEqual"
        signatures = {(FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER):  BOOL,
                      (STRING,   STRING):   BOOL,
                      (OBJECT,   OBJECT):   BOOL,
                      (DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            left, right = arguments

            if left.fieldType.dataType == "object" or (left.fieldType.dataType == "string" and left.fieldType.optype == "continuous" and right.fieldType.optype == "continuous"):
                ld = left.data
                rd = right.data
                data = NP("fromiter", (ld[i] >= rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif left.fieldType.dataType == "string":
                ld = left.data
                rd = right.data
                l2s = left.fieldType.valueToString
                r2s = right.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) >= r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("greater_equal", left.data, right.data)

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class LogicalAnd(Function):
        name = "and"

        def applyWithoutMask(self, data, mask, argument):
            data, allbad = data
            NP("logical_and", data, argument.data, data)
            if argument.mask is not None:
                NP("logical_and", allbad, NP(argument.mask != defs.VALID), allbad)
            return (data, allbad), mask

        def applyWithMask(self, data, mask, argument, mask2):
            data, allbad = data
            data[mask2] = NP("logical_and", data[mask2], argument.data[mask2])
            if argument.mask is not None:
                allbad[mask2] = NP("logical_and", NP(allbad[mask2] != defs.VALID), argument.mask[mask2])
            return (data, allbad), mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.allBooleanType(arguments, atleast=2)

            data = NP("ones", len(dataTable), dtype=fieldType.dtype)
            mask = None
            allbad = NP("ones", len(dataTable), dtype=NP.dtype(bool))

            (data, allbad), mask = self.applySkipMissing((data, allbad), mask, arguments)

            if allbad.any():
                if mask is None:
                    mask = allbad * defs.MISSING
                else:
                    NP("logical_and", allbad, NP(mask == defs.VALID), allbad)
                    mask[allbad] = defs.MISSING

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class LogicalOr(Function):
        name = "or"

        def applyWithoutMask(self, data, mask, argument):
            data, allbad = data
            NP("logical_or", data, argument.data, data)
            if argument.mask is not None:
                NP("logical_and", allbad, NP(argument.mask != defs.VALID), allbad)
            return (data, allbad), mask

        def applyWithMask(self, data, mask, argument, mask2):
            data, allbad = data
            data[mask2] = NP("logical_or", data[mask2], argument.data[mask2])
            if argument.mask is not None:
                allbad[mask2] = NP("logical_and", NP(allbad[mask2] != defs.VALID), argument.mask[mask2])
            return (data, allbad), mask

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.allBooleanType(arguments, atleast=2)

            data = NP("zeros", len(dataTable), dtype=fieldType.dtype)
            mask = None
            allbad = NP("ones", len(dataTable), dtype=NP.dtype(bool))

            (data, allbad), mask = self.applySkipMissing((data, allbad), mask, arguments)

            if allbad.any():
                if mask is None:
                    mask = allbad * defs.MISSING
                else:
                    NP("logical_and", allbad, NP(mask == defs.VALID), allbad)
                    mask[allbad] = defs.MISSING

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class LogicalNot(Function):
        name = "not"
        signatures = {(BOOL,): BOOL,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("logical_not", arguments[0].data)
            mask = arguments[0].mask

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class Contains(Function):
        name = "isIn"

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self._typeReverseMap[BOOL]
            if len(arguments) < 2:
                raise defs.PmmlValidationError("Function \"isIn\" requires at least two arguments")

            performanceTable.pause("built-in \"%s\"" % self.name)
            firstArgument = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            data = NP("zeros", len(dataTable), dtype=fieldType.dtype)
            if firstArgument.mask is None:
                mask = None
            else:
                mask = NP("copy", firstArgument.mask)

            for argument in arguments[1:]:
                if isinstance(argument, Constant):
                    value = firstArgument.fieldType.stringToValue(argument.evaluateOne(convertType=False))
                    NP("logical_or", data, NP(firstArgument.data == value), data)

                else:
                    performanceTable.pause("built-in \"%s\"" % self.name)
                    argument = argument.evaluate(dataTable, functionTable, performanceTable)
                    performanceTable.unpause("built-in \"%s\"" % self.name)
                    
                    if firstArgument.fieldType.dataType == "object" or (firstArgument.fieldType.dataType == "string" and firstArgument.fieldType.optype == "continuous" and argument.fieldType.optype == "continuous"):
                        ld = firstArgument.data
                        rd = argument.data
                        data2 = NP("fromiter", (ld[i] == rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

                    elif firstArgument.fieldType.dataType == "string":
                        ld = firstArgument.data
                        rd = argument.data
                        l2s = firstArgument.fieldType.valueToString
                        r2s = argument.fieldType.valueToString
                        data2 = NP("fromiter", (l2s(ld[i]) == r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

                    else:
                        data2 = NP("equal", firstArgument.data, argument.data)

                    NP("logical_or", data, data2, data)
                    if argument.mask is not None:
                        if mask is None:
                            mask = argument.mask
                        else:
                            mask = DataColumn.mapAnyMissingInvalid([mask, argument.mask])
                    
            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class NotContains(Function):
        name = "isNotIn"

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self._typeReverseMap[BOOL]
            if len(arguments) < 2:
                raise defs.PmmlValidationError("Function \"isNotIn\" requires at least two arguments")

            performanceTable.pause("built-in \"%s\"" % self.name)
            firstArgument = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            data = NP("ones", len(dataTable), dtype=fieldType.dtype)
            if firstArgument.mask is None:
                mask = None
            else:
                mask = NP("copy", firstArgument.mask)

            for argument in arguments[1:]:
                if isinstance(argument, Constant):
                    value = firstArgument.fieldType.stringToValue(argument.evaluateOne(convertType=False))
                    NP("logical_and", data, NP(firstArgument.data != value), data)

                else:
                    performanceTable.pause("built-in \"%s\"" % self.name)
                    argument = argument.evaluate(dataTable, functionTable, performanceTable)
                    performanceTable.unpause("built-in \"%s\"" % self.name)
                    
                    if firstArgument.fieldType.dataType == "object" or (firstArgument.fieldType.dataType == "string" and firstArgument.fieldType.optype == "continuous" and argument.fieldType.optype == "continuous"):
                        ld = firstArgument.data
                        rd = argument.data
                        data2 = NP("fromiter", (ld[i] != rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

                    elif firstArgument.fieldType.dataType == "string":
                        ld = firstArgument.data
                        rd = argument.data
                        l2s = firstArgument.fieldType.valueToString
                        r2s = argument.fieldType.valueToString
                        data2 = NP("fromiter", (l2s(ld[i]) != r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

                    else:
                        data2 = NP("equal", firstArgument.data, argument.data)

                    NP("logical_and", data, data2, data)
                    if argument.mask is not None:
                        if mask is None:
                            mask = argument.mask
                        else:
                            mask = DataColumn.mapAnyMissingInvalid([mask, argument.mask])
                    
            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class IfThenElse(Function):
        name = "if"
        signatures = {(BOOL, FLOAT):              FLOAT,
                      (BOOL, INTEGER):            INTEGER,
                      (BOOL, STRING):             STRING,
                      (BOOL, BOOL):               BOOL,
                      (BOOL, OBJECT):             OBJECT,
                      (BOOL, DATE):               DATE,
                      (BOOL, TIME):               TIME,
                      (BOOL, DATETIME):           DATETIME,
                      (BOOL, FLOAT,    FLOAT):    FLOAT,
                      (BOOL, INTEGER,  INTEGER):  INTEGER,
                      (BOOL, FLOAT,    INTEGER):  FLOAT,
                      (BOOL, INTEGER,  FLOAT):    FLOAT,
                      (BOOL, STRING,   STRING):   STRING,
                      (BOOL, BOOL,     BOOL):     BOOL,
                      (BOOL, OBJECT,   OBJECT):   OBJECT,
                      (BOOL, DATE,     DATE):     DATE,
                      (BOOL, TIME,     TIME):     TIME,
                      (BOOL, DATETIME, DATETIME): DATETIME,
                      }
        
        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            if not (2 <= len(arguments) <= 3):
                raise defs.PmmlValidationError("Function \"if\" requires 2 or 3 arguments (ifClause, thenClause, and maybe elseClause)")

            performanceTable.pause("built-in \"%s\"" % self.name)
            ifClause = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            if ifClause.fieldType.dataType != "boolean":
                raise defs.PmmlValidationError("First argument of function \"if\" must resolve to a boolean, not \"%s\"" % ifClause.fieldType.dataType)
            
            if len(arguments) == 2:
                iftrue = ifClause.data

                thenTable = dataTable.subTable(iftrue)

                performanceTable.pause("built-in \"%s\"" % self.name)
                thenClause = arguments[1].evaluate(thenTable, functionTable, performanceTable)
                performanceTable.unpause("built-in \"%s\"" % self.name)

                fieldType = self.fieldTypeFromSignature([ifClause, thenClause])

                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                mask = NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.MISSING)

                data[iftrue] = thenClause.data
                if thenClause.mask is None:
                    mask[iftrue] = defs.VALID
                else:
                    mask[iftrue] = thenClause.mask

                return DataColumn(fieldType, data, mask)

            else:
                iftrue = ifClause.data
                iffalse = NP("logical_not", iftrue)

                thenTable = dataTable.subTable(iftrue)
                elseTable = dataTable.subTable(iffalse)

                performanceTable.pause("built-in \"%s\"" % self.name)
                thenClause = arguments[1].evaluate(thenTable, functionTable, performanceTable)
                elseClause = arguments[2].evaluate(elseTable, functionTable, performanceTable)
                performanceTable.unpause("built-in \"%s\"" % self.name)

                fieldType = self.fieldTypeFromSignature([ifClause, thenClause, elseClause])

                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                data[iftrue] = thenClause.data
                data[iffalse] = elseClause.data

                if thenClause.mask is None and elseClause.mask is None:
                    mask = None

                elif thenClause.mask is not None and elseClause.mask is not None:
                    mask = NP("empty", len(dataTable), dtype=defs.maskType)
                    mask[iftrue] = thenClause.mask
                    mask[iffalse] = elseClause.mask

                elif thenClause.mask is not None:
                    mask = NP("zeros", len(dataTable), dtype=defs.maskType)
                    mask[iftrue] = thenClause.mask

                elif elseClause.mask is not None:
                    mask = NP("zeros", len(dataTable), dtype=defs.maskType)
                    mask[iffalse] = elseClause.mask

                performanceTable.end("built-in \"%s\"" % self.name)
                return DataColumn(fieldType, data, mask)

    class Uppercase(Function):
        name = "uppercase"
        signatures = {(STRING,): STRING}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            if arguments[0].fieldType.optype == "continuous":
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                for i, x in enumerate(arguments[0].data):
                    data[i] = x.upper()

            else:
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                toString = arguments[0].fieldType.valueToString
                for i, x in enumerate(arguments[0].data):
                    toString(data[i]).upper()

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, arguments[0].mask)

    class Lowercase(Function):
        name = "lowercase"
        signatures = {(STRING,): STRING}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            if arguments[0].fieldType.optype == "continuous":
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                for i, x in enumerate(arguments[0].data):
                    data[i] = x.lower()

            else:
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                toString = arguments[0].fieldType.valueToString
                for i, x in enumerate(arguments[0].data):
                    toString(data[i]).lower()

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, arguments[0].mask)

    class Substring(Function):
        name = "substring"
        signatures = {(STRING, INTEGER, INTEGER,): STRING}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            firstIndex = arguments[1].data - 1
            lastIndex = arguments[2].data + firstIndex

            invalid = NP("logical_or", NP(firstIndex < 0), NP(lastIndex < 0))

            data = NP("empty", len(dataTable), dtype=fieldType.dtype)
            if arguments[0].fieldType.optype == "continuous":
                for i, x in enumerate(arguments[0].data):
                    first = firstIndex[i]
                    last = lastIndex[i]
                    lenx = len(x)
                    if first > lenx:
                        invalid[i] = True
                    else:
                        data[i] = x[first:last]

            else:
                toString = arguments[0].fieldType.valueToString
                for i, x in enumerate(arguments[0].data):
                    first = firstIndex[i]
                    last = lastIndex[i]
                    x = toString(x)
                    lenx = len(x)
                    if first > lenx:
                        invalid[i] = True
                    else:
                        data[i] = x[first:last]

            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([(invalid * defs.INVALID), arguments[0].mask, arguments[1].mask, arguments[2].mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class TrimBlanks(Function):
        name = "trimBlanks"
        signatures = {(STRING,): STRING}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            if arguments[0].fieldType.optype == "continuous":
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                for i, x in enumerate(arguments[0].data):
                    data[i] = x.strip()

            else:
                data = NP("empty", len(dataTable), dtype=fieldType.dtype)
                toString = arguments[0].fieldType.valueToString
                for i, x in enumerate(arguments[0].data):
                    toString(data[i]).strip()

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, arguments[0].mask)

    class FormatNumber(Function):
        name = "formatNumber"
        signatures = {(FLOAT, STRING): STRING, (INTEGER, STRING): STRING,}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self._typeReverseMap[STRING]
            if len(arguments) != 2:
                raise defs.PmmlValidationError("Function \"formatNumber\" requires exactly two arguments")

            performanceTable.pause("built-in \"%s\"" % self.name)
            firstArgument = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            if firstArgument.fieldType.dataType not in ("float", "double", "integer"):
                raise defs.PmmlValidationError("First argument in function \"formatNumber\" must be a number, not \"%s\"" % firstArgument.fieldType.dataType)

            data = NP("empty", len(dataTable), dtype=fieldType.dtype)

            if isinstance(arguments[1], Constant):
                if arguments[1].fieldType.dataType != "string":
                    raise defs.PmmlValidationError("Second argument in function \"formatNumber\" must be a string, not \"%s\"" % arguments[1].fieldType.dataType)
                performanceTable.pause("built-in \"%s\"" % self.name)
                secondArgument = arguments[1].evaluateOne(convertType=False)
                performanceTable.unpause("built-in \"%s\"" % self.name)

                try:
                    for i, x in enumerate(firstArgument.data):
                        data[i] = secondArgument % x
                except TypeError:
                    return DataColumn(fieldType, data, NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.INVALID))

                return DataColumn(fieldType, data, firstArgument.mask)

            else:
                performanceTable.pause("built-in \"%s\"" % self.name)
                secondArgument = arguments[1].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("built-in \"%s\"" % self.name)

                self.fieldTypeFromSignature([firstArgument, secondArgument])

                mask = DataColumn.mapAnyMissingInvalid([firstArgument.mask, secondArgument.mask])

                if secondArgument.fieldType.optype == "continuous":
                    for i, x in enumerate(firstArgument.data):
                        second = secondArgument.data[i]
                        try:
                            data[i] = second % x
                        except TypeError:
                            mask[i] = defs.INVALID

                else:
                    toString = secondArgument.fieldType.valueToString
                    for i, x in enumerate(firstArgument.data):
                        second = toString(secondArgument.data[i])
                        try:
                            data[i] = second % x
                        except TypeError:
                            mask[i] = defs.INVALID

                if mask is not None and not mask.any():
                    mask = None

                performanceTable.end("built-in \"%s\"" % self.name)
                return DataColumn(fieldType, data, mask)

    class FormatDatetime(Function):
        name = "formatDatetime"
        signatures = {(DATE, STRING): STRING, (TIME, STRING): STRING, (DATETIME, STRING): STRING}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self._typeReverseMap[STRING]
            if len(arguments) != 2:
                raise defs.PmmlValidationError("Function \"formatDatetime\" requires exactly two arguments")

            performanceTable.pause("built-in \"%s\"" % self.name)
            firstArgument = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            if self._typeMap[firstArgument.fieldType.dataType] not in (DATE, TIME, DATETIME):
                raise defs.PmmlValidationError("First argument in function \"formatDatetime\" must be a date, time, or dateTime, not \"%s\"" % firstArgument.fieldType.dataType)

            data = NP("empty", len(dataTable), dtype=fieldType.dtype)

            if isinstance(arguments[1], Constant):
                if arguments[1].fieldType.dataType != "string":
                    raise defs.PmmlValidationError("Second argument in function \"formatDatetime\" must be a string, not \"%s\"" % arguments[1].fieldType.dataType)
                secondArgument = arguments[1].evaluateOne(convertType=False)

                try:
                    toDateTime = firstArgument.fieldType.valueToPython
                    for i, x in enumerate(firstArgument.data):
                        data[i] = toDateTime(x).strftime(secondArgument)
                except TypeError:
                    return DataColumn(fieldType, data, NP(NP("ones", len(dataTable), dtype=defs.maskType) * defs.INVALID))

                return DataColumn(fieldType, data, firstArgument.mask)

            else:
                performanceTable.pause("built-in \"%s\"" % self.name)
                secondArgument = arguments[1].evaluate(dataTable, functionTable, performanceTable)
                performanceTable.unpause("built-in \"%s\"" % self.name)

                self.fieldTypeFromSignature([firstArgument, secondArgument])

                mask = DataColumn.mapAnyMissingInvalid([firstArgument.mask, secondArgument.mask])

                if secondArgument.fieldType.optype == "continuous":
                    toDateTime = firstArgument.fieldType.valueToPython
                    for i, x in enumerate(firstArgument.data):
                        second = secondArgument.data[i]
                        try:
                            data[i] = toDateTime(x).strftime(second)
                        except TypeError:
                            mask[i] = defs.INVALID

                else:
                    toDateTime = firstArgument.fieldType.valueToPython
                    toString = secondArgument.fieldType.valueToString
                    for i, x in enumerate(firstArgument.data):
                        second = toString(secondArgument.data[i])
                        try:
                            data[i] = toDateTime(x).strftime(second)
                        except TypeError:
                            mask[i] = defs.INVALID

                if mask is not None and not mask.any():
                    mask = None

                performanceTable.end("built-in \"%s\"" % self.name)
                return DataColumn(fieldType, data, mask)

    class DateDaysSinceYear(Function):
        name = "dateDaysSinceYear"
        signatures = {(DATE, INTEGER): FLOAT,
                      (DATETIME, INTEGER): FLOAT,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)
            
            fieldType = self.fieldTypeFromSignature(arguments)

            dateTimes = arguments[0]

            references = {}
            referenceAsNumbers = NP("empty", len(dataTable), dtype=dateTimes.fieldType.dtype)
            dateTimeOrigin = dateTimes.fieldType._dateTimeOrigin
            for i, year in enumerate(arguments[1].data):
                if year not in references:
                    td = datetime.datetime(year, 1, 1) - dateTimeOrigin
                    references[year] = (td.days*86400 + td.seconds) * FieldType._dateTimeResolution + td.microseconds
                referenceAsNumbers[i] = references[year]
            
            data = NP(NP(dateTimes.data - referenceAsNumbers) / NP(86400.0 * FieldType._dateTimeResolution))
            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([dateTimes.mask, arguments[1].mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class DateSecondsSinceYear(Function):
        name = "dateSecondsSinceYear"
        signatures = {(DATE, INTEGER): FLOAT,
                      (DATETIME, INTEGER): FLOAT,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)
            
            fieldType = self.fieldTypeFromSignature(arguments)

            dateTimes = arguments[0]

            references = {}
            referenceAsNumbers = NP("empty", len(dataTable), dtype=dateTimes.fieldType.dtype)
            dateTimeOrigin = dateTimes.fieldType._dateTimeOrigin
            for i, year in enumerate(arguments[1].data):
                if year not in references:
                    td = datetime.datetime(year, 1, 1) - dateTimeOrigin
                    references[year] = (td.days*86400 + td.seconds) * FieldType._dateTimeResolution + td.microseconds
                referenceAsNumbers[i] = references[year]
            
            data = NP(NP(dateTimes.data - referenceAsNumbers) / float(FieldType._dateTimeResolution))
            dataColumn = DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([dateTimes.mask, arguments[1].mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class DateSecondsSinceMidnight(Function):
        name = "dateSecondsSinceMidnight"
        signatures = {(DATE,): FLOAT, (TIME,): FLOAT, (DATETIME,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)
            
            fieldType = self.fieldTypeFromSignature(arguments)

            dateTimes = arguments[0]
            
            data = NP(NP("mod", dateTimes.data, 86400 * FieldType._dateTimeResolution) / float(FieldType._dateTimeResolution))
            
            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, dateTimes.mask)

    class Negative(Function):   # technically not in the PMML spec, but Formula needs it so often that it's inconvenient to leave out
        name = "negative"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): INTEGER}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("negative", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

