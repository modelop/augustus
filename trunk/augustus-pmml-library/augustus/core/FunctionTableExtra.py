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

"""This module defines the FunctionTableExtra class."""

import re
import sre_constants

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.Function import Function, FLOAT, INTEGER, STRING, BOOL, OBJECT, DATE, TIME, DATETIME
from augustus.core.FunctionTable import FunctionTable
from augustus.core.DataColumn import DataColumn
from augustus.pmml.expression.Constant import Constant

class FunctionTableExtra(FunctionTable):
    """FunctionTableExtra is a FunctionTable with additional built-in
    functions for extended PMML.

    The FunctionTableExtra is not automatically loaded, even in
    C{augustus.odg}.  It must be explicitly passed to C{pmml.calc}
    or C{pmml.calculate} at run-time.
    """

    def _defineBuiltins(self):
        super(FunctionTableExtra, self)._defineBuiltins()
        for fcnClass in self.Between, self.NotBetween, self.Like, self.FloorDivision, self.Modulo, self.FModulo, self.LogicalXOr, self.Sine, self.Cosine, self.SineN, self.CosineN, self.Tangent, self.ArcSine, self.ArcCosine, self.ArcTangent, self.ArcTangent2, self.HyperbolicSine, self.HyperbolicCosine, self.HyperbolicTangent, self.HyperbolicArcSine, self.HyperbolicArcCosine, self.HyperbolicArcTangent:
            self[fcnClass.name] = fcnClass()

    class Between(Function):
        name = "between"
        signatures = {(FLOAT,    FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    FLOAT,    INTEGER):  BOOL,
                      (FLOAT,    INTEGER,  FLOAT):    BOOL,
                      (FLOAT,    INTEGER,  INTEGER):  BOOL,
                      (INTEGER,  FLOAT,    FLOAT):    BOOL,
                      (INTEGER,  FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER,  INTEGER):  BOOL,
                      (OBJECT,   OBJECT,   OBJECT):   BOOL,
                      (STRING,   STRING,   STRING):   BOOL,
                      (DATE,     DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            test, low, high = arguments

            if test.fieldType.dataType == "object" or (test.fieldType.dataType == "string" and test.fieldType.optype == "continuous" and low.fieldType.optype == "continuous"):
                ld = test.data
                rd = low.data
                data = NP("fromiter", (ld[i] >= rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif test.fieldType.dataType == "string":
                ld = test.data
                rd = low.data
                l2s = test.fieldType.valueToString
                r2s = low.fieldType.valueToString
                data = NP("fromiter", (l2s(ld[i]) >= r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                data = NP("greater_equal", test.data, low.data)

            if test.fieldType.dataType == "object" or (test.fieldType.dataType == "string" and test.fieldType.optype == "continuous" and high.fieldType.optype == "continuous"):
                ld = test.data
                rd = high.data
                datahigh = NP("fromiter", (ld[i] <= rd[i] for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            elif test.fieldType.dataType == "string":
                ld = test.data
                rd = high.data
                l2s = test.fieldType.valueToString
                r2s = high.fieldType.valueToString
                datahigh = NP("fromiter", (l2s(ld[i]) <= r2s(rd[i]) for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                datahigh = NP("less_equal", test.data, high.data)

            NP("logical_and", data, datahigh, data)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, DataColumn.mapAnyMissingInvalid([test.mask, low.mask, high.mask]))

    class NotBetween(Function):
        name = "notBetween"
        signatures = {(FLOAT,    FLOAT,    FLOAT):    BOOL,
                      (FLOAT,    FLOAT,    INTEGER):  BOOL,
                      (FLOAT,    INTEGER,  FLOAT):    BOOL,
                      (FLOAT,    INTEGER,  INTEGER):  BOOL,
                      (INTEGER,  FLOAT,    FLOAT):    BOOL,
                      (INTEGER,  FLOAT,    INTEGER):  BOOL,
                      (INTEGER,  INTEGER,  FLOAT):    BOOL,
                      (INTEGER,  INTEGER,  INTEGER):  BOOL,
                      (OBJECT,   OBJECT,   OBJECT):   BOOL,
                      (STRING,   STRING,   STRING):   BOOL,
                      (DATE,     DATE,     DATE):     BOOL,
                      (DATETIME, DATETIME, DATETIME): BOOL,
                      }

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            dataColumn = Between.evaluate(dataTable, functionTable, performanceTable, arguments)
            dataColumn._unlock()
            NP("logical_not", dataColumn.data, dataColumn.data)
            dataColumn._lock()
            return dataColumn

    class Like(Function):
        name = "like"
        signatures = {(STRING, STRING): BOOL}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self._typeReverseMap[BOOL]
            if len(arguments) != 2:
                raise defs.PmmlValidationError("Function \"like\" requires exactly two arguments")

            if isinstance(arguments[1], Constant):
                pattern = arguments[1].evaluateOne(convertType=False)
                try:
                    pattern = re.compile(pattern)
                except sre_constants as err:
                    raise defs.PmmlValidationError("Could not compile regex pattern \"%s\": %s" % (pattern, str(err)))
            else:
                raise defs.PmmlValidationError("Function \"like\" requires its second argument (the regex pattern) to be a Constant")

            performanceTable.pause("built-in \"%s\"" % self.name)
            test = arguments[0].evaluate(dataTable, functionTable, performanceTable)
            performanceTable.unpause("built-in \"%s\"" % self.name)

            if test.fieldType.optype == "continuous":
                d = test.data
                data = NP("fromiter", (re.match(pattern, d[i]) is not None for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            else:
                d = test.data
                ds = test.fieldType.valueToString
                data = NP("fromiter", (re.match(pattern, ds(d[i])) is not None for i in xrange(len(dataTable))), dtype=fieldType.dtype, count=len(dataTable))

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, test.mask)

    class FloorDivision(Function):
        name = "//"
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

            zeroDenominators = NP(NP(right.data == 0.0) * defs.INVALID)
            if not zeroDenominators.any():
                zeroDenominators = None

            mask = DataColumn.mapAnyMissingInvalid([zeroDenominators, left.mask, right.mask])

            dataColumn = DataColumn(fieldType, NP("floor_divide", left.data, right.data), mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Modulo(Function):
        name = "mod"
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
            dataColumn = DataColumn(fieldType, NP("mod", left.data, right.data), DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn
        
    class FModulo(Function):
        name = "fmod"
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
            dataColumn = DataColumn(fieldType, NP("fmod", left.data, right.data), DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn
        
    class LogicalXOr(Function):
        name = "xor"

        def applyWithoutMask(self, data, mask, argument):
            data, allbad = data
            NP("logical_xor", data, argument.data, data)
            if argument.mask is not None:
                NP("logical_and", allbad, NP(argument.mask != defs.VALID), allbad)
            return (data, allbad), mask

        def applyWithMask(self, data, mask, argument, mask2):
            data, allbad = data
            data[mask2] = NP("logical_xor", data[mask2], argument.data[mask2])
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

    class Sine(Function):
        name = "sin"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("sin", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Cosine(Function):
        name = "cos"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("cos", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class SineN(Function):
        name = "sinN"
        signatures = {(FLOAT, INTEGER): FLOAT, (INTEGER, INTEGER): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("sin", arguments[0].data * arguments[1].data), DataColumn.mapAnyMissingInvalid([arguments[0].mask, arguments[1].mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class CosineN(Function):
        name = "cosN"
        signatures = {(FLOAT, INTEGER): FLOAT, (INTEGER, INTEGER): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("cos", arguments[0].data * arguments[1].data), DataColumn.mapAnyMissingInvalid([arguments[0].mask, arguments[1].mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class Tangent(Function):
        name = "tan"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("tan", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class ArcSine(Function):
        name = "arcsin"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("arcsin", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class ArcCosine(Function):
        name = "arccos"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)

            data = NP("arccos", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)

    class ArcTangent(Function):
        name = "arctan"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("arctan", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class ArcTangent2(Function):
        name = "arctan2"
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
            dataColumn = DataColumn(fieldType, NP("arctan2", left.data, right.data), DataColumn.mapAnyMissingInvalid([left.mask, right.mask]))

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicSine(Function):
        name = "sinh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("sinh", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicCosine(Function):
        name = "cosh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("cosh", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicTangent(Function):
        name = "tanh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("tanh", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicArcSine(Function):
        name = "arcsinh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("arcsinh", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicArcCosine(Function):
        name = "arccosh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            dataColumn = DataColumn(fieldType, NP("arccosh", arguments[0].data), arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return dataColumn

    class HyperbolicArcTangent(Function):
        name = "arctanh"
        signatures = {(FLOAT,): FLOAT, (INTEGER,): FLOAT}

        def evaluate(self, dataTable, functionTable, performanceTable, arguments):
            arguments = [x.evaluate(dataTable, functionTable, performanceTable) for x in arguments]
            performanceTable.begin("built-in \"%s\"" % self.name)

            fieldType = self.fieldTypeFromSignature(arguments)
            data = NP("arctanh", arguments[0].data)
            mask = self.maskInvalid(data, arguments[0].mask)

            performanceTable.end("built-in \"%s\"" % self.name)
            return DataColumn(fieldType, data, mask)
