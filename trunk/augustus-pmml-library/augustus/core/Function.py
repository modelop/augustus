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

"""This module defines the Function class and a suite of TYPEOBJECTs."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.FakeFieldType import FakeFieldType

FLOAT = object()
INTEGER = object()
STRING = object()
BOOL = object()
OBJECT = object()
DATE = object()
TIME = object()
DATETIME = object()

class Function(object):
    """Base class for all built-in and custom built-in functions used
    by the Apply element.

    Its methods are used to simplify the implementation of its
    subclasses.

    TYPEOBJECTs are a set of objects in the C{Function} module that
    each represent a generic type.  They are used to make signatures
    more concise and readable.
    """

    signatures = {}

    _typeMap = {"float": FLOAT,
                "double": FLOAT,
                "integer": INTEGER,
                "string": STRING,
                "boolean": BOOL,
                "object": OBJECT,
                "date": DATE,
                "dateDaysSince[0]": DATE,
                "dateDaysSince[1960]": DATE,
                "dateDaysSince[1970]": DATE,
                "dateDaysSince[1980]": DATE,
                "time": TIME,
                "timeSeconds": TIME,
                "dateTime": DATETIME,
                "dateTimeSecondsSince[0]": DATETIME,
                "dateTimeSecondsSince[1960]": DATETIME,
                "dateTimeSecondsSince[1970]": DATETIME,
                "dateTimeSecondsSince[1980]": DATETIME,
                "dateTimeMillisecondsSince[0]": DATETIME,
                "dateTimeMillisecondsSince[1960]": DATETIME,
                "dateTimeMillisecondsSince[1970]": DATETIME,
                "dateTimeMillisecondsSince[1980]": DATETIME,
                }

    _typeReverseMap = {FLOAT: FakeFieldType("double", "continuous"),
                       INTEGER: FakeFieldType("integer", "continuous"),
                       STRING: FakeFieldType("string", "continuous"),
                       BOOL: FakeFieldType("boolean", "continuous"),
                       OBJECT: FakeFieldType("object", "any"),
                       DATE: FakeFieldType("date", "continuous"),
                       TIME: FakeFieldType("time", "continuous"),
                       DATETIME: FakeFieldType("dateTime", "continuous"),
                       }

    _typeNameMap = {FLOAT: "floating-point",
                    INTEGER: "integer",
                    STRING: "string",
                    BOOL: "boolean",
                    OBJECT: "Python object",
                    DATE: "date",
                    TIME: "time",
                    DATETIME: "dateTime",
                    }

    def fieldTypeFromSignature(self, arguments):   # arguments are dataColumns
        """Helper method to derive the resulting FieldType from the
        class's signature.

        This method assumes that the class has a class attribute named
        C{signatures} that maps TYPEOBJECT tuples (input type signature)
        to TYPEOBJECTs (output type).

        @type arguments: list of DataColumns
        @param arguments: Input DataColumns representing the pre-evaluated arguments of the function.
        @rtype: FieldType
        @return: The FieldType corresponding to the output type.
        @raise PmmlValidationError: If no matching signature is found, raise an error.
        """

        signature = []
        for argument in arguments:
            signature.append(self._typeMap[argument.fieldType.dataType])

        outputType = self.signatures.get(tuple(signature))
        if outputType is None:
            raise defs.PmmlValidationError("Function \"%s\" has no signature matching its arguments" % self.name)
        else:
            return self._typeReverseMap[outputType]

    def broadestNumberType(self, arguments, atleast=None, atmost=None, only=None):
        """Helper method to require only numerical arguments and
        report the broadest numerical type.

        A double is broader than a float and a float is broader than
        an integer.

        @type arguments: list of DataColumns
        @param arguments: Input DataColumns representing the pre-evaluated arguments of the function.
        @type atleast: int or None
        @param atleast: If None, no minimum number of arguments; otherwise, require at least this many.
        @type atmost: int or None
        @param atmost: If None, no minimum number of arguments; otherwise, require at most this many.
        @type only: TYPEOBJECT or None
        @param only: If not None, require all arguments to have type TYPEOBJECT.
        @rtype: FieldType
        @return: The broadest numerical type.
        @raise PmmlValidationError: If the condition is not met, raise an error.
        """

        if atleast is not None and len(arguments) < atleast:
            raise defs.PmmlValidationError("Function \"%s\" requires at least %d arguments" % (self.name, atleast))

        if atmost is not None and len(arguments) > atmost:
            raise defs.PmmlValidationError("Function \"%s\" requires at most %d arguments" % (self.name, atmost))

        types = [self._typeMap[argument.fieldType.dataType] for argument in arguments]
        
        floats = types.count(FLOAT)
        integers = types.count(INTEGER)
        dates = types.count(DATE)
        times = types.count(TIME)
        dateTimes = types.count(DATETIME)
        
        if integers == len(types):
            typeObject = INTEGER
        elif floats + integers == len(types):
            typeObject = FLOAT
        elif dates > 0:
            if dates == len(types):
                typeObject = DATE
            else:
                raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be dates if any one is a date" % self.name)
        elif times > 0:
            if times == len(types):
                typeObject = TIME
            else:
                raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be times if any one is a time" % self.name)
        elif dateTimes > 0:
            if dateTimes == len(types):
                typeObject = DATETIME
            else:
                raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be dateTimes if any one is a dateTime" % self.name)
        else:
            raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be numeric or dates/times/dateTimes" % self.name)

        if only is not None and typeObject not in only:
            raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be the following types: %s" % (self.name, "".join(self._typeNameMap[x] for x in only)))

        return self._typeReverseMap[typeObject]

    def allBooleanType(self, arguments, atleast=None, atmost=None):
        """Helper method to require only boolean arguments.

        @type arguments: list of DataColumns
        @param arguments: Input DataColumns representing the pre-evaluated arguments of the function.
        @type atleast: int or None
        @param atleast: If None, no minimum number of arguments; otherwise, require at least this many.
        @type atmost: int or None
        @param atmost: If None, no minimum number of arguments; otherwise, require at most this many.
        @raise PmmlValidationError: If the condition is not met, raise an error.  Otherwise, silently pass.
        """

        if atleast is not None and len(arguments) < atleast:
            raise defs.PmmlValidationError("Function \"%s\" requires at least %d arguments" % (self.name, atleast))

        if atmost is not None and len(arguments) > atmost:
            raise defs.PmmlValidationError("Function \"%s\" requires at most %d arguments" % (self.name, atmost))

        if all(argument.fieldType.dataType == "boolean" for argument in arguments):
            return self._typeReverseMap[BOOL]
        else:
            raise defs.PmmlValidationError("Function \"%s\" requires all arguments to be boolean" % self.name)

    def applySkipMissing(self, data, mask, arguments):
        """Helper method to apply a function to several arguments,
        skipping MISSING values in the arguments.

        Note that this skips columns that have MISSING values,
        producing a result for each row.  It is used by PMML built-in
        functions like "minimum", which finds the minimum of columns A,
        B, and C, but ignores individual rows in which A, B, or C have
        a MISSING value.

        This method assumes that the subclass has the following
        additional methods:
          - C{applyWithoutMask(argument)}
          - C{applyWithMask(argument, mask)}
          - C{simpleCopy(argument, mask)}

        See the source code for FunctionTable.Minimum for an example.

        @type data: 1d Numpy array
        @param data: The input data.
        @type mask: 1d Numpy array of C{defs.maskType}, or None
        @param mask: The input mask.
        @type arguments: list of DataColumns
        @param arguments: Input DataColumns representing the pre-evaluated arguments of the function (A, B, and C in the example above).
        @rtype: 2-tuple of 1d Numpy arrays
        @return: The new data and mask.
        """

        # requires the subclass to have applyWithoutMask(argument), applyWithMask(argument, mask), simpleCopy(argument, mask)
        for argument in arguments:
            if argument.mask is None:
                if mask is None:
                    # old data are valid, new data are valid: take the minimum of everything
                    data, mask = self.applyWithoutMask(data, mask, argument)
                else:
                    # some old data are not valid, all new data are valid:
                    # 1. take the minimum across all valid old data
                    data, mask = self.applyWithMask(data, mask, argument, mask2)
                    # 2. set invalid old data to the new values
                    if hasattr(self, "simpleCopy"):
                        mask2 = NP("logical_not", mask)
                        data, mask = self.simpleCopy(data, mask, argument, mask2)
                    # 3. now they are all valid
                    mask = None
            else:
                if mask is None:
                    # old data are valid, some new data are not valid:
                    # take the minimum across all valid new data
                    mask2 = NP(argument.mask == defs.VALID)
                    data, mask = self.applyWithMask(data, mask, argument, mask2)
                else:
                    # old data are invalid, new data are invalid:
                    newgood = NP(argument.mask == defs.VALID)
                    bothgood = NP("logical_and", mask, newgood)
                    oldbad_newgood = newgood - bothgood
                    # 1. take the minimum across the data in which both are good
                    data, mask = self.applyWithMask(data, mask, argument, bothgood)
                    # 2. set invalid old data to the new values that are good
                    if hasattr(self, "simpleCopy"):
                        data, mask = self.simpleCopy(data, mask, argument, oldbad_newgood)
                    # 3. the new mask is the union of oldgood and newgood
                    NP("logical_or", mask, newgood, mask)

        if mask is not None:
            NP("logical_not", mask, mask)
            mask = NP(mask * defs.MISSING)

        return data, mask

    def maskInvalid(self, data, mask):
        """Helper method to replace NaN and infinite values with
        INVALID after a potentially dangerous operation.

        Example::

            result = NP("log", dataColumn.data)    # log(0) = -inf, log(-x) = nan
            resultMask = self.maskInvalid(result, dataColumn.mask)
            return DataColumn(fakeFieldType, result, resultMask)

        The input C{data} and C{mask} are not modified by this
        method; a substitute mask is returned.

        @type data: 1d Numpy array
        @param data: The dataset that may contain NaN and infinite values.
        @type mask: 1d Numpy array of C{defs.maskType}, or None
        @param mask: The original mask.
        @rtype: 1d Numpy array of C{defs.maskType}, or None
        @return: The new mask.
        """

        bad = NP("logical_not", NP("isfinite", data))
        if bad.any():
            if mask is None:
                mask = bad * defs.INVALID
            else:
                NP("logical_and", bad, NP(mask == defs.VALID), bad)
                if not mask.flags.writeable:
                    mask = NP("copy", mask)
                    mask.setflags(write=True)
                mask[bad] = defs.INVALID
        if mask is not None and not mask.any():
            mask = None
        return mask

    # TODO: simplify user-defined functions

    # @classmethod
    # def wrapSimple(cls, name, function, signature=None):
    #     self.evaluate = ...

    # @classmethod
    # def wrapUfunc(cls, name, function, signature=None)
    #     self.evaluate = ...
