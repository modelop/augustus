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

"""This module defines the FieldType class."""

import re
import math
import datetime

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.DataColumn import DataColumn

class FieldType(object):
    """FieldType represents a PMML data type as defined by the
    dataType, optype, isCyclic, Values, and Intervals in its
    DataField/DerivedField definition, and it provides methods for
    creating and interpreting DataColumns.

    FieldTypes are created by PMML DataFields or DerivedFields.  To
    create a FieldType"by hand," see FakeFieldType.

    Most of the methods of this class are auto-generated, based on the
    inputs.  After construction, a FieldType instance will have the
    following member and methods:
    
      - C{dtype}: the dtype of the internal Numpy array
      - C{stringToValue(string)}: maps string input to internal values
      - C{valueToString(value)}: maps internal values to string output (displayValues)
      - C{valueToPython(value)}: maps internal values to Pythonic objects
      - C{toDataColumn(data, mask)}: creates a DataColumn from data and mask
      - C{fromDataColumn(dataColumn)}: returns a list of Pythonic objects

    The character of the "internal values" depends on what the FieldType
    represents.  Floating-point numbers are simply represented by a
    floating-point Numpy array, and C{stringToValue} and C{valueToString}
    are simply the C{float} and C{str} functions.  General date-time
    objects are ingested as ISO-8601 strings, internally stored as an
    integer number of microseconds since 1970, and presented to the
    Python user as C{datetime} instances from the Standard Library
    module.

    Strings are never stored in a character-based Numpy array.  Categorical
    strings are arbitrary 64-bit integers with a maps between the string
    values and the internal values.  Continuous strings are Numpy object
    arrays pointing to Python strings.  Ordinal strings are backed by
    integers in a prescribed order.
    
    The date and date-time representation was chosen to be
    microseconds since 1970 because this covers almost +-300,000 years
    of human history with reasonably fine accuracy.  (In fact, PMML
    cannot represent times shorter than seconds, but extensions to
    PMML might.)  Time strings are interpreted without any dependence
    on the host computer's timezone.

    Although not described by the PMML specification, FieldTypes can
    also have C{dataType="object"}, C{optype="any"} to refer to a Numpy
    object array of Python objects.  This is for C{selectAll}
    segmentation and other PMML constructs that return a type not
    describable as a DataField/DerivedField.

    @type dataType: string
    @param dataType: The dataType name as defined by PMML (e.g. "string", "double", "integer").
    @type optype: string
    @param optype: The optype name as defined by PMML ("categorical", "ordinal", "continuous").
    @type values: list of PmmlBinding
    @param values: List of allowed or special values, as defined by PMML.
    @type intervals: list of PmmlBinding
    @param intervals: List of allowed intervals, as defined by PMML.
    @type isCyclic: bool
    @param isCyclic: Labels the field as cyclic; used by some parts of PMML.
    """

    ### by default, the internal format for all dates, times, and dateTimes is number of microseconds since 1970 as a signed 64-bit integer;
    ### values are 1e6 times a Unix timestamp and they cover almost +-300,000 years of human history with microsecond resolution
    ### (microseconds is the limit of accuracy for libpcap/tcpdump network packet times: http://www.wireshark.org/docs/wsug_html_chunked/ChAdvTimestamps.html)

    _dateTimeOrigin = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)   # DON'T CHANGE THIS: 1970 is assumed in several places in the code
    _dateTimeResolution = 1000000    # although you can change this, the comments and variable names assume that the resolution is microseconds

    _iso8601 = re.compile("^(-?[0-9]{4})([-/]([0-9]{2})([-/]([0-9]{2})([T ]([0-9]{2}):([0-9]{2})(:([0-9]{2})(\.[0-9]+)?)?([-+][0-9]{2}:[0-9]{2}|Z)?)?)?)?$")
    _iso8601_date = re.compile("^(-?[0-9]{4})([-/]([0-9]{2})([-/]([0-9]{2}))?)?$")
    _iso8601_time = re.compile("^([0-9]{2}):([0-9]{2})(:([0-9]{2})(\.[0-9]+)?)?([-+][0-9]{2}:[0-9]{2}|Z)?$")
    _timezone = re.compile("^([-+])([0-9]{2}):([0-9]{2})$")
    _secondsPerDay = 86400

    @classmethod
    def setTimeResolution(cls, resolution):
        """Changes the internal date-time representation for all
        instances of FieldType.

        @type resolution: string or int
        @param resolution: If "second", use seconds as the unit; if "microsecond", use microseconds as the unit (the default); if "nanosecond", use nanoseconds as the unit and reduce the range of applicability to the years 1678 through 2261; if an integer C{x}, use C{1/x} seconds as the unit.
        """

        if isinstance(resolution, basestring):
            if resolution == "second":
                cls._dateTimeResolution = 1
            elif resolution == "microsecond":
                cls._dateTimeResolution = 1000000
            elif resolution == "nanosecond":
                cls._dateTimeResolution = 1000000000

        elif isinstance(resolution, (int, long)):
            cls._dateTimeResolution = long(resolution)

        else:
            raise TypeError("resolution must be one of \"second\", \"microsecond\", \"nanosecond\" or an integer number per second")

    def __init__(self, field):
        """Initialize the FieldType.

        @type field: PmmlBinding
        @param field: The PMML element that defines this FieldType.  It may be a DataField or a DerivedField.
        """

        self._field = field
        self._setup()

    def __getstate__(self):
        return {"field": self._field}

    def __setstate__(self, serialization):
        self._field = serialization["field"]
        self._setup()

    def _addCategorical(self, string):
        if self._newValuesAllowed:
            internal = id(string)
            self._stringToValue[string] = internal
            self._valueToString[internal] = string
            return internal
        else:
            return None

    def _addOrdinal(self, string):
        if self._newValuesAllowed:
            internal = len(self._stringToValue)
            self._stringToValue[string] = internal
            self._valueToString[internal] = string
            return internal
        else:
            return None

    def _setup(self):
        if self.optype != "continuous" and len(self.intervals) > 0:
            raise defs.PmmlValidationError("Non-continuous fields cannot have Intervals")

        self._displayValue = {}

        if self.dataType == "object":   # for scoring results that don't fit the PMML pattern
            self.toDataColumn = self._toDataColumn_object
            self.fromDataColumn = self._fromDataColumn_object
            self.dtype = NP.dtype(object)
            self.stringToValue = self._stringToValue_object
            self.valueToString = self._valueToString_object
            self.valueToPython = self._valueToPython

        elif self.dataType == "string":
            if self.optype == "categorical":
                self._stringToValue = {}            # TODO: merge categorical and ordinal <Value> handling
                self._valueToString = {}            # into _checkValues(data, mask)
                self._newValuesAllowed = True
                for value in self.values:
                    v = value.get("value")
                    displayValue = value.get("displayValue")
                    if displayValue is not None:
                        self._displayValue[v] = displayValue
                    if value.get("property", "valid") == "valid":
                        self._addCategorical(v)
                if len(self._stringToValue) > 0:
                    self._newValuesAllowed = False

                self.toDataColumn = self._toDataColumn_internal
                self.fromDataColumn = self._fromDataColumn
                self.dtype = NP.int64
                self.stringToValue = self._stringToValue_categorical
                self.valueToString = self._valueToString_categorical
                self.valueToPython = self._valueToString_categorical

            elif self.optype == "ordinal":
                self._stringToValue = {}            # TODO: see above
                self._valueToString = {}
                self._newValuesAllowed = True
                for value in self.values:
                    v = value.get("value")
                    displayValue = value.get("displayValue")
                    if displayValue is not None:
                        self._displayValue[v] = displayValue
                    if value.get("property", "valid") == "valid":
                        self._addOrdinal(v)
                self._newValuesAllowed = False

                self.toDataColumn = self._toDataColumn_internal
                self.fromDataColumn = self._fromDataColumn
                self.dtype = NP.dtype(int)
                self.stringToValue = self._stringToValue_ordinal
                self.valueToString = self._valueToString_ordinal
                self.valueToPython = self._valueToString_ordinal

            elif self.optype == "continuous":
                self.toDataColumn = self._toDataColumn_string
                self.fromDataColumn = self._fromDataColumn_object
                self.dtype = NP.dtype(object)
                self.stringToValue = self._stringToValue_string
                self.valueToString = self._valueToString_string
                self.valueToPython = self._valueToString_string

            else:
                raise defs.PmmlValidationError("Unrecognized optype: %s" % self.optype)

        elif self.dataType == "integer":
            self.toDataColumn = self._toDataColumn_number
            self.fromDataColumn = self._fromDataColumn_number
            self.dtype = NP.dtype(int)
            self.stringToValue = self._stringToValue_integer
            self.valueToString = self._valueToString_integer
            self.valueToPython = self._valueToPython

        elif self.dataType == "float":
            self.toDataColumn = self._toDataColumn_number
            self.fromDataColumn = self._fromDataColumn_number
            self.dtype = NP.float32
            self.stringToValue = self._stringToValue_float
            self.valueToString = self._valueToString_float
            self.valueToPython = self._valueToPython

        elif self.dataType == "double":
            self.toDataColumn = self._toDataColumn_number
            self.fromDataColumn = self._fromDataColumn_number
            self.dtype = NP.dtype(float)
            self.stringToValue = self._stringToValue_double
            self.valueToString = self._valueToString_double
            self.valueToPython = self._valueToPython

        elif self.dataType == "boolean":
            self.toDataColumn = self._toDataColumn_number
            self.fromDataColumn = self._fromDataColumn_number
            self.dtype = NP.dtype(bool)
            self.stringToValue = self._stringToValue_boolean
            self.valueToString = self._valueToString_boolean
            self.valueToPython = self._valueToPython

        elif self.dataType == "date":
            self.toDataColumn = self._toDataColumn_dateTime
            self.fromDataColumn = self._fromDataColumn
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_date
            self.valueToString = self._valueToString_date
            self.valueToPython = self._valueToPython_date

        elif self.dataType == "time":
            self.toDataColumn = self._toDataColumn_dateTime
            self.fromDataColumn = self._fromDataColumn
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_time
            self.valueToString = self._valueToString_time
            self.valueToPython = self._valueToPython_time

        elif self.dataType == "dateTime":
            self.toDataColumn = self._toDataColumn_dateTime
            self.fromDataColumn = self._fromDataColumn
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTime
            self.valueToString = self._valueToString_dateTime
            self.valueToPython = self._valueToPython_dateTime

        elif self.dataType == "dateDaysSince[0]":
            # _offset is the number of seconds between 1/1/1 B.C. and 1/1/1970, using the astronomical convention
            # that 1 B.C. is "year zero" (which does not exist, even in the proleptic Gregorian calendar)
            # and that this fictitious year would have been a leap year (366 full days)
            # http://en.wikipedia.org/wiki/Year_zero#Astronomers
            self._offset = -62167219200 * self._dateTimeResolution
            self._factor = 86400 * self._dateTimeResolution        # number of microseconds in a day
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateDaysSince[1960]":
            self._offset = -315619200 * self._dateTimeResolution   # number of seconds between 1/1/1960 and 1/1/1970, accounting for leap years/leap seconds
            self._factor = 86400 * self._dateTimeResolution        # number of microseconds in a day
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateDaysSince[1970]":
            self._offset = 0
            self._factor = 86400 * self._dateTimeResolution        # number of microseconds in a day
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateDaysSince[1980]":
            self._offset = 315532800 * self._dateTimeResolution    # number of seconds between 1/1/1980 and 1/1/1970, accounting for leap years/leap seconds
            self._factor = 86400 * self._dateTimeResolution        # number of microseconds in a day
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "timeSeconds":
            self._offset = 0
            self._factor = self._dateTimeResolution            # number of microseconds in a second
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_timeSeconds  # reports modulo 1 day
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_timeSeconds    # reports modulo 1 day
            self.valueToPython = self._valueToPython_timeSeconds    # reports modulo 1 day

        elif self.dataType == "dateTimeSecondsSince[0]":
            self._offset = -62167219200 * self._dateTimeResolution # number of seconds between 1/1/1 B.C. and 1/1/1970, accounting for leap years/leap seconds
            self._factor = self._dateTimeResolution            # number of microseconds in a second
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateTimeSecondsSince[1960]":
            self._offset = -315619200 * self._dateTimeResolution   # number of seconds between 1/1/1960 and 1/1/1970, accounting for leap years/leap seconds
            self._factor = self._dateTimeResolution            # number of microseconds in a second
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateTimeSecondsSince[1970]":
            self._offset = 0
            self._factor = self._dateTimeResolution            # number of microseconds in a second
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        elif self.dataType == "dateTimeSecondsSince[1980]":
            self._offset = 315532800 * self._dateTimeResolution    # number of seconds between 1/1/1980 and 1/1/1970, accounting for leap years/leap seconds
            self._factor = self._dateTimeResolution            # number of microseconds in a second
            self.toDataColumn = self._toDataColumn_dateTimeNumber
            self.fromDataColumn = self._fromDataColumn_dateTimeNumber
            self.dtype = NP.int64
            self.stringToValue = self._stringToValue_dateTimeNumber
            self.valueToString = self._valueToString_dateTimeNumber
            self.valueToPython = self._valueToPython_dateTimeNumber

        else:
            raise defs.PmmlValidationError("Unrecognized dataType: %s" % self.dataType)

        self._hash = hash((self.dataType, self.optype, tuple(self.values), tuple(self.intervals), self.isCyclic))

    @property
    def dataType(self):
        return self._field.get("dataType")

    @property
    def optype(self):
        return self._field.get("optype")

    @property
    def values(self):
        return self._field.xpath("pmml:Value")

    @property
    def intervals(self):
        return self._field.xpath("pmml:Interval")

    @property
    def isCyclic(self):
        isCyclic = self._field.get("isCyclic")
        if isCyclic is None:
            return False
        else:
            return isCyclic == "1"

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if not isinstance(other, FieldType):
            return False
        return (self._hash == other._hash)

    def __repr__(self):
        return "<FieldType %s %s at 0x%x>" % (self.optype, self.dataType, id(self))

    def isstring(self):
        """Determine if a field is a string.

        @rtype: bool
        @return: True if C{dataType} is "string".
        """

        return self.dataType == "string"

    def isboolean(self):
        """Determine if a field is boolean.

        @rtype: bool
        @return: True if C{dataType} is "boolean".
        """

        return self.dataType == "boolean"

    def isnumeric(self):
        """Determine if a field is an integer, float, or double.

        @rtype: bool
        @return: True if C{dataType} is "integer", "float", or "double".
        """

        return self.dataType in ("integer", "float", "double")

    def istemporal(self):
        """Determine if a field is a date, a time, or a date-time data type.

        @rtype: bool
        @return: True if C{isdate() or istime() or isdatetime()}.
        """

        return self.isdate() or self.istime() or self.isdatetime()

    def isdate(self):
        """Determine if a field is a date data type.

        @rtype: bool
        @return: True if C{dataType} is "date", "dateDaysSince[0]", "dateDaysSince[1960]", "dateDaysSince[1970]", or "dateDaysSince[1980]".
        """

        return self.dataType in ("date", "dateDaysSince[0]", "dateDaysSince[1960]", "dateDaysSince[1970]", "dateDaysSince[1980]")

    def istime(self):
        """Determine if a field is a time data type.

        @rtype: bool
        @return: True if C{dataType} is "time" or "timeSeconds".
        """

        return self.dataType in ("time", "timeSeconds")

    def isdatetime(self):
        """Determine if a field is a date-time data type.
        
        @rtype: bool
        @return: True if C{dataType} is "dateTime", "dateTimeSecondsSince[0]", "dateTimeSecondsSince[1960]", "dateTimeSecondsSince[1970]", or "dateTimeSecondsSince[1980]".
        """

        return self.dataType in ("dateTime", "dateTimeSecondsSince[0]", "dateTimeSecondsSince[1960]", "dateTimeSecondsSince[1970]", "dateTimeSecondsSince[1980]")

    ### toDataColumn functions

    def _checkNumpy(self, data, mask, tryToCast=True):
        if mask is None and isinstance(data, NP.ma.MaskedArray):
            m = NP.ma.getmask(data)
            if m is not None:
                mask = m

        if isinstance(data, NP.ma.MaskedArray):
            data = NP.ma.getdata(data)
        
        if isinstance(data, NP.ndarray):
            if len(data.shape) != 1:
                raise TypeError("DataColumns cannot be built from n > 1 dimensional arrays")
            if tryToCast and data.dtype != self.dtype:
                try:
                    data = NP("array", data, dtype=self.dtype)
                except (TypeError, ValueError):
                    pass

        if isinstance(mask, NP.ndarray):
            if mask.shape != data.shape:
                raise TypeError("Mask, if provided, must have the same shape as data")
            if mask.dtype != defs.maskType:
                mask = NP(NP(mask != 0) * defs.MISSING)
        
        return data, mask

    def _checkNonNumpy(self, data, mask):
        if isinstance(data, basestring):
            data = [data]
            if mask is not None:
                mask = [mask]
        else:
            try:
                iter(data)
            except TypeError:
                data = [data]
                if mask is not None:
                    mask = [mask]

        try:
            len(data)
            if mask is not None:
                len(mask)
        except TypeError:
            raise TypeError("DataColumns cannot be built from generators without a len() method")

        if mask is not None and len(data) != len(mask):
            raise TypeError("Mask, if provided, must have the same length as data")

        return data, mask

    def _checkValues(self, data, mask):
        values = self.values
        if len(values) == 0:
            return data, mask

        if mask is None:
            missing = NP("zeros", len(data), dtype=NP.dtype(bool))
            invalid = NP("zeros", len(data), dtype=NP.dtype(bool))
        else:
            missing = NP(mask == defs.MISSING)
            invalid = NP(mask == defs.INVALID)
        valid = NP("zeros", len(data), dtype=NP.dtype(bool))

        numberOfValidSpecified = 0
        for value in values:
            v = value.get("value")
            displayValue = value.get("displayValue")
            if displayValue is not None:
                self._displayValue[v] = displayValue

            prop = value.get("property", "valid")
            try:
                v2 = self.stringToValue(v)
            except ValueError:
                raise defs.PmmlValidationError("Improper value in Value specification: \"%s\"" % v)

            if prop == "valid":
                NP("logical_or", valid, NP(data == v2), valid)
                numberOfValidSpecified += 1
            elif prop == "missing":
                NP("logical_or", missing, NP(data == v2), missing)
            elif prop == "invalid":
                NP("logical_or", invalid, NP(data == v2), invalid)

        if numberOfValidSpecified > 0:
            # guilty until proven innocent
            NP("logical_and", valid, NP("logical_not", missing), valid)
            if valid.all():
                return data, None
            mask = NP(NP("ones", len(data), dtype=defs.maskType) * defs.INVALID)
            mask[missing] = defs.MISSING
            mask[valid] = defs.VALID

        else:
            # innocent until proven guilty
            NP("logical_and", invalid, NP("logical_not", missing), invalid)
            if not NP("logical_or", invalid, missing).any():
                return data, None
            mask = NP("zeros", len(data), dtype=defs.maskType)
            mask[missing] = defs.MISSING
            mask[invalid] = defs.INVALID

        return data, mask

    def _checkIntervals(self, data, mask):
        intervals = self.intervals
        if len(intervals) == 0:
            return data, mask

        # innocent until proven guilty
        invalid = NP("zeros", len(data), dtype=NP.dtype(bool))
        for interval in intervals:
            closure = interval["closure"]
            leftMargin = interval.get("leftMargin")
            rightMargin = interval.get("rightMargin")

            if leftMargin is not None:
                try:
                    leftMargin = self.stringToValue(leftMargin)
                except ValueError:
                    raise defs.PmmlValidationError("Improper value in Interval leftMargin specification: \"%s\"" % leftMargin)

                if closure in ("openClosed", "openOpen"):
                    invalid[NP(data <= leftMargin)] = True
                elif closure in ("closedOpen", "closedClosed"):
                    invalid[NP(data < leftMargin)] = True

            if rightMargin is not None:
                try:
                    rightMargin = self.stringToValue(rightMargin)
                except ValueError:
                    raise defs.PmmlValidationError("Improper value in Interval rightMargin specification: \"%s\"" % rightMargin)

                if closure in ("openOpen", "closedOpen"):
                    invalid[NP(data >= rightMargin)] = True
                elif closure in ("openClosed", "closedClosed"):
                    invalid[NP(data > rightMargin)] = True

        if not invalid.any():
            return data, mask

        if mask is None:
            return data, NP(invalid * defs.INVALID)
        else:
            NP("logical_and", invalid, NP(mask == defs.VALID), invalid)   # only change what wasn't already marked as MISSING
            mask[invalid] = defs.INVALID
            return data, mask

    def _toDataColumn_internal(self, data, mask):
        data, mask = self._checkNumpy(data, mask, tryToCast=False)
        data, mask = self._checkNonNumpy(data, mask)
        
        try:
            data = NP("fromiter", (self.stringToValue(d) for d in data), dtype=self.dtype, count=len(data))
            # mask is handled in the else statement after the except block

        except ValueError:
            data2 = NP("empty", len(data), dtype=self.dtype)
            if mask is None:
                mask2 = NP("zeros", len(data), dtype=defs.maskType)
            else:
                mask2 = NP("fromiter", (defs.VALID if not m else defs.MISSING for m in mask), dtype=defs.maskType, count=len(mask))

            for i, v in enumerate(data):
                if isinstance(v, float) and math.isnan(v):
                    data2[i] = defs.PADDING
                    mask2[i] = defs.MISSING
                else:
                    try:
                        data2[i] = self.stringToValue(v)
                    except (ValueError, TypeError):
                        data2[i] = defs.PADDING
                        mask2[i] = defs.INVALID

            if not mask2.any():
                mask2 = None

            data, mask = data2, mask2

        else:
            if mask is not None and not isinstance(mask, NP.ndarray):
                mask = NP("array", mask, dtype=defs.maskType)

        # this is the only _toDataColumn that doesn't check values and intervals because these were checked in _setup for categorical and ordinal strings

        return DataColumn(self, data, mask)

    def _toDataColumn_object(self, data, mask):
        data, mask = self._checkNumpy(data, mask)
        if isinstance(data, NP.ndarray) and (mask is None or isinstance(mask, NP.ndarray)) and data.dtype == self.dtype:
            pass  # proceed to return statement (after checking values and intervals)

        else:
            data, mask = self._checkNonNumpy(data, mask)
            data = NP.array(data, dtype=self.dtype)

            if mask is None:
                mask = NP("fromiter", (defs.MISSING if (isinstance(d, float) and math.isnan(d)) else defs.VALID for d in data), dtype=defs.maskType, count=len(data))
            else:
                mask = NP("fromiter", (defs.MISSING if (m != 0 or (isinstance(data[i], float) and math.isnan(data[i]))) else defs.VALID for i, m in enumerate(mask)), dtype=defs.maskType, count=len(mask))
            if not mask.any():
                mask = None

        data, mask = self._checkValues(data, mask)
        data, mask = self._checkIntervals(data, mask)
        return DataColumn(self, data, mask)

    def _toDataColumn_string(self, data, mask):
        dataColumn = self._toDataColumn_object(data, mask)

        data = dataColumn.data
        mask = dataColumn.mask
        data.setflags(write=True)
        if mask is not None:
            mask.setflags(write=True)

        if mask is not None:
            for i, x in enumerate(dataColumn.data):
                if (x is None or (isinstance(x, float) and math.isnan(x))) and mask[i] == defs.VALID:
                    mask[i] = defs.MISSING
                elif not isinstance(x, basestring):
                    data[i] = repr(x)

        else:
            for i, x in enumerate(dataColumn.data):
                if x is None or (isinstance(x, float) and math.isnan(x)):
                    if mask is None:
                        mask = NP("zeros", len(data), dtype=defs.maskType)
                    mask[i] = defs.MISSING
                elif not isinstance(x, basestring):
                    data[i] = repr(x)

            if mask is not None:
                dataColumn._mask = mask

        data, mask = self._checkValues(data, mask)
        data, mask = self._checkIntervals(data, mask)

        return DataColumn(self, data, mask)

    def _toDataColumn_number(self, data, mask):
        data, mask = self._checkNumpy(data, mask)
        if isinstance(data, NP.ndarray) and (mask is None or isinstance(mask, NP.ndarray)) and data.dtype == self.dtype:
            mask2 = NP("isnan", data)
            if mask is None:
                mask = NP("array", mask2, defs.maskType) * defs.MISSING
            else:
                mask[mask2] = defs.MISSING

        else:
            data, mask = self._checkNonNumpy(data, mask)
            try:
                data = NP("array", data, dtype=self.dtype)
                # mask is handled in the else statement after the except block

            except (ValueError, TypeError):
                data2 = NP("empty", len(data), dtype=self.dtype)
                if mask is None:
                    mask2 = NP("zeros", len(data), dtype=defs.maskType)
                else:
                    mask2 = NP("fromiter", ((defs.VALID if not m else defs.MISSING) for m in mask), dtype=defs.maskType, count=len(mask))

                for i, v in enumerate(data):
                    try:
                        data2[i] = v
                        if mask2[i] == defs.VALID and ((isinstance(v, float) and math.isnan(v)) or (isinstance(v, basestring) and v.upper() == "NAN")):
                            mask2[i] = defs.MISSING
                        if v is None:
                            raise TypeError
                    except (ValueError, TypeError):
                        data2[i] = defs.PADDING
                        if mask2[i] == defs.VALID:
                            if (isinstance(v, float) and math.isnan(v)) or (isinstance(v, basestring) and v.upper() == "NAN"):
                                mask2[i] = defs.MISSING
                            else:
                                mask2[i] = defs.INVALID

                if not mask2.any():
                    mask2 = None

                data, mask = data2, mask2

            else:
                mask2 = NP("isnan", data)
                if mask is None:
                    mask = NP("array", mask2, defs.maskType)
                else:
                    mask = NP(NP("array", NP("logical_or", mask2, NP("fromiter", (m != 0 for m in mask), dtype=NP.dtype(bool), count=len(mask))), defs.maskType) * defs.MISSING)
                if not mask.any():
                    mask = None

        data, mask = self._checkValues(data, mask)
        data, mask = self._checkIntervals(data, mask)
        return DataColumn(self, data, mask)

    def _toDataColumn_dateTime(self, data, mask):
        data, mask = self._checkNumpy(data, mask, tryToCast=False)
        data, mask = self._checkNonNumpy(data, mask)

        data2 = NP("empty", len(data), dtype=self.dtype)
        mask2 = NP("zeros", len(data), dtype=defs.maskType)

        for i, x in enumerate(data):
            if (mask is not None and mask[i]) or (isinstance(x, float) and math.isnan(x)) or (isinstance(x, basestring) and x.upper() == "NAN"):
                data2[i] = defs.PADDING
                mask2[i] = defs.MISSING
            else:
                try:
                    data2[i] = self.stringToValue(x)
                except (ValueError, TypeError):
                    data2[i] = defs.PADDING
                    mask2[i] = defs.INVALID

        if not mask2.any():
            data, mask = data2, None
        else:
            data, mask = data2, mask2

        data, mask = self._checkValues(data, mask)
        data, mask = self._checkIntervals(data, mask)
        return DataColumn(self, data, mask)

    def _toDataColumn_dateTimeNumber(self, data, mask):
        dataColumn = self._toDataColumn_number(data, mask)
        data, mask = NP(NP(dataColumn.data * self._factor) + self._offset), dataColumn.mask

        data, mask = self._checkValues(data, mask)
        data, mask = self._checkIntervals(data, mask)
        return DataColumn(self, data, mask)

    ### fromDataColumn functions

    def _fromDataColumn_object(self, dataColumn):    
        if dataColumn.mask is None:
            return dataColumn.data
        else:
            output = NP("copy", dataColumn.data)
            for i, x in enumerate(dataColumn.mask):
                if x == defs.MISSING:
                    output[i] = defs.NAN
                elif x == defs.INVALID:
                    output[i] = None
            return output

    def _fromDataColumn(self, dataColumn):
        # enumeration uses less memory and, interestingly, a little less time than a list comprehension (80 ns instead of 100 ns per record)
        output = NP("empty", len(dataColumn), dtype=NP.dtype(object))
        if dataColumn.mask is None:
            for i, x in enumerate(dataColumn.data):
                output[i] = self.valueToPython(x)
        else:
            mask = dataColumn.mask
            for i, x in enumerate(dataColumn.data):
                if mask[i] == defs.VALID:
                    output[i] = self.valueToPython(x)
                elif mask[i] == defs.MISSING:
                    output[i] = defs.NAN
                else:
                    output[i] = None
        return output

    def _fromDataColumn_number(self, dataColumn):
        if dataColumn.mask is None:
            return NP("array", dataColumn.data, dtype=NP.dtype(object))
        else:
            output = NP("empty", len(dataColumn), dtype=NP.dtype(object))
            mask = dataColumn.mask
            for i, x in enumerate(dataColumn.data):
                if mask[i] == defs.VALID:
                    output[i] = x
                elif mask[i] == defs.MISSING:
                    output[i] = defs.NAN
                else:
                    output[i] = None
            return output
        
    def _fromDataColumn_dateTimeNumber(self, dataColumn):
        transformedData = NP(NP(dataColumn.data - self._offset) / float(self._factor))
        return self._fromDataColumn_number(DataColumn(self, transformedData, dataColumn.mask))

    def _fromDataColumn_timeSeconds(self, dataColumn):
        transformedData = NP(NP("mod", NP(dataColumn.data - self._offset), self._microsecondsPerDay) / float(self._factor))
        return self._fromDataColumn_number(DataColumn(self, transformedData, dataColumn.mask))

    ### stringToValue functions

    def _stringToValue_object(self, string):
        return string

    def _stringToValue_categorical(self, string):
        if not isinstance(string, basestring):
            raise TypeError("This FieldType requires a string, not \"%r\"" % type(string))
        try:
            internal = self._stringToValue[string]
        except KeyError:
            internal = self._addCategorical(string)
            if internal is None:
                raise ValueError("invalid value for categorical string: \"%s\"" % string)
        return internal

    def _stringToValue_ordinal(self, string):
        if not isinstance(string, basestring):
            raise TypeError("This FieldType requires a string, not \"%r\"" % type(string))
        try:
            internal = self._stringToValue[string]
        except KeyError:
            internal = self._addOrdinal(string)
            if internal is None:
                raise ValueError("invalid value for internal string: \"%s\"" % string)
        return internal

    def _stringToValue_string(self, string):
        return string

    def _stringToValue_integer(self, string):
        return NP.int64(string)

    def _stringToValue_float(self, string):
        return NP.float32(string)

    def _stringToValue_double(self, string):
        return NP.double(string)

    def _stringToValue_boolean(self, string):
        if string in ("true", "1"): return True
        elif string in ("false", "0"): return False
        else:
            raise ValueError("invalid literal for XML boolean: \"%s\"" % string)

    def _stringToValue_date(self, string):
        regex = re.match(self._iso8601_date, string)
        if regex is None:
            raise ValueError("invalid ISO 8601 date string: \"%s\"" % string)

        year = regex.group(1)
        month = regex.group(3)
        day = regex.group(5)
        
        try:
            if year is not None and month is not None and day is not None:
                dateTimeObject = datetime.datetime(int(year), int(month), int(day))

            elif year is not None and month is not None:
                dateTimeObject = datetime.datetime(int(year), int(month), 1)

            elif year is not None:
                dateTimeObject = datetime.datetime(int(year), 1, 1)

            else:
                raise ValueError

        except ValueError:
            raise ValueError("invalid ISO 8601 date string: \"%s\"" % string)

        td = dateTimeObject - self._dateTimeOrigin
        return NP.int64(td.days*86400 * self._dateTimeResolution)

    def _stringToValue_time(self, string):
        regex = re.match(self._iso8601_time, string)
        if regex is None:
            raise ValueError("invalid ISO 8601 time string: \"%s\"" % string)

        hour = regex.group(1)
        minute = regex.group(2)
        second = regex.group(4)
        subsecond = regex.group(5)
        timezone = regex.group(6)

        timezoneOffset = 0
        try:
            if hour is not None and minute is not None and second is not None:
                if subsecond is None:
                    microsecond = 0
                else:
                    microsecond = int(round(float(subsecond) * 1e6))
                dateTimeObject = datetime.datetime(1970, 1, 1, int(hour), int(minute), int(second), microsecond)

            elif hour is not None and minute is not None:
                if subsecond is not None:
                    raise ValueError
                dateTimeObject = datetime.datetime(1970, 1, 1, int(hour), int(minute))

            if timezone is not None:
                regex2 = re.match(self._timezone, timezone)
                if regex2 is not None:
                    sign, hourOffset, minuteOffset = regex2.groups()
                    timezoneOffset = ((int(hourOffset) * 60) + int(minuteOffset)) * 60 * self._dateTimeResolution   # microseconds
                    if sign == "-":
                        timezoneOffset *= -1

        except ValueError:
            raise ValueError("invalid ISO 8601 time string: \"%s\"" % string)

        td = dateTimeObject - self._dateTimeOrigin
        return NP.int64(td.seconds * self._dateTimeResolution + td.microseconds - timezoneOffset)
                
    def _stringToValue_dateTime(self, string):
        # accept all of the ISO 8601 standards as well as the variants in which:
        #     the literal "T" is replaced by a space " "
        #     the hyphen delimiters "-" are replaced by slashes "/"
        #     the timezone is not specified

        regex = re.match(self._iso8601, string)
        if regex is None:
            raise ValueError("invalid ISO 8601 dateTime string: \"%s\"" % string)

        year = regex.group(1)
        month = regex.group(3)
        day = regex.group(5)
        hour = regex.group(7)
        minute = regex.group(8)
        second = regex.group(10)
        subsecond = regex.group(11)
        timezone = regex.group(12)

        timezoneOffset = 0
        try:
            if year is not None and month is not None and day is not None and hour is not None and minute is not None:
                if second is not None:
                    if subsecond is None:
                        microsecond = 0
                    else:
                        microsecond = int(round(float(subsecond) * 1e6))
                    dateTimeObject = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), microsecond)
                else:
                    if subsecond is not None:
                        raise ValueError
                    dateTimeObject = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))

                if timezone is not None:
                    regex2 = re.match(self._timezone, timezone)
                    if regex2 is not None:
                        sign, hourOffset, minuteOffset = regex2.groups()
                        timezoneOffset = ((int(hourOffset) * 60) + int(minuteOffset)) * 60 * self._dateTimeResolution   # microseconds
                        if sign == "-":
                            timezoneOffset *= -1

            elif year is not None and month is not None and day is not None and hour is not None:
                raise ValueError

            elif year is not None and month is not None and day is not None:
                dateTimeObject = datetime.datetime(int(year), int(month), int(day))

            elif year is not None and month is not None:
                dateTimeObject = datetime.datetime(int(year), int(month), 1)

            elif year is not None:
                dateTimeObject = datetime.datetime(int(year), 1, 1)

            else:
                raise ValueError

        except ValueError:
            raise ValueError("invalid ISO 8601 dateTime string: \"%s\"" % string)

        td = dateTimeObject - self._dateTimeOrigin
        return NP.int64((td.days*86400 + td.seconds) * self._dateTimeResolution + td.microseconds - timezoneOffset)

    def _stringToValue_dateTimeNumber(self, string):
        return (NP.int64(string) * self._factor) + self._offset

    ### valueToString functions

    def _valueToString_object(self, value, displayValue=True):
        x = repr(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_categorical(self, value, displayValue=True):
        x = self._valueToString.get(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_ordinal(self, value, displayValue=True):
        x = self._valueToString.get(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_string(self, value, displayValue=True):
        if displayValue:
            return self._displayValue.get(value, value)
        else:
            return value

    def _valueToString_integer(self, value, displayValue=True):
        x = repr(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_float(self, value, displayValue=True):
        x = repr(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_double(self, value, displayValue=True):
        x = repr(value)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_boolean(self, value, displayValue=True):
        if value:
            x = "true"
        else:
            x = "false"
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_date(self, value, displayValue=True):
        d = self._dateTimeOrigin + datetime.timedelta(microseconds=long(value))
        x = "%04d-%02d-%02d" % (d.year, d.month, d.day)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_time(self, value, displayValue=True):
        microsecondsSinceMidnight = value % self._microsecondsPerDay
        hours, remainder = divmod(microsecondsSinceMidnight, 3600 * self._dateTimeResolution)
        minutes, remainder = divmod(remainder, 60 * self._dateTimeResolution)
        seconds, microseconds = divmod(remainder, self._dateTimeResolution)
        x = "%02d:%02d:%02d.%06dZ" % (hours, minutes, seconds, microseconds)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_dateTime(self, value, displayValue=True):
        d = self._dateTimeOrigin + datetime.timedelta(microseconds=long(value))
        x = "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ" % (d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond)
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_dateTimeNumber(self, value, displayValue=True):
        x = repr((value - self._offset) / float(self._factor))
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    def _valueToString_timeSeconds(self, value, displayValue=True):
        x = repr(((value - self._offset) % self._microsecondsPerDay) / float(self._factor))
        if displayValue:
            return self._displayValue.get(x, x)
        else:
            return x

    ### valueToPython functions

    def _valueToPython(self, value):
        return value

    def _valueToPython_date(self, value):
        d = self._dateTimeOrigin + datetime.timedelta(microseconds=long(value))
        return datetime.date(d.year, d.month, d.day)

    def _valueToPython_time(self, value):
        microsecondsSinceMidnight = value % self._microsecondsPerDay
        hours, remainder = divmod(microsecondsSinceMidnight, 3600 * self._dateTimeResolution)
        minutes, remainder = divmod(remainder, 60 * self._dateTimeResolution)
        seconds, microseconds = divmod(remainder, self._dateTimeResolution)
        return datetime.time(hours, minutes, seconds, microseconds)

    def _valueToPython_dateTime(self, value):
        return self._dateTimeOrigin + datetime.timedelta(microseconds=long(value))

    def _valueToPython_dateTimeNumber(self, value):
        return (value - self._offset) / float(self._factor)

    def _valueToPython_timeSeconds(self, value):
        return ((value - self._offset) % self._microsecondsPerDay) / float(self._factor)
