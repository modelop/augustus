#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
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

"""Define an XSD for PMML 4.1."""

###############################################################################
# This file represents the entire PMML 4.1 specification in Python as of RC 3 #
###############################################################################

from augustus.core.python3transition import *

# system includes
import string
import math
import datetime
import re
import sre_constants
## FIXME: Should we try/except isinf/isnan from math.py before using numpys?
## (numpy.isnan/isinf is new in Python 2.6)
import numpy
import itertools
import operator

try:
    from functools import reduce
except ImportError:
    pass

# local includes
from augustus.core.xmlbase import XMLValidationError, load_xsdType, load_xsdGroup, load_xsdElement, validateBoolean
from augustus.core.defs import Atom, IMMATURE, INVALID, MISSING, UNKNOWN, InvalidDataError, NameSpace, NameSpaceReadOnly
import augustus.core.xmlbase as xmlbase
from augustus.algorithms.eventweighting import COUNT, SUM1, SUMX, SUMXX, MIN, MAX

# used by the model consumers but attached to PMML objects for convenience
from augustus.core.extramath import MAXFLOAT, sqrt, atan, exp, log, floor, pi, erf, gammq, gammln

class PMMLValidationError(XMLValidationError): pass

# global numpy behavior for everything that uses pmml41.py
numpy.seterr(all="raise")

# base class for all PMML (the XML token "<PMML>" is "root" in Python)
class PMML(xmlbase.XML):
    """Base class for all PMML objects."""

    topTag = "PMML"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            pmmlName = (pmmlName for pmmlName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("PMML class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, pmmlName, *children, **attrib)

    def makeVerbose(self, dataContext=None):
        for child in self.children:
            if isinstance(child, PMML):
                if hasattr(child, "dataContext"):
                    child.makeVerbose(child.dataContext)
                else:
                    child.makeVerbose(dataContext)

# use this to avoid an expensive reverse-lookup
def newInstance(tag, attrib={}, children=[], base=PMML):
    output = base.classMap[tag].__new__(base.classMap[tag])
    output.tag = tag
    output.attrib = dict(attrib)
    output.children = list(children)
    return output

# a very useful test function!
def nonExtension(x):
    """Return True if *not* an <Extension> block."""
    return not isinstance(x, Extension)

############################################################################### PMML field value types

def boolCheck(value):
    try:
        return validateBoolean(value)
    except PMMLValidationError:
        raise ValueError("\"%s\" is not a boolean (only \"true\", \"false\", \"1\", or \"0\")" % value)

class UTCTimeZoneType(datetime.tzinfo):
    zero = datetime.timedelta(0)
    def utcoffset(self, dt):
        return self.zero
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return self.zero

UTCTimeZone = UTCTimeZoneType()

# This was introduced in Python 2.7, so I need to implement it by hand for backward-compatibility.
def datetime_total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

class DateTimeType(object):
    iso8601 = re.compile("^(-?[0-9]{4})[-/]([0-9]{2})[-/]([0-9]{2})[T ]([0-9]{2}):([0-9]{2}):([0-9]{2})(\.[0-9]+)?([-+][0-9]{2}:[0-9]{2})?$")
    tz = re.compile("([-+])([0-9]{2}):([0-9]{2})")

    class TimeZone(datetime.tzinfo):
        zero = datetime.timedelta(0)
        def __init__(self, hh, mm, name):
            self.offset = datetime.timedelta(hours=hh, minutes=mm)
            self.name = name
        def utcoffset(self, dt):
            return self.offset
        def tzname(self, dt):
            return self.name
        def dst(self, dt):
            return self.zero
        def __repr__(self):
            return self.name

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return
        
        try:
            year, month, day, hour, minute, second, microsecond, timezone = re.match(self.iso8601, value).groups()
        except AttributeError:
            raise ValueError("dateTime value \"%s\" not in ISO-8601 format for date and time" % value)

        if microsecond is None:
            microsecond = 0
        else:
            microsecond = int(round(float(microsecond) * 1e6))

        if timezone is None:
            timezone = UTCTimeZone
        else:
            sign, hh, mm = re.match(self.tz, timezone).groups()
            hh, mm = int(hh), int(mm)
            if sign == "-":
                hh = -hh
                mm = -mm
            timezone = self.TimeZone(hh, mm, timezone)

        # FIXME: Python will fail if the year is negative, but XML demands it...
        self.t = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), microsecond, timezone)

    def __hash__(self):
        return hash((self.__class__, self.t))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.t.isoformat())

    def __str__(self):
        return self.t.isoformat()

    def __cmp__(self, other):
        if not isinstance(other, DateTimeType): return 1
        return cmp(self.t, other.t)

    def __eq__(self, other):
        if not isinstance(other, DateTimeType): return False
        return self.t == other.t

    def __ne__(self, other):
        return not (self == other)

    def strftime(self, format):
        return self.t.strftime(format)

    def __int__(self):
        return int(round(datetime_total_seconds(self.t - datetime.datetime(1970, 1, 1, 0, 0, 0, 0, self.t.tzinfo))))

    def __float__(self):
        return float(int(self))

class DateType(DateTimeType):
    iso8601 = re.compile("^(-?[0-9]{4})[-/]([0-9]{2})[-/]([0-9]{2})$")

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return

        try:
            year, month, day = re.match(self.iso8601, value).groups()
        except AttributeError:
            raise ValueError("date value \"%s\" not in ISO-8601 format for just date" % value)

        self.t = datetime.date(int(year), int(month), int(day))

    def __int__(self):
        return (self.t - datetime.date(1970, 1, 1)).days * 24*60*60

class TimeType(DateTimeType):
    iso8601 = re.compile("^([0-9]{2}):([0-9]{2}):([0-9]{2})(\.[0-9]+)?([-+][0-9]{2}:[0-9]{2})?$")

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            self.days = 0
            return

        try:
            hour, minute, second, microsecond, timezone = re.match(self.iso8601, value).groups()
        except AttributeError:
            raise ValueError("time value \"%s\" not in ISO-8601 format for just time" % value)

        if microsecond is None:
            microsecond = 0
        else:
            microsecond = int(round(float(microsecond) * 1e6))

        if timezone is None:
            timezone = UTCTimeZone
        else:
            sign, hh, mm = re.match(self.tz, timezone).groups()
            hh, mm = int(hh), int(mm)
            if sign == "-":
                hh = -hh
                mm = -mm
            timezone = self.TimeZone(hh, mm, timezone)

        self.t = datetime.time(int(hour), int(minute), int(second), microsecond, timezone)
        self.days = 0

    def __hash__(self):
        return hash((self.__class__, self.days, self.t))

    def __repr__(self):
        if self.days == 0:
            return "<%s %s>" % (self.__class__.__name__, self.t.isoformat())
        else:
            return "<%s %d days, %s>" % (self.__class__.__name__, self.days, self.t.isoformat())

    def __str__(self):
        return "%02d:%02d:%02d" % (self.days*24 + self.t.hour, self.t.minute, self.t.second)

    def __cmp__(self, other):
        if not isinstance(other, TimeType): return 1
        return cmp(self.days * 24*60*60 + self.hours*60*60 + self.minutes*60 + self.seconds,
                   other.days * 24*60*60 + other.hours*60*60 + other.minutes*60 + other.seconds)

    def __eq__(self, other):
        if not isinstance(other, TimeType): return False
        return self.t == other.t and self.days == other.days

    def __int__(self):
        return self.t.hour*60*60 + self.t.minute*60 + self.t.second

class TimeTypeSeconds(TimeType):
    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            self.days = 0
            return

        value = int(value)
        if value < 0:
            raise ValueError("%s %d should not be negative" % (self.__class__.__name__, value))

        minutes, seconds = divmod(value, 60)
        hours, minutes = divmod(minutes, 60)

        self.days = 0
        if hours >= 24:
            self.days, hours = divmod(hours, 24)

        self.t = datetime.time(hours, minutes, seconds)

class DateDaysSince(DateType):
    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return

        value = int(value)
        self.t = self.reference + datetime.timedelta(days=value)

    def __int__(self):
        return (self.t - self.reference).days

    def __str__(self):
        return str(int(self))

    def __repr__(self):
        return "<%s %d (%s)>" % (self.__class__.__name__, int(self), self.t.strftime("%b %d %Y"))

    def __add__(self, other):
        output = self.__class__.__new__(self.__class__)
        output.t = self.t + datetime.timedelta(days=other)
        return output

    def __radd__(self, other):
        output = self.__class__.__new__(self.__class__)
        output.t = self.t + datetime.timedelta(days=other)
        return output

    def __sub__(self, other):
        output = self.__class__.__new__(self.__class__)
        try:
            output.t = self.t - datetime.timedelta(days=other)
        except TypeError:
            diff = (self.t - other.t)
            return diff.days
        return output

    def __rsub__(self, other):
        output = self.__class__.__new__(self.__class__)
        try:
            output.t = self.t - datetime.timedelta(days=other)
        except TypeError:
            return diff.days
        return output

class DateDaysSince0(DateDaysSince):
    # unfortunately, Python doesn't support year zero (it's not really in the proleptic Gregorian calendar)
    reference = datetime.date(1, 1, 1)
    daysInYear0 = 366                 # year zero would have been a leap year

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return

        value = int(value)
        if value < self.daysInYear0:
            raise ValueError("%s should be at least %d, not %d" % (self.__class__.__name__, self.daysInYear0, value))

        self.t = self.reference + datetime.timedelta(days=(value - self.daysInYear0))

    def __int__(self):
        return (self.t - self.reference).days + self.daysInYear0

class DateDaysSince1960(DateDaysSince):
    reference = datetime.date(1960, 1, 1)

class DateDaysSince1970(DateDaysSince):
    reference = datetime.date(1970, 1, 1)

class DateDaysSince1980(DateDaysSince):
    reference = datetime.date(1980, 1, 1)

class DateTimeSecondsSince(DateTimeType):
    secondsInDay = 24 * 60 * 60

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return

        value = int(value)
        self.t = self.reference + datetime.timedelta(seconds=value)

    def __int__(self):
        diff = (self.t - self.reference)
        return diff.days * self.secondsInDay + diff.seconds

    def __float__(self):
        diff = (self.t - self.reference)
        return diff.days * self.secondsInDay + diff.seconds + 1e-6 * diff.microseconds

    def __str__(self):
        return str(int(self))

    def __repr__(self):
        return "<%s %d (%s)>" % (self.__class__.__name__, int(self), self.t.strftime("%b %d %Y, %H:%M:%S"))

    def __add__(self, other):
        output = self.__class__.__new__(self.__class__)
        output.t = self.t + datetime.timedelta(seconds=other)
        return output

    def __radd__(self, other):
        output = self.__class__.__new__(self.__class__)
        output.t = self.t + datetime.timedelta(seconds=other)
        return output

    def __sub__(self, other):
        output = self.__class__.__new__(self.__class__)
        try:
            output.t = self.t - datetime.timedelta(seconds=other)
        except TypeError:
            diff = (self.t - other.t)
            return diff.days * self.secondsInDay + diff.seconds + 1e-6 * diff.microseconds
        return output

    def __rsub__(self, other):
        output = self.__class__.__new__(self.__class__)
        try:
            output.t = self.t - datetime.timedelta(seconds=other)
        except TypeError:
            diff = (self.t - other.t)
            return diff.days * self.secondsInDay + diff.seconds + 1e-6 * diff.microseconds
        return output

class DateTimeSecondsSince0(DateTimeSecondsSince):
    # unfortunately, Python doesn't support year zero (it's not really in the proleptic Gregorian calendar)
    reference = datetime.datetime(1, 1, 1, 0, 0, 0, 0, UTCTimeZone)
    secondsInYear0 = 366 * 24 * 60 * 60    # year zero would have been a leap year

    def __init__(self, value):
        if isinstance(value, DateTimeType):
            self.t = value.t
            return

        value = int(value)
        if value < self.secondsInYear0:
            raise ValueError("%s should be at least %d, not %d" % (self.__class__.__name__, self.secondsInYear0, value))

        if value < 0:
            raise ValueError("%s %d should not be negative for DateTimeSecondsSince[0]" % (self.__class__.__name__, value))

        self.t = self.reference + datetime.timedelta(seconds=(value - self.secondsInYear0))

    def __int__(self):
        diff = (self.t - self.reference)
        return diff.days * self.secondsInDay + diff.seconds + self.secondsInYear0

    def __float__(self):
        diff = (self.t - self.reference)
        return diff.days * self.secondsInDay + diff.seconds + self.secondsInYear0 + 1e-6 * diff.microseconds

class DateTimeSecondsSince1960(DateTimeSecondsSince):
    reference = datetime.datetime(1960, 1, 1, 0, 0, 0, 0, UTCTimeZone)

class DateTimeSecondsSince1970(DateTimeSecondsSince):
    reference = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, UTCTimeZone)

class DateTimeSecondsSince1980(DateTimeSecondsSince):
    reference = datetime.datetime(1980, 1, 1, 0, 0, 0, 0, UTCTimeZone)

pmmlBuiltinType = {
    "string": str,
    "integer": int,
    "float": float,
    "double": float,
    "boolean": boolCheck,
    "date": DateType,
    "time": TimeType,
    "dateTime": DateTimeType,
    "dateDaysSince[0]": DateDaysSince0,
    "dateDaysSince[1960]": DateDaysSince1960,
    "dateDaysSince[1970]": DateDaysSince1970,
    "dateDaysSince[1980]": DateDaysSince1980,
    "timeSeconds": TimeTypeSeconds,
    "dateTimeSecondsSince[0]": DateTimeSecondsSince0,
    "dateTimeSecondsSince[1960]": DateTimeSecondsSince1960,
    "dateTimeSecondsSince[1970]": DateTimeSecondsSince1970,
    "dateTimeSecondsSince[1980]": DateTimeSecondsSince1980,
    }

pmmlBuiltinFloats = ["float", "double"]

############################################################################### make casting functions to strongly type the data

def castFunction(optype, dataType, pmmlIntervals, pmmlValues, isCyclic):
    if len(pmmlIntervals) > 0 and len(pmmlValues) > 0:
        raise NotImplementedError("DataFields with both Intervals and Values have not been implemented")

    # a special case: string-based ordinals need to be objects, so that we can do inequalities on them
    if optype == "ordinal" and dataType == "string":
        # each string-based ordinal gets its own class, generated on-the-fly right here
        class OrdinalStringType(object):
            values = [v.attrib["value"] for v in pmmlValues]
            displayValues = [v.attrib.get("displayValue", None) for v in pmmlValues]
            properties = [v.attrib.get("property", "valid") for v in pmmlValues]

            def __init__(self, value):
                try:
                    self.index = self.values.index(str(value))
                except ValueError:
                    raise ValueError("Cannot interpret \"%s\" as an ordinal-string because it is not in the list: %s" % (value, repr(self.values)))

                self.valueshash = hash(tuple(self.values))

            def __repr__(self):
                return "<\"%s\" from %s>" % (self.values[self.index], self.values)

            def __hash__(self):
                return hash(self.values[self.index])

            def __int__(self):
                return self.index

            def __float__(self):
                return float(int(self))

            def __eq__(self, other):
                return str(self) == str(other)

            def __ne__(self, other):
                return not (self == other)

            def __str__(self):
                return self.values[self.index]

            def __cmp__(self, other):
                if (self.__class__.__name__ != other.__class__.__name__) or (self.valueshash != other.valueshash):
                    raise TypeError("Cannot compare different types")
                else:
                    return cmp(self.index, other.index)

        return OrdinalStringType

    # a special case: continuous with intervals
    elif optype == "continuous" and len(pmmlIntervals) > 0:
        def intervalCheck(value):
            value = pmmlBuiltinType[dataType](value)
            for interval in pmmlIntervals:
                if interval.contains(value):
                    return value
            return INVALID

        return intervalCheck

    # a special case: continuous with values
    elif optype == "continuous" and len(pmmlValues) > 0:
        for val in pmmlValues:
            try:
                val.attrib["value"] = pmmlBuiltinType[dataType](val.attrib["value"])

                if dataType in pmmlBuiltinFloats:
                    if numpy.isnan(val.attrib["value"]) or numpy.isinf(val.attrib["value"]):
                        raise ValueError("NaN and Inf are not allowed")

            except ValueError, err:
                raise PMMLValidationError("Could not cast Value's \"%s\" as %s: %s" % (val.attrib["value"], dataType, str(err)))

        def valueCheck(value):
            value = pmmlBuiltinType[dataType](value)
            for val in pmmlValues:
                if value == val.attrib["value"]:
                    return value
            return INVALID

        return valueCheck

    # a special case: cyclic ordinals
    elif isCyclic and optype == "ordinal" and dataType == "integer":
        if len(pmmlValues) != 2:
            raise PMMLValidationError("Cyclic ordinal types must have two Values (first and last in the repeating sequence)")

        try:
            int(pmmlValues[0].attrib["value"])
            int(pmmlValues[1].attrib["value"])
        except ValueError:
            raise PMMLValidationError("Values for cyclic ordinal type must be integers")

        class CyclicOrdinalType(object):
            first = int(pmmlValues[0].attrib["value"])
            last = int(pmmlValues[1].attrib["value"])

            def _cycle(self, v):
                return (v - self.first) % (self.last - self.first + 1) + self.first

            def __init__(self, value):
                self.value = self._cycle(int(value))

            def __int__(self):
                return int(self.value)

            def __repr__(self):
                return "<%d cyclic between %d and %d>" % (self.value, self.first, self.last)

            def __add__(self, *args):
                return self._cycle(self.value.__add__(*args))
            def __sub__(self, *args):
                return self._cycle(self.value.__sub__(*args))
            def __mul__(self, *args):
                return self._cycle(self.value.__mul__(*args))
            def __mod__(self, *args):
                return self._cycle(self.value.__mod__(*args))
            def __divmod__(self, *args):
                return self._cycle(self.value.__divmod__(*args))
            def __pow__(self, *args):
                return self._cycle(self.value.__pow__(*args))
            def __lshift__(self, *args):
                return self._cycle(self.value.__lshift__(*args))
            def __rshift__(self, *args):
                return self._cycle(self.value.__rshift__(*args))
            def __div__(self, *args):
                return self._cycle(self.value.__div__(*args))
            def __radd__(self, *args):
                return self._cycle(self.value.__radd__(*args))
            def __rsub__(self, *args):
                return self._cycle(self.value.__rsub__(*args))
            def __rmul__(self, *args):
                return self._cycle(self.value.__rmul__(*args))
            def __rdiv__(self, *args):
                return self._cycle(self.value.__rdiv__(*args))
            def __rmod__(self, *args):
                return self._cycle(self.value.__rmod__(*args))
            def __rdivmod__(self, *args):
                return self._cycle(self.value.__rdivmod__(*args))
            def __rpow__(self, *args):
                return self._cycle(self.value.__rpow__(*args))
            def __rlshift__(self, *args):
                return self._cycle(self.value.__rlshift__(*args))
            def __rrshift__(self, *args):
                return self._cycle(self.value.__rrshift__(*args))
            def __neg__(self, *args):
                return self._cycle(self.value.__neg__(*args))
            def __pos__(self, *args):
                return self._cycle(self.value.__pos__(*args))
            def __abs__(self, *args):
                return self._cycle(self.value.__abs__(*args))
            def __invert__(self, *args):
                return self._cycle(self.value.__invert__(*args))
            def __str__ (self):
                return str(self.value)
            def __repr__ (self):
                return "<CyclicOrdinal %d >" % self.value

        return CyclicOrdinalType

    # a special case: cyclic continuous
    elif isCyclic and optype == "continuous":
        if len(pmmlIntervals) != 1 or "leftMargin" not in pmmlIntervals[0].attrib or "rightMargin" not in pmmlIntervals[0].attrib:
            raise PMMLValidationError("Cyclic continuous types must have one finite Interval")

        class CyclicContinuousType(object):
            low = pmmlIntervals[0].attrib["leftMargin"]
            high = pmmlIntervals[0].attrib["rightMargin"]

            def _cycle(self, v):
                while v < self.low:
                    v += (self.high - self.low)
                while v >= self.high:
                    v -= (self.high - self.low)
                return v

            def __init__(self, value):
                self.value = self._cycle(pmmlBuiltinType[dataType](value))

            def __float__(self):
                return float(self.value)
            def __int__(self):
                return int(self.value)

            def __repr__(self):
                return "<%g cyclic between %g and %g>" % (self.value, self.low, self.high)

            def __add__(self, *args):
                return self._cycle(self.value.__add__(*args))
            def __sub__(self, *args):
                return self._cycle(self.value.__sub__(*args))
            def __mul__(self, *args):
                return self._cycle(self.value.__mul__(*args))
            def __floordiv__(self, *args):
                return self._cycle(self.value.__floordiv__(*args))
            def __mod__(self, *args):
                return self._cycle(self.value.__mod__(*args))
            def __divmod__(self, *args):
                return self._cycle(self.value.__divmod__(*args))
            def __pow__(self, *args):
                return self._cycle(self.value.__pow__(*args))
            def __lshift__(self, *args):
                return self._cycle(self.value.__lshift__(*args))
            def __rshift__(self, *args):
                return self._cycle(self.value.__rshift__(*args))
            def __div__(self, *args):
                return self._cycle(self.value.__div__(*args))
            def __truediv__(self, *args):
                return self._cycle(self.value.__truediv__(*args))
            def __radd__(self, *args):
                return self._cycle(self.value.__radd__(*args))
            def __rsub__(self, *args):
                return self._cycle(self.value.__rsub__(*args))
            def __rmul__(self, *args):
                return self._cycle(self.value.__rmul__(*args))
            def __rdiv__(self, *args):
                return self._cycle(self.value.__rdiv__(*args))
            def __rtruediv__(self, *args):
                return self._cycle(self.value.__rtruediv__(*args))
            def __rfloordiv__(self, *args):
                return self._cycle(self.value.__rfloordiv__(*args))
            def __rmod__(self, *args):
                return self._cycle(self.value.__rmod__(*args))
            def __rdivmod__(self, *args):
                return self._cycle(self.value.__rdivmod__(*args))
            def __rpow__(self, *args):
                return self._cycle(self.value.__rpow__(*args))
            def __rlshift__(self, *args):
                return self._cycle(self.value.__rlshift__(*args))
            def __rrshift__(self, *args):
                return self._cycle(self.value.__rrshift__(*args))
            def __neg__(self, *args):
                return self._cycle(self.value.__neg__(*args))
            def __pos__(self, *args):
                return self._cycle(self.value.__pos__(*args))
            def __abs__(self, *args):
                return self._cycle(self.value.__abs__(*args))
            def __invert__(self, *args):
                return self._cycle(self.value.__invert__(*args))
            def __str__ (self):
                return str(self.value)
            def __repr__ (self):
                return "<CyclicContinuous %d >" % self.value

        return CyclicContinuousType

    # the usual case: type is a string that refers to a built-in
    else:
        return pmmlBuiltinType[dataType]

def treatmentFunction(miningField, cast):
    if "invalidValueTreatment" in miningField.attrib:
        if miningField.attrib["invalidValueTreatment"] == "asIs":
            invalidValueTreatment = None
        elif miningField.attrib["invalidValueTreatment"] == "asMissing":
            invalidValueTreatment = MISSING
        elif miningField.attrib["invalidValueTreatment"] == "returnInvalid":
            invalidValueTreatment = INVALID
    else:
        invalidValueTreatment = None
    
    if "missingValueReplacement" in miningField.attrib:
        try:
            missingValueReplacement = cast(miningField.attrib["missingValueReplacement"])
        except ValueError, err:
            raise PMMLValidationError("MiningField missingValueReplacement \"%s\" cannot be cast to the appropriate type: %s" % (miningField.attrib["missingValueReplacement"], str(err)))
    else:
        missingValueReplacement = None

    if "outliers" in miningField.attrib:
        if miningField.attrib["outliers"] == "asIs":
            outlierTreatment = None
        elif miningField.attrib["outliers"] == "asMissingValues":
            lowValue = miningField.attrib["lowValue"]
            highValue = miningField.attrib["highValue"]
            outlierTreatment = lambda x: MISSING if x < lowValue else MISSING if x > highValue else x
        elif miningField.attrib["outliers"] == "asExtremeValues":
            lowValue = miningField.attrib["lowValue"]
            highValue = miningField.attrib["highValue"]
            outlierTreatment = lambda x: lowValue if x < lowValue else highValue if x > highValue else x
    else:
        outlierTreatment = None

    # this looks redundant, but by putting more "don't do anything" checks here, we save CPU in the event-by-event loop

    if invalidValueTreatment is not None and missingValueReplacement is not None and outlierTreatment is not None:
        return lambda x: invalidValueTreatment if x is INVALID else missingValueReplacement if x is MISSING else outlierTreatment(x)

    if invalidValueTreatment is not None and missingValueReplacement is not None and outlierTreatment is None:
        return lambda x: invalidValueTreatment if x is INVALID else missingValueReplacement if x is MISSING else x

    if invalidValueTreatment is not None and missingValueReplacement is None and outlierTreatment is not None:
        return lambda x: invalidValueTreatment if x is INVALID else outlierTreatment(x)

    if invalidValueTreatment is not None and missingValueReplacement is None and outlierTreatment is None:
        return lambda x: invalidValueTreatment if x is INVALID else x

    if invalidValueTreatment is None and missingValueReplacement is not None and outlierTreatment is not None:
        return lambda x: missingValueReplacement if x is MISSING else outlierTreatment(x)

    if invalidValueTreatment is None and missingValueReplacement is not None and outlierTreatment is None:
        return lambda x: missingValueReplacement if x is MISSING else x

    if invalidValueTreatment is None and missingValueReplacement is None and outlierTreatment is not None:
        return outlierTreatment

    if invalidValueTreatment is None and missingValueReplacement is None and outlierTreatment is None:
        return lambda x: x

class DataContext(object):
    def __init__(self, parent, dataDictionary, transformationDictionary, miningSchema, localTransformations, functionName):
        self.parent = parent
        self.dataDictionary = dataDictionary
        self.transformationDictionary = transformationDictionary
        self.miningSchema = miningSchema
        self.localTransformations = localTransformations
        self.functionName = functionName
        self.priorOverrides = []

        # the MiningSchema and the TransformationDictionary must be disjoint
        if self.transformationDictionary is not None:
            conflict = set(self.miningSchema.miningFields.keys()).intersection(self.transformationDictionary.derivedFields.keys())
            if len(conflict) > 0:
                raise PMMLValidationError("MiningSchema field names and TransformationDictionary field names conflict: %s" % repr(conflict))

        # the MiningSchema and the LocalTransformations must be disjoint
        if self.localTransformations is not None:
            conflict = set(self.miningSchema.miningFields.keys()).intersection(self.localTransformations.derivedFields.keys())
            if len(conflict) > 0:
                raise PMMLValidationError("MiningSchema field names and LocalTransformations field names conflict: %s" % repr(conflict))

        # the TransformationDictionary and the LocalTransformations must be disjoint
        if self.transformationDictionary is not None and self.localTransformations is not None:
            conflict = set(self.transformationDictionary.derivedFields.keys()).intersection(self.localTransformations.derivedFields.keys())
            if len(conflict) > 0:
                raise PMMLValidationError("TransformationDictionary field names and LocalTransformations field names conflict: %s" % repr(conflict))

        # FieldRefs in the TransformationDictionary must be in this MiningSchema context
        if self.transformationDictionary is not None:
            context = self.names()
            for derivedField in self.transformationDictionary.matches(DerivedField):
                for fieldRef in derivedField.matches(FieldRef, maxdepth=None):
                    if fieldRef.attrib["field"] not in context:
                        raise PMMLValidationError("FieldRef \"%s\" in the TransformationDictionary (DerivedField %s) is not accessible from one of the MiningSchema contexts (%s)" % (fieldRef.attrib["field"], derivedField.attrib["name"], ", ".join(context)))

        self.derivedFields = {}
        self.cast = dict(self.miningSchema.cast)
        self.treatment = dict(self.miningSchema.treatment)
        self.optype = dict(self.miningSchema.optype)
        self.dataType = dict(self.miningSchema.dataType)

        if self.transformationDictionary is not None:
            self.derivedFields.update(self.transformationDictionary.derivedFields)
            self.cast.update(self.transformationDictionary.cast)
            self.optype.update(self.transformationDictionary.optype)
            self.dataType.update(self.transformationDictionary.dataType)

        if self.localTransformations is not None:
            self.derivedFields.update(self.localTransformations.derivedFields)
            self.cast.update(self.localTransformations.cast)
            self.optype.update(self.localTransformations.optype)
            self.dataType.update(self.localTransformations.dataType)

        parent = self.parent
        while isinstance(parent, DataContext):
            for f in parent.cast:
                if f not in self.cast:
                    self.cast[f] = parent.cast[f]
            for f in parent.treatment:
                if f not in self.treatment:
                    self.treatment[f] = parent.treatment[f]
            for f in parent.optype:
                if f not in self.optype:
                    self.optype[f] = parent.optype[f]
            for f in parent.dataType:
                if f not in self.dataType:
                    self.dataType[f] = parent.dataType[f]
            parent = parent.parent

    def names(self, usageTypes=("active",)):
        output = []
        for name, field in self.miningSchema.miningFields.items():
            if field.usageType in usageTypes:
                output.append(name)

        if self.transformationDictionary is not None:
            output.extend(self.transformationDictionary.derivedFields)
        if self.localTransformations is not None:
            output.extend(self.localTransformations.derivedFields)
        return output

    def allDerivedFields(self, usageTypes=("active",)):
        fields = dict(self.derivedFields)
        this = self
        while isinstance(this.parent, DataContext):
            for name, value in this.parent.derivedFields.items():
                if name in this.miningSchema.miningFields.keys() and this.miningSchema.miningFields[name].usageType in usageTypes:
                    fields[name] = value
            this = this.parent
        return fields

    def contextString(self, derived=False):
        if derived:
            names = self.allDerivedFields().keys()
        else:
            names = self.names()
        names.sort()
        output = ", ".join(names)
        while len(output) > 50:
            names.pop()
            if hasattr(self, "override"):
                output = ", ".join([
                    ":".join([str(name), str(self.get(name))]) for name in names]) + "..."
            else:
                output = ", ".join([name for name in names]) + "..."
        return "%s" % str(output)

    def clear(self):
        self.got = {}  # clear the cache

        # back-doors to temporarily change behavior
        self.override = {}
        self.cache = True
    
    def setOverride(self, override, cache):
        if len(self.override) > 0:
            self.priorOverrides.append((self.override, self.cache))
        self.override = override
        self.cache = cache
        if isinstance(self.parent, DataContext):
            self.parent.setOverride(override, cache)

    def releaseOverride(self):
        if len(self.priorOverrides) > 0:
            self.override, self.cache = self.priorOverrides.pop()
        else:
            self.override = {}
            self.cache = True
        if isinstance(self.parent, DataContext):
            self.parent.releaseOverride()
        
    def get(self, field):
        if field in self.override:
            return self.override[field]

        if self.cache and field in self.got:
            return self.got[field]

        if field in self.derivedFields:
            derivedField = self.derivedFields[field]
            try:
                value = derivedField.expression.evaluate(self.get)
            except InvalidDataError:  # jump to the end of the calculation once you get INVALID
                value = INVALID

        # this is not a derived field
        else:
            # get it from the parent
            value = self.parent.get(field)

        if value not in (MISSING, INVALID):
            try:
                cast = self.cast[field]
            except KeyError:
                value = MISSING
            else:
                try:
                    value = self.cast[field](value)
                except ValueError:
                    value = INVALID

        if field in self.treatment:
            value = self.treatment[field](value)

        if self.cache:
            self.got[field] = value

        return value

############################################################################### PMML types

PMML.xsdType["MULTIPLE-MODEL-METHOD"] = load_xsdType("""
    <xs:simpleType name="MULTIPLE-MODEL-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="majorityVote" />
            <xs:enumeration value="weightedMajorityVote" />
            <xs:enumeration value="average" />
            <xs:enumeration value="weightedAverage" />
            <xs:enumeration value="median" />
            <xs:enumeration value="max" />
            <xs:enumeration value="sum" />
            <xs:enumeration value="selectFirst" />
            <xs:enumeration value="selectAll" />
            <xs:enumeration value="modelChain" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["SVM-CLASSIFICATION-METHOD"] = load_xsdType("""
    <xs:simpleType name="SVM-CLASSIFICATION-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="OneAgainstAll" />
            <xs:enumeration value="OneAgainstOne" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["SVM-REPRESENTATION"] = load_xsdType("""
    <xs:simpleType name="SVM-REPRESENTATION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="SupportVectors" />
            <xs:enumeration value="Coefficients" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["VECTOR-ID"] = load_xsdType("""
    <xs:simpleType name="VECTOR-ID">
        <xs:restriction base="xs:string" />
    </xs:simpleType>
    """)

PMML.xsdType["COMPARE-FUNCTION"] = load_xsdType("""
    <xs:simpleType name="COMPARE-FUNCTION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="absDiff" />
            <xs:enumeration value="gaussSim" />
            <xs:enumeration value="delta" />
            <xs:enumeration value="equal" />
            <xs:enumeration value="table" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["REGRESSIONNORMALIZATIONMETHOD"] = load_xsdType("""
    <xs:simpleType name="REGRESSIONNORMALIZATIONMETHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="none" />
            <xs:enumeration value="simplemax" />
            <xs:enumeration value="softmax" />
            <xs:enumeration value="logit" />
            <xs:enumeration value="probit" />
            <xs:enumeration value="cloglog" />
            <xs:enumeration value="exp" />
            <xs:enumeration value="loglog" />
            <xs:enumeration value="cauchit" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["RESULT-FEATURE"] = load_xsdType("""
    <xs:simpleType name="RESULT-FEATURE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="predictedValue" />
            <xs:enumeration value="predictedDisplayValue" />
            <xs:enumeration value="transformedValue" />
            <xs:enumeration value="decision" />
            <xs:enumeration value="probability" />
            <xs:enumeration value="affinity" />
            <xs:enumeration value="residual" />
            <xs:enumeration value="standardError" />
            <xs:enumeration value="clusterId" />
            <xs:enumeration value="clusterAffinity" />
            <xs:enumeration value="entityId" />
            <xs:enumeration value="entityAffinity" />
            <xs:enumeration value="warning" />
            <xs:enumeration value="ruleValue" />
            <xs:enumeration value="reasonCode" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["RULE-FEATURE"] = load_xsdType("""
    <xs:simpleType name="RULE-FEATURE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="antecedent" />
            <xs:enumeration value="consequent" />
            <xs:enumeration value="rule" />
            <xs:enumeration value="ruleId" />
            <xs:enumeration value="confidence" />
            <xs:enumeration value="support" />
            <xs:enumeration value="lift" />
            <xs:enumeration value="leverage" />
            <xs:enumeration value="affinity" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["ACTIVATION-FUNCTION"] = load_xsdType("""
    <xs:simpleType name="ACTIVATION-FUNCTION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="threshold" />
            <xs:enumeration value="logistic" />
            <xs:enumeration value="tanh" />
            <xs:enumeration value="identity" />
            <xs:enumeration value="exponential" />
            <xs:enumeration value="reciprocal" />
            <xs:enumeration value="square" />
            <xs:enumeration value="Gauss" />
            <xs:enumeration value="sine" />
            <xs:enumeration value="cosine" />
            <xs:enumeration value="Elliott" />
            <xs:enumeration value="arctan" />
            <xs:enumeration value="radialBasis" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["NN-NORMALIZATION-METHOD"] = load_xsdType("""
    <xs:simpleType name="NN-NORMALIZATION-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="none" />
            <xs:enumeration value="simplemax" />
            <xs:enumeration value="softmax" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["NN-NEURON-ID"] = load_xsdType("""
    <xs:simpleType name="NN-NEURON-ID">
        <xs:restriction base="xs:string" />
    </xs:simpleType>
    """)

PMML.xsdType["NN-NEURON-IDREF"] = load_xsdType("""
    <xs:simpleType name="NN-NEURON-IDREF">
        <xs:restriction base="xs:string" />
    </xs:simpleType>
    """)

PMML.xsdType["TIMESERIES-ALGORITHM"] = load_xsdType("""
    <xs:simpleType name="TIMESERIES-ALGORITHM">
        <xs:restriction base="xs:string">
            <xs:enumeration value="ARIMA" />
            <xs:enumeration value="ExponentialSmoothing" />
            <xs:enumeration value="SeasonalTrendDecomposition" />
            <xs:enumeration value="SpectralAnalysis" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["TIMESERIES-USAGE"] = load_xsdType("""
    <xs:simpleType name="TIMESERIES-USAGE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="original" />
            <xs:enumeration value="logical" />
            <xs:enumeration value="prediction" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["TIME-ANCHOR"] = load_xsdType("""
    <xs:simpleType name="TIME-ANCHOR">
        <xs:restriction base="xs:string">
            <xs:enumeration value="dateTimeMillisecondsSince[0]" />
            <xs:enumeration value="dateTimeMillisecondsSince[1960]" />
            <xs:enumeration value="dateTimeMillisecondsSince[1970]" />
            <xs:enumeration value="dateTimeMillisecondsSince[1980]" />
            <xs:enumeration value="dateTimeSecondsSince[0]" />
            <xs:enumeration value="dateTimeSecondsSince[1960]" />
            <xs:enumeration value="dateTimeSecondsSince[1970]" />
            <xs:enumeration value="dateTimeSecondsSince[1980]" />
            <xs:enumeration value="dateDaysSince[0]" />
            <xs:enumeration value="dateDaysSince[1960]" />
            <xs:enumeration value="dateDaysSince[1970]" />
            <xs:enumeration value="dateDaysSince[1980]" />
            <xs:enumeration value="dateMonthsSince[0]" />
            <xs:enumeration value="dateMonthsSince[1960]" />
            <xs:enumeration value="dateMonthsSince[1970]" />
            <xs:enumeration value="dateMonthsSince[1980]" />
            <xs:enumeration value="dateYearsSince[0]" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["VALID-TIME-SPEC"] = load_xsdType("""
    <xs:simpleType name="VALID-TIME-SPEC">
        <xs:restriction base="xs:string">
            <xs:enumeration value="includeAll" />
            <xs:enumeration value="includeFromTo" />
            <xs:enumeration value="excludeFromTo" />
            <xs:enumeration value="includeSet" />
            <xs:enumeration value="excludeSet" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["TIME-EXCEPTION-TYPE"] = load_xsdType("""
    <xs:simpleType name="TIME-EXCEPTION-TYPE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="exclude" />
            <xs:enumeration value="include" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["INTERPOLATION-METHOD"] = load_xsdType("""
    <xs:simpleType name="INTERPOLATION-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="none" />
            <xs:enumeration value="linear" />
            <xs:enumeration value="exponentialSpline" />
            <xs:enumeration value="cubicSpline" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["OPTYPE"] = load_xsdType("""
    <xs:simpleType name="OPTYPE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="categorical" />
            <xs:enumeration value="ordinal" />
            <xs:enumeration value="continuous" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["DATATYPE"] = load_xsdType("""
    <xs:simpleType name="DATATYPE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="string" />
            <xs:enumeration value="integer" />
            <xs:enumeration value="float" />
            <xs:enumeration value="double" />
            <xs:enumeration value="boolean" />
            <xs:enumeration value="date" />
            <xs:enumeration value="time" />
            <xs:enumeration value="dateTime" />
            <xs:enumeration value="dateDaysSince[0]" />
            <xs:enumeration value="dateDaysSince[1960]" />
            <xs:enumeration value="dateDaysSince[1970]" />
            <xs:enumeration value="dateDaysSince[1980]" />
            <xs:enumeration value="timeSeconds" />
            <xs:enumeration value="dateTimeSecondsSince[0]" />
            <xs:enumeration value="dateTimeSecondsSince[1960]" />
            <xs:enumeration value="dateTimeSecondsSince[1970]" />
            <xs:enumeration value="dateTimeSecondsSince[1980]" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["ELEMENT-ID"] = load_xsdType("""
    <xs:simpleType name="ELEMENT-ID">
        <xs:restriction base="xs:string" />
    </xs:simpleType>
    """)

PMML.xsdType["DELIMITER"] = load_xsdType("""
    <xs:simpleType name="DELIMITER">
        <xs:restriction base="xs:string">
            <xs:enumeration value="sameTimeWindow" />
            <xs:enumeration value="acrossTimeWindows" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["GAP"] = load_xsdType("""
    <xs:simpleType name="GAP">
        <xs:restriction base="xs:string">
            <xs:enumeration value="true" />
            <xs:enumeration value="false" />
            <xs:enumeration value="unknown" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["MINING-FUNCTION"] = load_xsdType("""
    <xs:simpleType name="MINING-FUNCTION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="associationRules" />
            <xs:enumeration value="sequences" />
            <xs:enumeration value="classification" />
            <xs:enumeration value="regression" />
            <xs:enumeration value="clustering" />
            <xs:enumeration value="timeSeries" />
            <xs:enumeration value="mixed" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["NUMBER"] = load_xsdType("""
    <xs:simpleType name="NUMBER">
        <xs:restriction base="xs:double" />
    </xs:simpleType>
    """)

PMML.xsdType["INT-NUMBER"] = load_xsdType("""
    <xs:simpleType name="INT-NUMBER">
        <xs:restriction base="xs:integer" />
    </xs:simpleType>
    """)

PMML.xsdType["REAL-NUMBER"] = load_xsdType("""
    <xs:simpleType name="REAL-NUMBER">
        <xs:restriction base="xs:double" />
    </xs:simpleType>
    """)

PMML.xsdType["PROB-NUMBER"] = load_xsdType("""
    <xs:simpleType name="PROB-NUMBER">
        <xs:restriction base="xs:decimal" />
    </xs:simpleType>
    """)

PMML.xsdType["PERCENTAGE-NUMBER"] = load_xsdType("""
    <xs:simpleType name="PERCENTAGE-NUMBER">
        <xs:restriction base="xs:decimal" />
    </xs:simpleType>
    """)

PMML.xsdType["FIELD-NAME"] = load_xsdType("""
    <xs:simpleType name="FIELD-NAME">
        <xs:restriction base="xs:string" />
    </xs:simpleType>
    """)

PMML.xsdType["ArrayType"] = load_xsdType("""
    <xs:complexType mixed="true" name="ArrayType">
        <xs:attribute name="type" use="required">
            <xs:simpleType>
                <xs:restriction base="xs:string">
                    <xs:enumeration value="int" />
                    <xs:enumeration value="real" />
                    <xs:enumeration value="string" />
                </xs:restriction>
            </xs:simpleType>
        </xs:attribute>
        <xs:attribute name="n" type="INT-NUMBER" use="optional" />
    </xs:complexType>
    """)

PMML.xsdType["CONT-SCORING-METHOD"] = load_xsdType("""
    <xs:simpleType name="CONT-SCORING-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="median" />
            <xs:enumeration value="average" />
            <xs:enumeration value="weightedAverage" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["CAT-SCORING-METHOD"] = load_xsdType("""
    <xs:simpleType name="CAT-SCORING-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="majorityVote" />
            <xs:enumeration value="weightedMajorityVote" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["FIELD-USAGE-TYPE"] = load_xsdType("""
    <xs:simpleType name="FIELD-USAGE-TYPE">
        <xs:restriction base="xs:string">
            <xs:enumeration value="active" />
            <xs:enumeration value="predicted" />
            <xs:enumeration value="supplementary" />
            <xs:enumeration value="group" />
            <xs:enumeration value="order" />
            <xs:enumeration value="frequencyWeight" />
            <xs:enumeration value="analysisWeight" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["OUTLIER-TREATMENT-METHOD"] = load_xsdType("""
    <xs:simpleType name="OUTLIER-TREATMENT-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="asIs" />
            <xs:enumeration value="asMissingValues" />
            <xs:enumeration value="asExtremeValues" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["MISSING-VALUE-TREATMENT-METHOD"] = load_xsdType("""
    <xs:simpleType name="MISSING-VALUE-TREATMENT-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="asIs" />
            <xs:enumeration value="asMean" />
            <xs:enumeration value="asMode" />
            <xs:enumeration value="asMedian" />
            <xs:enumeration value="asValue" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["INVALID-VALUE-TREATMENT-METHOD"] = load_xsdType("""
    <xs:simpleType name="INVALID-VALUE-TREATMENT-METHOD">
        <xs:restriction base="xs:string">
            <xs:enumeration value="returnInvalid" />
            <xs:enumeration value="asIs" />
            <xs:enumeration value="asMissing" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["CUMULATIVE-LINK-FUNCTION"] = load_xsdType("""
    <xs:simpleType name="CUMULATIVE-LINK-FUNCTION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="logit" />
            <xs:enumeration value="probit" />
            <xs:enumeration value="cloglog" />
            <xs:enumeration value="loglog" />
            <xs:enumeration value="cauchit" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["LINK-FUNCTION"] = load_xsdType("""
    <xs:simpleType name="LINK-FUNCTION">
        <xs:restriction base="xs:string">
            <xs:enumeration value="cloglog" />
            <xs:enumeration value="identity" />
            <xs:enumeration value="log" />
            <xs:enumeration value="logc" />
            <xs:enumeration value="logit" />
            <xs:enumeration value="loglog" />
            <xs:enumeration value="negbin" />
            <xs:enumeration value="oddspower" />
            <xs:enumeration value="power" />
            <xs:enumeration value="probit" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["MISSING-VALUE-STRATEGY"] = load_xsdType("""
    <xs:simpleType name="MISSING-VALUE-STRATEGY">
        <xs:restriction base="xs:string">
            <xs:enumeration value="lastPrediction" />
            <xs:enumeration value="nullPrediction" />
            <xs:enumeration value="defaultChild" />
            <xs:enumeration value="weightedConfidence" />
            <xs:enumeration value="aggregateNodes" />
            <xs:enumeration value="none" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["NO-TRUE-CHILD-STRATEGY"] = load_xsdType("""
    <xs:simpleType name="NO-TRUE-CHILD-STRATEGY">
        <xs:restriction base="xs:string">
            <xs:enumeration value="returnNullPrediction" />
            <xs:enumeration value="returnLastPrediction" />
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["BASELINE-TEST-STATISTIC"] = load_xsdType("""
    <xs:simpleType name="BASELINE-TEST-STATISTIC">
        <xs:restriction base="xs:string">
            <xs:enumeration value="zValue" />
            <xs:enumeration value="chiSquareIndependence" />
            <xs:enumeration value="chiSquareDistribution" />
            <xs:enumeration value="CUSUM" />
            <xs:enumeration value="scalarProduct" />
            <xs:enumeration value="GLR" />  <!-- technically not part of PMML, but added for convenience -->
        </xs:restriction>
    </xs:simpleType>
    """)

PMML.xsdType["COUNT-TABLE-TYPE"] = load_xsdType("""
    <xs:complexType name="COUNT-TABLE-TYPE">
        <xs:sequence>
            <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            <xs:choice>
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldValue" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldValueCount" />
            </xs:choice>
        </xs:sequence>
        <xs:attribute name="sample" type="NUMBER" use="optional" />
    </xs:complexType>
    """)

############################################################################### PMML groups

PMML.xsdGroup["EmbeddedModel"] = load_xsdGroup("""
    <xs:group name="EmbeddedModel">
        <xs:sequence>
            <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            <xs:choice>
                <xs:element ref="Regression" />
                <xs:element ref="DecisionTree" />
            </xs:choice>
        </xs:sequence>
    </xs:group>
    """)

PMML.xsdGroup["Rule"] = load_xsdGroup("""
    <xs:group name="Rule">
        <xs:choice>
            <xs:element ref="SimpleRule" />
            <xs:element ref="CompoundRule" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["FrequenciesType"] = load_xsdGroup("""
    <xs:group name="FrequenciesType">
        <xs:sequence>
            <xs:group maxOccurs="3" minOccurs="1" ref="NUM-ARRAY" />
        </xs:sequence>
    </xs:group>
    """)

PMML.xsdGroup["EXPRESSION"] = load_xsdGroup("""
    <xs:group name="EXPRESSION">
        <xs:choice>
            <xs:element ref="Constant" />
            <xs:element ref="FieldRef" />
            <xs:element ref="NormContinuous" />
            <xs:element ref="NormDiscrete" />
            <xs:element ref="Discretize" />
            <xs:element ref="MapValues" />
            <xs:element ref="Apply" />
            <xs:element ref="Aggregate" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["FOLLOW-SET"] = load_xsdGroup("""
    <xs:group name="FOLLOW-SET">
        <xs:sequence>
            <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            <xs:element ref="Delimiter" />
            <xs:element minOccurs="0" ref="Time" />
            <xs:element ref="SetReference" />
        </xs:sequence>
    </xs:group>
    """)

PMML.xsdGroup["SEQUENCE"] = load_xsdGroup("""
    <xs:group name="SEQUENCE">
        <xs:sequence>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:element ref="SequenceReference" />
            <xs:element minOccurs="0" ref="Time" />
        </xs:sequence>
    </xs:group>
    """)

def isModel(x):
    return isinstance(x, (AssociationModel,
                          BaselineModel,
                          ClusteringModel,
                          GeneralRegressionModel,
                          MiningModel,
                          NaiveBayesModel,
                          NearestNeighborModel,
                          NeuralNetwork,
                          RegressionModel,
                          RuleSetModel,
                          SequenceModel,
                          Scorecard,
                          SupportVectorMachineModel,
                          TextModel,
                          TimeSeriesModel,
                          TreeModel,
                          ))

PMML.xsdGroup["MODEL-ELEMENT"] = load_xsdGroup("""
    <xs:group name="MODEL-ELEMENT">
        <xs:choice>
            <xs:element ref="AssociationModel" />
            <xs:element ref="BaselineModel" />
            <xs:element ref="ClusteringModel" />
            <xs:element ref="GeneralRegressionModel" />
            <xs:element ref="MiningModel" />
            <xs:element ref="NaiveBayesModel" />
            <xs:element ref="NearestNeighborModel" />
            <xs:element ref="NeuralNetwork" />
            <xs:element ref="RegressionModel" />
            <xs:element ref="RuleSetModel" />
            <xs:element ref="SequenceModel" />
            <xs:element ref="Scorecard" />
            <xs:element ref="SupportVectorMachineModel" />
            <xs:element ref="TextModel" />
            <xs:element ref="TimeSeriesModel" />
            <xs:element ref="TreeModel" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["NUM-ARRAY"] = load_xsdGroup("""
    <xs:group name="NUM-ARRAY">
        <xs:choice>
            <xs:element ref="Array" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["INT-ARRAY"] = load_xsdGroup("""
    <xs:group name="INT-ARRAY">
        <xs:choice>
            <xs:element ref="Array" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["REAL-ARRAY"] = load_xsdGroup("""
    <xs:group name="REAL-ARRAY">
        <xs:choice>
            <xs:element ref="Array" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["STRING-ARRAY"] = load_xsdGroup("""
    <xs:group name="STRING-ARRAY">
        <xs:choice>
            <xs:element ref="Array" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["PREDICATE"] = load_xsdGroup("""
    <xs:group name="PREDICATE">
        <xs:choice>
            <xs:element ref="SimplePredicate" />
            <xs:element ref="CompoundPredicate" />
            <xs:element ref="SimpleSetPredicate" />
            <xs:element ref="True" />
            <xs:element ref="False" />
        </xs:choice>
    </xs:group>
    """)

PMML.xsdGroup["CONTINUOUS-DISTRIBUTION-TYPES"] = load_xsdGroup("""
    <xs:group name="CONTINUOUS-DISTRIBUTION-TYPES">
        <xs:sequence>
            <xs:choice>
                <xs:element ref="AnyDistribution" />
                <xs:element ref="GaussianDistribution" />
                <xs:element ref="PoissonDistribution" />
                <xs:element ref="UniformDistribution" />
            </xs:choice>
            <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
        </xs:sequence>
    </xs:group>
    """)

PMML.xsdGroup["DISCRETE-DISTRIBUTION-TYPES"] = load_xsdGroup("""
    <xs:group name="DISCRETE-DISTRIBUTION-TYPES">
        <xs:choice>
            <xs:element ref="CountTable" />
            <xs:element ref="NormalizedCountTable" />
            <xs:element maxOccurs="unbounded" minOccurs="2" ref="FieldRef" />
        </xs:choice>
    </xs:group>
    """)

############################################################################### PMML elements

class root(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PMML">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Header" />
                <xs:element minOccurs="0" ref="MiningBuildTask" />
                <xs:element ref="DataDictionary" />
                <xs:element minOccurs="0" ref="TransformationDictionary" />
                <xs:sequence maxOccurs="unbounded" minOccurs="0">
                    <xs:group ref="MODEL-ELEMENT" />
                </xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="version" type="xs:string" use="required" />
            <xs:attribute name="xmlns" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        # get the DataDictionary and TransformationDictionary
        self.dataDictionary = self.child(DataDictionary)
        self.transformationDictionary = self.child(TransformationDictionary, exception=False)

        for pmmlApply in self.matches(Apply, maxdepth=None):
            pmmlApply.top_validate_transformationDictionary(self.transformationDictionary)

        # find all the models and give each a DataContext, pointing to its parent model/parent context
        self.topModels = self.matches(isModel)
        self.subModels = []
        for model in self.topModels:
            model.parent = None
            self.setupContext(model, model)

        # execute the top_validate() method for everything else that has one
        for model in self.topModels + self.subModels:
            if hasattr(model, "top_validate_model"):
                model.top_validate_model(model.dataContext)
            self.do_top_validate(model, model.child(Output, exception=False), model)

        # create all of the predicate tests: now, after the value attributes have been converted to the right formats
        for obj in self.matches(lambda x: isinstance(x, (Node, SimpleRule, CompoundRule)), maxdepth=None):
            obj.test = obj.predicate.createTest()

        if self.attrib["version"] != "4.1":
            raise PMMLValidationError("PMML version in this file is \"%s\" but this is a PMML 4.1 interpreter" % self.attrib["version"])

    def setupContext(self, model, elem):
        if isModel(elem):
            miningSchema = elem.child(MiningSchema)
            localTransformations = elem.child(LocalTransformations, exception=False)

            parentContext = None
            if elem.parent is not None:
                parentContext = elem.parent.dataContext

            miningSchema.top_validate_parentContext(parentContext, self.dataDictionary)
           
            elem.dataContext = DataContext(parentContext, self.dataDictionary, self.transformationDictionary, miningSchema, localTransformations, elem.attrib["functionName"])

        for child in elem:
            if isModel(child):
                self.subModels.append(child)
                child.parent = model
                self.setupContext(child, child)

            else:
                self.setupContext(model, child)

    def do_top_validate(self, model, pmmlOutput, elem):
        for child in elem:
            if hasattr(child, "top_validate_output"):
                child.top_validate_output(model.dataContext, pmmlOutput)

            if hasattr(child, "top_validate"):
                child.top_validate(model.dataContext)

            if not isModel(child):
                self.do_top_validate(model, pmmlOutput, child)

PMML.classMap["PMML"] = root

class Taxonomy(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Taxonomy">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="ChildParent" />
            </xs:sequence>
            <xs:attribute name="name" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Taxonomy"] = Taxonomy

class ChildParent(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ChildParent">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element ref="TableLocator" />
                    <xs:element ref="InlineTable" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="childField" type="xs:string" use="required" />
            <xs:attribute name="parentField" type="xs:string" use="required" />
            <xs:attribute name="parentLevelField" type="xs:string" use="optional" />
            <xs:attribute default="no" name="isRecursive" use="optional">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="no" />
                        <xs:enumeration value="yes" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ChildParent"] = ChildParent

class TableLocator(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TableLocator">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        extensions = self.matches(Extension)
        if len(extensions) != 1 or "name" not in extensions[0].attrib or extensions[0].attrib["name"] != "xmlfile" or "value" not in extensions[0].attrib:
            raise NotImplementedError("Since TableLocator is not fully defined by PMML, we require an Extension named \"xmlfile\" to point to an XML fileName in its \"value\"")

        try:
            self.rows = xmlbase.loadfile(extensions[0].attrib["value"]).matches("row")
        except IOError:
            raise PMMLValidationError("TableLocator xmlfile Extension points to a non-existent file \"%s\"" % extensions[0].attrib["value"])
        except xmlbase.XMLError, err:
            raise PMMLValidationError("TableLocator's XML file \"%s\" is not correctly formatted (%s)" % (extensions[0].attrib["value"], str(err)))

PMML.classMap["TableLocator"] = TableLocator

class InlineTable(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="InlineTable">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="row" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.rows = self.matches(row)

    def initialize(self):
        self.index = -1

    def next(self):
        self.index += 1
        self.got = {}

    def get(self, field):
        if field not in self.got:
            try:
                row = self.rows[self.index]
            except IndexError:
                raise StopIteration

            for col in row:
                self.got[col.tag] = col.textContent()

        return self.got.get(field, MISSING)

PMML.classMap["InlineTable"] = InlineTable

class row(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="row">
        <xs:complexType>
            <xs:complexContent mixed="true">
                <xs:restriction base="xs:anyType">
                    <xs:sequence>
                        <xs:any maxOccurs="unbounded" minOccurs="2" processContents="skip" />
                    </xs:sequence>
                </xs:restriction>
            </xs:complexContent>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["row"] = row

class MiningModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MiningModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:choice maxOccurs="unbounded" minOccurs="0">
                    <xs:element ref="Regression" />
                    <xs:element ref="DecisionTree" />
                </xs:choice>
                <xs:element minOccurs="0" ref="Segmentation" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        
        if self.exists(ModelVerification):
            for segmentation in self.matches(Segmentation):
                if segmentation.attrib["multipleModelMethod"] != "selectFirst":
                    raise PMMLValidationError("ModelVerification can only be used with selectFirst segmentation")

PMML.classMap["MiningModel"] = MiningModel

class Segmentation(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Segmentation">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Segment" />
            </xs:sequence>
            <xs:attribute name="multipleModelMethod" type="MULTIPLE-MODEL-METHOD" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Segmentation"] = Segmentation

class Segment(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Segment">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="PREDICATE" />
                <xs:group ref="MODEL-ELEMENT" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="optional" />
            <xs:attribute default="1" name="weight" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Segment"] = Segment

class ResultField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ResultField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="dataType" type="DATATYPE" />
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="displayName" type="xs:string" />
            <xs:attribute name="feature" type="RESULT-FEATURE" />
            <xs:attribute name="value" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["name"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

PMML.classMap["ResultField"] = ResultField

class Regression(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Regression">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ResultField" />
                <xs:element maxOccurs="unbounded" ref="RegressionTable" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="none" name="normalizationMethod" type="REGRESSIONNORMALIZATIONMETHOD" />
        </xs:complexType>
    </xs:element>
    """)

    CLASSIFICATION = Atom("Classification")
    REGRESSION = Atom("Regression")

    def post_validate(self):
        if self.attrib["functionName"] == "classification":
            self.functionName = self.CLASSIFICATION
        elif self.attrib["functionName"] == "regression":
            self.functionName = self.REGRESSION
        else:
            raise PMMLValidationError("The only valid Regression functionNames are: 'regression', 'classification'")

        self.regressionTables = self.matches(RegressionTable)
        normalizationMethod = self.attrib.get("normalizationMethod", "none")

        if self.functionName == self.REGRESSION:
            if len(self.regressionTables) != 1:
                raise PMMLValidationError("RegressionModels with functionName='regression' must have exactly one RegressionTable, not %d" % len(self.regressionTables))
            
            self.yvalue = lambda get: self.regressionTables[0].evaluate(get)

            if normalizationMethod == "none":
                self.prob = lambda y: y
                self.probinv = lambda f: f

            elif normalizationMethod == "softmax" or normalizationMethod == "logit":
                self.prob = lambda y: 1./(1. + exp(-y))
                self.probinv = lambda f: -log(1./f - 1.)

            elif normalizationMethod == "exp":
                self.prob = lambda y: exp(y)
                self.probinv = lambda f: log(f)

            else:
                raise PMMLValidationError("The only valid RegressionModel normalizationMethods with functionName='regression' are: 'none', 'softmax', 'logit' (same as 'softmax'), and 'exp'")

        elif self.functionName == self.CLASSIFICATION:
            normalizationMethod = self.attrib.get("normalizationMethod", "none")

            if normalizationMethod == "none": self.prob =        lambda yall: yall
            elif normalizationMethod == "simplemax": self.prob = lambda yall: yall/sum(yall)
            elif normalizationMethod == "exp": self.prob =       lambda yall: numpy.exp(yall)
            else:
                if normalizationMethod == "softmax": F =   lambda yall: numpy.exp(yall)/sum(numpy.exp(yall))
                elif normalizationMethod == "logit": F =   lambda yall: 1./(1. - yall)
                elif normalizationMethod == "probit": F =  lambda yall: numpy.array([0.5*erf(y/sqrt(2.)) + 0.5 for y in yall])
                elif normalizationMethod == "loglog": F =  lambda yall: numpy.exp(-numpy.exp(-yall))
                elif normalizationMethod == "cauchit": F = lambda yall: numpy.array([0.5 + (1./pi)*atan(y) for y in yall])
                elif normalizationMethod == "cloglog":
                    def F(yall):
                        out = numpy.empty(len(yall), dtype=yall.dtype)
                        for i in xrange(len(yall)):
                            # 1. - numpy.exp(-numpy.exp(yall))
                            try:
                                tmp = math.exp(yall[i])
                            except OverflowError:
                                out[i] = 1.
                            else:
                                try:
                                    out[i] = 1. - math.exp(-tmp)
                                except OverflowError:
                                    out[i] = float("-inf")
                        return out

                self.prob = F

            if len(self.regressionTables) < 2:
                raise PMMLValidationError("RegressionModels with functionName='classification' must have at least two RegressionTables, not %d" % len(self.regressionTables))

            self.targetValues = []
            valuesSeen = set()
            for regressionTable in self.regressionTables:
                targetCategory = regressionTable.attrib.get("targetCategory", None)
                if targetCategory is None:
                    raise PMMLValidationError("Regression with functionName='classification' must have a targetCategory in each RegressionTable")
                if targetCategory in valuesSeen:
                    raise PMMLValidationError("Regression with functionName='classification' are not allowed to have duplicate targetCategories: \"%s\" is seen more than once" % targetCategory)

                self.targetValues.append(targetCategory)

    def yvalue(self, get):
        return numpy.array([t.evaluate(get) for t in self.regressionTables])

    def winner(self, yvalues):
        i = numpy.argmax(yvalues)
        return i, self.targetValues[i]

    def evaluate(self, get):
        try:
            yvalue = self.yvalue(get)
        except InvalidDataError:
            return INVALID

        if self.functionName is self.REGRESSION:
            return yvalue
        else:
            index, winner = self.winner(yvalue)
            return winner

PMML.classMap["Regression"] = Regression

class DecisionTree(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DecisionTree">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ResultField" />
                <xs:element ref="Node" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="multiSplit" name="splitCharacteristic">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="binarySplit" />
                        <xs:enumeration value="multiSplit" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="none" name="missingValueStrategy" type="MISSING-VALUE-STRATEGY" />
            <xs:attribute default="1.0" name="missingValuePenalty" type="PROB-NUMBER" />
            <xs:attribute default="returnNullPrediction" name="noTrueChildStrategy" type="NO-TRUE-CHILD-STRATEGY" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        raise NotImplementedError("DecisionTree has not been implemented as an embedded model")

PMML.classMap["DecisionTree"] = DecisionTree

class AssociationModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="AssociationModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Item" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Itemset" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="AssociationRule" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="numberOfTransactions" type="INT-NUMBER" use="required" />
            <xs:attribute name="maxNumberOfItemsPerTA" type="INT-NUMBER" />
            <xs:attribute name="avgNumberOfItemsPerTA" type="REAL-NUMBER" />
            <xs:attribute name="minimumSupport" type="PROB-NUMBER" use="required" />
            <xs:attribute name="minimumConfidence" type="PROB-NUMBER" use="required" />
            <xs:attribute name="lengthLimit" type="INT-NUMBER" />
            <xs:attribute name="numberOfItems" type="INT-NUMBER" use="required" />
            <xs:attribute name="numberOfItemsets" type="INT-NUMBER" use="required" />
            <xs:attribute name="numberOfRules" type="INT-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("AssociationModel has not been implemented yet")
    
PMML.classMap["AssociationModel"] = AssociationModel

class Item(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Item">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="required" />
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="mappedValue" type="xs:string" />
            <xs:attribute name="weight" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Item"] = Item

class Itemset(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Itemset">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ItemRef" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="required" />
            <xs:attribute name="support" type="PROB-NUMBER" />
            <xs:attribute name="numberOfItems" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Itemset"] = Itemset

class ItemRef(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ItemRef">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="itemRef" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ItemRef"] = ItemRef

class AssociationRule(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="AssociationRule">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="optional" />
            <xs:attribute name="antecedent" type="xs:string" use="required" />
            <xs:attribute name="consequent" type="xs:string" use="required" />
            <xs:attribute name="support" type="PROB-NUMBER" use="required" />
            <xs:attribute name="confidence" type="PROB-NUMBER" use="required" />
            <xs:attribute name="lift" type="xs:float" use="optional" />
            <xs:attribute name="leverage" type="xs:float" use="optional" />
            <xs:attribute name="affinity" type="PROB-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["AssociationRule"] = AssociationRule

class TextModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="TextDictionary" />
                <xs:element ref="TextCorpus" />
                <xs:element ref="DocumentTermMatrix" />
                <xs:element minOccurs="0" ref="TextModelNormalization" />
                <xs:element minOccurs="0" ref="TextModelSimiliarity" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="numberOfTerms" type="xs:integer" use="required" />
            <xs:attribute name="numberOfDocuments" type="xs:integer" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("TextModel has not been implemented yet")

PMML.classMap["TextModel"] = TextModel

class TextDictionary(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextDictionary">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Taxonomy" />
                <xs:group ref="STRING-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TextDictionary"] = TextDictionary

class TextCorpus(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextCorpus">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="TextDocument" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TextCorpus"] = TextCorpus

class TextDocument(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextDocument">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="required" />
            <xs:attribute name="name" type="xs:string" use="optional" />
            <xs:attribute name="file" type="xs:string" use="optional" />
            <xs:attribute name="length" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TextDocument"] = TextDocument

class DocumentTermMatrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DocumentTermMatrix">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["DocumentTermMatrix"] = DocumentTermMatrix

class TextModelNormalization(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextModelNormalization">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute default="termFrequency" name="localTermWeights">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="termFrequency" />
                        <xs:enumeration value="binary" />
                        <xs:enumeration value="logarithmic" />
                        <xs:enumeration value="augmentedNormalizedTermFrequency" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="inverseDocumentFrequency" name="globalTermWeights">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="inverseDocumentFrequency" />
                        <xs:enumeration value="none" />
                        <xs:enumeration value="GFIDF" />
                        <xs:enumeration value="normal" />
                        <xs:enumeration value="probabilisticInverse" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="none" name="documentNormalization">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="none" />
                        <xs:enumeration value="cosine" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TextModelNormalization"] = TextModelNormalization

class TextModelSimiliarity(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TextModelSimiliarity">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="similarityType">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="euclidean" />
                        <xs:enumeration value="cosine" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TextModelSimiliarity"] = TextModelSimiliarity

class ModelExplanation(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ModelExplanation">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element maxOccurs="unbounded" minOccurs="0" ref="PredictiveModelQuality" />
                    <xs:element maxOccurs="unbounded" minOccurs="0" ref="ClusteringModelQuality" />
                </xs:choice>
                <xs:element minOccurs="0" ref="Correlations" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ModelExplanation"] = ModelExplanation

class PredictiveModelQuality(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PredictiveModelQuality">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="ConfusionMatrix" />
                <xs:element minOccurs="0" ref="LiftData" />
                <xs:element minOccurs="0" ref="ROC" />
            </xs:sequence>
            <xs:attribute name="targetField" type="xs:string" use="required" />
            <xs:attribute name="dataName" type="xs:string" use="optional" />
            <xs:attribute default="training" name="dataUsage">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="training" />
                        <xs:enumeration value="test" />
                        <xs:enumeration value="validation" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="meanError" type="NUMBER" use="optional" />
            <xs:attribute name="meanAbsoluteError" type="NUMBER" use="optional" />
            <xs:attribute name="meanSquaredError" type="NUMBER" use="optional" />
            <xs:attribute name="rootMeanSquaredError" type="NUMBER" use="optional" />
            <xs:attribute name="r-squared" type="NUMBER" use="optional" />
            <xs:attribute name="adj-r-squared" type="NUMBER" use="optional" />
            <xs:attribute name="sumSquaredError" type="NUMBER" use="optional" />
            <xs:attribute name="sumSquaredRegression" type="NUMBER" use="optional" />
            <xs:attribute name="numOfRecords" type="NUMBER" use="optional" />
            <xs:attribute name="numOfRecordsWeighted" type="NUMBER" use="optional" />
            <xs:attribute name="numOfPredictors" type="NUMBER" use="optional" />
            <xs:attribute name="degreesOfFreedom" type="NUMBER" use="optional" />
            <xs:attribute name="fStatistic" type="NUMBER" use="optional" />
            <xs:attribute name="AIC" type="NUMBER" use="optional" />
            <xs:attribute name="BIC" type="NUMBER" use="optional" />
            <xs:attribute name="AICc" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PredictiveModelQuality"] = PredictiveModelQuality

class ClusteringModelQuality(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ClusteringModelQuality">
        <xs:complexType>
            <xs:attribute name="dataName" type="xs:string" use="optional" />
            <xs:attribute name="SSE" type="NUMBER" use="optional" />
            <xs:attribute name="SSB" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ClusteringModelQuality"] = ClusteringModelQuality

class LiftData(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="LiftData">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="ModelLiftGraph" />
                <xs:element minOccurs="0" ref="OptimumLiftGraph" />
                <xs:element minOccurs="0" ref="RandomLiftGraph" />
            </xs:sequence>
            <xs:attribute name="targetFieldValue" type="xs:string" />
            <xs:attribute name="targetFieldDisplayValue" type="xs:string" />
            <xs:attribute name="rankingQuality" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["LiftData"] = LiftData

class ModelLiftGraph(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ModelLiftGraph">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="LiftGraph" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ModelLiftGraph"] = ModelLiftGraph

class OptimumLiftGraph(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="OptimumLiftGraph">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="LiftGraph" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["OptimumLiftGraph"] = OptimumLiftGraph

class RandomLiftGraph(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RandomLiftGraph">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="LiftGraph" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["RandomLiftGraph"] = RandomLiftGraph

class LiftGraph(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="LiftGraph">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="XCoordinates" />
                <xs:element ref="YCoordinates" />
                <xs:element minOccurs="0" ref="BoundaryValues" />
                <xs:element minOccurs="0" ref="BoundaryValueMeans" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["LiftGraph"] = LiftGraph

class XCoordinates(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="XCoordinates">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="NUM-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["XCoordinates"] = XCoordinates

class YCoordinates(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="YCoordinates">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="NUM-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["YCoordinates"] = YCoordinates

class BoundaryValues(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BoundaryValues">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="NUM-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BoundaryValues"] = BoundaryValues

class BoundaryValueMeans(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BoundaryValueMeans">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="NUM-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BoundaryValueMeans"] = BoundaryValueMeans

class ROC(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ROC">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="ROCGraph" />
            </xs:sequence>
            <xs:attribute name="positiveTargetFieldValue" type="xs:string" use="required" />
            <xs:attribute name="positiveTargetFieldDisplayValue" type="xs:string" />
            <xs:attribute name="negativeTargetFieldValue" type="xs:string" />
            <xs:attribute name="negativeTargetFieldDisplayValue" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ROC"] = ROC

class ROCGraph(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ROCGraph">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="XCoordinates" />
                <xs:element ref="YCoordinates" />
                <xs:element minOccurs="0" ref="BoundaryValues" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ROCGraph"] = ROCGraph

class ConfusionMatrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ConfusionMatrix">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="ClassLabels" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ConfusionMatrix"] = ConfusionMatrix

class ClassLabels(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ClassLabels">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="STRING-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ClassLabels"] = ClassLabels

class Correlations(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Correlations">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="CorrelationFields" />
                <xs:element ref="CorrelationValues" />
                <xs:element minOccurs="0" ref="CorrelationMethods" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Correlations"] = Correlations

class CorrelationFields(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CorrelationFields">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="STRING-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["CorrelationFields"] = CorrelationFields

class CorrelationValues(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CorrelationValues">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["CorrelationValues"] = CorrelationValues

class CorrelationMethods(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CorrelationMethods">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["CorrelationMethods"] = CorrelationMethods

class SupportVectorMachineModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SupportVectorMachineModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:sequence>
                    <xs:choice>
                        <xs:element ref="LinearKernelType" />
                        <xs:element ref="PolynomialKernelType" />
                        <xs:element ref="RadialBasisKernelType" />
                        <xs:element ref="SigmoidKernelType" />
                    </xs:choice>
                </xs:sequence>
                <xs:element ref="VectorDictionary" />
                <xs:element maxOccurs="unbounded" ref="SupportVectorMachine" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute default="0" name="threshold" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="SupportVectors" name="svmRepresentation" type="SVM-REPRESENTATION" use="optional" />
            <xs:attribute default="OneAgainstAll" name="classificationMethod" type="SVM-CLASSIFICATION-METHOD" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("SupportVectorMachineModel has not been implemented yet")

PMML.classMap["SupportVectorMachineModel"] = SupportVectorMachineModel

class LinearKernelType(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="LinearKernelType">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="description" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["LinearKernelType"] = LinearKernelType

class PolynomialKernelType(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PolynomialKernelType">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="description" type="xs:string" use="optional" />
            <xs:attribute default="1" name="gamma" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="1" name="coef0" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="1" name="degree" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PolynomialKernelType"] = PolynomialKernelType

class RadialBasisKernelType(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RadialBasisKernelType">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="description" type="xs:string" use="optional" />
            <xs:attribute default="1" name="gamma" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["RadialBasisKernelType"] = RadialBasisKernelType

class SigmoidKernelType(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SigmoidKernelType">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="description" type="xs:string" use="optional" />
            <xs:attribute default="1" name="gamma" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="1" name="coef0" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SigmoidKernelType"] = SigmoidKernelType

class VectorDictionary(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="VectorDictionary">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="VectorFields" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="VectorInstance" />
            </xs:sequence>
            <xs:attribute name="numberOfVectors" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["VectorDictionary"] = VectorDictionary

class VectorFields(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="VectorFields">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="FieldRef" />
            </xs:sequence>
            <xs:attribute name="numberOfFields" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["VectorFields"] = VectorFields

class VectorInstance(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="VectorInstance">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element ref="REAL-SparseArray" />
                    <xs:group ref="REAL-ARRAY" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="id" type="VECTOR-ID" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["VectorInstance"] = VectorInstance

class SupportVectorMachine(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SupportVectorMachine">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="SupportVectors" />
                <xs:element ref="Coefficients" />
            </xs:sequence>
            <xs:attribute name="targetCategory" type="xs:string" use="optional" />
            <xs:attribute name="alternateTargetCategory" type="xs:string" use="optional" />
            <xs:attribute name="threshold" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SupportVectorMachine"] = SupportVectorMachine

class SupportVectors(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SupportVectors">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="SupportVector" />
            </xs:sequence>
            <xs:attribute name="numberOfSupportVectors" type="INT-NUMBER" use="optional" />
            <xs:attribute name="numberOfAttributes" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SupportVectors"] = SupportVectors

class SupportVector(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SupportVector">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="vectorId" type="VECTOR-ID" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SupportVector"] = SupportVector

class Coefficients(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Coefficients">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Coefficient" />
            </xs:sequence>
            <xs:attribute name="numberOfCoefficients" type="INT-NUMBER" use="optional" />
            <xs:attribute default="0" name="absoluteValue" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Coefficients"] = Coefficients

class Coefficient(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Coefficient">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute default="0" name="value" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Coefficient"] = Coefficient

class Scorecard(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Scorecard">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="Characteristics" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute default="0" name="initialScore" type="NUMBER" />
            <xs:attribute default="true" name="useReasonCodes" type="xs:boolean" />
            <xs:attribute default="pointsBelow" name="reasonCodeAlgorithm">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="pointsAbove" />
                        <xs:enumeration value="pointsBelow" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="baselineScore" type="NUMBER" />
            <xs:attribute default="other" name="baselineMethod">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="max" />
                        <xs:enumeration value="min" />
                        <xs:enumeration value="mean" />
                        <xs:enumeration value="neutral" />
                        <xs:enumeration value="other" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("Scorecard has not been implemented yet")

PMML.classMap["Scorecard"] = Scorecard

class Characteristics(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Characteristics">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Characteristic" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Characteristics"] = Characteristics

class Characteristic(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Characteristic">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Attribute" />
            </xs:sequence>
            <xs:attribute name="name" type="FIELD-NAME" use="optional" />
            <xs:attribute name="reasonCode" type="xs:string" />
            <xs:attribute name="baselineScore" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["name"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

PMML.classMap["Characteristic"] = Characteristic

class Attribute(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Attribute">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="PREDICATE" />
            </xs:sequence>
            <xs:attribute name="reasonCode" type="xs:string" />
            <xs:attribute name="partialScore" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Attribute"] = Attribute

class Header(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Header">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Application" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Annotation" />
                <xs:element minOccurs="0" ref="Timestamp" />
            </xs:sequence>
            <xs:attribute name="copyright" type="xs:string" />
            <xs:attribute name="description" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Header"] = Header

class Application(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Application">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="name" type="xs:string" use="required" />
            <xs:attribute name="version" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Application"] = Application

class Annotation(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Annotation">
        <xs:complexType mixed="true">
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Annotation"] = Annotation

class Timestamp(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Timestamp">
        <xs:complexType mixed="true">
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Timestamp"] = Timestamp

class ModelVerification(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ModelVerification">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="VerificationFields" />
                <xs:element ref="InlineTable" />
            </xs:sequence>
            <xs:attribute name="recordCount" type="INT-NUMBER" use="optional" />
            <xs:attribute name="fieldCount" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.column = {}
        self.precision = {}
        self.zeroThreshold = {}

        for field in self.child(VerificationFields).matches(VerificationField):
            self.column[field.attrib["field"]] = field.attrib.get("column", field.attrib["field"])
            self.precision[field.attrib["field"]] = field.attrib.get("precision", 1E-6)
            self.zeroThreshold[field.attrib["field"]] = field.attrib.get("zeroThreshold", 1E-16)

        self.table = self.child(InlineTable)

    def top_validate_output(self, dataContext, pmmlOutput):
        self.dataContext = dataContext

        self.verificationRefersToOutput = False
        if pmmlOutput is not None:
            for field in pmmlOutput.fields:
                if field.attrib["name"] in self.column:
                    self.verificationRefersToOutput = True
                    break

        self.predicted = dataContext.names(usageTypes=("predicted",))

        if self.verificationRefersToOutput:
            self.predictedName = None

        else:
            if len(self.predicted) != 1:
                raise PMMLValidationError("If no Output is present or no VerificationFields refer to an OutputField, there must be only one predicted value")
            self.predictedName = self.predicted[0]

        for field in self.column.keys():
            if field not in self.dataContext.cast and field not in self.predicted:
                raise PMMLValidationError("VerificationField \"%s\" is neither in the current data context nor is it a predicted field in the MiningSchema" % field)

    def initialize(self):
        self.table.initialize()

    def next(self):
        self.table.next()

    def flush(self): pass

    def get(self, field):
        if field not in self.column: return MISSING

        output = self.table.get(self.column[field])
        if output is MISSING:
            return MISSING

        return self.dataContext.cast.get(field, lambda x: x)(output)

PMML.classMap["ModelVerification"] = ModelVerification

class VerificationFields(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="VerificationFields">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="VerificationField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["VerificationFields"] = VerificationFields

class VerificationField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="VerificationField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="xs:string" use="required" />
            <xs:attribute name="column" type="xs:string" use="optional" />
            <xs:attribute default="1E-6" name="precision" type="xs:double" />
            <xs:attribute default="1E-16" name="zeroThreshold" type="xs:double" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["VerificationField"] = VerificationField

class RuleSetModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RuleSetModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="RuleSet" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        if self.attrib["functionName"] != "classification":
            raise PMMLValidationError("The only valid RuleSetModel functionNames are: 'classification'")
        self.ruleset = self.child(RuleSet)

PMML.classMap["RuleSetModel"] = RuleSetModel

class RuleSet(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RuleSet">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="RuleSelectionMethod" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ScoreDistribution" />
                <xs:group maxOccurs="unbounded" minOccurs="0" ref="Rule" />
            </xs:sequence>
            <xs:attribute name="recordCount" type="NUMBER" use="optional" />
            <xs:attribute name="nbCorrect" type="NUMBER" use="optional" />
            <xs:attribute name="defaultScore" type="xs:string" use="optional" />
            <xs:attribute name="defaultConfidence" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    FIRSTHIT = Atom("FirstHit")
    WEIGHTEDMAX = Atom("WeightedMax")
    WEIGHTEDSUM = Atom("WeightedSum")

    def post_validate(self):
        self.configure()

    def configure(self):
        criterion = self.child(RuleSelectionMethod).attrib["criterion"]  # the first selection method is the default

        if criterion == "firstHit": self.criterion = self.FIRSTHIT
        elif criterion == "weightedMax": self.criterion = self.WEIGHTEDMAX
        elif criterion == "weightedSum": self.criterion = self.WEIGHTEDSUM

        self.rules = self.matches(lambda x: isinstance(x, (SimpleRule, CompoundRule)))

        simpleRules = self.matches(SimpleRule, maxdepth=None)

        # find duplicates
        usedNames = set()
        for rule in simpleRules:
            if "id" in rule.attrib:
                name = rule.attrib["id"]
                if name not in usedNames:
                    usedNames.add(name)
                else:
                    raise PMMLValidationError("SimpleRule id=\"%s\" appears more than once" % name)

        # give ids to the simpleRules that don't have them
        number = 1
        for rule in simpleRules:
            if "id" not in rule.attrib:
                newName = None
                while newName is None or newName in usedNames:
                    newName = "Untitled-%d" % number
                    number += 1
                rule.attrib["id"] = newName

    def evaluate(self, get):
        matches = []
        for rule in self.rules:
            if rule.evaluate(get, matches, self.criterion):
                break

        if len(matches) == 0:
            score = self.attrib.get("defaultScore", MISSING)
            weight = self.attrib.get("defaultConfidence", MISSING)
            entity = None

        else:
            if self.criterion is self.FIRSTHIT:
                score = matches[0].score
                weight = matches[0].weight
                entity = matches[0]

            elif self.criterion is self.WEIGHTEDMAX:
                score = None
                weight = None
                entity = None
                for rule in matches:
                    if weight is None or rule.weight > weight:
                        score = rule.score
                        weight = rule.weight
                        entity = rule

            elif self.criterion is self.WEIGHTEDSUM:
                weightByScore = {}
                for rule in matches:
                    if rule.score not in weightByScore:
                        weightByScore[rule.score] = 0.
                    weightByScore[rule.score] += rule.weight

                score = None
                weight = None
                entity = None
                for s, w in weightByScore.items():
                    if weight is None or w > weight:
                        score = s
                        weight = w

                weight /= len(matches)

        return score, weight, entity

PMML.classMap["RuleSet"] = RuleSet

class RuleSelectionMethod(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RuleSelectionMethod">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="criterion" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="weightedSum" />
                        <xs:enumeration value="weightedMax" />
                        <xs:enumeration value="firstHit" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["RuleSelectionMethod"] = RuleSelectionMethod

class SimpleRule(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SimpleRule">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="PREDICATE" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ScoreDistribution" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="optional" />
            <xs:attribute name="score" type="xs:string" use="required" />
            <xs:attribute name="recordCount" type="NUMBER" use="optional" />
            <xs:attribute name="nbCorrect" type="NUMBER" use="optional" />
            <xs:attribute name="confidence" type="NUMBER" use="optional" />
            <xs:attribute name="weight" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def calculateProbabilities(self):
        scoreDistribution = self.matches(ScoreDistribution)

        if "recordCount" in self.attrib:
            total = self.attrib["recordCount"]
        else:
            total = float(sum([i.attrib["recordCount"] for i in scoreDistribution]))

        self.scoreDistribution = {}
        for i in scoreDistribution:
            if "probability" in i.attrib:
                self.scoreDistribution[i.attrib["value"]] = i.attrib["probability"]
            elif total == 0.:
                self.scoreDistribution[i.attrib["value"]] = 0.
            else:
                self.scoreDistribution[i.attrib["value"]] = i.attrib["recordCount"]/total

        sumOfProbabilities = sum(self.scoreDistribution.values())
        if abs(sumOfProbabilities - 1.) > 1e-5:
            raise PMMLValidationError("Probabilities of ScoreDistributions in SimpleRule do not add to 1.0, they add to %g (are they a mixture of user-specified \"probability\" and probabilities calculated from \"recordCount\"?)" % sumOfProbabilities)

    def post_validate(self):
        self.configure()

    def configure(self):
        self.score = self.attrib["score"]
        self.weight = self.attrib.get("weight", 1.)
        self.predicate = self.child(nonExtension)

    def evaluate(self, get, matches, criterion):
        if self.test(get):
            matches.append(self)
            if criterion is RuleSet.FIRSTHIT:
                return True
        return False

PMML.classMap["SimpleRule"] = SimpleRule

class CompoundRule(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CompoundRule">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="PREDICATE" />
                <xs:group maxOccurs="unbounded" minOccurs="1" ref="Rule" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.configure()

    def configure(self):
        self.predicate = self.child(nonExtension)
        self.rules = self.matches(lambda x: isinstance(x, (SimpleRule, CompoundRule)))

    def evaluate(self, get, matches, criterion):
        if self.test(get):
            for rule in self.rules:
                if rule.evaluate(get, matches, criterion):
                    return True
        return False

PMML.classMap["CompoundRule"] = CompoundRule

class ClusteringModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ClusteringModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="ComparisonMeasure" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="ClusteringField" />
                <xs:element minOccurs="0" ref="MissingValueWeights" />
                <xs:element maxOccurs="unbounded" ref="Cluster" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="numberOfClusters" type="INT-NUMBER" use="required" />
            <xs:attribute name="modelClass" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="centerBased" />
                        <xs:enumeration value="distributionBased" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)

        if self.attrib["functionName"] != "clustering":
            raise PMMLValidationError("The only valid ClusteringModel functionNames are: 'clustering'")

        if self.attrib["modelClass"] != "centerBased":
            raise NotImplementedError("Only 'centerBased' ClusteringModels have been implemented")

        self.numberOfClusters = self.attrib["numberOfClusters"]
        clusters = self.matches(Cluster)
        ### Technically, not required by standard (could be an oversight of the standard)
        # if len(clusters) != self.numberOfClusters:
        #     raise PMMLValidationError("Number of observed Clusters (%d) does not match numberOfClusters attribute (%d)" % (len(clusters), self.numberOfClusters))
        self.numberOfClusters = len(clusters)
        self.attrib["numberOfClusters"] = len(clusters)
            
        # assume that the producer will change the cluster positions (numbers in these lists), but not anything else
        # the PMML will automatically stay up-to-date because Python works by reference
        usedIds = set([i.attrib["id"] for i in clusters if "id" in i.attrib])

        self.cluster = []
        self.ids = []
        occurrences = {}
        for i, cluster in enumerate(clusters):
            self.cluster.append(cluster.child(Array))

            if "id" not in cluster.attrib:
                newId = "%d" % (i+1)
                if newId in usedIds:
                    raise PMMLValidationError("Assigning \"%s\" to a cluster without an id, but this id is already in use by another cluster" % newId)
                cluster.attrib["id"] = newId

            theid = cluster.attrib["id"]
            self.ids.append(theid)
            if theid not in occurrences:
                occurrences[theid] = 0
            occurrences[theid] += 1
        
        for theid in occurrences.keys():
            if occurrences[theid] == 1:
                del occurrences[theid]
        if len(occurrences) != 0:
            raise PMMLValidationError("Multiple occurrences of some cluster ids: %s" % str(occurrences))

        comparisonMeasure = self.child(ComparisonMeasure)
        defaultCompareFunction = comparisonMeasure.attrib.get("compareFunction", "absDiff")
        self.metric = comparisonMeasure.child(nonExtension)

        self.fields = []
        fieldWeights = []
        similarityScales = []
        compareFunctions = []
        comparisonTables = []
        for clusteringField in self.matches(ClusteringField):
            if "isCenterField" not in clusteringField.attrib or clusteringField.attrib["isCenterField"]:
                self.fields.append(clusteringField.attrib["field"])
                fieldWeights.append(clusteringField.attrib.get("fieldWeight", 1.))
                similarityScales.append(clusteringField.attrib.get("similarityScale", 1.))
                compareFunctions.append(clusteringField.attrib.get("compareFunction", defaultCompareFunction))
                if clusteringField.exists(Comparisons):
                    comparisonTables.append(clusteringField.child(Comparisons).child(Matrix))
                else:
                    comparisonTables.append(None)

        if self.exists(MissingValueWeights):
            missingValueWeights = self.child(MissingValueWeights).child(Array).value
            if len(missingValueWeights) != len(self.fields):
                raise PMMLValidationError("Length of MissingValueWeights (%d) does not match the number of ClusteringFields (%d)" % (len(missingValueWeights), len(self.fields)))

            if missingValueWeights == [1.] * len(self.fields) or missingValueWeights == [1] * len(self.fields):
                missingValueWeights = None  # inform the metric that these are trivial missingValueWeights
        else:
            missingValueWeights = None

        self.metric.configure(fieldWeights, missingValueWeights, compareFunctions, similarityScales, comparisonTables)
        self.numberOfFields = len(self.fields)

        self.weightField = comparisonMeasure.attrib.get("weightField", None)

    def top_validate_model(self, dataContext):
        if self.child(ComparisonMeasure).attrib["kind"] == "distance":
            for clusteringField in self.matches(ClusteringField):
                field = clusteringField.attrib["field"]
                optype = dataContext.optype[field]
                if optype != "continuous":
                    raise PMMLValidationError("ClusteringModel has ComparisonMeasure.kind == 'distance' yet ClusteringField \"%s\" has optype \"%s\" in MiningSchema's active context %s" % (field, optype, dataContext.contextString()))

    def changeNumberOfClusters(self, newNumber):
        if newNumber < self.numberOfClusters:
            self.cluster = self.cluster[:newNumber]
            self.ids = self.ids[:newNumber]

            newChildren = []
            clustersSeen = 0
            for child in self.children:
                if isinstance(child, Cluster):
                    clustersSeen += 1
                    if clustersSeen <= newNumber:
                        newChildren.append(child)
                else:
                    newChildren.append(child)
            self.children = newChildren

        elif newNumber > self.numberOfClusters:
            usedIds = set([])
            lastIndex = None
            lastCluster = None
            for index, child in enumerate(self.children):
                if isinstance(child, Cluster):
                    usedIds.add(child.attrib.get("id", None))
                    lastIndex = index
                    lastCluster = child
            usedIds.discard(None)

            i = 0
            while len(self.ids) < newNumber:
                newId = "%d" % (i+1)
                while newId in usedIds:
                    i += 1
                    newId = "%d" % (i+1)

                newCluster = lastCluster.copy()
                newCluster.attrib["id"] = newId
                self.children.insert(lastIndex + 1, newCluster)
                lastIndex += 1

                self.cluster.append(newCluster.child(Array))
                self.ids.append(newId)
                i += 1

        self.numberOfClusters = newNumber
        self.attrib["numberOfClusters"] = newNumber

    def clusterDistance(self, i, x, anyMissing=False):
        if anyMissing:
            return self.metric.metricMissing(x, self.cluster[i].value)
        else:
            return self.metric.metric(x, self.cluster[i].value)

    def closestCluster(self, x):
        anyMissing = MISSING in x

        clusterId = None
        clusterNumber = None
        clusterAffinity = None
        for i in xrange(self.numberOfClusters):
            affinity = self.clusterDistance(i, x, anyMissing)

            if affinity is INVALID:
                return INVALID

            if clusterId is None or affinity < clusterAffinity:
                clusterId = self.ids[i]
                clusterNumber = i
                clusterAffinity = affinity

        # XSD guarantees at least one cluster; these can't be None
        return clusterId, clusterNumber, clusterAffinity

PMML.classMap["ClusteringModel"] = ClusteringModel

class MissingValueWeights(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MissingValueWeights">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="NUM-ARRAY" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["MissingValueWeights"] = MissingValueWeights

class Cluster(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Cluster">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="KohonenMap" />
                <xs:group minOccurs="0" ref="NUM-ARRAY" />
                <xs:element minOccurs="0" ref="Partition" />
                <xs:element minOccurs="0" ref="Covariances" />
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="optional" />
            <xs:attribute name="name" type="xs:string" use="optional" />
            <xs:attribute name="size" type="xs:nonNegativeInteger" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Cluster"] = Cluster

class KohonenMap(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="KohonenMap">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="coord1" type="xs:float" use="optional" />
            <xs:attribute name="coord2" type="xs:float" use="optional" />
            <xs:attribute name="coord3" type="xs:float" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["KohonenMap"] = KohonenMap

class Covariances(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Covariances">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Covariances"] = Covariances

class ClusteringField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ClusteringField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Comparisons" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute default="true" name="isCenterField">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="true" />
                        <xs:enumeration value="false" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="1" name="fieldWeight" type="REAL-NUMBER" />
            <xs:attribute name="similarityScale" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="compareFunction" type="COMPARE-FUNCTION" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        if "compareFunction" in self.attrib and self.attrib["compareFunction"] == "gaussSim":
            if "similarityScale" not in self.attrib:
                raise PMMLValidationError("ClusteringField with compareFunction == \"gaussSim\" requires a similarityScale")

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["ClusteringField"] = ClusteringField

class Comparisons(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Comparisons">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Matrix" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Comparisons"] = Comparisons

class ComparisonMeasure(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ComparisonMeasure">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element ref="euclidean" />
                    <xs:element ref="squaredEuclidean" />
                    <xs:element ref="chebychev" />
                    <xs:element ref="cityBlock" />
                    <xs:element ref="minkowski" />
                    <xs:element ref="simpleMatching" />
                    <xs:element ref="jaccard" />
                    <xs:element ref="tanimoto" />
                    <xs:element ref="binarySimilarity" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="kind" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="distance" />
                        <xs:enumeration value="similarity" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="absDiff" name="compareFunction" type="COMPARE-FUNCTION" />
            <xs:attribute name="minimum" type="NUMBER" use="optional" />
            <xs:attribute name="maximum" type="NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    # Note: minimum and maximum are for information only; they're not used for scoring (e.g. for truncating the compareFunction)

    def post_validate(self):
        kind = {"distance": Metric.DISTANCE, "similarity": Metric.SIMILARITY}[self.attrib["kind"]]
        metric = self.child(nonExtension)

        if metric.kind != kind:
            raise PMMLValidationError("ComparisonMeasure kind (%s) does not match the metric's kind (%s, %s)" % (self.attrib["kind"], metric.__class__.__name__, str(metric.kind)))

PMML.classMap["ComparisonMeasure"] = ComparisonMeasure

class Metric(object):
    DISTANCE = Atom("Distance")
    SIMILARITY = Atom("Similarity")

    def configure(self, fieldWeights, missingValueWeights, compareFunctions, similarityScales, comparisonTables):
        self.fieldWeights = fieldWeights
        self.missingValueWeights = missingValueWeights

        if self.kind is self.SIMILARITY:
            self.metricMissing = lambda x, y: INVALID

        self.compareFunctions = []
        for compareFunction, similarityScale, comparisonTable in zip(compareFunctions, similarityScales, comparisonTables):
            if compareFunction == "absDiff": self.compareFunctions.append(lambda x, y: abs(x - y))
            elif compareFunction == "gaussSim": self.compareFunctions.append(lambda x, y: 2.*math.exp(-((x - y) / similarityScale)**2))
            elif compareFunction == "delta": self.compareFunctions.append(lambda x, y: 0 if x == y else 1)
            elif compareFunction == "equal": self.compareFunctions.append(lambda x, y: 1 if x == y else 0)
            elif compareFunction == "table":
                def table(x, y):
                    try:
                        return comparisonTable[int(x), int(y)]
                    except (IndexError, ValueError):
                        raise InvalidDataError
                self.compareFunctions.append(table)

        self.N = len(self.compareFunctions)

        if self.missingValueWeights is None:
            self.missingValueWeights = [1.] * self.N

class squaredEuclidean(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="squaredEuclidean">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.DISTANCE

    def metric(self, data, cluster):
        return sum([c(x, y)**2 * w for c, x, y, w in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights)])

    def metricMissing(self, data, cluster):
        adjustNumer = 0.
        adjustDenom = 0.
        sumNonMissing = 0.
        numNonMissing = 0
        for c, x, y, w, q in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights, self.missingValueWeights):
            adjustNumer += q
            if x is not MISSING:
                adjustDenom += q
                sumNonMissing += c(x, y)**2 * w
                numNonMissing += 1
        if numNonMissing == self.N:
            adjustM = 1.
        else:
            try:
                adjustM = adjustNumer / adjustDenom
            except ZeroDivisionError:
                return INVALID

        return sumNonMissing * adjustM

PMML.classMap["squaredEuclidean"] = squaredEuclidean

class euclidean(squaredEuclidean):
    xsd = load_xsdElement(PMML, """
    <xs:element name="euclidean">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def metric(self, data, cluster):
        squared = squaredEuclidean.metric(self, data, cluster)
        if squared is INVALID: return INVALID
        return math.sqrt(squared)

    def metricMissing(self, data, cluster):
        squared = squaredEuclidean.metricMissing(self, data, cluster)
        if squared is INVALID: return INVALID
        return math.sqrt(squared)

PMML.classMap["euclidean"] = euclidean

class cityBlock(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="cityBlock">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.DISTANCE

    def metric(self, data, cluster):
        return sum([c(x, y) * w for c, x, y, w in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights)])

    def metricMissing(self, data, cluster):
        adjustNumer = 0.
        adjustDenom = 0.
        sumNonMissing = 0.
        numNonMissing = 0
        for c, x, y, w, q in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights, self.missingValueWeights):
            adjustNumer += q
            if x is not MISSING:
                adjustDenom += q
                sumNonMissing += c(x, y) * w
                numNonMissing += 1
        if numNonMissing == self.N:
            adjustM = 1.
        else:
            try:
                adjustM = adjustNumer / adjustDenom
            except ZeroDivisionError:
                return INVALID
        return sumNonMissing * adjustM

PMML.classMap["cityBlock"] = cityBlock

class chebychev(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="chebychev">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.DISTANCE

    def metric(self, data, cluster):
        return max([c(x, y) * w for c, x, y, w in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights)])

    def metricMissing(self, data, cluster):
        adjustNumer = 0.
        adjustDenom = 0.
        maxNonMissing = 0.
        numNonMissing = 0
        for c, x, y, w, q in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights, self.missingValueWeights):
            adjustNumer += q
            if x is not MISSING:
                adjustDenom += q
                this = c(x, y) * w
                if this > maxNonMissing:
                    maxNonMissing = this
                numNonMissing += 1
        if numNonMissing == self.N:
            adjustM = 1.
        else:
            try:
                adjustM = adjustNumer / adjustDenom
            except ZeroDivisionError:
                return INVALID
        return maxNonMissing * adjustM

PMML.classMap["chebychev"] = chebychev

class minkowski(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="minkowski">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="p-parameter" type="NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.p = self.attrib["p-parameter"]

    kind = Metric.DISTANCE

    def metric(self, data, cluster):
        return sum([c(x, y)**self.p * w for c, x, y, w in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights)])**(1./self.p)

    def metricMissing(self, data, cluster):
        adjustNumer = 0.
        adjustDenom = 0.
        sumNonMissing = 0.
        numNonMissing = 0
        for c, x, y, w, q in itertools.izip(self.compareFunctions, data, cluster, self.fieldWeights, self.missingValueWeights):
            adjustNumer += q
            if x is not MISSING:
                adjustDenom += q
                sumNonMissing += math.pow(c(x, y), self.p)
                numNonMissing += 1
        if numNonMissing == self.N:
            adjustM = 1.
        else:
            try:
                adjustM = adjustNumer / adjustDenom
            except ZeroDivisionError:
                return INVALID

        try:
            return math.pow(sumNonMissing * adjustM, 1./self.p)
        except ValueError:
            return INVALID

PMML.classMap["minkowski"] = minkowski

class simpleMatching(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="simpleMatching">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.SIMILARITY

    def metric(self, data, cluster):
        a11 = sum([1 if x == 1 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a10 = sum([1 if x == 1 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        a01 = sum([1 if x == 0 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a00 = sum([1 if x == 0 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        try:
            return float(a11 + a00) / float(a11 + a10 + a01 + a00)
        except ZeroDivisionError:
            return INVALID

PMML.classMap["simpleMatching"] = simpleMatching

class jaccard(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="jaccard">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.SIMILARITY

    def metric(self, data, cluster):
        a11 = sum([1 if x == 1 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a10 = sum([1 if x == 1 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        a01 = sum([1 if x == 0 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a00 = sum([1 if x == 0 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        try:
            return float(a11) / float(a11 + a10 + a01)
        except ZeroDivisionError:
            return INVALID

PMML.classMap["jaccard"] = jaccard

class tanimoto(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="tanimoto">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.SIMILARITY

    def metric(self, data, cluster):
        a11 = sum([1 if x == 1 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a10 = sum([1 if x == 1 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        a01 = sum([1 if x == 0 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a00 = sum([1 if x == 0 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        try:
            return float(a11 + a00) / float(a11 + 2.*(a10 + a01) + a00)
        except ZeroDivisionError:
            return INVALID

PMML.classMap["tanimoto"] = tanimoto

class binarySimilarity(PMML, Metric):
    xsd = load_xsdElement(PMML, """
    <xs:element name="binarySimilarity">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="c00-parameter" type="NUMBER" use="required" />
            <xs:attribute name="c01-parameter" type="NUMBER" use="required" />
            <xs:attribute name="c10-parameter" type="NUMBER" use="required" />
            <xs:attribute name="c11-parameter" type="NUMBER" use="required" />
            <xs:attribute name="d00-parameter" type="NUMBER" use="required" />
            <xs:attribute name="d01-parameter" type="NUMBER" use="required" />
            <xs:attribute name="d10-parameter" type="NUMBER" use="required" />
            <xs:attribute name="d11-parameter" type="NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    kind = Metric.SIMILARITY

    def post_validate(self):
        self.c00 = self.attrib["c00-parameter"]
        self.c01 = self.attrib["c01-parameter"]
        self.c10 = self.attrib["c10-parameter"]
        self.c11 = self.attrib["c11-parameter"]
        self.d00 = self.attrib["d00-parameter"]
        self.d01 = self.attrib["d01-parameter"]
        self.d10 = self.attrib["d10-parameter"]
        self.d11 = self.attrib["d11-parameter"]
        
    def metric(self, data, cluster):
        a11 = sum([1 if x == 1 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a10 = sum([1 if x == 1 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        a01 = sum([1 if x == 0 and y == 1 else 0 for x, y in itertools.izip(data, cluster)])
        a00 = sum([1 if x == 0 and y == 0 else 0 for x, y in itertools.izip(data, cluster)])
        try:
            return float(self.c11*a11 + self.c10*a10 + self.c01*a01 + self.c00*a00) / float(self.d11*a11 + self.d10*a10 + self.d01*a01 + self.d00*a00)
        except ZeroDivisionError:
            return INVALID

PMML.classMap["binarySimilarity"] = binarySimilarity

class RegressionModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RegressionModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element maxOccurs="unbounded" ref="RegressionTable" />
                
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="targetFieldName" type="FIELD-NAME" use="optional" />
            <xs:attribute name="modelType" use="optional">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="linearRegression" />
                        <xs:enumeration value="stepwisePolynomialRegression" />
                        <xs:enumeration value="logisticRegression" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="none" name="normalizationMethod" type="REGRESSIONNORMALIZATIONMETHOD" />
        </xs:complexType>
    </xs:element>
    """)

    CLASSIFICATION = Atom("Classification")
    REGRESSION = Atom("Regression")
    
    CATEGORICAL = Atom("Categorical")
    ORDINAL = Atom("Ordinal")
    CONTINUOUS = Atom("Continuous")

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)

        if self.attrib["functionName"] == "classification":
            self.functionName = self.CLASSIFICATION
        elif self.attrib["functionName"] == "regression":
            self.functionName = self.REGRESSION
        else:
            raise PMMLValidationError("The only valid RegressionModel functionNames are: 'regression', 'classification'")

        self.regressionTables = self.matches(RegressionTable)
        normalizationMethod = self.attrib.get("normalizationMethod", "none")

        if self.functionName == self.REGRESSION:
            if len(self.regressionTables) != 1:
                raise PMMLValidationError("RegressionModels with functionName='regression' must have exactly one RegressionTable, not %d" % len(self.regressionTables))
            
            self.yvalue = lambda get: self.regressionTables[0].evaluate(get)

            if normalizationMethod == "none":
                self.prob = lambda y: y
                self.probinv = lambda f: f

            elif normalizationMethod == "softmax" or normalizationMethod == "logit":
                self.prob = lambda y: 1./(1. + exp(-y))
                self.probinv = lambda f: -log(1./f - 1.)

            elif normalizationMethod == "exp":
                self.prob = lambda y: exp(y)
                self.probinv = lambda f: log(f)

            else:
                raise PMMLValidationError("The only valid RegressionModel normalizationMethods with functionName='regression' are: 'none', 'softmax', 'logit' (same as 'softmax'), and 'exp'")

        else:
            if len(self.regressionTables) < 2:
                raise PMMLValidationError("RegressionModels with functionName='classification' must have at least two RegressionTables, not %d" % len(self.regressionTables))

            # normalization methods require knowledge of targetField; see top_validate()

            self.targetValues = []
            valuesSeen = set()
            for regressionTable in self.regressionTables:
                targetCategory = regressionTable.attrib.get("targetCategory", None)
                if targetCategory is None:
                    raise PMMLValidationError("RegressionModels with functionName='classification' must have a targetCategory in each RegressionTable")
                if targetCategory in valuesSeen:
                    raise PMMLValidationError("RegressionModels with functionName='classification' are not allowed to have duplicate targetCategories: \"%s\" is seen more than once" % targetCategory)

                self.targetValues.append(targetCategory)

        self.targetFieldName = None
        self.targetFieldOptype = None

    def top_validate_model(self, dataContext):
        if "targetFieldName" in self.attrib:
            miningField = dataContext.miningSchema.child(lambda x: isinstance(x, MiningField) and x.attrib["name"] == self.attrib["targetFieldName"] and x.attrib.get("usageType", "active") == "predicted", exception=False)
            if miningField is None:
                raise PMMLValidationError("%s references \"%s\" with targetFieldName, but there is no predicted MiningField with this name" % (self.__class__.__name__, self.attrib["targetFieldName"]))

            self.targetFieldName = self.attrib["targetFieldName"]
            self.targetFieldOptype = {"categorical": self.CATEGORICAL,
                                      "ordinal": self.ORDINAL,
                                      "continuous": self.CONTINUOUS,
                                      }[dataContext.optype[self.targetFieldName]]

        if self.functionName == self.CLASSIFICATION:
            normalizationMethod = self.attrib.get("normalizationMethod", "none")

            if normalizationMethod == "none": self.prob =        lambda yall: yall
            elif normalizationMethod == "simplemax": self.prob = lambda yall: yall/sum(yall)
            elif normalizationMethod == "exp": self.prob =       lambda yall: numpy.exp(yall)
            else:
                if normalizationMethod == "softmax": F =   lambda yall: numpy.exp(yall)/sum(numpy.exp(yall))
                elif normalizationMethod == "logit": F =   lambda yall: 1./(1. - yall)
                elif normalizationMethod == "probit": F =  lambda yall: numpy.array([0.5*erf(y/sqrt(2.)) + 0.5 for y in yall])
                elif normalizationMethod == "loglog": F =  lambda yall: numpy.exp(-numpy.exp(-yall))
                elif normalizationMethod == "cauchit": F = lambda yall: numpy.array([0.5 + (1./pi)*atan(y) for y in yall])
                elif normalizationMethod == "cloglog":
                    def F(yall):
                        out = numpy.empty(len(yall), dtype=yall.dtype)
                        for i in xrange(len(yall)):
                            # 1. - numpy.exp(-numpy.exp(yall))
                            try:
                                tmp = math.exp(yall[i])
                            except OverflowError:
                                out[i] = 1.
                            else:
                                try:
                                    out[i] = 1. - math.exp(-tmp)
                                except OverflowError:
                                    out[i] = float("-inf")
                        return out

                if self.targetFieldOptype == self.ORDINAL and normalizationMethod == "softmax":
                    def prob(yall):
                        f1 = F(yall)
                        f2 = numpy.roll(f1, 1)
                        f2[0] = 0.
                        return f1 - f2
                    self.prob = prob

                elif self.targetFieldOptype == self.ORDINAL:
                    def prob(yall):
                        f1 = F(yall)
                        f2 = numpy.roll(f1, 1)
                        f2[0] = 0.
                        f1[-1] = 1.
                        return f1 - f2
                    self.prob = prob
                    
                else:
                    self.prob = F

    def yvalue(self, get):
        return numpy.array([t.evaluate(get) for t in self.regressionTables])

    def winner(self, yvalues):
        i = numpy.argmax(yvalues)
        return i, self.targetValues[i]

PMML.classMap["RegressionModel"] = RegressionModel

class RegressionTable(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="RegressionTable">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="NumericPredictor" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="CategoricalPredictor" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="PredictorTerm" />
            </xs:sequence>
            <xs:attribute name="intercept" type="REAL-NUMBER" use="required" />
            <xs:attribute name="targetCategory" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.numericTerms = self.matches(NumericPredictor)
        self.numericFields = []
        self.numericCoefficients = []
        self.numericExponents = []
        for numeric in self.numericTerms:
            self.numericFields.append(numeric.attrib["name"])
            self.numericExponents.append(numeric.attrib.get("exponent", 1))
            self.numericCoefficients.append(numeric.attrib["coefficient"])
        self.numericCoefficients = numpy.array(self.numericCoefficients)

        self.categoricalTerms = self.matches(CategoricalPredictor)
        self.categoryFields = []
        self.categoryValues = []
        self.categoryCoefficients = []
        for category in self.categoricalTerms:
            self.categoryFields.append(category.attrib["name"])
            self.categoryValues.append(category.attrib["value"])
            self.categoryCoefficients.append(category.attrib["coefficient"])
        self.categoryCoefficients = numpy.array(self.categoryCoefficients)

        self.predictorTerms = self.matches(PredictorTerm)
        self.predictorCoefficients = []
        for predictorTerm in self.predictorTerms:
            self.predictorCoefficients.append(predictorTerm.attrib["coefficient"])
        self.predictorCoefficients = numpy.array(self.predictorCoefficients)

    def values(self, get):
        numericX = [get(field) for field in self.numericFields]
        if INVALID in numericX: raise InvalidDataError
        if MISSING in numericX: return MISSING
        try:
            numericX = [float(x)**exponent for x, exponent in zip(numericX, self.numericExponents)]
        except (ValueError, AttributeError, ZeroDivisionError):
            raise InvalidDataError

        categoryX = [get(field) for field in self.categoryFields]
        if INVALID in categoryX: raise InvalidDataError
        categoryX = [1. if x == value else 0. for x, value in zip(categoryX, self.categoryValues)]

        predictorX = [predictorTerm.evaluateWithoutCoefficient(get) for predictorTerm in self.predictorTerms]
        if MISSING in predictorX: return MISSING

        return numericX, categoryX, predictorX

    def evaluate(self, get):
        vals = self.values(get)
        if vals is MISSING: return MISSING
        numericX, categoryX, predictorX = vals

        numericX = numpy.array(numericX)
        categoryX = numpy.array(categoryX)
        predictorX = numpy.array(predictorX)

        output = self.attrib["intercept"]
        try:
            output += sum(numericX * self.numericCoefficients)
            output += sum(categoryX * self.categoryCoefficients)
            output += sum(predictorX * self.predictorCoefficients)
        except FloatingPointError:
            raise InvalidDataError

        return output

PMML.classMap["RegressionTable"] = RegressionTable

class NumericPredictor(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NumericPredictor">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute default="1" name="exponent" type="INT-NUMBER" />
            <xs:attribute name="coefficient" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["name"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

PMML.classMap["NumericPredictor"] = NumericPredictor

class CategoricalPredictor(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CategoricalPredictor">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="coefficient" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["name"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

PMML.classMap["CategoricalPredictor"] = CategoricalPredictor

class PredictorTerm(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PredictorTerm">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldRef" />
            </xs:sequence>
            <xs:attribute name="name" type="FIELD-NAME" />
            <xs:attribute name="coefficient" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.fieldRefs = self.matches(FieldRef)

    def top_validate(self, dataContext):
        if "name" in self.attrib:
            if self.attrib["name"] in dataContext.cast:
                raise PMMLValidationError("The %s name parameter must be unique, but \"%s\" is found in the MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

        for fieldRef in self.fieldRefs:
            if dataContext.optype[fieldRef["field"]] != "continuous":
                raise PMMLValidationError("FieldRefs in a PredictorTerm must all refer to continuous features; \"%s\" is %s" % (fieldRef["field"], dataContext.optype[fieldRef["field"]]))

    def evaluateWithoutCoefficient(self, get):
        values = [f.evaluate(get) for f in self.fieldRefs]
        if INVALID in values: raise InvalidDataError
        if MISSING in values: return MISSING
        return reduce(operator.mul, values, 1.)

    def evaluate(self, get):
        return self.attrib["coefficient"] * self.evaluateWithoutCoefficient(get)

PMML.classMap["PredictorTerm"] = PredictorTerm

class Output(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Output">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="OutputField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.fields = self.matches(OutputField)

        self.featuredFields = {}
        for field in self.fields:
            field.pmmlOutput = self
            if "feature" in field.attrib:
                self.featuredFields[field.attrib["name"]] = field

    def evaluate(self, get, scores):
        return [field.evaluate(get, scores) for field in self.fields]

PMML.classMap["Output"] = Output

class OutputField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="OutputField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:sequence maxOccurs="1" minOccurs="0">
                    <xs:element maxOccurs="1" minOccurs="0" ref="Decisions" />
                    <xs:group maxOccurs="1" minOccurs="1" ref="EXPRESSION" />
                </xs:sequence>
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="dataType" type="DATATYPE" />
            <xs:attribute name="feature" type="RESULT-FEATURE" />
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="displayName" type="xs:string" />
            <xs:attribute name="targetField" type="FIELD-NAME" />
            <xs:attribute name="value" type="xs:string" />
            <xs:attribute default="consequent" name="ruleFeature" type="RULE-FEATURE" />
            <xs:attribute default="exclusiveRecommendation" name="algorithm">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="recommendation" />
                        <xs:enumeration value="exclusiveRecommendation" />
                        <xs:enumeration value="ruleAssociation" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="1" name="rank" type="INT-NUMBER" />
            <xs:attribute default="confidence" name="rankBasis">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="confidence" />
                        <xs:enumeration value="support" />
                        <xs:enumeration value="lift" />
                        <xs:enumeration value="leverage" />
                        <xs:enumeration value="affinity" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="descending" name="rankOrder">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="descending" />
                        <xs:enumeration value="ascending" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="0" name="isMultiValued" />
            <xs:attribute name="segmentId" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    predictedValue = Atom("predictedValue")
    predictedDisplayValue = Atom("predictedDisplayValue")
    transformedValue = Atom("transformedValue")
    decision = Atom("decision")
    probability = Atom("probability")
    residual = Atom("residual")
    standardError = Atom("standardError")
    clusterId = Atom("clusterId")
    clusterAffinity = Atom("clusterAffinity")
    entityId = Atom("entityId")
    entityAffinity = Atom("entityAffinity")
    affinity = Atom("affinity")
    warning = Atom("warning")
    ruleValue = Atom("ruleValue")
    reasonCode = Atom("reasonCode")

    def post_validate(self):
        self.name = self.attrib["name"]

        if "feature" in self.attrib:
            self.feature = None
            for thing in self.__class__.__dict__.values() + OutputField.__dict__.values():
                if isinstance(thing, Atom) and str(thing) == self.attrib["feature"]:
                    self.feature = thing
                    break
            if self.feature is None:
                raise PMMLValidationError("Unrecognized OutputField feature \"%s\"" % self.attrib["feature"])

        else:
            self.feature = None

        if "displayName" in self.attrib:
            self.displayName = self.attrib["displayName"]
        else:
            self.displayName = self.attrib["name"]

        badAnywhere = re.search("[!-,/;-@[-^`{-~]", self.displayName)
        if badAnywhere:
            raise PMMLValidationError("An OutputField has chosen tag %s which contains at least one disallowed character. Disallowed: %s" % (self.displayName, str(badAnywhere.group())))
        badInitial = re.match("[\-.0-9]", self.displayName)
        if badInitial:
            raise PMMLValidationError("An OutputField has chosen tag %s, which begins with a disallowed initial character. Disallowed: %s" % (self.displayName, str(badInitial.group())))

        if "segmentId" in self.attrib:
            raise NotImplementedError("Alternate OutputField format using the segmentId attribute has not been implemented yet")

        # handle an expression and any decisions
        self.expression = None
        for expr in self.matches(isExpression, maxdepth=None):
            expr.inOutput = True
            if self.expression is None:
                self.expression = expr
        for expr in self.matches(FieldColumnPair, maxdepth=None):
            expr.inOutput = True

        self.decisions = self.child(Decisions, exception=False)

        if self.feature is self.transformedValue:
            if self.expression is None:
                raise PMMLValidationError("If an OutputField's feature is transformedValue, then it must contain an EXPRESSION")

        if self.feature is self.decision:
            if self.expression is None:
                raise PMMLValidationError("If an OutputField's feature is decision, then it must contain an EXPRESSION")

            if self.decisions is None:
                raise PMMLValidationError("If an OutputField's feature is decision, then it must contain a Decisions block")

    def top_validate(self, dataContext):
        if self.feature is None:
            if self.attrib["name"] not in dataContext.cast:
                raise PMMLValidationError("%s references \"%s\" (without a feature attribute) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

        else:
            miningField = dataContext.miningSchema.child(lambda x: isinstance(x, MiningField) and x.attrib["name"] == self.attrib["name"] and x.attrib.get("usageType", "active") == "predicted", exception=False)
            if miningField is None:
                raise PMMLValidationError("%s references \"%s\" with feature %s, but there is no predicted MiningField with this name" % (self.__class__.__name__, self.attrib["name"], str(self.feature)))

            # maybe someday it will be useful to make a connection between the MiningField and the OutputField
            # self.miningField = miningField    OR
            # miningField.outputField = self

        if "targetField" in self.attrib:
            miningField = dataContext.miningSchema.child(lambda x: isinstance(x, MiningField) and x.attrib["name"] == self.attrib["targetField"] and x.attrib.get("usageType", "active") == "predicted", exception=False)
            if miningField is None:
                raise PMMLValidationError("%s references \"%s\" with targetField, but there is no predicted MiningField with this name" % (self.__class__.__name__, self.attrib["targetField"]))

    def evaluate(self, get, scores):
        if self.feature is None:
            return self.displayName, get(self.name)

        if self.feature is self.transformedValue or self.feature is self.decision:
            featuredFields = self.pmmlOutput.featuredFields

            def get2(field):
                if field in featuredFields:
                    return featuredFields[field].evaluate(get, scores)[1]
                else:
                    return get(field)

            try:
                value = self.expression.evaluate(get2)
            except InvalidDataError:
                value = INVALID

            if self.feature is self.transformedValue:
                return self.displayName, value

            else:
                return self.displayName, self.decisions.output.get(value, value)

        if scores is INVALID or scores is IMMATURE:
            return self.displayName, scores
        
        if scores is None:
            return self.displayName, MISSING

        if "value" not in self.attrib:
            return self.displayName, scores.get(self.feature, MISSING)

        else:
            return self.displayName, scores.get((self.feature, self.attrib["value"]), MISSING)

    def makeVerbose(self, dataContext=None):
        if "optype" not in self.attrib or "dataType" not in self.attrib:
            feature = self.attrib.get("feature", None)

            optype = dataContext.optype.get(self.attrib["name"], None)
            dataType = dataContext.dataType.get(self.attrib["name"], None)

            if feature in ("predictedValue", "predictedDisplayValue"):
                if dataContext.functionName in ("classification", "clustering"):
                    optype = "categorical"
                    dataType = "string"
                elif dataContext.functionName in ("regression",):
                    optype = "continuous"
                    dataType = "double"
                else:
                    raise NotImplementedError("need to define optype for functionName \"%s\"" % dataContext.functionName)

            elif feature in ("probability", "affinity", "residual", "standardError", "clusterAffinity", "entityAffinity"):
                optype = "continuous"
                dataType = "double"

            elif feature in ("clusterId", "entityId"):
                optype = "continuous"
                dataType = "integer"

            elif feature in ("warning", "ruleValue"):
                optype = "categorical"
                dataType = "string"

            if "optype" not in self.attrib and optype is not None:
                self.attrib["optype"] = optype

            if "dataType" not in self.attrib and dataType is not None:
                self.attrib["dataType"] = dataType

        PMML.makeVerbose(self, dataContext)

PMML.classMap["OutputField"] = OutputField

class Decisions(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Decisions">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="Decision" />
            </xs:sequence>
            <xs:attribute name="businessProblem" type="xs:string" />
            <xs:attribute name="description" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.output = {}
        for decision in self.matches(Decision):
            self.output[decision.attrib["value"]] = decision.attrib.get("displayValue", decision.attrib["value"])

PMML.classMap["Decisions"] = Decisions

class Decision(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Decision">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="displayValue" type="xs:string" />
            <xs:attribute name="description" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Decision"] = Decision

class NeuralNetwork(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralNetwork">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="NeuralInputs" />
                <xs:element maxOccurs="unbounded" ref="NeuralLayer" />
                <xs:element minOccurs="0" ref="NeuralOutputs" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="activationFunction" type="ACTIVATION-FUNCTION" use="required" />
            <xs:attribute default="none" name="normalizationMethod" type="NN-NORMALIZATION-METHOD" />
            <xs:attribute default="0" name="threshold" type="REAL-NUMBER" />
            <xs:attribute name="width" type="REAL-NUMBER" />
            <xs:attribute default="1.0" name="altitude" type="REAL-NUMBER" />
            <xs:attribute name="numberOfLayers" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("NeuralNetwork has not been implemented yet")

PMML.classMap["NeuralNetwork"] = NeuralNetwork

class NeuralInputs(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralInputs">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="NeuralInput" />
            </xs:sequence>
            <xs:attribute name="numberOfInputs" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NeuralInputs"] = NeuralInputs

class NeuralLayer(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralLayer">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Neuron" />
            </xs:sequence>
            <xs:attribute name="numberOfNeurons" type="xs:nonNegativeInteger" />
            <xs:attribute name="activationFunction" type="ACTIVATION-FUNCTION" />
            <xs:attribute name="threshold" type="REAL-NUMBER" />
            <xs:attribute name="width" type="REAL-NUMBER" />
            <xs:attribute name="altitude" type="REAL-NUMBER" />
            <xs:attribute name="normalizationMethod" type="NN-NORMALIZATION-METHOD" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NeuralLayer"] = NeuralLayer

class NeuralOutputs(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralOutputs">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="NeuralOutput" />
            </xs:sequence>
            <xs:attribute name="numberOfOutputs" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NeuralOutputs"] = NeuralOutputs

class NeuralInput(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralInput">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="DerivedField" />
            </xs:sequence>
            <xs:attribute name="id" type="NN-NEURON-ID" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NeuralInput"] = NeuralInput

class Neuron(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Neuron">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Con" />
            </xs:sequence>
            <xs:attribute name="id" type="NN-NEURON-ID" use="required" />
            <xs:attribute name="bias" type="REAL-NUMBER" />
            <xs:attribute name="width" type="REAL-NUMBER" />
            <xs:attribute name="altitude" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Neuron"] = Neuron

class Con(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Con">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="from" type="NN-NEURON-IDREF" use="required" />
            <xs:attribute name="weight" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Con"] = Con

class NeuralOutput(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NeuralOutput">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="DerivedField" />
            </xs:sequence>
            <xs:attribute name="outputNeuron" type="NN-NEURON-IDREF" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NeuralOutput"] = NeuralOutput

class NaiveBayesModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NaiveBayesModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="BayesInputs" />
                <xs:element ref="BayesOutput" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="threshold" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self): 
        self.isScorable = self.attrib.get("isScorable", True)

        if self["functionName"] != "classification":
            raise PMMLValidationError("The only valid RuleSetModel functionNames are: 'classification'")
        
        self.bayesInputs = self.child(BayesInputs).matches(BayesInput)
        self.bayesOutput = self.child(BayesOutput)

        self.bayesInput = {}
        for bi in self.bayesInputs:
            self.bayesInput[bi.attrib["fieldName"]] = bi

        # self.targetCategories are strings because that is how they're referenced by TargetValueCount["value"] and OutputField["value"]
        self.targetIndex = {}
        self.targetCategories = []
        self.targetCounts = []
        for i, (outputValue, tvc) in enumerate(self.bayesOutput.targetValueCounts.tvcMap.items()):
            self.targetIndex[outputValue] = i
            self.targetCategories.append(outputValue)
            self.targetCounts.append(tvc.attrib["count"])
        self.targetCounts = numpy.array(self.targetCounts)

    def evaluate(self, get):
        likelihood = numpy.array(self.targetCounts)

        for bayesInput in self.bayesInputs:
            ### get the input value; INVALID input -> skip all input fields, MISSING input -> skip only the missing field
            inputValue = bayesInput.evaluate(get)
            if inputValue is INVALID: return INVALID

            if inputValue is not MISSING:
                fractions = numpy.empty(len(likelihood))

                pairCounts = bayesInput.pairCounts
                denominators = bayesInput.denominators
                for i, outputValue in enumerate(self.targetCategories):
                    try:
                        pairCount = pairCounts[inputValue][outputValue]
                    except KeyError:
                        # not found in the table; this (inputValue, outputValue) combination has zero counts
                        pairCount = 0.

                    if pairCount == 0.:
                        fractions[i] = self.attrib["threshold"]
                    else:
                        # if pairCounts and denominators are truly in sync, denominator == 0 *only if* pairCount == 0
                        denominator = denominators[outputValue]
                        fractions[i] = pairCount / denominator

                likelihood *= fractions

        totalLikelihood = sum(likelihood)
        if totalLikelihood != 0.:
            probability = likelihood / totalLikelihood
        else:
            probability = likelihood

        return dict(zip(self.targetCategories, probability))

PMML.classMap["NaiveBayesModel"] = NaiveBayesModel

class BayesInputs(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BayesInputs">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="BayesInput" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BayesInputs"] = BayesInputs

class BayesInput(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BayesInput">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="DerivedField" />
                <xs:element maxOccurs="unbounded" ref="PairCounts" />
            </xs:sequence>
            <xs:attribute name="fieldName" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.derivedField = self.child(DerivedField, exception=False)

        self.pairCounts = {}
        self.denominators = {}
        self.pcMap = {}
        self.tvcMap = {}

        for pairCounts in self.matches(PairCounts):
            inputValue = pairCounts.attrib["value"]

            if inputValue in self.pairCounts:
                raise PMMLValidationError("Multiple PairCounts are associated with value \"%s\"" % inputValue)

            self.pairCounts[inputValue] = {}
            self.pcMap[inputValue] = pairCounts
            self.tvcMap[inputValue] = {}
            for outputValue, tvc in pairCounts.targetValueCounts.tvcMap.items():
                self.pairCounts[inputValue][outputValue] = tvc.attrib["count"]
                self.tvcMap[inputValue][outputValue] = tvc

                if outputValue not in self.denominators:
                    self.denominators[outputValue] = 0.
                self.denominators[outputValue] += tvc.attrib["count"]

        if self.derivedField is not None:
            expression = self.derivedField.child(isExpression)
            if not isinstance(expression, Discretize):
                raise PMMLValidationError("In a BayesInput's DerivedField, the only allowed expression is Discretize, not \"%s\"" % expression.__class__.__name__)

            if self.attrib["fieldName"] != expression.attrib["field"]:
                raise PMMLValidationError("The BayesInput fieldName \"%s\" is different from its Discretize field \"%s\"" % (self.attrib["fieldName"], expression.attrib["field"]))

    def top_validate(self, dataContext):
        if self.attrib["fieldName"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["fieldName"], dataContext.contextString()))

        if dataContext.optype[self.attrib["fieldName"]] == "continuous":
            if self.derivedField is None:
                raise PMMLValidationError("A continuous feature (\"%s\") cannot be used in a BayesInput without discretization: supply a DerivedField" % self.attrib["fieldName"])

        if self.derivedField is None:
            cast = dataContext.cast[self.attrib["fieldName"]]
        else:
            cast = castFunction(self.derivedField.attrib["optype"], self.derivedField.attrib["dataType"], [], [], False)
        
        for inputValue in self.pairCounts.keys():
            try:
                realInputValue = cast(inputValue)
            except ValueError, err:
                raise PMMLValidationError("Could not cast PairCounts value \"%s\" for BayesInput field \"%s\": %s" % (inputValue, self.attrib["fieldName"], str(err)))
            
            if hash(realInputValue) != hash(inputValue):
                self.pairCounts[realInputValue] = self.pairCounts[inputValue]
                del self.pairCounts[inputValue]

                self.pcMap[realInputValue] = self.pcMap[inputValue]
                del self.pcMap[inputValue]

                self.tvcMap[realInputValue] = self.tvcMap[inputValue]
                del self.tvcMap[inputValue]

    def evaluate(self, get):
        if self.derivedField is None:
            return get(self.attrib["fieldName"])
        else:
            try:
                return self.derivedField.expression.evaluate(get)
            except InvalidDataError:
                return INVALID

PMML.classMap["BayesInput"] = BayesInput

class BayesOutput(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BayesOutput">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="TargetValueCounts" />
            </xs:sequence>
            <xs:attribute name="fieldName" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.targetValueCounts = self.child(TargetValueCounts)

    def top_validate(self, dataContext):
        miningField = dataContext.miningSchema.child(lambda x: isinstance(x, MiningField) and x.attrib["name"] == self.attrib["fieldName"] and x.attrib.get("usageType", "active") == "predicted", exception=False)
        if miningField is None:
            raise PMMLValidationError("%s references \"%s\" with fieldName, but there is no predicted MiningField with this name" % (self.__class__.__name__, self.attrib["fieldName"]))

PMML.classMap["BayesOutput"] = BayesOutput

class PairCounts(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PairCounts">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="TargetValueCounts" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.targetValueCounts = self.child(TargetValueCounts)

PMML.classMap["PairCounts"] = PairCounts

class TargetValueCounts(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TargetValueCounts">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="TargetValueCount" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.tvcMap = {}
        for tvc in self.matches(TargetValueCount):
            self.tvcMap[tvc.attrib["value"]] = tvc

PMML.classMap["TargetValueCounts"] = TargetValueCounts

class TargetValueCount(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TargetValueCount">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="count" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        if self.attrib["count"] < 0.:
            raise PMMLValidationError("The count must be non-negative, not %g" % self.attrib["count"])

PMML.classMap["TargetValueCount"] = TargetValueCount

class ModelStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ModelStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="UnivariateStats" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="MultivariateStats" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ModelStats"] = ModelStats

class UnivariateStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="UnivariateStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Counts" />
                <xs:element minOccurs="0" ref="NumericInfo" />
                <xs:element minOccurs="0" ref="DiscrStats" />
                <xs:element minOccurs="0" ref="ContStats" />
                <xs:element minOccurs="0" ref="Anova" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" />
            <xs:attribute default="0" name="weighted">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="0" />
                        <xs:enumeration value="1" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if "field" in self.attrib and self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["UnivariateStats"] = UnivariateStats

class Counts(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Counts">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="totalFreq" type="NUMBER" use="required" />
            <xs:attribute name="missingFreq" type="NUMBER" />
            <xs:attribute name="invalidFreq" type="NUMBER" />
            <xs:attribute name="cardinality" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Counts"] = Counts

class NumericInfo(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NumericInfo">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Quantile" />
            </xs:sequence>
            <xs:attribute name="minimum" type="NUMBER" />
            <xs:attribute name="maximum" type="NUMBER" />
            <xs:attribute name="mean" type="NUMBER" />
            <xs:attribute name="standardDeviation" type="NUMBER" />
            <xs:attribute name="median" type="NUMBER" />
            <xs:attribute name="interQuartileRange" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["NumericInfo"] = NumericInfo

class Quantile(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Quantile">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="quantileLimit" type="PERCENTAGE-NUMBER" use="required" />
            <xs:attribute name="quantileValue" type="NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Quantile"] = Quantile

class DiscrStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DiscrStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="2" minOccurs="0" ref="Array" />
            </xs:sequence>
            <xs:attribute name="modalValue" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["DiscrStats"] = DiscrStats

class ContStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ContStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Interval" />
                <xs:group minOccurs="0" ref="FrequenciesType" />
            </xs:sequence>
            <xs:attribute name="totalValuesSum" type="NUMBER" />
            <xs:attribute name="totalSquaresSum" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ContStats"] = ContStats

class MultivariateStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MultivariateStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="MultivariateStat" />
            </xs:sequence>
            <xs:attribute name="targetCategory" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["MultivariateStats"] = MultivariateStats

class MultivariateStat(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MultivariateStat">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="name" type="xs:string" />
            <xs:attribute name="category" type="xs:string" />
            <xs:attribute default="1" name="exponent" type="INT-NUMBER" />
            <xs:attribute default="false" name="isIntercept" type="xs:boolean" />
            <xs:attribute name="importance" type="PROB-NUMBER" />
            <xs:attribute name="stdError" type="NUMBER" />
            <xs:attribute name="tValue" type="NUMBER" />
            <xs:attribute name="chiSquareValue" type="NUMBER" />
            <xs:attribute name="fStatistic" type="NUMBER" />
            <xs:attribute name="dF" type="NUMBER" />
            <xs:attribute name="pValueAlpha" type="PROB-NUMBER" />
            <xs:attribute name="pValueInitial" type="PROB-NUMBER" />
            <xs:attribute name="pValueFinal" type="PROB-NUMBER" />
            <xs:attribute default="0.95" name="confidenceLevel" type="PROB-NUMBER" />
            <xs:attribute name="confidenceLowerBound" type="NUMBER" />
            <xs:attribute name="confidenceUpperBound" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["MultivariateStat"] = MultivariateStat

class Anova(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Anova">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="3" minOccurs="3" ref="AnovaRow" />
            </xs:sequence>
            <xs:attribute name="target" type="FIELD-NAME" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if "target" in self.attrib and self.attrib["target"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["target"], dataContext.contextString()))

PMML.classMap["Anova"] = Anova

class AnovaRow(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="AnovaRow">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="type" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="Model" />
                        <xs:enumeration value="Error" />
                        <xs:enumeration value="Total" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="sumOfSquares" type="NUMBER" use="required" />
            <xs:attribute name="degreesOfFreedom" type="NUMBER" use="required" />
            <xs:attribute name="meanOfSquares" type="NUMBER" />
            <xs:attribute name="fValue" type="NUMBER" />
            <xs:attribute name="pValue" type="PROB-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["AnovaRow"] = AnovaRow

class Partition(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Partition">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="PartitionFieldStats" />
            </xs:sequence>
            <xs:attribute name="name" type="xs:string" use="required" />
            <xs:attribute name="size" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Partition"] = Partition

class PartitionFieldStats(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PartitionFieldStats">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Counts" />
                <xs:element minOccurs="0" ref="NumericInfo" />
                <xs:group minOccurs="0" ref="FrequenciesType" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute default="0" name="weighted">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="0" />
                        <xs:enumeration value="1" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["PartitionFieldStats"] = PartitionFieldStats

class TimeSeriesModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeSeriesModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element maxOccurs="3" minOccurs="0" ref="TimeSeries" />
                <xs:element maxOccurs="1" minOccurs="0" ref="SpectralAnalysis" />
                <xs:element maxOccurs="1" minOccurs="0" ref="ARIMA" />
                <xs:element maxOccurs="1" minOccurs="0" ref="ExponentialSmoothing" />
                <xs:element maxOccurs="1" minOccurs="0" ref="SeasonalTrendDecomposition" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="bestFit" type="TIMESERIES-ALGORITHM" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("TimeSeriesModel has not been implemented yet")

PMML.classMap["TimeSeriesModel"] = TimeSeriesModel

class TimeSeries(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeSeries">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="1" minOccurs="0" ref="TimeAnchor" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="TimeValue" />
            </xs:sequence>
            <xs:attribute default="original" name="usage" type="TIMESERIES-USAGE" />
            <xs:attribute name="startTime" type="REAL-NUMBER" />
            <xs:attribute name="endTime" type="REAL-NUMBER" />
            <xs:attribute default="none" name="interpolationMethod" type="INTERPOLATION-METHOD" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TimeSeries"] = TimeSeries

class TimeValue(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeValue">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="1" minOccurs="0" ref="Timestamp" />
            </xs:sequence>
            <xs:attribute name="index" type="INT-NUMBER" use="optional" />
            <xs:attribute name="time" type="NUMBER" use="optional" />
            <xs:attribute name="value" type="REAL-NUMBER" use="required" />
            <xs:attribute name="standardError" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TimeValue"] = TimeValue

class TimeAnchor(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeAnchor">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="TimeCycle" />
                <xs:element maxOccurs="2" minOccurs="0" ref="TimeException" />
            </xs:sequence>
            <xs:attribute name="type" type="TIME-ANCHOR" />
            <xs:attribute name="offset" type="INT-NUMBER" />
            <xs:attribute name="stepsize" type="INT-NUMBER" />
            <xs:attribute name="displayName" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TimeAnchor"] = TimeAnchor

class TimeCycle(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeCycle">
        <xs:complexType>
            <xs:sequence>
                <xs:group maxOccurs="1" minOccurs="0" ref="INT-ARRAY" />
            </xs:sequence>
            <xs:attribute name="length" type="INT-NUMBER" />
            <xs:attribute name="type" type="VALID-TIME-SPEC" />
            <xs:attribute name="displayName" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TimeCycle"] = TimeCycle

class TimeException(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TimeException">
        <xs:complexType>
            <xs:sequence>
                <xs:group minOccurs="1" ref="INT-ARRAY" />
            </xs:sequence>
            <xs:attribute name="type" type="TIME-EXCEPTION-TYPE" />
            <xs:attribute name="count" type="INT-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TimeException"] = TimeException

class ExponentialSmoothing(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ExponentialSmoothing">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="1" minOccurs="1" ref="Level" />
                <xs:element maxOccurs="1" minOccurs="0" ref="Trend_ExpoSmooth" />
                <xs:element maxOccurs="1" minOccurs="0" ref="Seasonality_ExpoSmooth" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="TimeValue" />
            </xs:sequence>
            <xs:attribute name="RMSE" type="REAL-NUMBER" />
            <xs:attribute default="none" name="transformation">
                <xs:simpleType>
                    <xs:restriction base="xs:NMTOKEN">
                        <xs:enumeration value="none" />
                        <xs:enumeration value="logarithmic" />
                        <xs:enumeration value="squareroot" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ExponentialSmoothing"] = ExponentialSmoothing

class Level(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Level">
        <xs:complexType>
            <xs:attribute name="alpha" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="smoothedValue" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Level"] = Level

class Trend_ExpoSmooth(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Trend_ExpoSmooth">
        <xs:complexType>
            <xs:sequence>
                <xs:group minOccurs="0" ref="REAL-ARRAY" />
            </xs:sequence>
            <xs:attribute default="additive" name="trend">
                <xs:simpleType>
                    <xs:restriction base="xs:NMTOKEN">
                        <xs:enumeration value="additive" />
                        <xs:enumeration value="damped_additive" />
                        <xs:enumeration value="multiplicative" />
                        <xs:enumeration value="damped_multiplicative" />
                        <xs:enumeration value="polynomial_exponential" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="gamma" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="1" name="phi" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="smoothedValue" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Trend_ExpoSmooth"] = Trend_ExpoSmooth

class Seasonality_ExpoSmooth(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Seasonality_ExpoSmooth">
        <xs:complexType>
            <xs:sequence>
                <xs:group ref="REAL-ARRAY" />
            </xs:sequence>
            <xs:attribute name="type" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:NMTOKEN">
                        <xs:enumeration value="additive" />
                        <xs:enumeration value="multiplicative" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="period" type="INT-NUMBER" use="required" />
            <xs:attribute name="unit" type="xs:string" use="optional" />
            <xs:attribute name="phase" type="INT-NUMBER" use="optional" />
            <xs:attribute name="delta" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Seasonality_ExpoSmooth"] = Seasonality_ExpoSmooth

class SpectralAnalysis(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SpectralAnalysis" />
    """)

PMML.classMap["SpectralAnalysis"] = SpectralAnalysis

class ARIMA(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ARIMA" />
    """)

PMML.classMap["ARIMA"] = ARIMA

class SeasonalTrendDecomposition(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SeasonalTrendDecomposition" />
    """)

PMML.classMap["SeasonalTrendDecomposition"] = SeasonalTrendDecomposition

class TransformationDictionary(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TransformationDictionary">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="DefineFunction" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="DerivedField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    builtin = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b,   # integer division if the numbers are integers (according to regression tests)
        "min": lambda *values: min(values),
        "max": lambda *values: max(values),
        "sum": lambda *values: sum(values),
        "avg": lambda *values: float(sum(values)) / float(len(values)),   # true division for avg
        "log10": lambda a: math.log10(a),
        "ln": lambda a: math.log(a),
        "sqrt": lambda a: math.sqrt(a),
        "abs": lambda a: abs(a),
        "exp": lambda a: math.exp(a),
        "pow": lambda a, b: math.pow(a, b),
        "threshold": lambda a, b: 1 if a > b else 0,
        "floor": lambda a: math.floor(a),
        "ceil": lambda a: math.ceil(a),
        "round": lambda a: round(a),
        "isMissing": lambda a: a is MISSING,
        "isNotMissing": lambda a: a is not MISSING,
        "equal": lambda a, b: a.rstrip(" \t\n\r") == b.rstrip(" \t\n\r") if isinstance(a, basestring) and isinstance(b, basestring)
                              else a == b,
        "notEqual": lambda a, b: a.rstrip(" \t\n\r") != b.rstrip(" \t\n\r") if isinstance(a, basestring) and isinstance(b, basestring)
                                 else a != b,           # "In PMML, trailing blanks are not significant" (according to spec).  Why is that???
        "lessThan": lambda a, b: a < b,
        "lessOrEqual": lambda a, b: a <= b,
        "greaterThan": lambda a, b: a > b,
        "greaterOrEqual": lambda a, b: a >= b,
        "and": lambda a, b: a and b,
        "or": lambda a, b: a or b,
        "not": lambda a: not a,
        "isIn": lambda a, *values: a in values,
        "isNotIn": lambda a, *values: a not in values,
        "if": lambda test, forTrue, forFalse: forTrue if test else forFalse,
        "lowercase": lambda a: a.lower(),
        "uppercase": lambda a: a.upper(),
        "substring": lambda a, start, length: INVALID if (min(0, start-1) < 0 or length < 0) else a[start-1:start-1+length],
                                                                                       # FIXME: return INVALID for badindexes?
        "trimBlanks": lambda a: a.lstrip(" \t\n\r").rstrip(" \t\n\r"),
        "formatNumber": lambda num, format: format % num,
        "formatDatetime": lambda t, format: t.strftime(format),
        "dateDaysSinceYear": lambda t, year: (t.t - datetime.date(year, 1, 1)).days if isinstance(t, DateType)
                                                else (t.t - datetime.datetime(year, 1, 1, 0, 0, 0, 0, t.t.tzinfo)).days,
        "dateSecondsSinceYear": lambda t, year: (t.t - datetime.date(year, 1, 1)).days * 24*60*60 if isinstance(t, DateType)
                                                else int(round(datetime_total_seconds(t.t - datetime.datetime(year, 1, 1, 0, 0, 0, 0, t.t.tzinfo)))),
        "dateSecondsSinceMidnight": lambda t: 0 if isinstance(t, DateType)
                                                else t.t.hour*60*60 + t.t.minute*60 + t.t.second,
        }

    builtinAllowMissing = ["isMissing", "isNotMissing"]

    def post_validate(self):
        self.user = {}
        for defineFunction in self.matches(DefineFunction):
            self.user[defineFunction.attrib["name"]] = defineFunction

        conflict = set(self.user.keys()).intersection(self.builtin.keys())
        if len(conflict) > 0:
            raise PMMLValidationError("User-defined function names conflict with built-in functions: %s" % repr(conflict))

        self.derivedFields = {}
        self.cast = {}
        self.optype = {}
        self.dataType = {}
        for derivedField in self.matches(DerivedField):
            if "name" not in derivedField.attrib:
                raise PMMLValidationError("All DerivedFields in a LocalTransformations must have \"name\" attributes")

            name = derivedField.attrib["name"]
            if name in self.derivedFields:
                raise PMMLValidationError("DerivedField \"%s\" appears more than once" % name)

            self.derivedFields[name] = derivedField
            self.cast[name] = castFunction(derivedField.attrib["optype"], derivedField.attrib["dataType"], [], [], False)
            self.optype[name] = derivedField.attrib["optype"]
            self.dataType[name] = derivedField.attrib["dataType"]

PMML.classMap["TransformationDictionary"] = TransformationDictionary

class LocalTransformations(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="LocalTransformations">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="DerivedField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.derivedFields = {}
        self.cast = {}
        self.optype = {}
        self.dataType = {}
        for derivedField in self.matches(DerivedField):
            if "name" not in derivedField.attrib:
                raise PMMLValidationError("All DerivedFields in a LocalTransformations must have \"name\" attributes")

            name = derivedField.attrib["name"]
            if name in self.derivedFields:
                raise PMMLValidationError("DerivedField \"%s\" appears more than once in the same LocalTransformation" % name)

            self.derivedFields[name] = derivedField
            self.cast[name] = castFunction(derivedField.attrib["optype"], derivedField.attrib["dataType"], [], [], False)
            self.optype[name] = derivedField.attrib["optype"]
            self.dataType[name] = derivedField.attrib["dataType"]

PMML.classMap["LocalTransformations"] = LocalTransformations

class DerivedField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DerivedField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="EXPRESSION" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Value" />
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" use="required" />
            <xs:attribute name="dataType" type="DATATYPE" use="required" />
            <xs:attribute name="name" type="FIELD-NAME" />
            <xs:attribute name="displayName" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.expression = self.child(lambda x: not isinstance(x, (Extension, Value)))

        self.groupField = None
        for aggregate in self.matches(Aggregate, maxdepth=None):
            if "groupField" in aggregate.attrib:
                if self.groupField is None:
                    self.groupField = aggregate.attrib["groupField"]
                else:
                    if self.groupField != aggregate.attrib["groupField"]:
                        raise PMMLValidationError("DerivedField contains Aggregates with different groupFields (\"%s\" and \"%s\")" % (self.groupField, aggregate.attrib["groupField"]))

    def top_validate(self, dataContext):
        for fieldRef in self.matches(FieldRef, maxdepth=None):
            derivedField = dataContext.derivedFields.get(fieldRef.attrib["field"], None)
            if derivedField is not None and derivedField.groupField is not None:
                if self.groupField is None:
                    self.groupField = derivedField.groupField

                if self.groupField != derivedField.groupField:
                    raise PMMLValidationError("DerivedField contains Aggregates with different groupFields (\"%s\" and \"%s\", through a \"%s\" reference)" % (self.groupField, derivedField.groupField, fieldRef.attrib["field"]))

PMML.classMap["DerivedField"] = DerivedField

class Constant(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Constant">
        <xs:complexType>
            <xs:simpleContent>
                <xs:extension base="xs:string">
                    <xs:attribute name="dataType" type="DATATYPE" />
                </xs:extension>
            </xs:simpleContent>
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        if "dataType" in self.attrib:
            self.value = pmmlBuiltinType[self.attrib["dataType"]](self.value)

            if self.attrib["dataType"] in pmmlBuiltinFloats:
                if numpy.isnan(self.value) or numpy.isinf(self.value):
                    raise PMMLValidationError("Floating-point constant should not be NaN/Inf")

        else:
            # infer type from the content (required by PMML spec)
            if self.value == "true":
                self.value = True
            elif self.value == "false":
                self.value = False
            else:
                try:
                    self.value = int(self.value)
                except ValueError:
                    if self.value not in ("NaN", "Inf"):
                        try:
                            self.value = float(self.value)
                        except ValueError:
                            pass

    def evaluate(self, get):
        return self.value

PMML.classMap["Constant"] = Constant

class FieldRef(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="FieldRef">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="mapMissingTo" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        self.field = self.attrib["field"]

        # note that this will not be overwritten with a typed value
        # if the <FieldRef> is inside of a global <TransformationDictionary>,
        # where no dataContext is known
        self.mapMissingTo = self.attrib.get("mapMissingTo", MISSING)

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

        if "mapMissingTo" in self.attrib:
            try:
                self.mapMissingTo = dataContext.cast[self.attrib["field"]](self.attrib["mapMissingTo"])
            except ValueError, err:
                raise PMMLValidationError("Could not cast FieldRef mapMissingTo \"%s\": %s" % (self.attrib["mapMissingTo"], str(err)))
        else:
            self.mapMissingTo = MISSING

    def evaluate(self, get):
        value = get(self.field)
        if value is INVALID:
            raise InvalidDataError
        elif value is MISSING:
            return self.mapMissingTo
        else:
            return value

PMML.classMap["FieldRef"] = FieldRef

class NormContinuous(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NormContinuous">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="2" ref="LinearNorm" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="mapMissingTo" type="NUMBER" />
            <xs:attribute default="asIs" name="outliers" type="OUTLIER-TREATMENT-METHOD" />
        </xs:complexType>
    </xs:element>
    """)

    ASIS = Atom("AsIs")
    ASMISSINGVALUES = Atom("AsMissingValues")
    ASEXTREMEVALUES = Atom("AsExtremeValues")

    inOutput = False

    def post_validate(self):
        linearNorms = self.matches(LinearNorm)

        origs = [x.attrib["orig"] for x in linearNorms]
        origs2 = list(origs)
        origs2.sort()
        if origs != origs2 or len(origs) != len(set(origs2)):
            raise PMMLValidationError("LinearNorms in NormContinuous must be strictly increasing by \"orig\" (no duplicates, no transpositions)")

        self.intervals = []
        for i in xrange(len(linearNorms) - 1):
            self.intervals.append((linearNorms[i].attrib["orig"], linearNorms[i+1].attrib["orig"], linearNorms[i].attrib["norm"], linearNorms[i+1].attrib["norm"]))

        if "outliers" not in self.attrib:
            self.outliers = self.ASIS
        else:
            self.outliers = {"asIs": self.ASIS,
                             "asMissingValues": self.ASMISSINGVALUES,
                             "asExtremeValues": self.ASEXTREMEVALUES,
                             }[self.attrib["outliers"]]

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

    def evaluate(self, get):
        value = get(self.attrib["field"])
        if value is INVALID:
            raise InvalidDataError

        if value is MISSING:
            if "mapMissingTo" in self.attrib:
                return self.attrib["mapMissingTo"]
            else:
                return MISSING

        for a1, a2, b1, b2 in self.intervals:
            if value < a2:
                break

        interpolation = b1 + (value - a1)/(a2 - a1)*(b2 - b1)

        if value < a1 or value > a2:
            if self.outliers == self.ASIS:
                return interpolation
            elif self.outliers == self.ASMISSINGVALUES:
                return MISSING
            elif self.outliers == self.ASEXTREMEVALUES:
                if value < a1: return b1
                if value > a2: return b2

        if numpy.isnan(interpolation) or numpy.isinf(interpolation):
            raise InvalidDataError

        return interpolation

PMML.classMap["NormContinuous"] = NormContinuous

class LinearNorm(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="LinearNorm">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="orig" type="NUMBER" use="required" />
            <xs:attribute name="norm" type="NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["LinearNorm"] = LinearNorm

class NormDiscrete(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NormDiscrete">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute fixed="indicator" name="method">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="indicator" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="mapMissingTo" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

    def evaluate(self, get):
        value = get(self.attrib["field"])
        if value is INVALID:
            raise InvalidDataError

        if value is MISSING:
            if "mapMissingTo" in self.attrib:
                return self.attrib["mapMissingTo"]
            else:
                return MISSING

        if value == self.attrib["value"]:
            return 1
        else:
            return 0

PMML.classMap["NormDiscrete"] = NormDiscrete

class Discretize(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Discretize">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="DiscretizeBin" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="mapMissingTo" type="xs:string" />
            <xs:attribute name="defaultValue" type="xs:string" />
            <xs:attribute name="dataType" type="DATATYPE" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        self._discretizeBins = self.matches(DiscretizeBin)

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

    def evaluate(self, get):
        value = get(self.attrib["field"])
        if value is INVALID:
            raise InvalidDataError

        if value is MISSING:
            if "mapMissingTo" in self.attrib:
                return self.attrib["mapMissingTo"]
            else:
                return MISSING

        for bin in self._discretizeBins:
            if bin.interval.contains(value):
                return bin.attrib["binValue"]

        if "defaultValue" in self.attrib:
            return self.attrib["defaultValue"]
        else:
            return MISSING

PMML.classMap["Discretize"] = Discretize

class DiscretizeBin(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DiscretizeBin">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Interval" />
            </xs:sequence>
            <xs:attribute name="binValue" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.interval = self.child(Interval)

PMML.classMap["DiscretizeBin"] = DiscretizeBin

class MapValues(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MapValues">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="FieldColumnPair" />
                <xs:choice minOccurs="0">
                    <xs:element ref="TableLocator" />
                    <xs:element ref="InlineTable" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="mapMissingTo" type="xs:string" />
            <xs:attribute name="defaultValue" type="xs:string" />
            <xs:attribute name="outputColumn" type="xs:string" use="required" />
            <xs:attribute name="dataType" type="DATATYPE" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        self.fieldColumnPairs = {}
        for fcp in self.matches(FieldColumnPair):
            self.fieldColumnPairs[fcp.attrib["field"]] = fcp.attrib["column"]

        self.table = self.child(lambda x: isinstance(x, (TableLocator, InlineTable)))

        columnsToCheck = self.fieldColumnPairs.values() + [self.attrib["outputColumn"]]

        for index, row in enumerate(self.table.rows):
            for column in columnsToCheck:
                try:
                    column_xml = row.child(column)
                except ValueError:
                    raise PMMLValidationError("Column \"%s\" missing from row %d of MapValues table%s" % (column, index, self.fileAndLine("<PMML>")))

                try:
                    column_text = column_xml[0]
                    if not isinstance(column_text, xmlbase.XMLText):
                        raise IndexError
                except IndexError:
                    raise PMMLValidationError("Column \"%s\" in row %d of MapValues table%s has no text" % (column, index, self.fileAndLine("<PMML>")))

    def evaluate(self, get):
        values = {}
        for field in self.fieldColumnPairs.keys():
            value = get(field)

            if value is INVALID:
                raise InvalidDataError

            if value is MISSING:
                if "mapMissingTo" in self.attrib:
                    return self.attrib["mapMissingTo"]
                else:
                    return MISSING

            values[field] = value

        for row in self.table.rows:
            satisfied = True
            for field, column in self.fieldColumnPairs.items():
                if str(row.child(column)[0]) != values[field]:
                    satisfied = False
                    break
            if satisfied:
                return str(row.child(self.attrib["outputColumn"])[0])

        if "defaultValue" in self.attrib:
            return self.attrib["defaultValue"]
        else:
            return MISSING

PMML.classMap["MapValues"] = MapValues

class FieldColumnPair(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="FieldColumnPair">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="column" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["FieldColumnPair"] = FieldColumnPair

class Aggregate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Aggregate">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="function" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="count" />
                        <xs:enumeration value="sum" />
                        <xs:enumeration value="average" />
                        <xs:enumeration value="min" />
                        <xs:enumeration value="max" />
                        <xs:enumeration value="multiset" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="groupField" type="FIELD-NAME" />
            <xs:attribute name="sqlWhere" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        self.field = self.attrib["field"]
        self.groupField = self.attrib.get("groupField", None)
        if self.attrib["function"] == "multiset":
            raise NotImplementedError("The 'multiset' option for \"Aggregate\" function is not implemented.")

        self.sqlWhere = self.attrib.get("sqlWhere", None)
        if self.sqlWhere is None:
            self.sqlWhere = lambda get: True
            return
        else:
            m = re.match("\s*(.*[^\s=!<>])\s*==(.+)", self.sqlWhere)
            if m is None:
                m = re.match("\s*(.*[^\s=!<>])\s*=(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) == value
                return

            m = re.match("\s*(.*[^\s=!<>])\s*<>(.+)", self.sqlWhere)
            if m is None:
                m = re.match("\s*(.*[^\s=!<>])\s*!=(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) != value
                return
                
            m = re.match("\s*(.*[^\s=!<>])\s*>(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) > value
                return

            m = re.match("\s*(.*[^\s=!<>])\s*<(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) < value
                return

            m = re.match("\s*(.*[^\s=!<>])\s*>=(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) >= value
                return

            m = re.match("\s*(.*[^\s=!<>])\s*<=(.+)", self.sqlWhere)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) <= value
                return

            m = re.match("\s*(.*[^\s=!<>])\s+BETWEEN\s+(.+)AND\s+(.+)", self.sqlWhere, re.I)
            if m is not None:
                try:
                    field = m.group(1)
                    lower = eval(m.group(2))
                    upper = eval(m.group(3))
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: lower <= get(field) <= upper
                return

            m = re.match("\s*(.*[^\s=!<>])\s+LIKE\s+(.+)", self.sqlWhere, re.I)
            if m is not None:
                try:
                    field = m.group(1)
                    pattern = re.compile(eval(m.group(2)))
                except (SyntaxError, TypeError, sre_constants.error), err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                # FIXME: this evaluates the 'LIKE' clause as a Python regular expression, but SQL has a different regular expression syntax
                self.sqlWhere = lambda get: (re.match(pattern, get(field)) is not None)
                return

            m = re.match("\s*(.*[^\s=!<>])\s+IN\s+(\(.+\))\s*", self.sqlWhere, re.I)
            if m is not None:
                try:
                    field = m.group(1)
                    value = eval(m.group(2))
                    if not isinstance(value, tuple):
                        value = (value,)
                except SyntaxError, err:
                    raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (%s)" % (self.sqlWhere, str(err)))
                self.sqlWhere = lambda get: get(field) in value
                return

            raise PMMLValidationError("Illegal expression in sqlWhere: \"%s\" (doesn't match any SQL WHERE patterns)" % (self.sqlWhere))

    def top_validate_output(self, dataContext, pmmlOutput):
        if self.attrib["field"] not in dataContext.cast:
            if self.inOutput and pmmlOutput is not None:
                if self.attrib["field"] not in pmmlOutput.featuredFields:
                    raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s or Output section %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(), pmmlOutput.featuredFields.keys()))
            else:
                raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

        if "groupField" in self.attrib and self.attrib["groupField"] not in dataContext.cast:
            raise PMMLValidationError("%s references groupField \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

    # this is just the default function; it will usually be replaced by initialize()
    def evaluate(self, get):
        return MISSING

    def initialize(self, updateScheme):
        if self.attrib["function"] == "count":
            if self.groupField is None:
                self.updator = updateScheme.updator(COUNT)

                def evaluate(get):
                    output = self.updator.count()
                    if output is INVALID:
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate
            else:
                self.updators = {}
                self.updateScheme = updateScheme
                self.counters = (COUNT,)
                self.updatorFunction = "count"

                def evaluate(get):
                    updator = self.updators.get(get(self.groupField), None)
                    if updator is None: return MISSING
                    output = updator.count()
                    if output is INVALID:
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate

        elif self.attrib["function"] == "sum":
            if self.groupField is None:
                self.updator = updateScheme.updator(SUMX)

                def evaluate(get):
                    output = self.updator.sum()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate
            else:
                self.updators = {}
                self.updateScheme = updateScheme
                self.counters = (SUMX,)
                self.updatorFunction = "sum"

                def evaluate(get):
                    updator = self.updators.get(get(self.groupField), None)
                    if updator is None: return MISSING
                    output = updator.sum()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate

        elif self.attrib["function"] == "average":
            if self.groupField is None:
                self.updator = updateScheme.updator(SUM1, SUMX)

                def evaluate(get):
                    output = self.updator.mean()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate
            else:
                self.updators = {}
                self.updateScheme = updateScheme
                self.counters = (SUM1, SUMX,)
                self.updatorFunction = "mean"

                def evaluate(get):
                    updator = self.updators.get(get(self.groupField), None)
                    if updator is None: return MISSING
                    output = updator.mean()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate

        elif self.attrib["function"] == "min":
            if self.groupField is None:
                self.updator = updateScheme.updator(MIN)

                def evaluate(get):
                    output = self.updator.min()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate
            else:
                self.updators = {}
                self.updateScheme = updateScheme
                self.counters = (MIN,)
                self.updatorFunction = "min"

                def evaluate(get):
                    updator = self.updators.get(get(self.groupField), None)
                    if updator is None: return MISSING
                    output = updator.min()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate

        elif self.attrib["function"] == "max":
            if self.groupField is None:
                self.updator = updateScheme.updator(MAX)

                def evaluate(get):
                    output = self.updator.max()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate
            else:
                self.updators = {}
                self.updateScheme = updateScheme
                self.counters = (MAX,)
                self.updatorFunction = "max"

                def evaluate(get):
                    updator = self.updators.get(get(self.groupField), None)
                    if updator is None: return MISSING
                    output = updator.max()
                    if output is INVALID or numpy.isnan(output) or numpy.isinf(output):
                        raise InvalidDataError
                    else:
                        return output
                self.evaluate = evaluate

    def increment(self, syncNumber, get):
        value = get(self.field)

        if value is not INVALID and value is not MISSING:
            try:
                okay = self.sqlWhere(get)
            except ValueError:
                okay = False

            if okay:
                if self.groupField is None:
                    self.updator.increment(syncNumber, value)
                else:
                    groupValue = get(self.groupField)
                    if groupValue is not INVALID and groupValue is not MISSING:
                        updator = self.updators.get(groupValue, None)
                        if updator is None:
                            updator = self.updateScheme.updator(*self.counters)
                            self.updators[groupValue] = updator
                        updator.increment(syncNumber, value)

    def flush(self):
        if self.groupField is None:
            self.updator.clear()
        else:
            self.updators = {}

PMML.classMap["Aggregate"] = Aggregate

class DataDictionary(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DataDictionary">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="DataField" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Taxonomy" />
            </xs:sequence>
            <xs:attribute name="numberOfFields" type="xs:nonNegativeInteger" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        if "numberOfFields" in self.attrib and len(self.matches(DataField)) != self.attrib["numberOfFields"]:
            raise PMMLValidationError("DataDictionary numberOfFields is %d but the number of DataFields is %d" % (self.attrib["numberOfFields"], len(self.matches(DataField))))

        self.dataFields = {}
        for dataField in self.matches(DataField):
            name = dataField.attrib["name"]
            if name in self.dataFields:
                raise PMMLValidationError("DataField \"%s\" appears more than once" % name)
            self.dataFields[name] = dataField

PMML.classMap["DataDictionary"] = DataDictionary

class DataField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DataField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:sequence>
                    <xs:element maxOccurs="unbounded" minOccurs="0" ref="Interval" />
                    <xs:element maxOccurs="unbounded" minOccurs="0" ref="Value" />
                </xs:sequence>
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" use="required" />
            <xs:attribute name="dataType" type="DATATYPE" use="required" />
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="displayName" type="xs:string" />
            <xs:attribute name="taxonomy" type="xs:string" />
            <xs:attribute default="0" name="isCyclic">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="0" />
                        <xs:enumeration value="1" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        if self.attrib["optype"] == "ordinal" and self.attrib["dataType"] == "string":
            values = self.matches(Value)
            if len(values) == 0:
                raise PMMLValidationError("DataFields with ordinal optypes and string dataTypes are required to have enumerated Values")

        if "isCyclic" in self.attrib:
            if self.attrib["isCyclic"] == "0": self.isCyclic = False
            elif self.attrib["isCyclic"] == "1": self.isCyclic = True
        else:
            self.isCyclic = False

PMML.classMap["DataField"] = DataField

class Value(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Value">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="displayValue" type="xs:string" />
            <xs:attribute default="valid" name="property">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="valid" />
                        <xs:enumeration value="invalid" />
                        <xs:enumeration value="missing" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Value"] = Value

class Interval(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Interval">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="closure" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="closedOpen" />
                        <xs:enumeration value="closedClosed" />
                        <xs:enumeration value="openClosed" />
                        <xs:enumeration value="openOpen" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="leftMargin" type="NUMBER" />
            <xs:attribute name="rightMargin" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

    class MinusInfinity(Atom):
        def __lt__(self, other):
            return True
        def __le__(self, other):
            return True
        def __gt__(self, other):
            return False
        def __ge__(self, other):
            return False

    class PlusInfinity(Atom):
        def __lt__(self, other):
            return False
        def __le__(self, other):
            return False
        def __gt__(self, other):
            return True
        def __ge__(self, other):
            return True

    minusInfinity = MinusInfinity("MinusInfinity")
    plusInfinity = PlusInfinity("PlusInfinity")

    def post_validate(self):
        if "leftMargin" not in self.attrib and "rightMargin" not in self.attrib:
            raise PMMLValidationError("In an Interval, \"leftMargin\", \"rightMargin\", or both must be specified")

        if "leftMargin" in self.attrib:
            self.leftMargin = self.attrib["leftMargin"]
        else:
            self.leftMargin = self.minusInfinity

        if "rightMargin" in self.attrib:
            self.rightMargin = self.attrib["rightMargin"]
        else:
            self.rightMargin = self.plusInfinity

        if self.leftMargin >= self.rightMargin:
            raise PMMLValidationError("In an Interval, the leftMargin must be less than the rightMargin")

    def contains(self, value):
        if self.leftMargin < value < self.rightMargin:
            return True

        if value == self.leftMargin:
            return self.attrib["closure"] in ("closedOpen", "closedClosed")

        if value == self.rightMargin:
            return self.attrib["closure"] in ("openClosed", "closedClosed")

        return False

PMML.classMap["Interval"] = Interval

class SequenceModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SequenceModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element minOccurs="0" ref="Constraints" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Item" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Itemset" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="SetPredicate" />
                <xs:element maxOccurs="unbounded" ref="Sequence" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="SequenceRule" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="numberOfTransactions" type="INT-NUMBER" />
            <xs:attribute name="maxNumberOfItemsPerTransaction" type="INT-NUMBER" />
            <xs:attribute name="avgNumberOfItemsPerTransaction" type="REAL-NUMBER" />
            <xs:attribute name="numberOfTransactionGroups" type="INT-NUMBER" />
            <xs:attribute name="maxNumberOfTAsPerTAGroup" type="INT-NUMBER" />
            <xs:attribute name="avgNumberOfTAsPerTAGroup" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("SequenceModel has not been implemented yet")

PMML.classMap["SequenceModel"] = SequenceModel

class Constraints(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Constraints">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute default="1" name="minimumNumberOfItems" type="INT-NUMBER" />
            <xs:attribute name="maximumNumberOfItems" type="INT-NUMBER" />
            <xs:attribute default="1" name="minimumNumberOfAntecedentItems" type="INT-NUMBER" />
            <xs:attribute name="maximumNumberOfAntecedentItems" type="INT-NUMBER" />
            <xs:attribute default="1" name="minimumNumberOfConsequentItems" type="INT-NUMBER" />
            <xs:attribute name="maximumNumberOfConsequentItems" type="INT-NUMBER" />
            <xs:attribute default="0" name="minimumSupport" type="REAL-NUMBER" />
            <xs:attribute default="0" name="minimumConfidence" type="REAL-NUMBER" />
            <xs:attribute default="0" name="minimumLift" type="REAL-NUMBER" />
            <xs:attribute default="0" name="minimumTotalSequenceTime" type="REAL-NUMBER" />
            <xs:attribute name="maximumTotalSequenceTime" type="REAL-NUMBER" />
            <xs:attribute default="0" name="minimumItemsetSeparationTime" type="REAL-NUMBER" />
            <xs:attribute name="maximumItemsetSeparationTime" type="REAL-NUMBER" />
            <xs:attribute default="0" name="minimumAntConsSeparationTime" type="REAL-NUMBER" />
            <xs:attribute name="maximumAntConsSeparationTime" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Constraints"] = Constraints

class SetPredicate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SetPredicate">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="STRING-ARRAY" />
            </xs:sequence>
            <xs:attribute name="id" type="ELEMENT-ID" use="required" />
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute fixed="supersetOf" name="operator" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["SetPredicate"] = SetPredicate

class Delimiter(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Delimiter">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="delimiter" type="DELIMITER" use="required" />
            <xs:attribute name="gap" type="GAP" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Delimiter"] = Delimiter

class Time(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Time">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="min" type="NUMBER" />
            <xs:attribute name="max" type="NUMBER" />
            <xs:attribute name="mean" type="NUMBER" />
            <xs:attribute name="standardDeviation" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Time"] = Time

class Sequence(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Sequence">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="SetReference" />
                <xs:sequence maxOccurs="unbounded" minOccurs="0">
                    <xs:group ref="FOLLOW-SET" />
                </xs:sequence>
                <xs:element minOccurs="0" ref="Time" />
            </xs:sequence>
            <xs:attribute name="id" type="ELEMENT-ID" use="required" />
            <xs:attribute name="numberOfSets" type="INT-NUMBER" />
            <xs:attribute name="occurrence" type="INT-NUMBER" />
            <xs:attribute name="support" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Sequence"] = Sequence

class SetReference(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SetReference">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="setId" type="ELEMENT-ID" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SetReference"] = SetReference

class SequenceRule(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SequenceRule">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="AntecedentSequence" />
                <xs:element ref="Delimiter" />
                <xs:element minOccurs="0" ref="Time" />
                <xs:element ref="ConsequentSequence" />
                <xs:element minOccurs="0" ref="Time" />
            </xs:sequence>
            <xs:attribute name="id" type="ELEMENT-ID" use="required" />
            <xs:attribute name="numberOfSets" type="INT-NUMBER" use="required" />
            <xs:attribute name="occurrence" type="INT-NUMBER" use="required" />
            <xs:attribute name="support" type="REAL-NUMBER" use="required" />
            <xs:attribute name="confidence" type="REAL-NUMBER" use="required" />
            <xs:attribute name="lift" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SequenceRule"] = SequenceRule

class SequenceReference(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SequenceReference">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="seqId" type="ELEMENT-ID" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["SequenceReference"] = SequenceReference

class AntecedentSequence(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="AntecedentSequence">
        <xs:complexType>
            <xs:sequence>
                <xs:group ref="SEQUENCE" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["AntecedentSequence"] = AntecedentSequence

class ConsequentSequence(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ConsequentSequence">
        <xs:complexType>
            <xs:sequence>
                <xs:group ref="SEQUENCE" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ConsequentSequence"] = ConsequentSequence

class Targets(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Targets">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="Target" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Targets"] = Targets

class Target(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Target">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="TargetValue" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="castInteger">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="round" />
                        <xs:enumeration value="ceiling" />
                        <xs:enumeration value="floor" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="min" type="xs:double" />
            <xs:attribute name="max" type="xs:double" />
            <xs:attribute default="0" name="rescaleConstant" type="xs:double" />
            <xs:attribute default="1" name="rescaleFactor" type="xs:double" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["Target"] = Target

class TargetValue(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TargetValue">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element minOccurs="0" ref="Partition" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" />
            <xs:attribute name="displayValue" type="xs:string" />
            <xs:attribute name="priorProbability" type="PROB-NUMBER" />
            <xs:attribute name="defaultValue" type="NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TargetValue"] = TargetValue

class DefineFunction(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="DefineFunction">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="ParameterField" />
                <xs:group ref="EXPRESSION" />
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" use="required" />
            <xs:attribute name="dataType" type="DATATYPE" />
            <xs:attribute name="name" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.parameters = []
        last = None
        for child in self:
            if isinstance(child, ParameterField):
                self.parameters.append(child.attrib["name"])
            last = child
        self.expression = last

        if "dataType" in self.attrib:
            self.cast = castFunction(self.attrib["optype"], self.attrib["dataType"], [], [], False)
        else:
            self.cast = None
        
    def evaluate(self, get, args):
        context = dict(zip(self.parameters, args))
        def getInContext(field):
            value = context.get(field, None)
            if value is not None:
                return value
            else:
                return get(field)

        output = self.expression.evaluate(getInContext)
        if self.cast is not None:
            try:
                return self.cast(output)
            except ValueError:
                return INVALID
        else:
            return output

PMML.classMap["DefineFunction"] = DefineFunction

class ParameterField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ParameterField">
        <xs:complexType>
            <xs:attribute name="name" type="xs:string" use="required" />
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="dataType" type="DATATYPE" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ParameterField"] = ParameterField

class Apply(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Apply">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group maxOccurs="unbounded" minOccurs="0" ref="EXPRESSION" />
            </xs:sequence>
            <xs:attribute name="function" type="xs:string" use="required" />
            <xs:attribute name="mapMissingTo" type="xs:string" />
            <xs:attribute default="returnInvalid" name="invalidValueTreatment" type="INVALID-VALUE-TREATMENT-METHOD" />
        </xs:complexType>
    </xs:element>
    """)

    inOutput = False

    def post_validate(self):
        self.arglist = self.matches(nonExtension)

        self.mapMissingTo = self.attrib.get("mapMissingTo", MISSING)

        invalidValueTreatment = self.attrib.get("invalidValueTreatment", None)
        if invalidValueTreatment in (None, "asIs", "returnInvalid"):
            def treatment():
                raise InvalidDataError
            self.invalidValueTreatment = treatment

        elif invalidValueTreatment == "asMissing":
            self.invalidValueTreatment = lambda: self.mapMissingTo

    def top_validate_transformationDictionary(self, transformationDictionary):
        function = self.attrib["function"]

        if function in TransformationDictionary.builtin:
            self.userDefined = False
            self.function = TransformationDictionary.builtin[function]
            self.allowMissing = function in TransformationDictionary.builtinAllowMissing

            minParameters = self.function.func_code.co_argcount
            variableParameters = (len(self.function.func_code.co_varnames) != minParameters)

            if not variableParameters and minParameters != len(self.arglist):
                raise PMMLValidationError("Built-in function \"%s\" takes exactly %d arguments (%d applied)" % (function, minParameters, len(self.arglist)))

            if minParameters > len(self.arglist):
                raise PMMLValidationError("Built-in function \"%s\" takes at least %d arguments (%d applied)" % (function, minParameters, len(self.arglist)))

        elif transformationDictionary is not None and function in transformationDictionary.user:
            self.userDefined = True
            self.function = transformationDictionary.user[function]
                
            if len(self.function.parameters) != len(self.arglist):
                raise PMMLValidationError("User-defined function \"%s\" takes exactly %d arguments (%d applied)" % (function, len(self.function.parameters), len(self.arglist)))

        else:
            raise PMMLValidationError("Apply function \"%s\" not recognized (not built-in and not user-defined)" % function)

    def evaluate(self, get):
        try:
            args = [pmmlArg.evaluate(get) for pmmlArg in self.arglist]
        except InvalidDataError:
            return self.invalidValueTreatment()

        if self.userDefined:
            value = self.function.evaluate(get, args)
            if value is MISSING:
                return self.mapMissingTo
            else:
                return value

        else:
            if not self.allowMissing:
                if MISSING in args:
                    return self.invalidValueTreatment()

            try:
                value = self.function(*args)
            except Exception, err:   # used to be only ZeroDivisionError, ValueError, OverflowError
                return self.invalidValueTreatment()
            ### no input should ever stop processing
            #     raise RuntimeError("Exception in PMML function \"%s\"%s: %s (args: %s)" % (self.attrib["function"], self.fileAndLine("<PMML>"), str(err), repr(args)))

            if value is INVALID or (isinstance(value, float) and ((numpy.isnan(value) or numpy.isinf(value)))):
                return self.invalidValueTreatment()

            if value is MISSING:
                return self.mapMissingTo
            else:
                return value

PMML.classMap["Apply"] = Apply

class MiningBuildTask(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MiningBuildTask">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["MiningBuildTask"] = MiningBuildTask

class Extension(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Extension">
        <xs:complexType>
            <xs:complexContent mixed="true">
                <xs:restriction base="xs:anyType">
                    <xs:sequence>
                        <xs:any maxOccurs="unbounded" minOccurs="0" processContents="skip" />
                    </xs:sequence>
                    <xs:attribute name="extender" type="xs:string" use="optional" />
                    <xs:attribute name="name" type="xs:string" use="optional" />
                    <xs:attribute name="value" type="xs:string" use="optional" />
                </xs:restriction>
            </xs:complexContent>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Extension"] = Extension

class Array(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Array" type="ArrayType" />
    """)

    def post_validate(self):
        if self.attrib["type"] == "int":
            textExpected = xmlbase.XSDList("Array", "xs:integer")
        elif self.attrib["type"] == "real":
            textExpected = xmlbase.XSDList("Array", "xs:double")
        elif self.attrib["type"] == "string":
            textExpected = xmlbase.XSDList("Array", "xs:string")

        xmlText = []
        xmlChildren = []
        for child in self.children:
            if isinstance(child, xmlbase.XMLText):
                xmlText.append(child)
            else:
                xmlChildren.append(child)

        if len(xmlText) == 0 and hasattr(self, "value"):
            pass

        elif not hasattr(self, "value"):
            self.value = textExpected.validateText("".join([str(x) for x in xmlText]).lstrip(string.whitespace).rstrip(string.whitespace))
            self.children = xmlChildren

        else:
            raise PMMLValidationError("Array is in an inconsistent state: it has a 'values' member (Pythonic) and it contains text (XMLian)")

        if "n" in self.attrib:
            if self.attrib["n"] != len(self.value):
                raise PMMLValidationError("Array[\"n\"] should be equal to len(Array.values)")
        else:
            self.attrib["n"] = len(self.value)

PMML.classMap["Array"] = Array

class INT_SparseArray(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="INT-SparseArray">
        <xs:complexType>
            <xs:sequence>
                <xs:element minOccurs="0" ref="Indices" />
                <xs:element minOccurs="0" ref="INT-Entries" />
            </xs:sequence>
            <xs:attribute name="n" type="INT-NUMBER" use="optional" />
            <xs:attribute default="0" name="defaultValue" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["INT-SparseArray"] = INT_SparseArray

class REAL_SparseArray(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="REAL-SparseArray">
        <xs:complexType>
            <xs:sequence>
                <xs:element minOccurs="0" ref="Indices" />
                <xs:element minOccurs="0" ref="REAL-Entries" />
            </xs:sequence>
            <xs:attribute name="n" type="INT-NUMBER" use="optional" />
            <xs:attribute default="0" name="defaultValue" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["REAL-SparseArray"] = REAL_SparseArray

class Indices(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Indices">
        <xs:simpleType>
            <xs:list itemType="xs:int" />
        </xs:simpleType>
    </xs:element>
    """)

PMML.classMap["Indices"] = Indices

class INT_Entries(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="INT-Entries">
        <xs:simpleType>
            <xs:list itemType="xs:int" />
        </xs:simpleType>
    </xs:element>
    """)

PMML.classMap["INT-Entries"] = INT_Entries

class REAL_Entries(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="REAL-Entries">
        <xs:simpleType>
            <xs:list itemType="xs:double" />
        </xs:simpleType>
    </xs:element>
    """)

PMML.classMap["REAL-Entries"] = REAL_Entries

class Matrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Matrix">
        <xs:complexType>
            <xs:choice minOccurs="0">
                <xs:group maxOccurs="unbounded" ref="NUM-ARRAY" />
                <xs:element maxOccurs="unbounded" ref="MatCell" />
            </xs:choice>
            <xs:attribute default="any" name="kind" use="optional">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="diagonal" />
                        <xs:enumeration value="symmetric" />
                        <xs:enumeration value="any" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="nbRows" type="INT-NUMBER" use="optional" />
            <xs:attribute name="nbCols" type="INT-NUMBER" use="optional" />
            <xs:attribute name="diagDefault" type="REAL-NUMBER" use="optional" />
            <xs:attribute name="offDiagDefault" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    # should be easy to change values, but not structure
    # outputs more explicitly than it was input

    def post_validate(self):
        self.attrib["kind"] = self.attrib.get("kind", "any")

        if self.attrib["kind"] == "diagonal":
            if self.exists(MatCell) or len(self.matches(Array)) != 1:
                raise PMMLValidationError("Diagonal matrix should be a single Array (no MatCells)")

            array = self.child(Array)
            length = len(array.value)
            if "nbRows" not in self.attrib: self.attrib["nbRows"] = length
            if "nbCols" not in self.attrib: self.attrib["nbCols"] = length
            if self.attrib["nbRows"] != length or self.attrib["nbCols"] != length:
                raise PMMLValidationError("Diagonal matrix represented by %d-element Array, yet nbRows, nbCols are %d, %d" % (length, self.attrib["nbRows"], self.attrib["nbCols"]))

            self.lookup = array
            self.sparse = False

        elif self.attrib["kind"] == "symmetric":
            if self.exists(MatCell):
                raise PMMLValidationError("Symmetric matrix should be a sequence of Arrays (no MatCells)")

            arrays = self.matches("Array")
            for i, array in enumerate(arrays):
                if i != len(array.value):
                    raise PMMLValidationError("Symmetric matrix should be a sequence of incrementally growing Arrays: row %d has a %d-length Array" % (i, len(array.value)))

            if "nbRows" not in self.attrib: self.attrib["nbRows"] = i
            if "nbCols" not in self.attrib: self.attrib["nbCols"] = i
            if self.attrib["nbRows"] != length or self.attrib["nbCols"] != i:
                raise PMMLValidationError("Symmetric matrix represented by an incrementally growing sequence of Arrays, ending with length %d, yet nbRows, nbCols are %d, %d" % (i, self.attrib["nbRows"], self.attrib["nbCols"]))

            self.lookup = arrays
            self.sparse = False

        elif self.attrib["kind"] == "any":
            arrays = self.matches(Array)
            matcells = self.matches(MatCell)
            if len(arrays) > 0 and len(matcells) > 0:
                raise PMMLValidationError("Matrix must consist of Arrays or MatCells, but not both")

            if len(arrays) > 0:
                length = len(arrays[0].value)
                for array in arrays:
                    if len(array.value) != length:
                        raise PMMLValidationError("Matrix must be rectangular (first Array has length %d, but a later one has length %d)" % (length, len(array.value)))

                if "nbRows" not in self.attrib: self.attrib["nbRows"] = len(arrays)
                if "nbCols" not in self.attrib: self.attrib["nbCols"] = length
                if self.attrib["nbRows"] != len(arrays) or self.attrib["nbCols"] != length:
                    raise PMMLValidationError("Matrix contains %d Arrays of length %d, yet nbRows, nbCols are %d, %d" % (len(arrays), length, self.attrib["nbRows"], self.attrib["nbCols"]))

                self.lookup = arrays
                self.sparse = False

            else:
                if "nbRows" not in self.attrib or "nbCols" not in self.attrib:
                    raise PMMLValidationError("Matrix is sparsely defined by MatCells, so nbRows, nbCols must be set explicitly")

                if "diagDefault" not in self.attrib or "offDiagDefault" not in self.attrib:
                    raise PMMLValidationError("Matrix is sparsely defined by MatCells, so diagDefault, offDiagDefault must be set explicitly")

                self.lookup = {}
                for matcell in matcells:
                    index = matcell.attrib["row"] - 1, matcell.attrib["col"] - 1
                    if index in self.lookup:
                        raise PMMLValidationError("More than one MatCell is occupying row %d, col %d" % (matcell.attrib["row"], matcell.attrib["col"]))
                    if index[0] < 0 or index[0] >= self.attrib["nbRows"] or index[1] < 0 or index[1] >= self.attrib["nbCols"]:
                        raise PMMLValidationError("MatCell at %d, %d is out of range (nbRows, nbCols is %d, %d)" % (matcell.attrib["row"], matcell.attrib["col"], self.attrib["nbRows"], self.attrib["nbCols"]))
                    self.lookup[index] = matcell

                self.sparse = True

    def __getitem__(self, index):
        if len(index) == 2 and isinstance(index[0], (int, long)) and isinstance(index[1], (int, long)):
            i, j = index

            if i < 0 or i >= self.attrib["nbRows"] or j < 0 or j >= self.attrib["nbCols"]:
                raise IndexError("Index %d, %d is out-of-range for an nbRows, nbCols %d, %d matrix" % (i, j, self.attrib["nbRows"], self.attrib["nbCols"]))

            if self.sparse:
                matcell = self.lookup.get((i, j), None)
                if matcell is None:
                    if i == j:
                        return self.attrib["diagDefault"]
                    else:
                        return self.attrib["offDiagDefault"]
                else:
                    return matcell.value

            else:
                if self.attrib["kind"] == "any":
                    return self.lookup[i].value[j]

                elif self.attrib["kind"] == "symmetric":
                    if j > i:
                        i, j = j, i
                    return self.lookup[i].value[j]

                else:
                    if i != j:
                        return 0.
                    else:
                        return self.array.value[i]

        else:
            return PMML.__getitem__(self, index)

    def __setitem__(self, index, value):
        if len(index) == 2 and isinstance(index[0], (int, long)) and isinstance(index[1], (int, long)):
            i, j = index

            if i < 0 or i >= self.attrib["nbRows"] or j < 0 or j >= self.attrib["nbCols"]:
                raise IndexError("Index %d, %d is out-of-range for an nbRows, nbCols %d, %d matrix" % (i, j, self.attrib["nbRows"], self.attrib["nbCols"]))

            if self.sparse:
                matcell = self.lookup.get((i, j), None)
                if matcell is None:
                    pmml.newInstance("MatCell", attrib={"row": i + 1, "col": j + 1})
                    self.children.append(matcell)
                    self.lookup[(i, j)] = matcell

                matcell.value = value

            else:
                if self.attrib["kind"] == "any":
                    self.lookup[i].value[j] = value

                elif self.attrib["kind"] == "symmetric":
                    if j > i:
                        i, j = j, i
                    self.lookup[i].value[j] = value

                else:
                    if i != j:
                        raise IndexError("Attempting to set an off-diagonal element in a diagonal matrix")
                    else:
                        self.array.value[i] = value

        else:
            PMML.__setitem__(self, index, value)

    def __delitem__(self, index):
        if len(index) == 2 and isinstance(index[0], (int, long)) and isinstance(index[1], (int, long)):
            i, j = index

            if i < 0 or i >= self.attrib["nbRows"] or j < 0 or j >= self.attrib["nbCols"]:
                raise IndexError("Index %d, %d is out-of-range for an nbRows, nbCols %d, %d matrix" % (i, j, self.attrib["nbRows"], self.attrib["nbCols"]))

            if self.sparse:
                matcell = self.lookup.get((i, j), None)
                if matcell is None:
                    raise IndexError("Index %d, %d does not have a MatCell to delete" % (i, j))

                del self.children[self.children.index(matcell)]
                del self.lookup[(i, j)]

            else:
                raise ValueError("Cannot delete indexes from a non-sparse matrix")

        else:
            PMML.__delitem__(self, index)

PMML.classMap["Matrix"] = Matrix

class MatCell(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MatCell">
        <xs:complexType>
            <xs:simpleContent>
                <xs:extension base="xs:string">
                    <xs:attribute name="row" type="INT-NUMBER" use="required" />
                    <xs:attribute name="col" type="INT-NUMBER" use="required" />
                </xs:extension>
            </xs:simpleContent>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.value = float(self.value)

PMML.classMap["MatCell"] = MatCell

class NearestNeighborModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NearestNeighborModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="TrainingInstances" />
                <xs:element ref="ComparisonMeasure" />
                <xs:element ref="KNNInputs" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="numberOfNeighbors" type="INT-NUMBER" use="required" />
            <xs:attribute default="average" name="continuousScoringMethod" type="CONT-SCORING-METHOD" />
            <xs:attribute default="majorityVote" name="categoricalScoringMethod" type="CAT-SCORING-METHOD" />
            <xs:attribute name="instanceIdVariable" type="xs:string" />
            <xs:attribute default="0.001" name="threshold" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("NearestNeighborModel has not been implemented yet")

PMML.classMap["NearestNeighborModel"] = NearestNeighborModel

class TrainingInstances(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TrainingInstances">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="InstanceFields" />
                <xs:choice>
                    <xs:element ref="TableLocator" />
                    <xs:element ref="InlineTable" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute default="false" name="isTransformed" type="xs:boolean" />
            <xs:attribute name="recordCount" type="INT-NUMBER" use="optional" />
            <xs:attribute name="fieldCount" type="INT-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["TrainingInstances"] = TrainingInstances

class InstanceFields(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="InstanceFields">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="InstanceField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["InstanceFields"] = InstanceFields

class InstanceField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="InstanceField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="xs:string" use="required" />
            <xs:attribute name="column" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["InstanceField"] = InstanceField

class KNNInputs(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="KNNInputs">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="KNNInput" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["KNNInputs"] = KNNInputs

class KNNInput(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="KNNInput">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute default="1" name="fieldWeight" type="REAL-NUMBER" />
            <xs:attribute name="compareFunction" type="COMPARE-FUNCTION" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

PMML.classMap["KNNInput"] = KNNInput

class MiningSchema(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MiningSchema">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="MiningField" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.miningFields = {}
        for miningField in self.matches(MiningField):
            name = miningField.attrib["name"]
            if name in self.miningFields:
                raise PMMLValidationError("MiningField \"%s\" appears more than once in the same MiningSchema" % name)
            self.miningFields[name] = miningField

    def top_validate_parentContext(self, parentContext, dataDictionary):
        self.cast = {}
        self.treatment = {}
        self.optype = {}
        self.dataType = {}

        if parentContext is not None:
            parentContext_names = parentContext.names()
        else:
            parentContext_names = None

        for miningField in self.matches(MiningField):
            name = miningField.attrib["name"]
            usageType = miningField.attrib.get("usageType", "active")
            required = (usageType in ("active", "supplementary"))

            if parentContext is None:
                if required and name not in dataDictionary.dataFields:
                    raise PMMLValidationError("Top-level MiningSchema %s field \"%s\" not found in the DataDictionary" % (usageType, name))

            else:
                if required and name not in parentContext_names:
                    raise PMMLValidationError("Nested MiningSchema %s field \"%s\" not found in the DataDictionary or parent context" % (usageType, name))
            
            if name in dataDictionary.dataFields:
                dataField = dataDictionary.dataFields[name]
                pmmlIntervals = dataField.matches(Interval)
                pmmlValues = dataField.matches(Value)
                isCyclic = dataField.isCyclic

            elif miningField.attrib.get("usageType", "active") == "predicted":
                dataField = None

            else:
                dataField = parentContext.derivedFields[name]
                pmmlIntervals = []
                pmmlValues = []
                isCyclic = False

            if dataField is not None:
                if "optype" in miningField.attrib:
                    optype = miningField.attrib["optype"]
                else:
                    optype = dataField.attrib["optype"]

                self.cast[name] = castFunction(optype, dataField.attrib["dataType"], pmmlIntervals, pmmlValues, isCyclic)
                self.treatment[name] = treatmentFunction(miningField, self.cast[name])
                self.optype[name] = optype
                self.dataType[name] = dataField.attrib["dataType"]

PMML.classMap["MiningSchema"] = MiningSchema

class MiningField(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="MiningField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute default="active" name="usageType" type="FIELD-USAGE-TYPE" />
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="importance" type="PROB-NUMBER" />
            <xs:attribute default="asIs" name="outliers" type="OUTLIER-TREATMENT-METHOD" />
            <xs:attribute name="lowValue" type="NUMBER" />
            <xs:attribute name="highValue" type="NUMBER" />
            <xs:attribute name="missingValueReplacement" type="xs:string" />
            <xs:attribute name="missingValueTreatment" type="MISSING-VALUE-TREATMENT-METHOD" />
            <xs:attribute default="returnInvalid" name="invalidValueTreatment" type="INVALID-VALUE-TREATMENT-METHOD" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        if "outliers" in self.attrib and self.attrib["outliers"] in ("asMissingValues", "asExtremeValues"):
            if "lowValue" not in self.attrib or "highValue" not in self.attrib:
                raise PMMLValidationError("MiningField with outliers == \"asMissingValues\" or \"asExtremeValues\" requires a lowValue and a highValue")

        self.usageType = self.attrib.get("usageType", "active")

    def makeVerbose(self, dataContext=None):
        if "optype" not in self.attrib:
            if self.usageType == "predicted":
                if dataContext.functionName in ("classification", "clustering"):
                    optype = "categorical"
                elif dataContext.functionName in ("regression",):
                    optype = "continuous"
                else:
                    raise NotImplementedError("need to define optype for functionName \"%s\"" % dataContext.functionName)
            else:
                optype = dataContext.optype[self.attrib["name"]]

            self.attrib["optype"] = optype

        PMML.makeVerbose(self, dataContext)

PMML.classMap["MiningField"] = MiningField

class GeneralRegressionModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="GeneralRegressionModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="ParameterList" />
                <xs:element minOccurs="0" ref="FactorList" />
                <xs:element minOccurs="0" ref="CovariateList" />
                <xs:element ref="PPMatrix" />
                <xs:element minOccurs="0" ref="PCovMatrix" />
                <xs:element ref="ParamMatrix" />
                <xs:element minOccurs="0" ref="EventValues" />
                <xs:element minOccurs="0" ref="BaseCumHazardTables" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute name="modelType" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="regression" />
                        <xs:enumeration value="generalLinear" />
                        <xs:enumeration value="multinomialLogistic" />
                        <xs:enumeration value="ordinalMultinomial" />
                        <xs:enumeration value="generalizedLinear" />
                        <xs:enumeration value="CoxRegression" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="targetVariableName" type="FIELD-NAME" />
            <xs:attribute name="targetReferenceCategory" type="xs:string" />
            <xs:attribute name="cumulativeLink" type="CUMULATIVE-LINK-FUNCTION" />
            <xs:attribute name="linkFunction" type="LINK-FUNCTION" />
            <xs:attribute name="linkParameter" type="REAL-NUMBER" />
            <xs:attribute name="trialsVariable" type="FIELD-NAME" />
            <xs:attribute name="trialsValue" type="INT-NUMBER" />
            <xs:attribute name="distribution">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="binomial" />
                        <xs:enumeration value="gamma" />
                        <xs:enumeration value="igauss" />
                        <xs:enumeration value="negbin" />
                        <xs:enumeration value="normal" />
                        <xs:enumeration value="poisson" />
                        <xs:enumeration value="tweedie" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="distParameter" type="REAL-NUMBER" />
            <xs:attribute name="offsetVariable" type="FIELD-NAME" />
            <xs:attribute name="offsetValue" type="REAL-NUMBER" />
            <xs:attribute name="modelDF" type="REAL-NUMBER" />
            <xs:attribute name="endTimeVariable" type="FIELD-NAME" />
            <xs:attribute name="startTimeVariable" type="FIELD-NAME" />
            <xs:attribute name="subjectIDVariable" type="FIELD-NAME" />
            <xs:attribute name="statusVariable" type="FIELD-NAME" />
            <xs:attribute name="baselineStrataVariable" type="FIELD-NAME" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        raise NotImplementedError("GeneralRegressionModel has not been implemented yet")

    def top_validate(self, dataContext):
        if "targetVariableName" in self.attrib and self.attrib["targetVariableName"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as targetVariableName) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["targetVariableName"], dataContext.contextString()))

        if "trialsVariable" in self.attrib and self.attrib["trialsVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as trialsVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["trialsVariable"], dataContext.contextString()))

        if "offsetVariable" in self.attrib and self.attrib["offsetVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as offsetVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["offsetVariable"], dataContext.contextString()))

        if "endTimeVariable" in self.attrib and self.attrib["endTimeVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as endTimeVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["endTimeVariable"], dataContext.contextString()))

        if "startTimeVariable" in self.attrib and self.attrib["startTimeVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as startTimeVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["startTimeVariable"], dataContext.contextString()))

        if "subjectIDVariable" in self.attrib and self.attrib["subjectIDVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as subjectIDVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["subjectIDVariable"], dataContext.contextString()))

        if "statusVariable" in self.attrib and self.attrib["statusVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as statusVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["statusVariable"], dataContext.contextString()))

        if "baselineStrataVariable" in self.attrib and self.attrib["baselineStrataVariable"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" (as baselineStrataVariable) but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["baselineStrataVariable"], dataContext.contextString()))

PMML.classMap["GeneralRegressionModel"] = GeneralRegressionModel

class ParameterList(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ParameterList">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Parameter" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ParameterList"] = ParameterList

class Parameter(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Parameter">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="name" type="xs:string" use="required" />
            <xs:attribute name="label" type="xs:string" />
            <xs:attribute default="0" name="referencePoint" type="REAL-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Parameter"] = Parameter

class FactorList(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="FactorList">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Predictor" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["FactorList"] = FactorList

class CovariateList(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CovariateList">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Predictor" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["CovariateList"] = CovariateList

class Predictor(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Predictor">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="1" minOccurs="0" ref="Categories" />
                <xs:element minOccurs="0" ref="Matrix" />
            </xs:sequence>
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="contrastMatrixType" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["name"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["name"], dataContext.contextString()))

PMML.classMap["Predictor"] = Predictor

class Categories(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Categories">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="1" ref="Category" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Categories"] = Categories

class Category(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Category">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Category"] = Category

class PPMatrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PPMatrix">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="PPCell" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PPMatrix"] = PPMatrix

class PPCell(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PPCell">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="predictorName" type="FIELD-NAME" use="required" />
            <xs:attribute name="parameterName" type="xs:string" use="required" />
            <xs:attribute name="targetCategory" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["predictorName"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["predictorName"], dataContext.contextString()))

PMML.classMap["PPCell"] = PPCell

class PCovMatrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PCovMatrix">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" ref="PCovCell" />
            </xs:sequence>
            <xs:attribute name="type">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="model" />
                        <xs:enumeration value="robust" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PCovMatrix"] = PCovMatrix

class PCovCell(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PCovCell">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="pRow" type="xs:string" use="required" />
            <xs:attribute name="pCol" type="xs:string" use="required" />
            <xs:attribute name="tRow" type="xs:string" />
            <xs:attribute name="tCol" type="xs:string" />
            <xs:attribute name="value" type="REAL-NUMBER" use="required" />
            <xs:attribute name="targetCategory" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PCovCell"] = PCovCell

class ParamMatrix(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ParamMatrix">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="PCell" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ParamMatrix"] = ParamMatrix

class PCell(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PCell">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="targetCategory" type="xs:string" />
            <xs:attribute name="parameterName" type="xs:string" use="required" />
            <xs:attribute name="beta" type="REAL-NUMBER" use="required" />
            <xs:attribute name="df" type="INT-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["PCell"] = PCell

class BaseCumHazardTables(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BaseCumHazardTables">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element maxOccurs="unbounded" ref="BaselineStratum" />
                    <xs:element maxOccurs="unbounded" ref="BaselineCell" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="maxTime" type="REAL-NUMBER" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BaseCumHazardTables"] = BaseCumHazardTables

class BaselineStratum(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BaselineStratum">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="BaselineCell" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="label" type="xs:string" />
            <xs:attribute name="maxTime" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BaselineStratum"] = BaselineStratum

class BaselineCell(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BaselineCell">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="time" type="REAL-NUMBER" use="required" />
            <xs:attribute name="cumHazard" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["BaselineCell"] = BaselineCell

class EventValues(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="EventValues">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Value" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Interval" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["EventValues"] = EventValues

class TreeModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TreeModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="Node" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" />
            <xs:attribute name="algorithmName" type="xs:string" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" />
            <xs:attribute default="multiSplit" name="splitCharacteristic">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="binarySplit" />
                        <xs:enumeration value="multiSplit" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="none" name="missingValueStrategy" type="MISSING-VALUE-STRATEGY" />
            <xs:attribute default="1.0" name="missingValuePenalty" type="PROB-NUMBER" />
            <xs:attribute default="returnNullPrediction" name="noTrueChildStrategy" type="NO-TRUE-CHILD-STRATEGY" />
        </xs:complexType>
    </xs:element>
    """)

    LASTPREDICTION = Atom("lastPrediction")
    NULLPREDICTION = Atom("nullPrediction")
    DEFAULTCHILD = Atom("defaultChild")
    WEIGHTEDCONFIDENCE = Atom("weightedConfidence")
    AGGREGATENODES = Atom("aggregateNodes")
    NONE = Atom("none")

    RETURNNULLPREDICTION = Atom("returnNullPrediction")
    RETURNLASTPREDICTION = Atom("returnLastPrediction")

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)

        nodes = self.matches(Node, maxdepth=None)

        if self.attrib["functionName"] == "regression":
            for node in nodes:
                try:
                    node.score = float(node.score)
                except:
                    if "id" in node.attrib:
                        raise PMMLValidationError("If the TreeModel's functionName is 'regression', Nodes must have numerical scores; Node \"%s\"%s does not" % (node.attrib["id"], node.fileAndLine()))
                    else:
                        raise PMMLValidationError("If the TreeModel's functionName is 'regression', Nodes must have numerical scores; Node (no id)%s does not" % (node.attrib["id"], node.fileAndLine()))

        elif self.attrib["functionName"] == "classification":
            pass

        else:
            raise PMMLValidationError("The only valid TreeModel functionNames are: 'regression', 'classification'")

        # find duplicates
        usedNames = set()
        for node in nodes:
            if "id" in node.attrib:
                name = node.attrib["id"]
                if name not in usedNames:
                    usedNames.add(name)
                else:
                    raise PMMLValidationError("Node id=\"%s\" appears more than once" % name)

        # give ids to the nodes that don't have them
        number = 1
        for node in nodes:
            if "id" not in node.attrib:
                newName = None
                while newName is None or newName in usedNames:
                    newName = "Untitled-%d" % number
                    number += 1
                node.attrib["id"] = newName

        # if the PMML promises a binarySplit, verify that it truly is a binary split
        # (not that it matters for processing; just truth-in-advertising)
        if self.attrib.get("splitCharacteristic", "multiSplit") == "binarySplit":
            for node in nodes:
                if len(node.subnodes) not in (0, 2):
                    raise PMMLValidationError("TreeModel splitCharacteristic is 'binarySplit', but node \"%s\" has %d sub-nodes" % (node.attrib["id"], len(node.subnodes)))

        self.metadata = NameSpace()

        missingValueStrategy = self.attrib.get("missingValueStrategy", "none")
        if missingValueStrategy == "lastPrediction": self.metadata.strategy = self.LASTPREDICTION
        if missingValueStrategy == "nullPrediction": self.metadata.strategy = self.NULLPREDICTION
        if missingValueStrategy == "defaultChild": self.metadata.strategy = self.DEFAULTCHILD
        if missingValueStrategy == "weightedConfidence": self.metadata.strategy = self.WEIGHTEDCONFIDENCE
        if missingValueStrategy == "aggregateNodes": self.metadata.strategy = self.AGGREGATENODES
        if missingValueStrategy == "none": self.metadata.strategy = self.NONE

        if self.metadata.strategy in (self.WEIGHTEDCONFIDENCE, self.AGGREGATENODES):
            raise NotImplementedError("missingValueStrategy in ('weightedConfidence', 'aggregateNodes') has not been implemented yet")

        if self.metadata.strategy == self.DEFAULTCHILD:
            for node in nodes:
                if len(node.subnodes) != 0:
                    if "defaultChild" not in node.attrib:
                        raise PMMLValidationError("TreeModel missingValueStrategy is 'defaultChild', but node \"%s\" does not have a defaultChild attribute" % node.attrib["id"])

        missingValuePenalty = self.attrib.get("missingValuePenalty", 1.)
        if missingValuePenalty == 1.:
            self.metadata.penalty = None
        else:
            self.metadata.penalty = missingValuePenalty

        noTrueChildStrategy = self.attrib.get("noTrueChildStrategy", "returnNullPrediction")
        if noTrueChildStrategy == "returnNullPrediction": self.metadata.notrue = self.RETURNNULLPREDICTION
        if noTrueChildStrategy == "returnLastPrediction": self.metadata.notrue = self.RETURNLASTPREDICTION

        self.configure()

        if not isinstance(self.node.predicate, pmmlTrue):
            raise PMMLValidationError("Top-level node of TreeModel must have <True /> as its predicate, not %s" % repr(self.node.predicate))  # FIXME: is that correct?

    def configure(self):
        self.node = self.child(Node)

    def evaluate(self, get):
        # here we assume that the top-level node is <True /> by evaluating it right away
        self.metadata.unknowns = 0
        return self.node.evaluate(get, self.metadata), self.metadata

PMML.classMap["TreeModel"] = TreeModel

class Node(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Node">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group ref="PREDICATE" />
                <xs:choice>
                    <xs:sequence>
                        <xs:element minOccurs="0" ref="Partition" />
                        <xs:element maxOccurs="unbounded" minOccurs="0" ref="ScoreDistribution" />
                        <xs:element maxOccurs="unbounded" minOccurs="0" ref="Node" />
                    </xs:sequence>
                    <xs:group ref="EmbeddedModel" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" />
            <xs:attribute name="score" type="xs:string" />
            <xs:attribute name="recordCount" type="NUMBER" />
            <xs:attribute name="defaultChild" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def calculateProbabilities(self):
        scoreDistribution = self.matches(ScoreDistribution)

        if "recordCount" in self.attrib:
            total = self.attrib["recordCount"]
        else:
            total = float(sum([i.attrib["recordCount"] for i in scoreDistribution]))

        self.scoreDistribution = {}
        for i in scoreDistribution:
            if "probability" in i.attrib:
                self.scoreDistribution[i.attrib["value"]] = i.attrib["probability"]
            elif total == 0.:
                self.scoreDistribution[i.attrib["value"]] = 0.
            else:
                self.scoreDistribution[i.attrib["value"]] = i.attrib["recordCount"]/total

        sumOfProbabilities = sum(self.scoreDistribution.values())
        if abs(sumOfProbabilities - 1.) > 1e-5:
            raise PMMLValidationError("Probabilities of ScoreDistributions in Node do not add to 1.0, they add to %g (are they a mixture of user-specified \"probability\" and probabilities calculated from \"recordCount\"?)" % sumOfProbabilities)

    def post_validate(self):
        self.configure()

    def configure(self):
        if "score" in self.attrib:
            self.score = self.attrib["score"]
        else:
            self.calculateProbabilities()
            best = None
            self.score = None
            for value, prob in self.scoreDistribution.items():
                if best is None or prob > best:
                    best = prob
                    self.score = value

        if self.score is None:
            raise PMMLValidationError("Node%s has no score attribute and no ScoreDistribution" % (" %s" % self.attrib["id"] if "id" in self.attrib else ""))

        self.predicate = self.child(nonExtension)
        self.subnodes = self.matches(Node)

        if "defaultChild" in self.attrib:
            self.defaultChild = None
            for node in self.subnodes:
                if node.attrib.get("id", None) == self.attrib["defaultChild"]:
                    self.defaultChild = node
                    break

            if self.defaultChild is None:
                raise PMMLValidationError("Node%s has defaultChild='%s' but none of its immediate children have that id" % (" %s" % self.attrib["id"] if "id" in self.attrib else "", self.attrib["defaultChild"]))

        self.regression = self.child(Regression, exception=False)

    def evaluate(self, get, metadata):
        if metadata.penalty is None:
            m = None
        else:
            m = metadata

        if len(self.subnodes) == 0:
            return self

        for subnode in self.subnodes:
            decision = subnode.test(get, m)

            if decision is UNKNOWN:
                if metadata.strategy is TreeModel.LASTPREDICTION:
                    return self

                elif metadata.strategy is TreeModel.NULLPREDICTION:
                    return None

                elif metadata.strategy is TreeModel.DEFAULTCHILD:
                    if metadata.penalty is not None:
                        metadata.unknowns += 1
                    return self.defaultChild.evaluate(get, metadata)

                elif metadata.strategy is TreeModel.WEIGHTEDCONFIDENCE:
                    raise NotImplementedError  # fill in here

                elif metadata.strategy is TreeModel.AGGREGATENODES:
                    raise NotImplementedError  # fill in here

                elif metadata.strategy is TreeModel.NONE:
                    decision = False

            # descend to this successfully matched node
            if decision:
                return subnode.evaluate(get, metadata)

        # nothing matched: now what?
        if metadata.notrue is TreeModel.RETURNNULLPREDICTION:
            return None

        elif metadata.notrue is TreeModel.RETURNLASTPREDICTION:
            return self

PMML.classMap["Node"] = Node

class SimplePredicate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SimplePredicate">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="operator" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="equal" />
                        <xs:enumeration value="notEqual" />
                        <xs:enumeration value="lessThan" />
                        <xs:enumeration value="lessOrEqual" />
                        <xs:enumeration value="greaterThan" />
                        <xs:enumeration value="greaterOrEqual" />
                        <xs:enumeration value="isMissing" />
                        <xs:enumeration value="isNotMissing" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute name="value" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.needsValue = (self.attrib["operator"] not in ("isMissing", "isNotMissing"))

        if self.needsValue and "value" not in self.attrib:
            raise PMMLValidationError("SimplePredicate must have a value unless operator is \"isMissing\" or \"isNotMissing\"")

        if self.attrib["operator"] == "equal": self.operator = (lambda x, y: x == y)
        elif self.attrib["operator"] == "notEqual": self.operator = (lambda x, y: x != y)
        elif self.attrib["operator"] == "lessThan": self.operator = (lambda x, y: x < y)
        elif self.attrib["operator"] == "lessOrEqual": self.operator = (lambda x, y: x <= y)
        elif self.attrib["operator"] == "greaterThan": self.operator = (lambda x, y: x > y)
        elif self.attrib["operator"] == "greaterOrEqual": self.operator = (lambda x, y: x >= y)
        elif self.attrib["operator"] == "isMissing": self.operator = (lambda x: x is MISSING)
        elif self.attrib["operator"] == "isNotMissing": self.operator = (lambda x: x is not MISSING)

    def top_validate(self, dataContext):
        cast = dataContext.cast.get(self.attrib["field"], None)
        if cast is None:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))
        
        if "value" in self.attrib:
            try:
                self.attrib["value"] = cast(self.attrib["value"])
            except ValueError, err:
                raise PMMLValidationError("Could not cast SimplePredicate value \"%s\": %s" % (self.attrib["value"], str(err)))

    def createTest(self, streamlined=False):
        if self.needsValue:
            myValue = self.attrib["value"]

            if streamlined:
                def test(get):
                    value = get(self.attrib["field"])
                    if value is INVALID or value is MISSING:
                        return UNKNOWN
                    else:
                        return self.operator(value, myValue)

            else:
                def test(get, metadata=None):
                    value = get(self.attrib["field"])
                    if value is INVALID or value is MISSING:
                        return UNKNOWN
                    else:
                        return self.operator(value, myValue)

        else:
            if streamlined:
                def test(get):
                    value = get(self.attrib["field"])
                    if value is INVALID:
                        return UNKNOWN
                    else:
                        return self.operator(value)

            else:
                def test(get, metadata=None):
                    value = get(self.attrib["field"])
                    if value is INVALID:
                        return UNKNOWN
                    else:
                        return self.operator(value)

        return test

    def expressionTree(self):
        if "value" in self.attrib:
            value = self.attrib["value"]
            if isinstance(value, basestring):
                value = "'%s'" % value
            else:
                value = str(value)

        if self.attrib["operator"] == "equal": return "%s EQ %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "notEqual": return "%s NE %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "lessThan": return "%s LT %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "lessOrEqual": return "%s LE %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "greaterThan": return "%s GT %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "greaterOrEqual": return "%s GE %s" % (self.attrib["field"], value)
        elif self.attrib["operator"] == "isMissing": return "%s MISSING" % self.attrib["field"]
        elif self.attrib["operator"] == "isNotMissing": return "%s NOT-MISSING" % self.attrib["field"]

PMML.classMap["SimplePredicate"] = SimplePredicate

class CompoundPredicate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CompoundPredicate">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:sequence maxOccurs="unbounded" minOccurs="2">
                    <xs:group ref="PREDICATE" />
                </xs:sequence>
            </xs:sequence>
            <xs:attribute name="booleanOperator" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="or" />
                        <xs:enumeration value="and" />
                        <xs:enumeration value="xor" />
                        <xs:enumeration value="surrogate" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def createTest(self, streamlined=False):
        childfuncs = [child.createTest(streamlined) for child in self.matches(nonExtension)]

        if self.attrib["booleanOperator"] == "or":
            if streamlined:
                def test(get):
                    for f in childfuncs:
                        if f(get): return True
                    return False

            else:
                def test(get, metadata=None):
                    unknowns = 0
                    for f in childfuncs:
                        b = f(get, metadata)
                        if b is True: return True
                        if b is UNKNOWN: unknowns += 1
                    if unknowns > 0:
                        return UNKNOWN
                    else:
                        return False

        elif self.attrib["booleanOperator"] == "and":
            if streamlined:
                def test(get):
                    for f in childfuncs:
                        if not f(get): return False
                    return True

            else:
                def test(get, metadata=None):
                    unknowns = 0
                    for f in childfuncs:
                        b = f(get, metadata)
                        if b is False: return False
                        if b is UNKNOWN: unknowns += 1
                    if unknowns > 0:
                        return UNKNOWN
                    else:
                        return True

        elif self.attrib["booleanOperator"] == "xor":
            if streamlined:
                trues = 0
                def test(get):
                    if f(get): trues += 1
                return (trues % 2 == 1)
                    
            def test(get, metadata=None):
                unknowns = 0
                trues = 0
                for f in childfuncs:
                    b = f(get, metadata)
                    if b is True: trues += 1
                    if b is UNKNOWN: unknowns += 1
                if unknowns > 0:
                    return UNKNOWN
                elif trues % 2 == 1:
                    return True
                else:
                    return False

        elif self.attrib["booleanOperator"] == "surrogate":
            if streamlined:
                raise Exception("CompoundPredicates with surrogate operators can't use the streamlined calculations (this is a programmer error)")

            def test(get, metadata=None):
                b = UNKNOWN
                for f in childfuncs:
                    b = f(get, metadata)
                    if b is not UNKNOWN:
                        return b
                    else:
                        if metadata is not None:
                            metadata.unknowns += 1
                return b

        return test

    def expressionTree(self):
        return (" %s " % self.attrib["booleanOperator"]).join(["(%s)" % p.expressionTree() for p in self.matches(lambda x: isinstance(x, (SimplePredicate, CompoundPredicate, SimpleSetPredicate)))])

PMML.classMap["CompoundPredicate"] = CompoundPredicate

class SimpleSetPredicate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="SimpleSetPredicate">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="Array" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="booleanOperator" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="isIn" />
                        <xs:enumeration value="isNotIn" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

    def createTest(self, streamlined=False):
        myValue = self.child(Array).value
        if self.attrib["booleanOperator"] == "isIn":
            if streamlined:
                def test(get, metadata=None):
                    return (get(self.attrib["field"]) in myValue)

            else:
                def test(get, metadata=None):
                    value = get(self.attrib["field"])
                    if value is INVALID or value is MISSING:
                        return False
                    else:
                        return value in myValue

        else:
            if streamlined:
                def test(get, metadata=None):
                    return (get(self.attrib["field"]) not in myValue)

            else:
                def test(get, metadata=None):
                    value = get(self.attrib["field"])
                    if value is INVALID or value is MISSING:
                        return False
                    else:
                        return value not in myValue

        return test

    def expressionTree(self):
        if self.attrib["booleanOperator"] == "isIn":
            return "%s in %s" % (self.attrib["field"], self.child(Array).value)

        if self.attrib["booleanOperator"] == "isNotIn":
            return "%s not in %s" % (self.attrib["field"], self.child(Array).value)

PMML.classMap["SimpleSetPredicate"] = SimpleSetPredicate

class pmmlTrue(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="True">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def createTest(self, streamlined=False):
        if streamlined:
            return lambda get: True

        else:
            return lambda get, metadata=None: True

    def expressionTree(self):
        return "True"

PMML.classMap["True"] = pmmlTrue

class pmmlFalse(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="False">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    """)

    def createTest(self, streamlined=False):
        if streamlined:
            return lambda get: False

        else:
            return lambda get, metadata=None: False

    def expressionTree(self):
        return "False"

PMML.classMap["False"] = pmmlFalse

class ScoreDistribution(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="ScoreDistribution">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="value" type="xs:string" use="required" />
            <xs:attribute name="recordCount" type="NUMBER" use="required" />
            <xs:attribute name="confidence" type="PROB-NUMBER" />
            <xs:attribute name="probability" type="PROB-NUMBER" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["ScoreDistribution"] = ScoreDistribution

class BaselineModel(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="BaselineModel">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:element ref="MiningSchema" />
                <xs:element minOccurs="0" ref="Output" />
                <xs:element minOccurs="0" ref="ModelStats" />
                <xs:element minOccurs="0" ref="ModelExplanation" />
                <xs:element minOccurs="0" ref="Targets" />
                <xs:element minOccurs="0" ref="LocalTransformations" />
                <xs:element ref="TestDistributions" />
                <xs:element minOccurs="0" ref="ModelVerification" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
            <xs:attribute name="modelName" type="xs:string" use="optional" />
            <xs:attribute name="algorithmName" type="xs:string" use="optional" />
            <xs:attribute default="true" name="isScorable" type="xs:boolean" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.isScorable = self.attrib.get("isScorable", True)
        if self.attrib["functionName"] != "regression":
            raise PMMLValidationError("The only valid BaselineModel functionNames are: 'regression'")

PMML.classMap["BaselineModel"] = BaselineModel

class TestDistributions(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="TestDistributions">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Baseline" />
                <xs:element minOccurs="0" ref="Alternate" />
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="testStatistic" type="BASELINE-TEST-STATISTIC" use="required" />
            <xs:attribute default="0.0" name="resetValue" type="REAL-NUMBER" use="optional" />
            <xs:attribute default="0" name="windowSize" type="INT-NUMBER" use="optional" />
            <xs:attribute name="weightField" type="FIELD-NAME" use="optional" />
            <xs:attribute name="normalizationScheme" type="xs:string" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        ### models

        if self.attrib["testStatistic"] == "CUSUM":
            if "resetValue" not in self.attrib:
                self.attrib["resetValue"] = 0.

            if not self.exists(Alternate):
                raise PMMLValidationError("Alternate distribution must be supplied if TestDistributions \"testStatistic\" == \"CUSUM\"")

        if self.attrib["testStatistic"] in ("chiSquareDistribution", "scalarProduct"):
            table = self.child(Baseline).child(lambda x: isinstance(x, (CountTable, NormalizedCountTable)), exception=False)
            if table is None:
                raise PMMLValidationError("TestDistributions \"%s\" requires either a CountTable or a NormalizedCountTable" % self.attrib["testStatistic"])
            for child in table.matches(nonExtension):
                if not isinstance(child, FieldValueCount):
                    raise PMMLValidationError("TestDistributions \"%s\" requires a one-dimensional count table (only FieldValueCounts)" % self.attrib["testStatistic"])

        ### attributes

        if "weightField" in self.attrib and self.attrib["testStatistic"] not in ("scalarProduct", "chiSquareDistribution"):
            raise PMMLValidationError("TestDistributions \"weightField\" can only be supplied if \"testStatistic\" in (\"scalarProduct\", \"chiSquareDistribution\")")

        if "normalizationScheme" in self.attrib:
            if self.attrib["testStatistic"] != "scalarProduct":
                raise PMMLValidationError("TestDistributions \"normalizationScheme\" can only be supplied if \"testStatistic\" == \"scalarProduct\"")

            if self.attrib["normalizationScheme"] not in ("Independent", "SizeWeighted"):
                raise PMMLValidationError("TestDistributions \"normalizationScheme\" is not in (\"Independent\", \"SizeWeighted\")")

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

        if "weightField" in self.attrib and self.attrib["weightField"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["weightField"], dataContext.contextString()))

PMML.classMap["TestDistributions"] = TestDistributions

class Baseline(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Baseline">
        <xs:complexType>
            <xs:choice>
                <xs:group minOccurs="1" ref="CONTINUOUS-DISTRIBUTION-TYPES" />
                <xs:group minOccurs="1" ref="DISCRETE-DISTRIBUTION-TYPES" />
            </xs:choice>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Baseline"] = Baseline

class Alternate(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="Alternate">
        <xs:complexType>
            <xs:choice>
                <xs:group minOccurs="1" ref="CONTINUOUS-DISTRIBUTION-TYPES" />
            </xs:choice>
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["Alternate"] = Alternate

class AnyDistribution(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="AnyDistribution">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="mean" type="REAL-NUMBER" use="required" />
            <xs:attribute name="variance" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

PMML.classMap["AnyDistribution"] = AnyDistribution

class GaussianDistribution(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="GaussianDistribution">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="mean" type="REAL-NUMBER" use="required" />
            <xs:attribute name="variance" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def cdf(self, x):
        return (erf((x - self.attrib["mean"])/sqrt(2.*self.attrib["variance"])) + 1.)/2.

    def pdf(self, x):
        return exp(-(x - self.attrib["mean"])**2 / 2. / self.attrib["variance"])/(sqrt(2. * pi * self.attrib["variance"]))

    def logpdf(self, x):
        return -(x - self.attrib["mean"])**2 / 2. / self.attrib["variance"] - log(sqrt(2. * pi * self.attrib["variance"]))

PMML.classMap["GaussianDistribution"] = GaussianDistribution

class PoissonDistribution(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="PoissonDistribution">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="mean" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def cdf(self, x):
        return gammq(floor(x+1), self.attrib["mean"])

    def pdf(self, x):
        return exp(self.logpdf(x))

    def logpdf(self, x):
        return x*log(self.attrib["mean"]) - gammln(x+1) - self.attrib["mean"]

PMML.classMap["PoissonDistribution"] = PoissonDistribution

class UniformDistribution(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="UniformDistribution">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="lower" type="REAL-NUMBER" use="required" />
            <xs:attribute name="upper" type="REAL-NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def cdf(self, x):
        if x <= self.attrib["lower"]: return 0.
        if x >= self.attrib["upper"]: return 1.
        return (x - self.attrib["lower"])/(self.attrib["upper"] - self.attrib["lower"])

    def pdf(self, x):
        if self.attrib["lower"] <= x < self.attrib["upper"]:
            return 1./(self.attrib["upper"] - self.attrib["lower"])
        else:
            return 0.

    def logpdf(self, x):
        if self.attrib["lower"] <= x < self.attrib["upper"]:
            return -log(self.attrib["upper"] - self.attrib["lower"])
        else:
            return -MAXFLOAT

PMML.classMap["UniformDistribution"] = UniformDistribution

class CountTable(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="CountTable" type="COUNT-TABLE-TYPE" />
    """)

    def post_validate(self):
        nonExtensions = self.matches(nonExtension)
        first_field = nonExtensions[0].attrib["field"]
        for node in nonExtensions:
            if node.attrib["field"] != first_field:
                raise PMMLValidationError("All children of a FieldValue must have the same field name")

PMML.classMap["CountTable"] = CountTable

class NormalizedCountTable(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="NormalizedCountTable" type="COUNT-TABLE-TYPE" />
    """)

    def post_validate(self):
        nonExtensions = self.matches(nonExtension)
        first_field = nonExtensions[0].attrib["field"]
        for node in nonExtensions:
            if node.attrib["field"] != first_field:
                raise PMMLValidationError("All children of a FieldValue must have the same field name")

        if "sample" not in self.attrib:
            any_nonzero = False
            for fieldValueCount in self.matches(pmml.FieldValueCount, maxdepth=None):
                if fieldValueCount.attrib["count"] != 0.:
                    any_nonzero = True
                    break

            if any_nonzero:
                raise PMMLValidationError("NormalizedCountTable has non-zero FieldValueCounts but no \"sample\" attribute to know how they were normalized")
            self.attrib["sample"] = 0.

PMML.classMap["NormalizedCountTable"] = NormalizedCountTable

class FieldValue(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="FieldValue">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldValue" />
                    <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldValueCount" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="value" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        nonExtensions = self.matches(nonExtension)
        first_field = nonExtensions[0].attrib["field"]
        for node in nonExtensions:
            if node.attrib["field"] != first_field:
                raise PMMLValidationError("All children of a FieldValue must have the same field name")

    def top_validate(self, dataContext):
        cast = dataContext.cast.get(self.attrib["field"], None)
        if cast is None:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))
        try:
            self.attrib["value"] = cast(self.attrib["value"])
        except ValueError, err:
            raise PMMLValidationError("Could not cast FieldValue value \"%s\": %s" % (self.attrib["value"], str(err)))

PMML.classMap["FieldValue"] = FieldValue

class FieldValueCount(PMML):
    xsd = load_xsdElement(PMML, """
    <xs:element name="FieldValueCount">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
            </xs:sequence>
            <xs:attribute name="field" type="FIELD-NAME" use="required" />
            <xs:attribute name="value" use="required" />
            <xs:attribute name="count" type="NUMBER" use="required" />
        </xs:complexType>
    </xs:element>
""")

    def top_validate(self, dataContext):
        cast = dataContext.cast.get(self.attrib["field"], None)
        if cast is None:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))
        try:
            self.attrib["value"] = cast(self.attrib["value"])
        except ValueError, err:
            raise PMMLValidationError("Could not cast FieldValueCount value \"%s\": %s" % (self.attrib["value"], str(err)))

PMML.classMap["FieldValueCount"] = FieldValueCount

############################################################################### ODG extensions to PMML

# base class for all ODG extensions must be defined *after* the whole PMML.classMap is full!
class X_ODG_PMML(PMML):
    xsdType = dict(PMML.xsdType)    # copy and update
    xsdGroup = dict(PMML.xsdGroup)
    classMap = dict(PMML.classMap)

class X_ODG_Eventstamp(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-Eventstamp">
    <xs:complexType>
      <xs:attribute name="number" type="INT-NUMBER" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-Eventstamp"] = X_ODG_Eventstamp

class X_ODG_RandomSeed(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-RandomSeed">
    <xs:complexType>
      <xs:attribute name="value" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-RandomSeed"] = X_ODG_RandomSeed

class X_ODG_BinsOfInterest(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-BinsOfInterest">
    <xs:complexType>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        textExpected = xmlbase.XSDList("X-ODG-BinsOfInterest", "xs:string")

        xmlText = []
        for child in self.children:
            if isinstance(child, xmlbase.XMLText):
                xmlText.append(child)

        self.value = textExpected.validateText("".join([str(x) for x in xmlText]).lstrip(string.whitespace).rstrip(string.whitespace))
        self.children = []

X_ODG_PMML.classMap["X-ODG-BinsOfInterest"] = X_ODG_BinsOfInterest

class X_ODG_PartialSums(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-PartialSums">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="optional" />
      <xs:attribute name="COUNT" type="INT-NUMBER" use="optional" />
      <xs:attribute name="SUM1" type="REAL-NUMBER" use="optional" />
      <xs:attribute name="SUMX" type="REAL-NUMBER" use="optional" />
      <xs:attribute name="SUMXX" type="REAL-NUMBER" use="optional" />
      <xs:attribute name="RUNMEAN" type="REAL-NUMBER" use="optional" />
      <xs:attribute name="RUNSN" type="REAL-NUMBER" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-PartialSums"] = X_ODG_PartialSums

class X_ODG_ModelMaturity(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-ModelMaturity">
    <xs:complexType>
      <xs:attribute name="numUpdates" type="xs:nonNegativeInteger" use="required" />
      <xs:attribute name="locked" type="xs:boolean" use="required" />
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-ModelMaturity"] = X_ODG_ModelMaturity

class X_ODG_Convergence(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-Convergence">
    <xs:complexType>
      <xs:attribute name="converged" type="xs:boolean" use="required" />
      <xs:attribute name="iterations" type="xs:nonNegativeInteger" use="required" />
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-Convergence"] = X_ODG_Convergence

def isExpression(x):
    return isinstance(x, (Constant,
                          FieldRef,
                          NormContinuous,
                          NormDiscrete,
                          Discretize,
                          MapValues,
                          Apply,
                          Aggregate,
                          X_ODG_RegularExpression,
                          X_ODG_AggregateReduce,
                          X_ODG_CountSubstrings,
                          ))

X_ODG_PMML.xsdGroup["X-ODG-EXPRESSION"] = load_xsdGroup("""
  <xs:group name="X-ODG-EXPRESSION">
    <xs:choice>
      <xs:element ref="Constant" />
      <xs:element ref="FieldRef" />
      <xs:element ref="NormContinuous" />
      <xs:element ref="NormDiscrete" />
      <xs:element ref="Discretize" />
      <xs:element ref="MapValues" />
      <xs:element ref="Apply" />
      <xs:element ref="Aggregate" />
      <xs:element ref="X-ODG-RegularExpression" />
      <xs:element ref="X-ODG-AggregateReduce" />
      <xs:element ref="X-ODG-Apply" />
      <xs:element ref="X-ODG-CountSubstrings" />
    </xs:choice>
  </xs:group>
  """)

class X_ODG_DerivedField(X_ODG_PMML, DerivedField):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="DerivedField">
    <xs:complexType>
      <xs:sequence>
        <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
        <xs:group ref="X-ODG-EXPRESSION" />
        <xs:element maxOccurs="unbounded" minOccurs="0" ref="Value" />
      </xs:sequence>
      <xs:attribute name="optype" type="OPTYPE" use="required" />
      <xs:attribute name="dataType" type="DATATYPE" use="required" />
      <xs:attribute name="name" type="FIELD-NAME" />
      <xs:attribute name="displayName" type="xs:string" />
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-DerivedField"] = X_ODG_DerivedField

class X_ODG_RegularExpression(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-RegularExpression">
    <xs:complexType>
      <xs:attribute name="field" type="FIELD-NAME" use="required" />
      <xs:attribute name="pattern" type="xs:string" use="required" />
      <xs:attribute name="replacement" type="xs:string" use="required" />
      <xs:attribute name="flags" type="xs:string" />
      <xs:attribute name="mapMissingTo" type="xs:string" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "flags" in self.attrib:
            theFlags = self.attrib["flags"]
        else:
            theFlags = ""
        
        flags = 0
        identified = 0
        if "I" in theFlags or "i" in theFlags:
            flags |= re.I
            identified += 1
        if "L" in theFlags or "l" in theFlags:
            flags |= re.L
            identified += 1
        if "M" in theFlags or "m" in theFlags:
            flags |= re.M
            identified += 1
        if "S" in theFlags or "s" in theFlags:
            flags |= re.S
            identified += 1
        if "U" in theFlags or "u" in theFlags:
            flags |= re.U
            identified += 1
        if "X" in theFlags or "x" in theFlags:
            flags |= re.X
            identified += 1

        if len(theFlags) > identified:
            raise PMMLValidationError("Unrecognized regular expression flag in \"%s\"" % theFlags)

        try:
            self.pattern = re.compile(self.attrib["pattern"], flags)
        except sre_constants.error, err:
            raise PMMLValidationError("Failed to parse regular expression \"%s\"%s: %s" % (self.attrib["pattern"], "" if theFlags == "" else (" (flags \"%s\")" % theFlags), str(err)))

    def evaluate(self, get):
        value = get(self.attrib["field"])
        if value is INVALID:
            raise InvalidDataError

        if value is MISSING:
            if "mapMissingTo" in self.attrib:
                return self.attrib["mapMissingTo"]
            else:
                return MISSING
        
        match = self.pattern.match(value)
        if match is None:
            return MISSING

        try:
            return match.expand(self.attrib["replacement"])
        except sre_constants.error:
            raise InvalidDataError

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

X_ODG_PMML.classMap["X-ODG-RegularExpression"] = X_ODG_RegularExpression

class X_ODG_AggregateReduce(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-AggregateReduce">
    <xs:complexType>
      <xs:attribute name="function" type="xs:string" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="count" />
            <xs:enumeration value="sum" />
            <xs:enumeration value="average" />
            <xs:enumeration value="min" />
            <xs:enumeration value="max" />
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="field" type="FIELD-NAME" use="required" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if self.attrib["function"] == "count": self.evaluate = self.count
        if self.attrib["function"] == "sum": self.evaluate = self.sum
        if self.attrib["function"] == "average": self.evaluate = self.average
        if self.attrib["function"] == "min": self.evaluate = self.min
        if self.attrib["function"] == "max": self.evaluate = self.max
        
    def top_validate(self, dataContext):
        self.dataContext = dataContext

        self.derivedField = dataContext.allDerivedFields().get(self.attrib["field"], None)
        if self.derivedField is None:
            raise PMMLValidationError("%s references derived field \"%s\" but no such derived field is accessible within its MiningSchema's active context {%s}" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString(derived=True)))

        self.groupField = self.derivedField.groupField
        self.groupValues = set()

    def initialize(self, updateScheme):
        pass  # quack!

    def flush(self):
        pass  # quack!

    def increment(self, syncNumber, get):
        if self.groupField is not None:
            self.groupValues.add(get(self.groupField))

    def values(self, get):
        output = []

        for groupValue in self.groupValues:
            self.dataContext.setOverride({self.groupField: groupValue}, False)

            value = self.derivedField.expression.evaluate(get)

            if value is not MISSING:
                output.append(value)

            self.dataContext.releaseOverride()
        return output

    def count(self, get):
        return len(self.values(get))

    def sum(self, get):
        return sum(self.values(get))

    def average(self, get):
        values = self.values(get)
        if len(values) == 0:
            raise InvalidDataError
        return sum(values)/float(len(values))

    def min(self, get):
        values = self.values(get)
        if len(values) == 0:
            raise InvalidDataError
        return min(values)

    def max(self, get):
        values = self.values(get)
        if len(values) == 0:
            raise InvalidDataError
        return max(values)

X_ODG_PMML.classMap["X-ODG-AggregateReduce"] = X_ODG_AggregateReduce

X_ODG_PMML.xsdType["X-ODG-RESULT-FEATURE"] = load_xsdType("""
  <xs:simpleType name="X-ODG-RESULT-FEATURE">
    <xs:restriction base="xs:string">
      <xs:enumeration value="predictedValue" />
      <xs:enumeration value="predictedDisplayValue" />
      <xs:enumeration value="transformedValue" />
      <xs:enumeration value="decision" />
      <xs:enumeration value="probability" />
      <xs:enumeration value="residual" />
      <xs:enumeration value="standardError" />
      <xs:enumeration value="clusterId" />
      <xs:enumeration value="clusterAffinity" />
      <xs:enumeration value="entityId" />
      <xs:enumeration value="entityAffinity" />
      <xs:enumeration value="warning" />
      <xs:enumeration value="ruleValue" />
      <xs:enumeration value="reasonCode" />

      <xs:enumeration value="pValue" />
      <xs:enumeration value="chiSquare" />
      <xs:enumeration value="degreesOfFreedom" />
      <xs:enumeration value="thresholdTime" />
      <xs:enumeration value="weight" />
    </xs:restriction>
  </xs:simpleType>
  """)

class X_ODG_OutputField(X_ODG_PMML, OutputField):
    xsd = load_xsdElement(X_ODG_PMML, """
    <xs:element name="OutputField">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:sequence maxOccurs="1" minOccurs="0">
                    <xs:element maxOccurs="1" minOccurs="0" ref="Decisions" />
                    <xs:group maxOccurs="1" minOccurs="1" ref="X-ODG-EXPRESSION" />
                </xs:sequence>
            </xs:sequence>
            <xs:attribute name="optype" type="OPTYPE" />
            <xs:attribute name="dataType" type="DATATYPE" />
            <xs:attribute name="feature" type="X-ODG-RESULT-FEATURE" />
            <xs:attribute name="name" type="FIELD-NAME" use="required" />
            <xs:attribute name="displayName" type="xs:string" />
            <xs:attribute name="targetField" type="FIELD-NAME" />
            <xs:attribute name="value" type="xs:string" />
            <xs:attribute default="consequent" name="ruleFeature" type="RULE-FEATURE" />
            <xs:attribute default="exclusiveRecommendation" name="algorithm">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="recommendation" />
                        <xs:enumeration value="exclusiveRecommendation" />
                        <xs:enumeration value="ruleAssociation" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="1" name="rank" type="INT-NUMBER" />
            <xs:attribute default="confidence" name="rankBasis">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="confidence" />
                        <xs:enumeration value="support" />
                        <xs:enumeration value="lift" />
                        <xs:enumeration value="leverage" />
                        <xs:enumeration value="affinity" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="descending" name="rankOrder">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="descending" />
                        <xs:enumeration value="ascending" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="0" name="isMultiValued" />
            <xs:attribute name="segmentId" type="xs:string" />
        </xs:complexType>
    </xs:element>
    """)

    pValue = Atom("pValue")
    chiSquare = Atom("chiSquare")
    degreesOfFreedom = Atom("degreesOfFreedom")
    thresholdTime = Atom("thresholdTime")
    weight = Atom("weight")
    
X_ODG_PMML.classMap["X-ODG-OutputField"] = X_ODG_OutputField

class X_ODG_CUSUMInitialization(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-CUSUMInitialization">
    <xs:complexType>
      <xs:attribute name="value" type="REAL-NUMBER" use="required" />
    </xs:complexType>
  </xs:element>
  """)

X_ODG_PMML.classMap["X-ODG-CUSUMInitialization"] = X_ODG_CUSUMInitialization

class X_ODG_Apply(X_ODG_PMML, Apply):
    xsd = load_xsdElement(X_ODG_PMML, """
    <xs:element name="X-ODG-Apply">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:group maxOccurs="unbounded" minOccurs="0" ref="X-ODG-EXPRESSION" />
            </xs:sequence>
            <xs:attribute name="function" type="xs:string" use="required" />
            <xs:attribute name="mapMissingTo" type="xs:string" />
            <xs:attribute default="returnInvalid" name="invalidValueTreatment" type="INVALID-VALUE-TREATMENT-METHOD" />
            <xs:attribute name="pythonExpression" type="xs:string" use="optional" /> 
            <xs:attribute name="pythonLibraries" type="xs:string" use="optional" />
       </xs:complexType>
    </xs:element>
    """)

    def post_validate(self):
        self.expression = None
        if self.attrib["function"] == "python":
            if "pythonExpression" not in self.attrib:
                raise PMMLValidationError("If X-ODG-Apply has function=\"python\", then it must have a \"pythonExpression\"")
            else:
                self.expression = compile(self.attrib["pythonExpression"], "", "eval")

            Apply.post_validate(self)

            if self.expression is not None:
                if self.exists(lambda x: isExpression(x) and not isinstance(x, FieldRef)):
                    raise PMMLValidationError("If X-ODG-Apply has a \"pythonExpression\", then it can only contain FieldRefs")

                self.fieldRefs = self.matches(FieldRef)
                self.fieldNames = [f.attrib["field"] for f in self.fieldRefs]

                self.context = {}
                if "pythonLibraries" in self.attrib:
                    for library in re.split("[\s,;]+", self.attrib["pythonLibraries"].lstrip(" \t\n\r").rstrip(" \t\n\r")):
                        tmp = {}
                        try:
                            exec "import %s" % library in tmp
                        except Exception:
                            raise PMMLValidationError("Could not load library \"%s\"" % library)
                        self.context.update(tmp[library].__dict__)

        if self.expression is None:
            Apply.post_validate(self)

    def top_validate_transformationDictionary(self, transformationDictionary):
        if self.expression is None:
            return Apply.top_validate_transformationDictionary(self, transformationDictionary)

    def evaluate(self, get):
        if self.expression is None:
            return Apply.evaluate(self, get)

        values = [f.evaluate(get) for f in self.fieldRefs]
        if MISSING in values: return MISSING

        context = dict(self.context)
        context.update(dict([(f, v) for f, v in zip(self.fieldNames, values)]))

        try:
            output = eval(self.expression, context)
        except Exception:
            return self.invalidValueTreatment()

        return output

X_ODG_PMML.classMap["X-ODG-Apply"] = X_ODG_Apply

class X_ODG_PredictorTerm(X_ODG_PMML, PredictorTerm):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-PredictorTerm">
    <xs:complexType>
      <xs:sequence>
        <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
        <xs:element maxOccurs="unbounded" minOccurs="1" ref="FieldRef" />
      </xs:sequence>
      <xs:attribute name="name" type="FIELD-NAME" />
      <xs:attribute name="coefficient" type="REAL-NUMBER" use="required" />
      <xs:attribute name="pythonExpression" type="xs:string" use="optional" />
      <xs:attribute name="pythonLibraries" type="xs:string" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "pythonExpression" not in self.attrib:
            self.expression = None
        else:
            self.expression = compile(self.attrib["pythonExpression"], "", "eval")

        PredictorTerm.post_validate(self)

        self.fieldNames = [f.attrib["field"] for f in self.fieldRefs]

        self.context = {}
        if "pythonLibraries" in self.attrib:
            for library in re.split("[\s,;]+", self.attrib["pythonLibraries"].lstrip(" \t\n\r").rstrip(" \t\n\r")):
                tmp = {}
                try:
                    exec "import %s" % library in tmp
                except Exception:
                    raise PMMLValidationError("Could not load library \"%s\"" % library)
                self.context.update(tmp[library].__dict__)

    def evaluateWithoutCoefficient(self, get):
        if self.expression is None:
            return PredictorTerm.evaluateWithoutCoefficient(self, get)

        values = [f.evaluate(get) for f in self.fieldRefs]
        if MISSING in values: return MISSING

        context = dict(self.context)
        context.update(dict([(f, v) for f, v in zip(self.fieldNames, values)]))

        try:
            output = float(eval(self.expression, context))
        except Exception:
            raise InvalidDataError
        return output

    def evaluate(self, get):
        return self.attrib["coefficient"] * self.evaluateWithoutCoefficient(get)

X_ODG_PMML.classMap["X-ODG-PredictorTerm"] = X_ODG_PredictorTerm

class X_ODG_AlgorithmParameter(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-AlgorithmParameter">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="required"/>
      <xs:attribute name="value" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""") 	 

X_ODG_PMML.classMap["X-ODG-AlgorithmParameter"] = X_ODG_AlgorithmParameter

class X_ODG_CustomProcessingConstants(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-CustomProcessingConstants">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="X-ODG-CustomProcessingConstant" minOccurs="0" maxOccurs="unbounded" />
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""") 	 

    def post_validate(self):
        self.nameSpace = NameSpace()

        namesSeen = set()
        for c in self.matches(X_ODG_CustomProcessingConstant):
            name, value = c.items()
            if name in namesSeen:
                raise PMMLValidationError("Multiple X-ODG-CustomProcessingConstants with the name \"%s\"" % name)
            namesSeen.add(name)

            self.nameSpace[name] = value

        self.nameSpace = NameSpaceReadOnly(**dict(self.nameSpace))

X_ODG_PMML.classMap["X-ODG-CustomProcessingConstants"] = X_ODG_CustomProcessingConstants

class X_ODG_CustomProcessingConstant(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-CustomProcessingConstant">
    <xs:complexType>
      <xs:choice>
        <xs:element ref="Constant" minOccurs="1" maxOccurs="1" />
        <xs:element ref="Array" minOccurs="1" maxOccurs="1" />
        <xs:element ref="Matrix" minOccurs="1" maxOccurs="1" />
      </xs:choice>
      <xs:attribute name="name" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""") 	 

    def items(self):
        value = self.child(Constant, exception=False)
        if value is not None:
            return self.attrib["name"], value.value

        value = self.child(Array, exception=False)
        if value is not None:
            return self.attrib["name"], value.value

        value = self.child(Matrix, exception=False)
        if value is not None:
            rows = value.attrib["nbRows"]
            cols = value.attrib["nbCols"]
            output = numpy.matrix(numpy.empty((rows, cols)))
            for i in xrange(rows):
                for j in xrange(cols):
                    output[i,j] = value[i,j]
            return self.attrib["name"], output

X_ODG_PMML.classMap["X-ODG-CustomProcessingConstant"] = X_ODG_CustomProcessingConstant

class X_ODG_CountSubstrings(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-CountSubstrings">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="X-ODG-Substring" minOccurs="1" maxOccurs="unbounded" />
      </xs:sequence>
      <xs:attribute name="field" type="FIELD-NAME" use="required" />
      <xs:attribute name="mapMissingTo" type="xs:string" />
      <xs:attribute name="caseSensitive" type="xs:boolean" use="optional" default="true" />
      <xs:attribute name="whitespaceSensitive" type="xs:boolean" use="optional" default="true" />
      <xs:attribute name="ignore" type="xs:string" use="optional" />
      <xs:attribute name="delimiter" type="xs:string" use="optional" />
      <xs:attribute name="nGramGroups" type="xs:nonNegativeInteger" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

    whitespacePattern = re.compile("\s+")

    def post_validate(self):
        self.substrings = self.matches(X_ODG_Substring)

        self.caseSensitive = self.attrib.get("caseSensitive", True)
        if not self.caseSensitive:
            for s in self.substrings:
                s.v = s.v.upper()

        self.whitespaceSensitive = self.attrib.get("whitespaceSensitive", True)
        if not self.whitespaceSensitive:
            for s in self.substrings:
                s.v = re.sub(self.whitespacePattern, " ", s.v)

        self.ignore = self.attrib.get("ignore", None)
        if self.ignore is not None:
            try:
                self.ignore = re.compile(self.ignore)
            except sre_constants.error:
                raise PMMLValidationError("Attribute 'ignore' is not a regular expression: \"%s\"" % self.ignore)

            for s in self.substrings:
                s.v = re.sub(self.ignore, "", s.v)

        self.delimiter = self.attrib.get("delimiter", None)

        self.nGramGroups = self.attrib.get("nGramGroups", None)
        if self.nGramGroups is not None and self.delimiter is None:
            raise PMMLValidationError("You can only group words into n-grams if you have first grouped characters into words with a delimiter")

    def evaluate(self, get):
        value = get(self.attrib["field"])
        if value is INVALID:
            raise InvalidDataError

        if value is MISSING:
            if "mapMissingTo" in self.attrib:
                return self.attrib["mapMissingTo"]
            else:
                return MISSING

        if not self.caseSensitive:
            value = value.upper()

        if not self.whitespaceSensitive:
            value = re.sub(self.whitespacePattern, " ", value)

        if self.ignore is not None:
            value = re.sub(self.ignore, "", value)

        if self.delimiter is not None:
            value = value.split(self.delimiter)

        if self.nGramGroups is not None:
            value = [" ".join(value[i:i+self.nGramGroups]) for i in xrange(len(value)+1 - self.nGramGroups)]

        return sum([value.count(s.v) * s.w for s in self.substrings])

    def top_validate(self, dataContext):
        if self.attrib["field"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["field"], dataContext.contextString()))

        if dataContext.cast[self.attrib["field"]] != pmmlBuiltinType["string"]:
            raise PMMLValidationError("CountSubstrings field \"%s\" must be a string in its MiningSchema context; instead, it is \"%s\"" % (self.attrib["field"], str(dataContext.cast[self.attrib["field"]])))

X_ODG_PMML.classMap["X-ODG-CountSubstrings"] = X_ODG_CountSubstrings

class X_ODG_Substring(X_ODG_PMML):
    xsd = load_xsdElement(X_ODG_PMML, """
  <xs:element name="X-ODG-Substring">
    <xs:complexType>
      <xs:simpleContent>
        <xs:extension base="xs:string">
          <xs:attribute name="weight" type="REAL-NUMBER" use="optional" default="1" />
          <xs:attribute name="value" type="xs:string" use="optional" />
        </xs:extension>
      </xs:simpleContent>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.w = self.attrib.get("weight", 1)
        self.v = self.attrib.get("value", self.value)

X_ODG_PMML.classMap["X-ODG-Substring"] = X_ODG_Substring

class X_ODG_ComparisonMeasure(X_ODG_PMML, ComparisonMeasure):
    xsd = load_xsdElement(X_ODG_PMML, """
    <xs:element name="X-ODG-ComparisonMeasure">
        <xs:complexType>
            <xs:sequence>
                <xs:element maxOccurs="unbounded" minOccurs="0" ref="Extension" />
                <xs:choice>
                    <xs:element ref="euclidean" />
                    <xs:element ref="squaredEuclidean" />
                    <xs:element ref="chebychev" />
                    <xs:element ref="cityBlock" />
                    <xs:element ref="minkowski" />
                    <xs:element ref="simpleMatching" />
                    <xs:element ref="jaccard" />
                    <xs:element ref="tanimoto" />
                    <xs:element ref="binarySimilarity" />
                </xs:choice>
            </xs:sequence>
            <xs:attribute name="kind" use="required">
                <xs:simpleType>
                    <xs:restriction base="xs:string">
                        <xs:enumeration value="distance" />
                        <xs:enumeration value="similarity" />
                    </xs:restriction>
                </xs:simpleType>
            </xs:attribute>
            <xs:attribute default="absDiff" name="compareFunction" type="COMPARE-FUNCTION" />
            <xs:attribute name="minimum" type="NUMBER" use="optional" />
            <xs:attribute name="maximum" type="NUMBER" use="optional" />
            <xs:attribute name="weightField" type="FIELD-NAME" use="optional" />
        </xs:complexType>
    </xs:element>
    """)

    def top_validate(self, dataContext):
        if "weightField" in self.attrib and self.attrib["weightField"] not in dataContext.cast:
            raise PMMLValidationError("%s references field \"%s\" but no such field is accessible within its MiningSchema's active context %s" % (self.__class__.__name__, self.attrib["weightField"], dataContext.contextString()))

X_ODG_PMML.classMap["X-ODG-ComparisonMeasure"] = X_ODG_ComparisonMeasure
