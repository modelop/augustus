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

"""This module defines the PlotTickMarks class."""

import math
import re
import json
import datetime

from augustus.core.defs import defs
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.plot.PlotNumberFormat import PlotNumberFormat

class PlotTickMarks(object):
    """PlotTickMarks is a bag of functions to generate convenient tick
    marks for a numerical range.
    """

    @staticmethod
    def interpret(tickSpecification, low, high):
        """Interpret a tick specification string and generate
        tickmarks and mini-tickmarks.

        A tick specification string can be formed like one of the
        following:

          - C{linear(~N)}: approximately N major ticks; mini-ticks fill
            in between.
          - C{linear(N)}: exactly N major ticks; mini-ticks fill in
            between.
          - C{log(~N)}: approximately N major logarithmic ticks (base
            10); mini-ticks fill in between.
          - C{logB(~N)}: approximately N major logarithmic ticks (base
            B); mini-ticks fill in between.
          - C{explicit({#: "num1", #: "num2"})}: an explicit set of ticks
            as a dictionary that maps locations (numbers) to display
            values (strings); this does not generate mini-ticks.
          - C{explicit([#, #, #, #])}: an explicit set of ticks as a list
            of locations (numbers); the display values are generated
            from the numbers.
          - C{fillmini(...)}: an explicit set of ticks, following the
            same convention as C{explicit}, but filled in with mini-ticks.
          - C{time()}: temporally meaningful ticks, automatically chosen
            based on the range.

        The output is a C{ticks, miniticks} pair, where C{ticks} is a
        dictionary mapping locations (numbers) to display values
        (strings) and C{miniticks} is a list of locations.

        @type tickSpecification: string
        @param tickSpecification: As described above.
        @type low: number
        @param low: Minimum edge of the plot window to mark with ticks.
        @type high: number
        @param high: Maximum edge of the plot window to mark with ticks.
        @rtype: 2-tuple of C{ticks}, C{miniticks}
        @return: As described above.
        @raise ValueError: If the tick specification is not well formed, this function raises an error.
        """

        if low >= high:
            raise ValueError, "low must be strictly less than high"

        m = re.match("^\s*linear\s*\(\s*(~?)([0-9]+)\s*\)\s*$", tickSpecification)
        if m is not None:
            tilde, N = m.group(1), int(m.group(2))
            if N < 2:
                raise ValueError("N must be greater than 1 in tick-marks specification: \"%s\"" % tickSpecification)
            if tilde == "~":
                N = -N

            ticks = PlotTickMarks._computeTicks(low, high, N)
            miniticks = PlotTickMarks._computeMiniticks(low, high, ticks)
            return ticks, miniticks

        m = re.match("^\s*log([0-9]*)\s*\(\s*(~?)([0-9]+)\s*\)\s*$", tickSpecification)
        if m is not None:
            base, tilde, N = m.group(1), m.group(2), int(m.group(3))
            if base == "": base = 10
            else: base = int(base)

            if base < 2:
                raise ValueError("log base must be greater than 1 in tick-marks specification: \"%s\"" % tickSpecification)
            if N < 2:
                raise ValueError("N must be greater than 1 in tick-marks specification: \"%s\"" % tickSpecification)
            if tilde == "~":
                N = -N

            if low <= 0.0 or high <= 0.0:
                # fallback (might be encountered in production, so don't raise an error)
                ticks = PlotTickMarks._computeTicks(low, high, N)
                miniticks = PlotTickMarks._computeMiniticks(low, high, ticks)
                return ticks, miniticks

            else:
                ticks = PlotTickMarks._computeLogticks(low, high, base, N)
                miniticks = PlotTickMarks._computeLogminiticks(low, high, base)
                return ticks, miniticks

        m = re.match("^\s*(explicit|fillmini)\s*\((.*)\)$", tickSpecification)
        if m is not None and m.group(1) in ("explicit", "fillmini"):
            spec = m.group(2)
            if spec[0] == "[":
                try:
                    spec = json.loads(spec)
                    spec = map(float, spec)
                    low, high = min(spec), max(spec)
                    ticks = dict((x, PlotNumberFormat.toUnicode(x, low, high)) for x in spec)
                    if m.group(1) == "fillmini":
                        miniticks = PlotTickMarks._computeMiniticks(low, high, ticks)
                    else:
                        miniticks = []
                    return ticks, miniticks
                except ValueError:
                    pass

            elif spec[0] == "{":
                try:
                    ticks = {}
                    for x, label in json.loads(re.sub(r"\b([+-]? *(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*:", r'"\1":', spec)).items():
                        if isinstance(label, (float, int, long)):
                            label = repr(label)
                        if not isinstance(label, basestring):
                            raise ValueError
                        ticks[float(x)] = label
                    if m.group(1) == "fillmini":
                        miniticks = PlotTickMarks._computeMiniticks(low, high, ticks)
                    else:
                        miniticks = []
                    return ticks, miniticks
                except ValueError, AttributeError:
                    pass

        m = re.match("^\s*time\s*\(\s*\)\s*$", tickSpecification)
        if m is not None:
            return PlotTickMarks._computeTimeticks(low, high)

        raise ValueError("Tick-marks specification not understood: \"%s\"" % tickSpecification)

    @staticmethod
    def _computeTicks(low, high, N):
        eps = defs.EPSILON * (high - low)
        low, high = low - eps, high + eps

        if N >= 0:
            output = {}
            x = low
            for i in xrange(N):
                if abs(x) < eps:
                    label = u"0"
                else:
                    label = PlotNumberFormat.toUnicode(x, low, high)
                output[x] = label
                x += (high - low)/(N - 1.0)
            return output

        N = -N

        counter = 0
        granularity = 10**math.ceil(math.log10(max(abs(low), abs(high))))
        lowN = math.ceil(low / granularity)
        highN = math.floor(high / granularity)

        while lowN > highN:
            countermod3 = counter % 3
            if countermod3 == 0:
                granularity *= 0.5
            elif countermod3 == 1:
                granularity *= 0.4
            else:
                granularity *= 0.5
            counter += 1
            lowN = math.ceil(low / granularity)
            highN = math.floor(high / granularity)

        lastGranularity = granularity
        lastTrial = None

        while True:
            trial = {}

            for n in range(long(lowN), long(highN)+1):
                x = n * granularity
                if abs(x) < eps:
                    label = u"0"
                else:
                    label = PlotNumberFormat.toUnicode(x, low, high)
                trial[x] = label

            if long(highN)+1 - long(lowN) >= N:
                if lastTrial is None:
                    v1, v2 = low, high
                    return {v1: PlotNumberFormat.toUnicode(v1, low, high), v2: PlotNumberFormat.toUnicode(v2, low, high)}
                else:
                    return lastTrial

            lastGranularity = granularity
            lastTrial = trial

            countermod3 = counter % 3
            if countermod3 == 0:
                granularity *= 0.5
            elif countermod3 == 1:
                granularity *= 0.4
            else:
                granularity *= 0.5
            counter += 1
            lowN = math.ceil(low / granularity)
            highN = math.floor(high / granularity)

    @staticmethod
    def _computeMiniticks(low, high, originalTicks):
        originalTicks = originalTicks.keys()
        originalTicks.sort()
        if len(originalTicks) < 2:
            originalTicks = [low, high]

        granularities = []
        for i in xrange(len(originalTicks)-1):
            granularities.append(originalTicks[i+1] - originalTicks[i])
        spacing = 10**(math.ceil(math.log10(min(granularities)) - 1))

        output = []
        x = originalTicks[0] - math.ceil((originalTicks[0] - low) / spacing) * spacing

        while x <= high:
            if x >= low:
                alreadyInTicks = False
                for t in originalTicks:
                    if abs(x-t) < defs.EPSILON * (high - low):
                        alreadyInTicks = True
                if not alreadyInTicks:
                    output.append(x)
            x += spacing
        return output

    @staticmethod
    def _computeLogticks(low, high, base, N):
        eps = (high/low)**defs.EPSILON
        low, high = low/eps, high*eps

        if N >= 0:
            output = {}
            x = low
            for i in xrange(N):
                output[x] = PlotNumberFormat.toUnicode(x)
                x += (high - low)/(N - 1.0)
            return output

        N = -N

        lowN = math.floor(math.log(low, base))
        highN = math.ceil(math.log(high, base))
        output = {}
        for n in range(long(lowN), long(highN)+1):
            x = base**n
            label = PlotNumberFormat.toUnicode(x)
            if low <= x <= high:
                output[x] = label

        for i in xrange(1, len(output)):
            keys = output.keys()
            keys.sort()
            keys = keys[::i]
            values = map(lambda k: output[k], keys)
            if len(values) <= N:
                for k in output.keys():
                    if k not in keys:
                        output[k] = u""
                break

        if len(output) <= 2:
            output2 = PlotTickMarks._computeTicks(low, high, -long(math.ceil(N/2.0)))
            lowest = min(output2)

            for k in output:
                if k < lowest:
                    output2[k] = output[k]
            output = output2

        return output

    @staticmethod
    def _computeLogminiticks(low, high, base):
        lowN = math.floor(math.log(low, base))
        highN = math.ceil(math.log(high, base))
        output = []
        numTicks = 0

        for n in range(long(lowN), long(highN)+1):
            x = base**n
            if low <= x <= high:
                numTicks += 1
            for m in xrange(2, long(math.ceil(base))):
                minix = m * x
                if low <= minix <= high:
                    output.append(minix)

        if numTicks <= 2:
            return []
        else:
            return output

    # approximate lengths of idealized days, months, and years (everything else is exact)
    _SECOND = FakeFieldType._dateTimeResolution
    _MINUTE = 60 * _SECOND
    _HOUR = 60 * _MINUTE
    _DAY = 24 * _HOUR
    _WEEK = 7 * _DAY
    _MONTH = 31 * _DAY
    _YEAR = 365 * _DAY
    _monthName = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    _fieldType = FakeFieldType("dateTime", "continuous")

    @staticmethod
    def _explicitTimeTicks(low, high, initialize, skip, bigTick, contextGranularity, firstIsContext, anyContext, renderContext, renderOther):
        lowDateTime = PlotTickMarks._fieldType.valueToPython(low)
        highDateTime = PlotTickMarks._fieldType.valueToPython(high)

        ticks = {}
        miniticks = []

        runner = PlotTickMarks._fieldType.valueToPython(low).replace(**initialize)
        while runner <= highDateTime:
            td = runner - FakeFieldType._dateTimeOrigin
            raw = ((td.days * 86400) + td.seconds) * 1000000 + td.microseconds

            if runner >= lowDateTime:
                if bigTick(runner):
                    ticks[raw] = runner
                else:
                    miniticks.append(raw)

            runner = runner + datetime.timedelta(**skip)

        sawContext = False
        for raw in sorted(ticks):
            dateTime = ticks[raw]

            ticks[raw] = renderOther(dateTime)
            if (firstIsContext and not sawContext) or contextGranularity(dateTime):
                if anyContext or (not sawContext):
                    ticks[raw] = renderContext(dateTime)
                sawContext = True

        return ticks, miniticks

    @staticmethod
    def _computeTimeticks(low, high):
        eps = defs.EPSILON * (high - low)
        low, high = low - eps, high + eps

        if high - low < PlotTickMarks._MINUTE:
            lowDateTime = PlotTickMarks._fieldType.valueToPython(low)
            highDateTime = PlotTickMarks._fieldType.valueToPython(high)

            if lowDateTime.minute == highDateTime.minute:
                lowSecond = lowDateTime.second + (lowDateTime.microsecond / 1e6)
                highSecond = highDateTime.second + (highDateTime.microsecond / 1e6)
                    
                ticks = PlotTickMarks._computeTicks(lowSecond, highSecond, -10)
                miniticks = PlotTickMarks._computeMiniticks(lowSecond, highSecond, ticks)

                scale = (high - low)/(highSecond - lowSecond)
                minimumTick = min(ticks.keys())
                def transformLabel(x, label):
                    y = (x - lowSecond)*scale + low
                    if x == minimumTick:
                        newLabel = PlotNumberFormat.toUnicode(x, lowSecond, highSecond, tryPrecision=True)
                        leadingZeros = re.match("^([0-9]*)", newLabel, re.UNICODE).group(1)
                        if len(leadingZeros) < 2:
                            newLabel = (u"0" * (2 - len(leadingZeros))) + newLabel
                        return y, u"%02d:%02d:%s" % (lowDateTime.hour, lowDateTime.minute, newLabel)
                    else:
                        newLabel = PlotNumberFormat.toUnicode(x, lowSecond, highSecond, tryPrecision=True)
                        return y, ":" + newLabel

                def transform(x):
                    return (x - lowSecond)*scale + low

                return dict(transformLabel(x, label) for x, label in ticks.items()), [transform(x) for x in miniticks]

            else:
                lowSecond = lowDateTime.second + (lowDateTime.microsecond / 1e6)
                highSecond = 60 + highDateTime.second + (highDateTime.microsecond / 1e6)
                    
                ticks = PlotTickMarks._computeTicks(lowSecond, highSecond, -10)
                miniticks = PlotTickMarks._computeMiniticks(lowSecond, highSecond, ticks)

                scale = (high - low)/(highSecond - lowSecond)
                minimumNewMinute = min(x for x in ticks.keys() if x >= 60)
                def transformLabel(x, label):
                    y = (x - lowSecond)*scale + low

                    newLabel = PlotNumberFormat.toUnicode(x % 60, lowSecond, highSecond)
                    leadingZeros = re.match("^([0-9]*)", newLabel, re.UNICODE).group(1)
                    if len(leadingZeros) < 2:
                        newLabel = (u"0" * (2 - len(leadingZeros))) + newLabel
                    if x == minimumNewMinute:
                        return y, u"%02d:%02d:%s" % (highDateTime.hour, highDateTime.minute, newLabel)
                    else:
                        return y, ":" + newLabel

                def transform(x):
                    return (x - lowSecond)*scale + low

                return dict(transformLabel(x, label) for x, label in ticks.items()), [transform(x) for x in miniticks]

        elif high - low < 2 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 1},
                                                    bigTick=(lambda x: x.second % 20 == 0),
                                                    contextGranularity=(lambda x: x.second == 0),
                                                    firstIsContext=True,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%02d:%02d:%02d" % (x.hour, x.minute, x.second)),
                                                    renderOther=(lambda x: u":%02d:%02d" % (x.minute, x.second)))

        elif high - low < 4 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 10},
                                                    bigTick=(lambda x: x.second % 30 == 0),
                                                    contextGranularity=(lambda x: x.second == 0),
                                                    firstIsContext=True,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%02d:%02d:%02d" % (x.hour, x.minute, x.second)),
                                                    renderOther=(lambda x: u":%02d:%02d" % (x.minute, x.second)))

        elif high - low < 8 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 30},
                                                    bigTick=(lambda x: x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 20 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 30},
                                                    bigTick=(lambda x: x.minute % 2 == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 40 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 60},
                                                    bigTick=(lambda x: x.minute % 5 == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 60 * PlotTickMarks._MINUTE:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"second": 0, "microsecond": 0},
                                                    skip={"seconds": 60},
                                                    bigTick=(lambda x: x.minute % 10 == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 2 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 300},
                                                    bigTick=(lambda x: x.minute % 20 == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 3 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 600},
                                                    bigTick=(lambda x: x.minute % 20 == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 6 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 600},
                                                    bigTick=(lambda x: x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 8 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 1800},
                                                    bigTick=(lambda x: x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 12 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 3600},
                                                    bigTick=(lambda x: x.hour % 2 == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 24 * PlotTickMarks._HOUR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 3600},
                                                    bigTick=(lambda x: x.hour % 4 == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 2 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 3600},
                                                    bigTick=(lambda x: x.hour % 6 == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 3 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 3600},
                                                    bigTick=(lambda x: x.hour % 12 == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)),
                                                    renderOther=(lambda x: u"%02d:%02d" % (x.hour, x.minute)))

        elif high - low < 7 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"seconds": 21600},
                                                    bigTick=(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d, %d" % (PlotTickMarks._monthName[x.month], x.day, x.year)),
                                                    renderOther=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)))

        elif high - low < 14 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 1},
                                                    bigTick=(lambda x: x.day % 2 == 0 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 2 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d, %d" % (PlotTickMarks._monthName[x.month], x.day, x.year)),
                                                    renderOther=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)))

        elif high - low < 21 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 1},
                                                    bigTick=(lambda x: x.day % 4 == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d, %d" % (PlotTickMarks._monthName[x.month], x.day, x.year)),
                                                    renderOther=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)))

        elif high - low < 31 * PlotTickMarks._DAY:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 1},
                                                    bigTick=(lambda x: x.day % 7 == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%s %d, %d" % (PlotTickMarks._monthName[x.month], x.day, x.year)),
                                                    renderOther=(lambda x: u"%s %d" % (PlotTickMarks._monthName[x.month], x.day)))

        elif high - low < 2 * PlotTickMarks._MONTH:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 1},
                                                    bigTick=(lambda x: x.day in (1, 7, 14, 21, 28) and x.hour == 0 and x.minute == 0 and x.second == 0 and not (x.month == 2 and x.day == 28)),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d/%02d" % (x.month, x.day)))

        elif high - low < 3 * PlotTickMarks._MONTH:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 1},
                                                    bigTick=(lambda x: x.day in (1, 15) and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day == 1 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d/%02d" % (x.month, x.day)))

        elif high - low < PlotTickMarks._YEAR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 7},
                                                    bigTick=(lambda x: x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d/%02d" % (x.month, x.day)))

        elif high - low < 1.5 * PlotTickMarks._YEAR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 7},
                                                    bigTick=(lambda x: x.month % 2 == 1 and x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d/%02d" % (x.month, x.day)))

        elif high - low < 3 * PlotTickMarks._YEAR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 7},
                                                    bigTick=(lambda x: x.month % 3 == 1 and x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    contextGranularity=(lambda x: x.month == 1 and x.day <= 7 and x.hour == 0 and x.minute == 0 and x.second == 0),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d/%02d" % (x.month, x.day)))

        elif high - low < 10 * PlotTickMarks._YEAR:
            return PlotTickMarks._explicitTimeTicks(low, high,
                                                    initialize={"month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                                    skip={"days": 30, "seconds": 36000},
                                                    bigTick=(lambda x: x.month == 1 and x.day <= 30),
                                                    contextGranularity=(lambda x: False),
                                                    firstIsContext=False,
                                                    anyContext=True,
                                                    renderContext=(lambda x: u"%d" % x.year),
                                                    renderOther=(lambda x: u"%d" % x.year))

        else:
            lowDateTime = PlotTickMarks._fieldType.valueToPython(low)
            highDateTime = PlotTickMarks._fieldType.valueToPython(high)

            lowYear = lowDateTime.year + ((lowDateTime - datetime.datetime(lowDateTime.year, 1, 1)).days / ((datetime.datetime(lowDateTime.year, 12, 31) - datetime.datetime(lowDateTime.year, 1, 1)).days + 1.0))
            highYear = highDateTime.year + ((highDateTime - datetime.datetime(highDateTime.year, 1, 1)).days / ((datetime.datetime(highDateTime.year, 12, 31) - datetime.datetime(highDateTime.year, 1, 1)).days + 1.0))

            scale = (high - low)/(highYear - lowYear)
            def transform(x):
                return (x - lowYear)*scale + low

            ticks = PlotTickMarks._computeTicks(lowYear, highYear, -10)
            miniticks = PlotTickMarks._computeMiniticks(lowYear, highYear, ticks)
            return dict((transform(x), label) for x, label in ticks.items()), [transform(x) for x in miniticks]
