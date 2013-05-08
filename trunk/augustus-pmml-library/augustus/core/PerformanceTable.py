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

"""This module defines the PerformanceTable class."""

import os
import sys
import time
import logging

from augustus.core.NumpyInterface import NP

class PerformanceTable(object):
    """PerformanceTable accumulates and presents timing and memory
    profiles of the current PMML implementation.

    All PMML calculations pass a PerformanceTable as a central
    service.  If you want to profile a new PMML class, add::

        performanceTable.begin("My identifier")

    at the beginning and::

        performanceTable.end("My identifier")

    at the end.  Be careful of multiple return paths: it is essential
    that PerformanceTable keys C{begin} and C{end} in a perfect stack.
    Nested keys (keys that C{begin} before the calling function has
    C{end}ed) are reported differently in the results table.  If you
    do not wish to count certain external calls in your performance
    metric, wrap them with C{pause} and C{unpause}, like this::

        performanceTable.pause("My identifier")
        externalCallThatIsIrrelevantToMyMetric()
        performanceTable.unpause("My identifier")

    Again, make sure that all C{begin}-C{pause}-C{unpause}-C{end} are
    in the proper order for all possible paths through your algorithm,
    neglecting exceptions.  (Exceptions invalidate the
    PerformanceTable anyway, so it is not necessary to include
    C{try}-C{finally} blocks everywhere.)
    """

    def __init__(self):
        """Initialize a new PerformanceTable."""

        self._begin = {}
        self._time = {}
        self._calls = {}
        self._pauseBegin = {}
        self._pauseTime = {}

        self._memBegin = {}
        self._mem = {}
        self._memPauseBegin = {}
        self._memPause = {}

        self._globalBegin = None
        self._globalMemBegin = None
        self._globalFromOtherSources = 0
        self._globalMemFromOtherSources = 0

        self._depthCount = 0
        self._depthCountBelowZero = False

        self._keyStack = []
        self._badKeyStack = None
        self._pauseStack = []
        self._blocked = False

        self._logger = logging.getLogger("PerformanceTable")

    def __repr__(self):
        return "<PerformanceTable at 0x%x>" % id(self)

    @staticmethod
    def combine(performanceTables):
        """Combine a list of PerformanceTables and output a new
        PerformanceTable representing the total of their partial sums.

        The PerformanceTables in the list are not modified, and the
        result is a new object.

        @type performanceTables: list of PerformanceTables
        @param performanceTables: The PerformanceTables to combine.
        @rtype: PerformanceTable
        @return: A new PerformanceTable representing the sum.
        """

        output = PerformanceTable()

        for performanceTable in performanceTables:
            if hasattr(performanceTable, "_globalEnd"):
                output._globalFromOtherSources += (performanceTable._globalEnd - performanceTable._globalBegin)
            if hasattr(performanceTable, "_globalMemEnd"):
                output._globalMemFromOtherSources += (performanceTable._globalMemEnd - performanceTable._globalMemBegin)

            for name in "_time", "_calls", "_mem":
                tofill = getattr(output, name)
                for tag, value in getattr(performanceTable, name).items():
                    if tag in tofill:
                        tofill[tag] += value
                    else:
                        tofill[tag] = value

        return output

    def absorb(self, performanceTable):
        """Add a given PerformanceTable's partial sums to this
        PerformanceTable in-place.

        When C{a.absorb(b)} is called, C{a} is modified and C{b} is
        not.

        @type performanceTable: PerformanceTable
        @param performanceTable: The PerformanceTable to absorb.
        """

        combination = PerformanceTable.combine([self, performanceTable])
        self.__dict__ = combination.__dict__

    def begin(self, key):
        """Starts the stopwatch and memory counter for a given key.

        Must be paired with C{end(key)} in a way that maintains proper
        nesting.

        @type key: string
        @param key: The key to start.
        """

        if self._blocked: return

        self._logger.debug("begin \"%s\", keyStack: %r", key, self._keyStack)

        now = time.time()
        memNow = NP._numberOfBytes

        self._keyStack.append(key)
        keyStack = tuple(self._keyStack)

        self._begin[keyStack] = now
        self._pauseTime[keyStack] = 0.0

        self._memBegin[keyStack] = memNow
        self._memPause[keyStack] = 0

        if self._globalBegin is None:
            self._globalBegin = now        
            self._globalMemBegin = NP._numberOfBytes

        self._depthCount += 1

    def end(self, key):
        """Stops the stopwatch and memory counter for a given key.

        Must be paired with C{begin(key)} in a way that maintains
        proper nesting.

        @type key: string
        @param key: The key to stop.
        """

        if self._blocked: return

        now = time.time()
        memNow = NP._numberOfBytes

        keyStack = tuple(self._keyStack)

        thisTime = now - self._begin[keyStack] - self._pauseTime[keyStack]

        if keyStack in self._time:
            self._time[keyStack] += thisTime
            self._calls[keyStack] += 1
        else:
            self._time[keyStack] = thisTime
            self._calls[keyStack] = 1

        memIncrease = (memNow - self._memBegin[keyStack] - self._memPause[keyStack])
        if keyStack in self._mem:
            self._mem[keyStack] += memIncrease
        else:
            self._mem[keyStack] = memIncrease

        self._globalEnd = now
        self._globalMemEnd = memNow

        self._depthCount -= 1
        if self._depthCount < 0:
            self._depthCountBelowZero = True

        try:
            poppedKey = self._keyStack.pop()
            if poppedKey != key:
                self._badKeyStack = (key, poppedKey)
        except IndexError:
            pass

        self._logger.debug("end \"%s\", keyStack: %r", key, self._keyStack)

    def pause(self, key):
        """Pauses the stopwatch and memory counter for a given key so
        that inner loops of unrelated calculations do not get counted
        as part of the key.

        Must be paired with C{unpause(key)} in a way that maintains
        proper nesting.

        @type key: string
        @param key: The key to pause.  Other keys continue to accumulate data.
        """

        if self._blocked: return

        self._logger.debug("pause \"%s\", keyStack: %r", key, self._keyStack)

        keyStack = tuple(self._keyStack)
        self._pauseBegin[keyStack] = time.time()
        self._memPauseBegin[keyStack] = NP._numberOfBytes
        self._pauseStack.append(self._keyStack.pop())

    def unpause(self, key):
        """Unpauses a given key.

        Must be paired with C{pause(key)} in a way that maintains
        proper nesting.

        @type key: string
        @param key: The key to unpause.
        """

        if self._blocked: return

        self._keyStack.append(self._pauseStack.pop())
        keyStack = tuple(self._keyStack)
        self._pauseTime[keyStack] += time.time() - self._pauseBegin[keyStack]
        self._memPause[keyStack] += NP._numberOfBytes - self._memPauseBegin[keyStack]

        self._logger.debug("unpause \"%s\", keyStack: %r", key, self._keyStack)

    def block(self):
        """Turns off PerformanceTable data collection."""

        self._blocked = True

    def unblock(self):
        """Turns PerformanceTable data collection back on."""

        self._blocked = False

    def _makeGroups(self, sortby="time"):
        """Used by C{report} and C{look}."""

        if self._depthCount != 0:
            raise RuntimeError("PerformanceTable wasn't properly prepared: depthCount is %d" % self._depthCount)
        if self._depthCountBelowZero:
            raise RuntimeError("PerformanceTable wasn't properly prepared: depthCount dropped below zero")
        if self._badKeyStack is not None:
            raise RuntimeError("PerformanceTable wasn't properly prepared: end \"%s\" encountered when end \"%s\" expected" % self._badKeyStack)

        groups = {}
        for key in self._time.keys():
            n, head = len(key), key[:-1]
            if (n, head) not in groups:
                groups[n, head] = []
            groups[n, head].append(key)
            
        if sortby == "time":
            for keys in groups.values():
                keys.sort(lambda a, b: cmp(self._time[a], self._time[b]))
        elif sortby == "calls":
            for keys in groups.values():
                keys.sort(lambda a, b: cmp(self._calls[a], self._calls[b]))
        elif sortby == "timePerCall":
            for keys in groups.values():
                keys.sort(lambda a, b: cmp(self._time[a]/self._calls[a], self._time[b]/self._calls[b]))
        elif sortby == "memory":
            for keys in groups.values():
                keys.sort(lambda a, b: cmp(self._mem[a], self._mem[b]))
        else:
            raise ValueError("Unrecognized sortby \"%s\": expected one of \"time\", \"calls\", \"timePerCall\", \"memory\"" % sortby)

        return groups

    def report(self, sortby="time"):
        """Produces the same information as C{look}, but returns a
        JSON-like dictionary that can be analyzed programmatically.

        Structure of the output::

            {"TotalTime": ##.##, "TotalNumpyMem": ##.##, "SortedBy": sortby, "Profile": [...]}

        where items in the C{"Profile"} list are::

            {"Location": "name", "calls": ###, "timePerCall": ##.##, "time": ##.##, "NumpyMemory": ##.##}

        If any locations are nested (indented names in the C{look}
        output), this item would have an additional C{"Profile"}
        key pointing to the sub-list.

        @type sortby: string
        @param sortby: The field used for sorting, may be "time", "calls", "timePerCall", or "memory".
        @rtype: dict
        @return: Keys and values are defined above.
        """

        groups = self._makeGroups(sortby)
        if len(groups) == 0: return {}

        if self._globalBegin is None:
            totalTime = self._globalFromOtherSources
        else:
            totalTime = (self._globalEnd - self._globalBegin) + self._globalFromOtherSources
        
        if self._globalMemBegin is None:
            totalMem = self._globalMemFromOtherSources
        else:
            totalMem = (self._globalMemEnd - self._globalMemBegin) + self._globalMemFromOtherSources

        output = {"TotalTime": totalTime, "TotalNumpyMem": totalMem/1024.0/1024.0, "SortedBy": sortby, "Profile": []}
        tofill = output["Profile"]

        def fillIt(key, n, head, tofill):
            suboutput = {"Location": key[-1], "calls": self._calls[key], "timePerCall": self._time[key]/self._calls[key], "time": self._time[key], "NumpyMemory": self._mem[key]/1024.0/1024.0}

            n += 1
            head += (key[-1],)
            next = (n, head)
            if next in groups:
                suboutput["Profile"] = []
                for key in groups[next]:
                    fillIt(key, n, head, suboutput["Profile"])

            tofill.append(suboutput)

        for key in groups[1, ()]:
            fillIt(key, 1, (), tofill)

        return output

    def look(self, sortby="time", stream=None, columnWidth=30):
        """An informative representation of the PerformanceTable,
        intended for interactive use.

        @type sortby: string
        @param sortby: The field used for sorting, may be "time", "calls", "timePerCall", or "memory".
        @type stream: file-like object or None
        @param stream: If None, print to C{sys.stdout}; otherwise, write to the specified stream.
        @type columnWidth: int
        @param columnWidth: Number of characters to reserve for each column.
        @rtype: None
        @return: None; human-readable output is written to the console or a specified stream.
        """

        if stream is None:
            stream = sys.stdout

        groups = self._makeGroups(sortby)
        if len(groups) == 0:
            stream.write("(empty PerformanceTable)%s" % os.linesep)
            return

        formatter = "%%-%ds   %%12s   %%-12s   %%-12s   %%-8s   %%13s%s" % (columnWidth, os.linesep)
        stream.write(formatter % ("Location", "calls", "time/call (s)", "time (s)", "frac (%)", "Numpy mem (MB)"))
        stream.write("-" * (columnWidth + 74))
        stream.write(os.linesep)

        formatter  = "%%-%ds   %%12g   %%-12g    %%-12g                %%12.3f%s" % (columnWidth, os.linesep)
        formatter2 = "%%-%ds   %%12g   %%-12g    %%-12g    %%6.2f      %%12.3f%s" % (columnWidth, os.linesep)
        formatter3 = "%%-%ds   %%12s   %%-12s    %%-12g    %%6.2f%s" % (columnWidth, os.linesep)

        def showIt(key, n, head, denom):
            name = "    " * (len(key) - 1) + key[-1]
            if len(name) > columnWidth:
                name = name[:(columnWidth - 3)] + "..."

            if denom is None:
                stream.write(formatter % (name, self._calls[key], self._time[key]/self._calls[key], self._time[key], self._mem[key]/1024.0/1024.0))
            else:
                stream.write(formatter2 % (name, self._calls[key], self._time[key]/self._calls[key], self._time[key], (100.0 * self._time[key]/denom), self._mem[key]/1024.0/1024.0))

            n += 1
            head += (key[-1],)
            next = (n, head)
            if next in groups:
                total = 0.0
                for subkey in groups[next]:
                    total += showIt(subkey, n, head, self._time[key])

                name = "    " * len(key) + "(remainder)"
                if len(name) > columnWidth:
                    name = name[:(columnWidth - 3)] + "..."
                stream.write(formatter3 % (name, "", "", self._time[key] - total, 100.0*(self._time[key] - total)/self._time[key]))
                
            return self._time[key]

        for key in groups[1, ()]:
            showIt(key, 1, (), None)

        if self._globalBegin is None:
            totalTime = self._globalFromOtherSources
        else:
            totalTime = (self._globalEnd - self._globalBegin) + self._globalFromOtherSources
        
        if self._globalMemBegin is None:
            totalMem = self._globalMemFromOtherSources
        else:
            totalMem = (self._globalMemEnd - self._globalMemBegin) + self._globalMemFromOtherSources

        stream.write("%sTotal time (s): %g   Total Numpy mem (MB): %g%s" % (os.linesep, totalTime, totalMem/1024.0/1024.0, os.linesep))
        stream.flush()
