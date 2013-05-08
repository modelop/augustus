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

import sys
import itertools
from augustus.core.scoresfile import ScoresFile

class ObservedDifference(Exception): pass

def sigfigs(num, n):
    """Round a number to n significant figures and return the result as a string."""
    formatstring = "{{0:0.{0}}}".format(n)
    return formatstring.format(num)

def compare_tags(one, two, count, counts, number, segment, n):
    for i, (t1, t2) in enumerate(itertools.izip(one.children, two.children)):
        if t1 != t2:
            content1 = t1.content().lstrip().rstrip()
            content2 = t2.content().lstrip().rstrip()

            if content1 != content2:
                try:
                    float1 = float(content1)
                    float2 = float(content2)
                except ValueError:
                    float1 = None
                    float2 = None

                if float1 is None or sigfigs(float1, n) != sigfigs(float2, n):
                    if count and t1.tag == t2.tag:
                        if t1.tag not in counts:
                            counts[t1.tag] = 0
                        counts[t1.tag] += 1
                    else:
                        raise ObservedDifference, "Tag at position %d differs in segment %s of event number %s: %s versus %s" % (i, segment, number, t1.xml(indent="", linesep=""), t2.xml(indent="", linesep=""))

def compare_segments(one, two, count, counts, number, n):
    ids1 = dict([(i["id"], i) for i in one.children])
    ids2 = dict([(i["id"], i) for i in two.children])
    ids1keys = ids1.keys()
    ids2keys = ids2.keys()
    ids1keys.sort()
    ids2keys.sort()
    
    if ids1keys != ids2keys:
        if len(ids1keys) > 10 or len(ids2keys) > 10:
            report = "in first, not in second: %s in second, not in first: %s" % (set(ids1keys).difference(set(ids2keys)), set(ids2keys).difference(set(ids1keys)))
        else:
            report = "%s versus %s" % (",".join(ids1keys), ",".join(ids2keys))

        raise ObservedDifference, "Segment ids do not match: %s in event number %s" % (report, number)

    for key in ids1keys:
        compare_tags(ids1[key], ids2[key], count, counts, number, key, n)

def compare_files(fileName1, fileName2, count, excludeTag, maxLinesInBuffer, n):
    counts = {}

    onefile = ScoresFile(fileName1, excludeTag=excludeTag, attributeCast={}, contentCast={}, maxLinesInBuffer=maxLinesInBuffer)
    twofile = ScoresFile(fileName2, excludeTag=excludeTag, attributeCast={}, contentCast={}, maxLinesInBuffer=maxLinesInBuffer)

    for one, two in itertools.izip(onefile, twofile):
        number1 = one["number"]
        number2 = two["number"]

        if number1 != number2:
            raise ObservedDifference, "Event numbers do not match: %s versus %s" % (number1, number2)

        if set([i.tag for i in one.children + two.children]) == set(["Segment"]):
            compare_segments(one, two, count, counts, number1, n)
            
        else:
            compare_tags(one, two, count, counts, number1, "(none)", n)

    if count and len(counts) > 0:
        raise ObservedDifference, "The following tags differed: %s" % counts

    return True
