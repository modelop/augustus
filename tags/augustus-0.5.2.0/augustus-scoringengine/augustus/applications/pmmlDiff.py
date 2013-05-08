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
import math
from augustus.core.defs import Atom
from augustus.core.xmlbase import XML, loadfile
import augustus.core.pmml41 as pmml

def sigfigs(num, n):
    """Round a number to n significant figures and return the result as a string."""
    # stolen from Cassius:
    if num == 0.:
        level = 1
    else:
        level = n - int(math.ceil(math.log10(abs(num))))
    num = round(num, level)
    format = "%."+str(max(level, 0))+"f"
    return format % num

BROKEN = Atom("Broken")
NOTFOUND = Atom("NotFound")

# def _show(index, pmmlFile, index_width=20):
#     if index is None:
#         return "%s %s" % (("%%-%ds" % index_width) % "index", repr(pmmlFile))
#     if index is BROKEN:
#         return "%s %s" % (("%%-%ds" % index_width) % "???", "???")
#     return "%s %s%s" % (("%%-%ds" % index_width) % repr(index), ". . " * len(index), repr(pmmlFile[index]))

# def _showUpTo(i, index1, file1, index2, file2):
#     output = []
#     for j, (j1, j2) in enumerate(zip(index1, index2)):
#         if j < i:
#             output.append("%-70s     versus     %-70s" % (_show(j1, file1)[:70], _show(j2, file2)[:70]))
#         elif j == i:
#             output.append("%-70s   DIFFERENT!   %-70s" % (_show(j1, file1)[:70], _show(j2, file2)[:70]))
#             return "\n".join(output)
    
def _comparitor(name1, elem1, name2, elem2):
    if elem1 is BROKEN:
        out1 = "%s: (no such element)" % name1
    else:
        out1 = "%s lines %d-%d: %s" % (name1, elem1.lineStart, elem1.lineEnd, repr(elem1))

    if elem2 is BROKEN:
        out2 = "%s: (no such element)" % name2
    else:
        out2 = "%s lines %d-%d: %s" % (name2, elem2.lineStart, elem2.lineEnd, repr(elem2))

    return "\n    %s\n    %s" % (out1, out2)

def pmmlDiff(name1, name2, validate=False, numSigfigs=6, header=False, extensions=False):
    if validate:
        file1 = loadfile(name1, pmml.X_ODG_PMML, lineNumbers=True)
        file2 = loadfile(name2, pmml.X_ODG_PMML, lineNumbers=True)
    else:
        file1 = loadfile(name1, lineNumbers=True)
        file2 = loadfile(name2, lineNumbers=True)

    if not header:
        index = file1.index(lambda x: x.tag=="Header", exception=False)
        if index is not None:
            del file1[index]

        index = file2.index(lambda x: x.tag=="Header", exception=False)
        if index is not None:
            del file2[index]

    if not extensions:
        while True:
            index = file1.index(lambda x: x.tag == "Extension", maxdepth=None, exception=False)
            if index is None:
                break
            else:
                del file1[index]

        while True:
            index = file2.index(lambda x: x.tag == "Extension", maxdepth=None, exception=False)
            if index is None:
                break
            else:
                del file2[index]

    index1 = [i for i, x in file1.walk()]; index1.insert(0, None)
    index2 = [i for i, x in file2.walk()]; index2.insert(0, None)
        
    if len(index1) < len(index2):
        index1 += [BROKEN] * (len(index2) - len(index1))
    if len(index2) < len(index1):
        index2 += [BROKEN] * (len(index1) - len(index2))

    # show problems in the order that they appear in the files
    for i, (i1, i2) in enumerate(zip(index1, index2)):
        if i1 is None:
            elem1 = file1
        elif i1 is BROKEN:
            elem1 = BROKEN
        else:
            elem1 = file1[i1]

        if i2 is None:
            elem2 = file2
        elif i2 is BROKEN:
            elem2 = BROKEN
        else:
            elem2 = file2[i2]

        # if we have a structure problem
        if i1 != i2:
            return "Different structure:%s" % _comparitor(name1, elem1, name2, elem2)

        else:
            if elem1.tag != elem2.tag:
                return "Different tag: \"%s\" vs. \"%s\"%s" % (elem1.tag, elem2.tag, _comparitor(name1, elem1, name2, elem2))

            if set(elem1.attrib.keys()) !=  set(elem2.attrib.keys()):
                return "Different attributes: %s vs. %s%s" % (sorted(elem1.attrib.keys()), sorted(elem2.attrib.keys()), _comparitor(name1, elem1, name2, elem2))

            for k in sorted(elem1.attrib.keys()):
                value1 = elem1.attrib[k]
                value2 = elem2.attrib[k]

                if isinstance(value1, float) and isinstance(value2, float):
                    value1 = sigfigs(value1, numSigfigs)
                    value2 = sigfigs(value2, numSigfigs)

                if value1 != value2:
                    return "Different attribute value for \"%s\": %s %s vs. %s %s%s" % (k, value1, str(type(elem1.attrib[k])), value2, str(type(elem2.attrib[k])), _comparitor(name1, elem1, name2, elem2))

            v1 = getattr(elem1, "value", NOTFOUND)
            v2 = getattr(elem2, "value", NOTFOUND)
            if v1 is not NOTFOUND or v2 is not NOTFOUND:
                value1, value2 = v1, v2

                if isinstance(value1, (tuple, list)) and isinstance(value2, (tuple, list)) and len(value1) == len(value2):
                    out1, out2 = [], []
                    for val1, val2 in zip(value1, value2):
                        if isinstance(val1, float) and isinstance(val2, float):
                            val1 = sigfigs(val1, numSigfigs)
                            val2 = sigfigs(val2, numSigfigs)
                        out1.append(val1)
                        out2.append(val2)
                    value1, value2 = out1, out2

                elif isinstance(v1, float) and isinstance(v2, float):
                    value1 = sigfigs(v1, numSigfigs)
                    value2 = sigfigs(v2, numSigfigs)

                if value1 != value2:
                    return "Different value for \"%s\": %s %s vs. %s %s%s" % (k, value1, str(type(value1)), value2, str(type(v2)), _comparitor(name1, elem1, name2, elem2))

            # if both elements are leaves
            if len(elem1.matches()) == 0 and len(elem2.matches()) == 0:
                content1 = elem1.content()
                content2 = elem2.content()

                if content1 != content2:
                    return "Different content: \"%s\" vs. \"%s\"%s" % (content1, content2, _comparitor(name1, elem1, name2, elem2))

    return None
