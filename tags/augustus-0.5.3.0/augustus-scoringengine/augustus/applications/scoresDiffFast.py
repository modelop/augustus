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

import xml

from itertools import izip
from StringIO import StringIO
from subprocess import Popen
from subprocess import PIPE
from xml import sax

from augustus.core.xmlbase import XMLError
class ObservedDifference(Exception): pass

class XMLStreamLoader(xml.sax.handler.ContentHandler):
    """Loader for scores data.

    Each score row has a format like:
    <OuterTag id="number"><Tag1>Content</Tag1><Tag2><Content</Tag2></OuterTag>
    <Outer id="number"><Segment id="foo" other="bar"><Tag1>Content</Tag1></Segment></Outer>
    """
    def __init__(self):
        self._parser = xml.sax.make_parser()
        self._parser.setContentHandler(self)

    def getRowFrom(self, string):
        self._parser.reset()

        try:
            self._parser.parse(StringIO(string))

        except xml.sax.SAXParseException, err:
            raise XMLError("Bad data encountered in stream: %s" % str(err))

        return self._stack

    ################################### ContentHandler interface's required methods
    def startDocument(self):
        self._stack = [ (None, None, []), ]

    def endDocument(self):
        self._stack = self._stack[1]

    def startElement(self, name, attrs):
        segmentID = None
        if "id" in attrs.getNames():
            segmentID = attrs['id']
        self._stack.append( (name, segmentID, []) )

    def characters(self, content):
        content = str(content)
        if content.strip():
            # if not completely whitespace
            name, segmentID, children = self._stack[-1]
            if len(children) != 0 and isinstance(children[-1], basestring):
                children[-1] += content
            else:
                children.append(content)

    def endElement(self, name):
        children = []
        currentName, currentID, currentChildren = self._stack.pop()

        while name != currentName:
            children.append( (currentName, currentID, currentChildren) )

            if len(self._stack) > 1:
                currentName, currentID, currentChildren = self._stack.pop()
            else:
                raise XMLError("Error reading the xml..failed in element %s" % name)

        children.append(currentChildren)
        children.reverse()
        self._stack.append( (name, currentID, children) )

def compare_elements(elementA, elementB, precision):
    tagA, idA, childrenA = elementA 
    tagB, idB, childrenB = elementB

    if tagA != tagB:
        print "Tags differ:", tagA, "(first file)", tagB, "(second file)",
        return False
    elif idA != idB:
        print "Segment IDs differ:", idA, "(first file)", idB, "(second file)",
        return False
    else:
        for childA, childB in izip(childrenA, childrenB):
            if isinstance(childA, list) and len(childA) == 1 and isinstance(childB, list) and len(childB) == 1:
                childA = childA[0]
                childB = childB[0]

            if isinstance(childA, tuple) and isinstance(childB, tuple):
                matched = compare_elements(childA, childB, precision)
                if not matched:
                    print "within", tagA, "(first file) and", tagB, "(second file)",
                    return False
            elif isinstance(childA, basestring) and isinstance(childB, basestring):
                childA = childA.strip()
                childB = childB.strip()

                if childA != childB:
                    try:
                        childA = float(childA)
                        childB = float(childB)
                    except ValueError:
                        print "Content differs:", childA, "within", tagA, "(first file)",
                        print "and", childB, "within", tagB, "(second file)",
                        return False

                    try:
                        percentDiff = abs(childA - childB)/((childA + childB)/2.)
                    except ZeroDivisionError:
                        percentDiff = abs(childA - childB)

                    if percentDiff > precision:
                        print "Content differs:", childA, "within", tagA, "(first file)",
                        print "and", childB, "within", tagB, "(second file)",
                        return False
            elif childA == childB:
                pass
            else:
                print "Content differs:", childA, "within", tagA, "(first file)",
                print "and", childB, "within", tagB, "(second file)",
                return False
        return True

readerA = XMLStreamLoader()
readerB = XMLStreamLoader()

def compare_lines(lineA, lineB, precision):
    if len(lineA.split("<")) <= 2 and len(lineB.split("<")) <= 2 :
        tagA = lineA.split("<")[1].split()[0]
        tagB = lineA.split("<")[1].split()[0]
        if tagA != tagB:
            print "Tags differ:", tagA, "(first file)", tagB, "(second file)",
            return False

    rowA = readerA.getRowFrom(lineA)
    rowB = readerB.getRowFrom(lineB)
    return compare_elements(rowA, rowB, precision)


def compare_files(fileNameA, fileNameB, sigfigs, maxlines):
    fileA = file(fileNameA)
    fileB = file(fileNameB)

    precision = pow(0.1, sigfigs)

    lines = 0
    for lineA, lineB in izip(fileA, fileB):
        success = compare_lines(lineA, lineB, precision)
        if not success:
            print "at line", lines
            return False
        if maxlines > -1 and lines > maxlines:
            print "All", lines, "lines matched. ",
            return True
        else:
            lines += 1

    fileA.close()
    fileB.close()
    return True
