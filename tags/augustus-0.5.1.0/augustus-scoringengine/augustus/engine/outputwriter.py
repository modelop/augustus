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

"""Classes to create scoring output (XML or JSON)."""

import datetime
import os
from xml.sax.saxutils import escape, quoteattr

from augustus.core.defs import Atom
from augustus.engine.segmentrecord import SELECTFIRST, SELECTALL, SELECTONLY

########################################################### output records and formatting

class OutputMiniEvent:
    """Output formatter for one mini-event.

    A mini-event is a part of a pseudoevent corresponding to one
    value of an aggregate's groupField."""

    def __init__(self):
        self.fields = []

    def xml(self):
        """Output as an XML string."""

        return ["<%s>%s</%s>" % (displayName, escape(str(value)), displayName) for displayName, value in self.fields]

class OutputSegment:
    """Output formatter for one matched segment.

    Zero, one, or more of these may appear in an OutputRecord.
    """

    def __init__(self, segmentRecord):
        self.segmentRecord = segmentRecord
        self.fields = []
        self.minievents = []

    def xml(self, multipleModelMethod, segmentName, segmentExpressionName, outputName):
        """Output as an XML string."""

        if multipleModelMethod is SELECTFIRST or multipleModelMethod is SELECTALL:
            output = ["<%s id=\"%s\" expression=\"%s\">" % (segmentName, self.segmentRecord.name(), self.segmentRecord.expressionTree)]
        elif multipleModelMethod is SELECTONLY:
            output = []
            
        output.extend(["<%s>%s</%s>" % (displayName, escape(str(value)), displayName) for displayName, value in self.fields])

        if len(self.minievents) == 1:
            output.extend(self.minievents[0].xml())

        elif len(self.minievents) > 1:
            output.extend(["".join(["<MiniEvent>"] + i.xml() + ["</MiniEvent>"]) for i in self.minievents])

        if multipleModelMethod is SELECTFIRST or multipleModelMethod is SELECTALL:
            output.append("</%s>" % segmentName)
        elif multipleModelMethod is SELECTONLY:
            pass

        return output

    def json(self, multipleModelMethod, segmentName, segmentExpressionName, outputName):
        """Output as a JSON string; not implemented (correctly)."""

        raise NotImplementedError, "JSON writing is currently invalid.  Fix it before using it!"

    # def json(self, multipleModelMethod, segmentName, segmentExpressionName, outputName):
    #     if multipleModelMethod is SELECTFIRST or multipleModelMethod is SELECTONLY:
    #         return json.dumps(dict(self.fields))
    #     elif multipleModelMethod is SELECTALL:
    #         return json.dumps({segmentName: self.segmentRecord.name(), segmentExpressionName: self.segmentRecord.expressionTree, outputName: dict(self.fields)})

class OutputRecord:
    """Output formatter for one event or pseudoevent record."""

    def __init__(self, eventNumber, multipleModelMethod, pseudo=False):
        self.eventNumber = eventNumber
        self.pseudo = pseudo
        self.segments = []
        self.multipleModelMethod = multipleModelMethod

    def xml(self, eventTags=None, eventName="Event", pseudoEventName="PseudoEvent", segmentName="Segment", segmentExpressionName="SegmentExpression", outputName="Output", matchingSegmentsName="MatchingSegments"):
        """Output as an XML string."""

        if eventTags is None:
            eventTags = ""
        else:
            eventTags = " ".join(["%s=%s" % (n, quoteattr(str(v))) for n, v in eventTags] + [""])

        output = ["<%s %snumber=\"%d\">" % (pseudoEventName if self.pseudo else eventName, eventTags, self.eventNumber)]
        for segment in self.segments:
            output.extend(segment.xml(self.multipleModelMethod, segmentName, segmentExpressionName, outputName))
        output.append("</%s>" % (pseudoEventName if self.pseudo else eventName))
        return "".join(output)

    def json(self, eventTags=None, eventName="Event", pseudoEventName="PseudoEvent", segmentName="Segment", segmentExpressionName="SegmentExpression", outputName="Output", matchingSegmentsName="MatchingSegments"):
        """Output as a JSON string; not implemented (correctly)."""

        raise NotImplementedError, "JSON writing is currently invalid.  Fix it before using it!"

    # def json(self, eventTags=None, eventName="Event", pseudoEventName="PseudoEvent", segmentName="Segment", segmentExpressionName="SegmentExpression", outputName="Output", matchingSegmentsName="MatchingSegments"):
    #     fields = []
    #     for segment in self.segments:
    #         fields.append(segment.json(self.multipleModelMethod, segmentName, segmentExpressionName, outputName))

    #     if self.multipleModelMethod is SELECTFIRST:
    #         return "{\"%s\": %d, \"%s\": %s, \"%s\": %s, \"%s\": %s}" % (eventName, self.eventNumber, segmentName, json.dumps(self.segments[0].segmentRecord.name()), segmentExpressionName, json.dumps(self.segments[0].segmentRecord.expressionTree), outputName, ", ".join(fields))
    #     elif self.multipleModelMethod is SELECTALL:
    #         return "{\"%s\": %d, \"%s\": [%s]}" % (eventName, self.eventNumber, matchingSegmentsName, ", ".join(fields))
    #     elif self.multipleModelMethod is SELECTONLY:
    #         return "{\"%s\": %d, \"%s\": %s}" % (eventName, self.eventNumber, outputName, fields[0])

    def __repr__(self):
        return self.xml()

class OutputWriter:
    """Writes all scoring output.

    Opened at the beginning of a job, written to with each
    event/pseudoevent, and closed at the end of a job.
    """

    XML = Atom("xml")
    JSON = Atom("json")

    def __init__(
        self,
        fileName, mode="xml",
        reportName=None, pmmlFileName=None, eventName="Event", pseudoEventName="pseudoEvent",
        segmentName="Segment", segmentExpressionName="SegmentExpression",
        outputName="Output", matchingSegmentsName="MatchingSegments"):
        """Create an OutputWriter with specified tag names."""

        self.fileName = fileName
        if mode == "xml":
            self.mode = self.XML
        elif mode == "json":
            self.mode = self.JSON
        else:
            raise NotImplementedError,\
                "Only 'xml' and 'json' output modes have been implemented"

        self.pmmlFileName = pmmlFileName
        self.reportName = reportName
        self.eventName = eventName
        self.pseudoEventName = pseudoEventName
        self.segmentName = segmentName
        self.segmentExpressionName = segmentExpressionName
        self.outputName = outputName
        self.matchingSegmentsName = matchingSegmentsName

    def open(self, append=True):
        """Open an output file for writing.

        If a reportName is given, open the outermost XML or JSON
        object.
        """

        if isinstance(self.fileName, basestring):
            self.ostream = open(self.fileName, "a" if append else "w")
        else:
            self.ostream = self.fileName
            self.fileName = self.ostream.name

        if self.reportName is not None:
            if self.mode is self.XML:
                label = dict(timestamp=datetime.datetime.now())
                if self.pmmlFileName is not None:
                    label["model"] = self.pmmlFileName
                label = " ".join(["%s=%s" % (k,quoteattr(str(v))) for k,v in label.iteritems()])
                self.ostream.write("<%s %s>%s" % (self.reportName, label, os.linesep))
            elif self.mode is self.JSON:
                self.ostream.write("{\"%s\": [%s" % (self.reportName, os.linesep))
                self.needsComma = False

    def write(self, outputRecord, eventTags=None, eventName=None, pseudoEventName=None, segmentName=None, segmentExpressionName=None, outputName=None, matchingSegmentsName=None):
        """Write one record to the output file."""

        if self.mode is self.XML:
            self.ostream.write(outputRecord.xml(
                eventTags,
                self.eventName if eventName is None else eventName,
                self.pseudoEventName if pseudoEventName is None else pseudoEventName,
                self.segmentName if segmentName is None else segmentName,
                self.segmentExpressionName if segmentExpressionName is None else segmentExpressionName,
                self.outputName if outputName is None else outputName,
                self.matchingSegmentsName if matchingSegmentsName is None else matchingSegmentsName))
            self.ostream.write(os.linesep)

        elif self.mode is self.JSON:
            if self.reportName is not None:
                if self.needsComma:
                    self.ostream.write(",")
                    self.ostream.write(os.linesep)

            self.ostream.write(outputRecord.json(
                eventTags,
                self.eventName if eventName is None else eventName,
                self.pseudoEventName if pseudoEventName is None else pseudoEventName,
                self.segmentName if segmentName is None else segmentName,
                self.segmentExpressionName if segmentExpressionName is None else segmentExpressionName,
                self.outputName if outputName is None else outputName,
                self.matchingSegmentsName if matchingSegmentsName is None else matchingSegmentsName))
            
            if self.reportName is not None:
                self.needsComma = True
            else:
                self.ostream.write(os.linesep)

    def close(self):
        """Close the output record.

        If a reportName is given, close the outermost XML or JSON
        object.
        """

        if self.reportName is not None:
            if self.mode is self.XML:
                self.ostream.write("</%s>%s" % (self.reportName, os.linesep))
            elif self.mode is self.JSON:
                self.ostream.write("]}")
                self.ostream.write(os.linesep)
