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

"""Define an XML quick-reader that doesn't load the entire scores file into memory."""

import xml.sax
import xml.sax.handler
import xml.sax._exceptions
import new
import StringIO
import re
import string

import augustus.core.xmlbase as xmlbase

class ScoresError(Exception): pass

class ScoresFile(xml.sax.handler.ContentHandler):
    """XML quick-reader that doesn't load the entire scores file into memory."""

    ERROR_MESSAGE = re.compile("^<unknown>:[0-9]+:[0-9]+: (.*)$")

    def __init__(self, fileName, attributeCast={}, contentCast={}, exception=False, excludeTag=None, maxLinesInBuffer=3):
        """Create an iterator that loads fileName one line at a time
        and applies casting functions.

        Parses a file quickly if each line has one complete XML
        fragment and there is *no* top-level tag.  (If the file has a
        top-level tag, use 'excludeTag' to ignore it.)

        Attributes::

            attributeCast (dict): tag name -> casting function for all
                attributes that you wish to convert to
                numerical/object values

            contentCast (dict): tag name -> casting function for all
                content that you wish to convert to xmlobj.value

            exception (bool): if True, raise an exception when the
                casting function fails

            excludeTag (str): tag to exclude, usually the top-level
                tag, if present

            maxLinesInBuffer (int): number of lines before the
                algorithm gives up
        """

        self.fileName = fileName
        self.attributeCast = attributeCast
        self.contentCast = contentCast
        self.exception = exception
        self.excludeTag = excludeTag
        self.maxLinesInBuffer = maxLinesInBuffer

    def __iter__(self):
        self.stream = file(self.fileName).xreadlines()
        self.lineNumber = 0
        self.buffer = ""
        self.fillBuffer = True
        self.linesInBuffer = 0
        self.eof = False
        if self.excludeTag is None:
            self.excludePattern = None
        else:
            self.excludePattern = re.compile("<\s*/?\s*%s[^>]*>" % self.excludeTag)
        return self

    def startElement(self, tag, attrib):
        s = xmlbase.XML(tag)
        s.goodCast = True

        for n, v in attrib.items():
            attributeCast = self.attributeCast.get(tag + "." + n, None)
            if attributeCast is not None:
                try:
                    s.attrib[n] = attributeCast(v)
                except ValueError, err:
                    if self.exception:
                        raise ScoresError("Could not cast %s attribute %s (\"%s\") with %s: %s (line %d of %s)" % (tag, n, v, attributeCast, str(err), self.lineNumber, self.fileName))
                    else:
                        s.attrib[n] = None
                        s.goodCast = False

            else:
                s.attrib[n] = v

        s.value = []
        self.CDATA = False

        if len(self.stack) > 0:
            self.stack[-1].children.append(s)

        self.stack.append(s)

    def characters(self, text):
        if not self.CDATA:
            s = self.stack[-1]
            s.value.append(text)

    def endElement(self, tag):
        last = self.stack[-1]
        if len(filter(lambda x: not x.goodCast, last.children)) > 0:
            last.goodCast = False

        s = "".join(last.value).rstrip(string.whitespace).lstrip(string.whitespace)

        contentCast = self.contentCast.get(tag, None)
        if contentCast is not None:
            try:
                last.value = contentCast(s)
            except ValueError, err:
                if self.exception:
                    raise ScoresError("Could not cast %s content (\"%s\") with %s: %s (line %d of %s)" % (tag, s, contentCast, str(err), self.lineNumber, self.fileName))
                else:
                    last.value = None
                    last.goodCast = False

        else:
            del last.value
            if s != "":
                last.children.append(xmlbase.XMLText(s))
            
        if len(self.stack) == 1:
            self.output = self.stack.pop()
        else:
            self.stack.pop()

    def processingInstruction(self, target, data):
        pass
    
    def comment(self, comment):
        pass
    
    def startCDATA(self):
        self.CDATA = True

    def endCDATA(self):
        self.CDATA = False
    
    def startDTD(self, name, public_id, system_id):
        pass
    
    def endDTD(self):
        pass
    
    def startEntity(self, name):
        pass
    
    def endEntity(self, name):
        pass

    def next(self):
        if self.eof: raise StopIteration

        if self.fillBuffer:
            self.lineNumber += 1
            try:
                line = self.stream.next()
            except StopIteration:
                self.eof = True
            else:
                self.buffer += line
                self.linesInBuffer += 1

        if self.linesInBuffer > self.maxLinesInBuffer:
            raise ScoresError("Buffer contains %d lines; perhaps you need to set excludeTag='Report' to exclude the '<Report>'?" % self.linesInBuffer)

        if self.excludeTag is not None and self.buffer.find(self.excludeTag) != -1:
            self.buffer = re.sub(self.excludePattern, "", self.buffer)

        self.stack = []
        self.output = None
        self.skip = False

        parser = xml.sax.make_parser()
        parser.setContentHandler(self)
        parser.setProperty(xml.sax.handler.property_lexical_handler, self)
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        parser.setFeature(xml.sax.handler.feature_external_ges, 0)

        try:
            parser.parse(StringIO.StringIO(self.buffer))
        except xml.sax._exceptions.SAXParseException, err:
            message = re.match(self.ERROR_MESSAGE, str(err)).group(1)
            if message == "no element found":
                # try to add more text to the buffer and see if you can parse it
                self.fillBuffer = True
                return self.next()

            elif message == "junk after document element":
                # remove the good element that you've already parsed from the buffer
                tmpbuffer = StringIO.StringIO(self.buffer)
                for i in xrange(parser.getLineNumber() - 1):
                    tmpbuffer.readline()
                    self.linesInBuffer -= 1
                tmpbuffer.read(parser.getColumnNumber())
                self.buffer = tmpbuffer.read()

                # don't add any more to the buffer until you've parsed everything in it
                self.fillBuffer = False

            else:
                raise xmlbase.XMLError("%s on line %d of %s" % (message, self.lineNumber, self.fileName))

        else:
            self.buffer = ""
            self.linesInBuffer = 0

        return self.output
