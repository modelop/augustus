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

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import StringIO
import threading
import time
import logging

from augustus.core.defs import Atom, MISSING
import augustus.core.xmlbase as xmlbase

class AugustusHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_error(400, "Only the POST method is supported")

    def do_POST(self):
        while self.server.dataStream.request is not None:
            time.sleep(0)

        logger = logging.getLogger()
        logDebug = logger.getEffectiveLevel() <= logging.DEBUG

        try:
            plen = self.headers.getheader("Content-length")
            if plen is None:
                self.send_error(400, "Content-length header is missing")
                logger.error("HTTP request with no Content-length: %s" % str(self.headers))
                return

            data = self.rfile.read(int(plen))

            try:
                content = xmlbase.load(data)
            except (xmlbase.XMLError, xmlbase.XMLValidationError), err:
                self.send_error(400, "Content is not valid (%s: %s)" % (err.__class__.__name__, str(err)))
                logger.error("HTTP request with invalid content (%s: %s):\n%s" % (err.__class__.__name__, str(err), data))
                return

            if content.tag == "Event":
                if "id" not in content.attrib:
                    self.send_error(400, "Event must have an 'id' attribute")
                    logger.error("HTTP request without 'id': %s" % data)
                    return
            else:
                self.send_error(400, "Request has unrecognized tag: %s" % content.tag)
                logger.error("HTTP request unrecognized tag: %s" % content.tag)
                return

        except (Exception, KeyboardInterrupt), err:
            self.send_error(500, "Augustus exception (%s: %s)" % (err.__class__.__name__, str(err)))
            logger.error("HTTP request caused an exception")
            self.server.dataStream.exception = err
            self.server.shutdown()
            raise SystemExit

        if logDebug:
            logger.debug("HTTP received request: %s" % data)

        if self.server.dataStream.checkAddress:
            self.server.dataStream.address = self.address_string()
        else:
            self.server.dataStream.address = None

        self.server.dataStream.response = None
        self.server.dataStream.request = content
        while self.server.dataStream.response is None:
            time.sleep(0)

        try:
            if logDebug:
                logger.debug("HTTP sending response: %s" % self.server.dataStream.response)

            self.send_response(200)
            self.wfile.write("Content-type: text/xml\nContent-length:%d\n\n%s\n" %
                             (len(self.server.dataStream.response), self.server.dataStream.response))
            self.wfile.close()
            self.server.dataStream.response = None

        except (Exception, KeyboardInterrupt), err:
            self.send_error(500, "Augustus exception (%s: %s)" % (err.__class__.__name__, str(err)))
            logger.error("HTTP response caused an exception")
            self.server.dataStream.exception = err
            self.server.shutdown()
            raise SystemExit

    def log_error(self, *args):
        pass

    def log_debug(self, format, *args):
        pass

    def log_message(self, format, *args):
        pass

class AugustusHTTPServer(HTTPServer):
    def __init__(self, dataStream, address, request_handler=AugustusHTTPRequestHandler):
        self.dataStream = dataStream
        self.got = {}
        HTTPServer.__init__(self, address, request_handler)

    def die(self):
        self.server.shutdown()
        raise SystemExit

class AugustusHTTPDataStream(object):
    def __init__(self, fromHTTP):
        self.host = fromHTTP.attrib.get("host", "")
        self.port = fromHTTP.attrib["port"]
        self.respond = fromHTTP.attrib.get("respond", True)
        self.checkAddress = fromHTTP.attrib.get("checkAddress", True)

        self.name = "<AugustusHTTPServer host=%s port=%d>" % (self.host, self.port)
        
        self.outputWriter = None
        self.server = None
        self.thread = None

    def initialize(self):
        if self.server is not None:
            self.server.die()

        self.address = None
        self.request = None
        self.exception = None
        self.response = None

        self.server = AugustusHTTPServer(self, (self.host, self.port))
        self.thread = threading.Thread(target=self.server.serve_forever, name="AugustusHTTPServer-%d" % id(self.server))
        self.thread.start()

    def setupOutput(self, outputWriter):
        self.outputWriter = outputWriter
        self.outputWriter.streams.append(self)
        self.ostream = StringIO.StringIO()
        self.write = self.ostream.write

    def next(self):
        self.address = None
        self.request = None
        self.exception = None
        while self.request is None and self.exception is None:
            time.sleep(0)
        if self.exception is not None:
            raise self.exception

        if self.outputWriter is not None:
            self.outputWriter.identifier = self.request.attrib["id"]
            self.outputWriter.address = self.address

        self.got = {}
        for child in self.request:
            if len(child.children) > 0 and isinstance(child.children[0], xmlbase.XMLText):
                self.got[child.tag] = child.content()

        if not self.respond:
            self.response = "<NoResponse />"
            while self.response is not None and self.exception is None:
                time.sleep(0)
            if self.exception is not None:
                raise self.exception

    def get(self, field):
        return self.got.get(field, MISSING)

    def finishedEvent(self):
        self.response = self.ostream.getvalue()

        while self.response is not None and self.exception is None:
            time.sleep(0)
        if self.exception is not None:
            raise self.exception

        self.ostream = StringIO.StringIO()
        self.write = self.ostream.write
