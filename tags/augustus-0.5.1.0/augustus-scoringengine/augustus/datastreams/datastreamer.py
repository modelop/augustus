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

"""Defines a class that abstracts the process of reading in data."""

import logging
import Queue
import StringIO
import sys
import threading
## FIXME: Should we try/except isinf/isnan from math.py before using numpys?
## (numpy.isnan/isinf is new in Python 2.6)
# import math
import numpy
from time import sleep

import augustus.core.pmml41 as pmml
import augustus.core.config as config
from augustus.core.defs import NameSpace, MISSING, INVALID
from augustus.runlib.http_server import HTTPInterfaceServer
from augustus.runlib.any_reader import Reader
from augustus.kernel.unitable.unitable import UniDict

########################################################## DataStreamer
class DataStreamer:
    """Contains a queue of data.  Its internal read function uses threading.

    Usage:

        dataStreamer = DataStreamer(config_options)
        # Get data_fields from the PMML model (assigned in this example).
        data_fields = {'field1':int, 'field2':str}

        # If reading from a source handle:
        dataStreamer.start_streaming()
        for record in dataStreamer:
            # process the record

        # Else if in interactive mode, add data using 'enqueue'.
        # (In a thread if the data must be buffered...)
        dataStreamer.enqueue(data) 

    Public methods:

        get, next, and __iter__

        enqueue(self, dictionary)
        start_streaming(self)  # Can only call this once.

    Internal methods:

        _read(self)
        _unitableCallback(self, uni_record)
        _xmlCallback(self, native_element)

    Data Members:

        _runOptions(NameSpace; contains initialization arguments)
        _logger (Logger)
        _queue (Queue): Queue of unprocessed, but read, data elements.
                        These should be either dictionaries or UniTables.
        _reader (Either None, a Reader, or HTTPInterfaceServer)
        _thread (Thread): Thread that will run the reader.
        _values (dictionary): The most recent row of data from _queue.
    """
    def __init__(
        self,
        fromHTTP=False, interactive=False, isXML=True, runForever=False,
        maxsize=0, filename=None,
        **kwargs):
        """Set up the reading function and queue for the DataStreamer.

        DataStreamer's constructor is typically invoked by
        calling getDataStreamer(config_options), defined below.
        Error checking for appropriate configuration settings,
        and for sufficient contents in **kwargs is presumed to be
        done during XSD validation.  The reason this initialization
        function is separate is to allow an advanced user to call
        the streamer from a script and bypass having to make an
        XML object containing configuration settings.

        Arguments:

            fromHTTP (boolean; default False):
            If True, the reader will be an HTTPInterfaceServer.

            interactive (boolean; default False):
            If True, the reader will be None and the user will push
            data to the queue to score using self.enqueue(self, dictionary)
            in which dictionary is a dictionary or a UniRecord; a row in a
            UniTable.

            isXML (boolean; default False):
            If True, the reader will process the input stream as XML.

            runForever (boolean; default False):
            If True, run forever. Otherwise read all data and then exit.

            maxsize (integer; default 0):
            The maximum number of objects allowed in self.queue.
            If zero, the Queue can be arbitrarily long.

            **kwargs (arguments for the Reader)
        """
        self._runOptions =\
            NameSpace(
                fromHTTP=fromHTTP,
                interactive=interactive,
                isXML=isXML,
                runForever=runForever)
        self._fileList = filename  # None or else will become a list...
        self.currentFileNumber = 0
        self._logger = logging.getLogger()
        self._metadata = logging.getLogger('metadata')
        self._logSettings = NameSpace(
                DEBUG=self._logger.getEffectiveLevel() <= logging.DEBUG,
                INFO=self._logger.getEffectiveLevel() <= logging.INFO,
                WARNING=self._logger.getEffectiveLevel() <= logging.WARNING,
                ERROR=self._logger.getEffectiveLevel() <= logging.ERROR)
        self._thread = None
        self._values = None
        self._queue = Queue.Queue(maxsize)
        callback = self._xmlCallback if isXML else self._unitableCallback

        if interactive:
            self._reader = None
        elif fromHTTP:
            def http_callback(data):
                wrapper = StringIO.StringIO(data)
                rdr =\
                    Reader(callback,
                        source=wrapper,
                        logger=self._logger,
                        magicheader=False,
                        unitable=not isXML,
                        wholeUniTable=not isXML)
                pipe = rdr.new_pipe()
                try:
                    result = rdr.feed_pipe(None, pipe)
                except:
                    raise IOError("Problem reading data over HTTP.")
                return result
    
            self._reader =\
                HTTPInterfaceServer(
                    ('', kwargs['port']), logger=logging.getLogger(''))
            self._reader.register_callback(kwargs['url'], http_callback)
        else:
            if filename == '-':
                self._fileList = ['-']
            else:
                import glob
                self._fileList = glob.glob(filename)
                self._fileList.sort()
                self._fileList.reverse()
            if len(self._fileList) == 0:
                raise RuntimeError, "No Data Input files matched %s" % filename

            self._reader = Reader(callback, unitable=not isXML, wholeUniTable=not isXML, **kwargs)
            self._reader.source = self._fileList.pop()

    def enqueue(self, dictionary):
        """Add the dictionary (or UniTable) to the queue.

        Arguments:

            dictionary format is {field1:value1, field2:value2}
        """
        try:
            self._queue.put(dictionary, timeout=0.1)
        except Queue.Full:
            self._logger.error("Data stream queue dropped:%s" % dictionary)

    def get(self, field):
        """Return the element's value or MISSING.
        
        Arguments:

            field (string): The name of a field in the dictionary/UniRecord

            * MISSING means the value is absent.
        """
        if self._values is None:
            return MISSING

        if field not in self._values.keys():
            self._logger.warning("Data not found for field: %s" % field)
            return MISSING

        output = self._values[field]

        if isinstance(output, float) and \
            (numpy.isnan(output) or numpy.isinf(output)):
            return INVALID

        return output

    def __iter__(self):
        return self

    def next(self):
        if self._runOptions.isXML:
            self._values = None
            # reset in order to get the next item
        elif self._values is not None:
            try:
                # Iterate over the UniTable
                self._values = self._values.next()
                if self._logSettings.DEBUG:
                    self._logger.debug("This record: %s" %self._values)
                return self._values
            except StopIteration:
                self._values = None
                # keep going; try to get the next UniTable

        try:
            self._values = self._queue.get(timeout=0.1)
            self._queue.task_done()
        except Queue.Empty:
            while self._values is None:
                if self._thread and self._thread.isAlive():
                    # If the Reader thread is still going, block until
                    # another result comes.
                    self._values = self._queue.get()
                    self._queue.task_done()
                    sleep(0)
                else:
                    # Otherwise reset my thread.
                    self._thread = None
                    # Step to the next file, if it exists.
                    if len(self._fileList) > 0:
                        self._reader.source = self._fileList.pop()
                        self.currentFileNumber += 1
                        self.start_streaming()
                    else:
                        raise StopIteration
        if not self._runOptions.isXML and self._values is not None:
            # Step into the UniTable
            self._values = self._values[0]
            if self._logSettings.DEBUG:
                self._logger.debug("This record: %s" %self._values)
            return self._values
        return self._values

    def _read(self):
        """The thread callback for enqueueing the data.

        When reading forever from a file handle (per the XSD,
        can only read forever FromHTTP or FromFifo) loop continuously
        around single reads.
        """
        if self._runOptions.interactive:
            self._logger.error(
                "DataStreamer._read is not for interactive mode.  "+\
                "Instead use DataStreamer.enqueue.")
            return

        if self._runOptions.fromHTTP:
            self._reader.serve_forever()
            return

        if self._runOptions.runForever:
            while True:
                try:
                    self._reader.read_forever()
                except KeyboardInterrupt:
                    self._logger.error("Keyboard Interrupt.")
                    raise
                except:
                    self._logger.error(
                        "error reading data: %s" % sys.exc_info()[0])
                sleep(0)
        else:
            self._metadata.startTiming('Time Reading Data')
            self._reader.read_once()
            self._metadata.stopTiming('Time Reading Data')

    def start_streaming(self):
        """Start receiving/reading information and posting to my queue.

        Launch a thread with target method to start reading from
        the data source.  The method depends on the run options set
        on initialization.
        """
        if self._runOptions.interactive:
            self._logger.error(
                "DataStreamer.start_streaming is not for interactive mode.  "+\
                "Instead use DataStreamer.enqueue.")
            return

        if self._thread:
            # Current implementation is to only read the source once.
            self._logger.error("DataStream streaming invoked again on the same source.")
            return

        self._thread = threading.Thread(target=self._read)
        # Don't quit Python until after the thread is finished running.
        self._thread.daemon = False
        self._thread.start()

    def _unitableCallback(self, uni_table):
        self.enqueue(uni_table)

    def _xmlCallback(self, native_element):
        for row in native_element:
            obj = dict([(str(k), row.attr[k]) for k in row.attr])
            self.enqueue(obj)

############################################################## FFConfig
class FFConfig:
    """ This is a helper class containing information to help
    UniTable and AnyReader read from fixed record filed.

    The call:
        UniTable().from_fixed_width_file(self.handle, ffConvert.fields)
        uses this object's data attribute ``fields``.
    """
    """
    It is a direct copy of the same class definition in
    augustus.pmmllib.pmmlConsumer.py

    It is passed to a Reader defined in augustus.runlib.any_reader.py
    and used with the name ``ffConvert`` inside function
    ``receive_unitable()``.
    """
    def __init__ (self,
        fieldnames, fieldstarts, fieldends, fieldtypes=None, cr=None):

        self.fieldspecs = {}
        if fieldtypes is None:
            fieldtypes = len(fieldnames)*['str']
        fields = []
        for ind in range(len(fieldnames)):
           self.fieldspecs[fieldnames[ind]] =\
               (fieldstarts[ind],fieldends[ind],fieldtypes[ind])
           fields.append((fieldnames[ind],fieldends[ind]-fieldstarts[ind]))
        self.fields = tuple(fields)
        self.cr = cr


################################################### Top level functions
def getDataStreamer(config_options=None):
        """Set reader with user configuration options.
    
        Arguments:
    
            config_options (list of XML objects, defined in xmlbase):
                Either an empty list, or the XML element
                <DataInput>...</DataInput> which contains
                the source location for the PMML model.
    
            scoring_function (function with one argument):
                The function to be named as the callback for the
                Reader that will be created.  It should be the
                pmmlConsumer instance's member function 'score'.

        Exceptions raised:

            If a file is named for opening and that file cannot
            be opened, IOError will be raised.
        """
        logger = logging.getLogger()
        if not config_options:
            logger.error("No DataInput configuration options.")
            return None
        else:
            config_options = config_options
        if config_options.exists(config.Interactive):
            # Interactive: there is a calling program that will send data.
            logger.debug("Interactive mode; no DataStreamer created.")
            return DataStreamer(interactive=True)
        # ``settings`` will contain all of the configuration options
        # for the HTTPInterfaceServer or the Reader.
        settings = {}
        # Gather configuration options.
        fromHTTP = config_options.exists(config.FromHTTP)
        runForever = not config_options.exists(config.ReadOnce)
        # Save the reader with callback
        if fromHTTP:
            # HTTP: input is sent as XML over HTTP.
            child = config_options.child(config.FromHTTP)
            settings["port"] = int(child["port"])
            settings["url"] = child["url"]
            isXML = True
            #xsd: this is the only choice
        else:
            # From Files: all of the options for reading from a file.
            settings["ffConvert"] = None
            child = config_options.child(lambda x: x.tag.startswith("From"))

            isXML = child.attrib.get("type", "XML") == "XML"
            #xsd: The default type is XML.
            filename = child.attrib.get("name", '-')
            #xsd: The default source is standard in, represented by '-' here.
            settings["framing"] = child.attrib.get("framing", "EOF")
            #xsd: The default framing is the whole file, otherwise integer bytes.
            settings["header"] = child.attrib.get("header", None)
            settings["sep"] = child.attrib.get("sep", None)
            settings["types"] = child.attrib.get("types", None)

            if config_options.exists(config.FromCSVFile):
                isXML = False
            else:
                if config_options.exists(config.FromFixedRecordFile):
                    isXML = False
                    settings["framing"] = None
                    # Get the fixed record file names and record width.
                    #xsd: the record name and length are required attributes
                    ffCR = child.attrib.get("cr", None)
                    ffnames = [field["name"] for field in child]
                    lengths = [int(field["length"]) for field in child]
                    ffends = [sum(lengths[:i+1]) for i in xrange(len(lengths))]
                    ffstarts = [0] + ffends[:-1]
                    settings["ffConvert"] = FFConfig(
                        ffnames, ffstarts, ffends, cr=ffCR)
        settings.update({
            'logger':logging.getLogger(), 'magicheader':False, 'autoattr':True})

        return DataStreamer (
            fromHTTP=fromHTTP,
            isXML=isXML,
            runForever=runForever,
            filename=filename,
            **settings)
