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

"""
This module implements robust, fault-tolerant reading of an XML event stream.
"""

import sys
import os
import time
import logging
import traceback
import datetime
from pprint import pformat
from augustus.kernel.unitable import UniTable

__all__ = ( 'Reader', 'FileReader',
            'WrapperElement', 'TruncateElement', 'NativeElement',
)

CHUNKSIZE = 1000000

#################################################################

class ReaderException(Exception):
  """Base exception for internal module use

  These are intended for internal signaling with this module
  only.  It is an internal error for this exception to be
  raised and not caught here."""
  def __init__(self,msg=None,exc_info=False):
    self._msg = msg
    self._exception = None
    if exc_info:
      self._exception = sys.exc_info()
    Exception.__init__(self,msg)

  def __str__(self):
    out = [str(self._msg)]
    if self._exception:
      out += traceback.format_exception(*self._exception)
    return '\n'.join(out)

class ReaderExit(ReaderException):
  """Exception used to terminate"""


class ReaderContinue(ReaderException):
  """Exception used to return control to higher level

  This is used after an error has already been logged at
  a lower level and should not be logged again."""
  def __init__(self): pass

class ReaderDone(ReaderException):
  """Exception used to signal normal end of input."""
  def __init__(self): pass

class Reader(object):
  """top level event reader
  
  reads an input source, parses well-formed XML, instantiates application
  objects using a supplied classmap, and calls a registered callback
  function for each valid incoming event.
  the intent is to log all problems without raising any exceptions
  to the application."""
  def __init__(self, callback, classmap=None, source=None, logger=None, openwait=5, magicheader=True, unitable=False, autoattr=True, header=None, sep=None, types=None, ffConvert=None, framing=None, wholeUniTable=False):
    """Set internal values from the argument keyword list.

      Arguments:

        callback (function with a single required argument):
        The function that will process each UniTable, UniRecords, or
        NativeElement (a lightweight representation of XML objects)
        that the Reader reads.

        source (string, unicode string, StringIO object, or stream handle):
        The input source.

        logger (Logger object)

        openwait (number):
        The time in seconds to wait if a stream handle is not open, to
        allow for a delay in opening.

        magicheader (boolean):
        If True, the MagicHeaderFramer will be used to frame the data.
        This identifies the beginning of a new block of information using
        the special string MagicHeaderBegin.

        unitable (boolean):
        If True, data input from the stream will be handled using
        the unitable module's reading functions.  Otherwise XML reading
        functions will be used.

        autoattr (boolean):
        If True, simple XML elements read using the XML reader will
        automatically be converted to attributes:

            <Outer> <Inner1>A</Inner1><Inner2>B</Inner2> </Outer>
            becomes:
            <Outer Inner1="A" Inner2="B"/>

        header (string):
        The header for a CSV file.  The file must have no header string.

        sep (string):
        The separator, or a list of candidate separators in descending
        order of preference.

        types (string):
        A separator-separated list of type names for the CSV file.
        Currently this argument is discarded downstream.

        ffConvert (an ffConfig object; defined in pmmllib/pmmlConsumer
        and copied to datastreams/datastreamer)
        Contains the specifications for a fixed record file.

        framing (integer or "EOF"):
        Either a number of bytes; the amount of a CSV file to read at a
        time; or the string "EOF", meaning the whole file will be read
        at once if possible.

        wholeUniTable (boolean):
        If True, use WholeUnitableCallbackHandler to handle the Reader's
        callback function.  This gives the callback the entire UniTable
        rather than iterating over UniRecords.
      """
    self.callback = callback
    self.classmap = classmap or {'*':NativeElement}
    self.source = source or sys.stdin
    self.log = logger or logging.getLogger()
    self.openwait = openwait
    self.magicheader = magicheader
    self.unitable = unitable
    self.autoattr = autoattr
    self.header = header
    self.sep = sep
    self.types = types
    self.done_on_eof = True
    self.ffConvert = ffConvert
    self.metadata = None
    self.framing = framing
    self.wholeUniTable = wholeUniTable

  def read_forever(self, timelimit=None):
    """This is the main event loop for streaming input

    The timelimit parameter is mainly intended for testing,
    and will cause an early exit after timelimit has passed
    (but it does not interrupt the process -- the condition
    is only checked after each full event is read).
    It may contain an int (seconds from now), a timedelta
    object (relative to now), or a datetime object (an
    absolute stop time).
    """
    if timelimit and not isinstance(timelimit,datetime.datetime):
      if isinstance(timelimit,datetime.timedelta):
        timelimit = datetime.datetime.now() + timelimit
      else:
        timelimit = datetime.datetime.now() + datetime.timedelta(seconds=timelimit)
    pipe = self.new_pipe()
    if self.metadata is not None:
      pipecounts = len(pipe)*[0]
      starttime = datetime.datetime.now()    
      feedtimes = len(pipe)*[starttime]
    else:
      pipecounts = feedtimes = None
    while True:
      if timelimit and datetime.datetime.now() >= timelimit:
        return
      try:
        self.feed_pipe(None,pipe,pipecounts,feedtimes)
      except ReaderContinue:
        continue
      except ReaderDone:
        if self.metadata is not None:
          stages=['Frames','Loads','Mappings','Callbacks']
          pipelist = ['%s : %s'%p for p in zip(stages, [str(cnt) for cnt in pipecounts])]
          self.metadata = self.metadata + pipelist
          self.metadata.append('Data Access : '+str(feedtimes[-1] - starttime))
        return
      except ReaderExit, err:
        self.log.critical(str(err))
        sys.exit()
      except ReaderException, err:
        self.log.error(str(err))
      except KeyboardInterrupt:
        raise
      except StandardError:
        self.log.exception('internal error')

  def read_once(self,timelimit=None):
    """This function is almost a direct copy of read_forever except that it doesn't have a while true loop.

    The timelimit parameter is mainly intended for testing,
    and will cause an early exit after timelimit has passed
    (but it does not interrupt the process -- the condition
    is only checked after each full event is read).
    It may contain an int (seconds from now), a timedelta
    object (relative to now), or a datetime object (an
    absolute stop time).
    """
    if timelimit and not isinstance(timelimit, datetime.datetime):
      if isinstance(timelimit, datetime.timedelta):
        timelimit = datetime.datetime.now() + timelimit
      else:
        timelimit = datetime.datetime.now() + datetime.timedelta(seconds=timelimit)
    pipe = self.new_pipe()
    if self.metadata is not None:
      pipecounts = len(pipe)*[0]
      starttime = datetime.datetime.now()    
      feedtimes = len(pipe)*[starttime]
    else:
      pipecounts = feedtimes = None
    if timelimit and datetime.datetime.now() >= timelimit:
        return
    try:
        self.feed_pipe(None, pipe, pipecounts, feedtimes)
    except ReaderContinue:
        pass
    except ReaderDone:
        return
    except ReaderExit, err:
        self.log.critical(str(err))
        sys.exit()
    except ReaderException, err:
        self.log.error(str(err))
    except KeyboardInterrupt:
        raise
    except StandardError:
        self.log.exception('internal error')
    if self.metadata is not None:
      stages=['Frames','Loads','Mappings','Callbacks']
      pipelist = ['%s : %s'%p for p in zip(stages, [str(cnt) for cnt in pipecounts])]
      self.metadata = self.metadata + pipelist
      self.metadata.append('Data Access : '+str(feedtimes[-1] - starttime))
    return
  
  def feed_pipe(self, data, pipe, pipecounts=None, feedtimes=None):
    head = pipe[0]
    tail = pipe[1:]
    try:
      stage = head.__class__.__name__ # just info used in case of exception.
      return_val = head.feed(data)
    except StandardError, err:
      raise ReaderException('event failed: %s: %s' % (stage,repr(data)),exc_info=True)
    # last stage of pipe is consumer, never has results
    # note that head is a PipeStage that has an iterator but is not an
    # extension of a list or such.
    if pipecounts:
      pipecounts[len(pipecounts)-len(tail)-1] += len(head.queue)
    if len(head.queue)>0:
      if feedtimes:
        feedtimes.append(datetime.datetime.now())
    for result in head:
      return_val = self.feed_pipe(result, tail, pipecounts, feedtimes)
    return return_val

  def new_pipe(self):
    src = self.open_source()
    if self.magicheader:
      Framer = MagicHeaderFramer
    elif self.unitable or self.ffConvert:
      Framer = UniTableFramer
      self.done_on_eof = True
    else:
      Framer = SingleFileFramer
      self.done_on_eof = True
    if self.unitable:
      framer = Framer(src,log=self.log,done_on_eof=self.done_on_eof,header=self.header,sep=self.sep,types=self.types,ffConvert=self.ffConvert,framing=self.framing)
      loader=UniTableLoader(log=self.log)
      mapper = UniClassMapper(classmap=self.classmap,log=self.log)
    else:
      framer = Framer(src,log=self.log,done_on_eof=self.done_on_eof,header=self.header,sep=self.sep,types=self.types,ffConvert=self.ffConvert)
      loader = FrameLoader(log=self.log,autoattr = self.autoattr)
      mapper = ClassMapper(classmap=self.classmap,log=self.log)
    if self.wholeUniTable:
      framer = Framer(src,log=self.log,done_on_eof=self.done_on_eof,header=self.header,sep=self.sep,types=self.types,ffConvert=self.ffConvert)
      caller = WholeUnitableCallbackHandler(callback=self.callback,log=self.log)
    else:
        caller = CallbackHandler(callback=self.callback,log=self.log)
    pipe = (framer,loader,mapper,caller)
    return pipe

  def open_source(self):
    '''return open handle to input source

      if source is not a string, assume it to be an open handle
      otherwise try to open it, allowing for delayed creation
      of an input file or named pipe.
    '''
    self.done_on_eof = True
    if not isinstance(self.source, basestring):
      return self.source
    if self.source == '-':
      return sys.stdin
    while True:
      try:
        if hasattr(self, "isCSV") and self.isCSV:
          fd = open(self.source,'rbU')
        else:
          fd = open(self.source,'rb')
        self.done_on_eof = os.path.isfile(self.source)
        return fd
      except IOError, err:
        if not self.openwait:
          self.log.error('cannot open "%s" (%s)',self.source,err)
          return None
        self.log.warning('will retry opening "%s" in %s seconds (%s)',
              self.source,self.openwait,err)
      time.sleep(self.openwait)

  def enableMetaDataCollection(self):
    self.metadata = []

  def getMetaData(self):
    return self.metadata


class FileReader(Reader):
  """a specialized Reader intended for reading whole files

    this class provides its own callback function so that an
    application can make one call, for example, to read a complete
    XML configuration file
  """
  def __init__(self,classmap=None,source=None,logger=None,magicheader=False):
    callback = self.handle_event
    Reader.__init__(self,callback=callback,classmap=classmap,source=source,logger=logger,
                          openwait=None,magicheader=magicheader)
    self.queue = []

  def handle_event(self,event):
    self.queue.append(event)

  def read(self,classmap=None,source=None):
    if classmap is not None:
      self.classmap = classmap
    if source is not None:
      self.source = source
    self.read_forever()
    out = self.queue
    self.queue = []
    return out

#################################################################

class PipeStage(object):
  def __init__(self,log):
    self.log = log
    self.queue = []

  def __iter__(self):
    """act as own iterator."""
    return self

  def next(self):
    """let caller iterate over any processed objects."""
    try:
      return self.queue.pop(0)
    except IndexError:
      raise StopIteration

#################################################################

class CallbackHandler(PipeStage):
  def __init__(self,callback,log):
    PipeStage.__init__(self,log)
    self.callback = callback

  def feed(self,data):
    """given instantiated data, execute callback"""
    try:
      if isinstance(data,UniTableHolder):
        if data.data:
          #Store what's in data in a new variable
          uni = data
          #And now we can re-use the variable named data.
          #This lets the exception handling below output which row we failed on.
          for data in uni.data:
            result = self.callback(data)
        else:
          raise StopIteration
      else:
        result = self.callback(data)
    except ReaderDone:
      raise
    except StopIteration:
      raise ReaderDone()
    except:
      raise ReaderException('failed callback for %s' % (repr(data)),exc_info=True)
    return result

#################################################################

class WholeUnitableCallbackHandler(PipeStage):
  def __init__(self,callback,log):
    PipeStage.__init__(self,log)
    self.callback = callback

  def feed(self,data):
    """given instantiated data, execute callback"""
    try:
      if data.data:
          result = self.callback(data.data)
      else:
        raise StopIteration
    except ReaderDone:
      raise
    except StopIteration:
      raise ReaderDone()
    except:
      raise ReaderException('failed callback for %s' % (repr(data)),exc_info=True)
    return result

#################################################################

class ClassMapper(PipeStage):
  def __init__(self,classmap,log):
    PipeStage.__init__(self,log)
    self.classmap = classmap
    self.defaultclass = classmap.get('*')

  def feed_elemtree(self,element):
    """for standalone use, allow feed of an ElementTree object"""
    data = self._expand_elemtree(element)
    self.feed(data)

  def _expand_elemtree(self,element):
    """convert elementtree to internal format"""
    name = element.tag
    attr = dict(element.items())
    children = []
    if element.text is not None:
      children.append(element.text)
    for child in element.getchildren():
      children.append(self._expand_elemtree(child))
    if element.tail is not None:
      children.append(element.tail)
    return (name,attr,children)

  def feed(self,data):
    """given XML parsed data, instantiate according to classmap"""
    obj = self.handle_element(data)
    self.queue.append(obj)

  def handle_element(self,obj,context=[]):
    (name,attr,children) = obj
    klass = self.lookup_class(name,context)
    if issubclass(klass,TruncateElement):
      return None
    # process children, if any
    subcontext = context + [name]
    (cnames,cobjs) = self.handle_peers(children,subcontext)
    real_obj = self.make_object(klass,name,attr,cnames,cobjs)
    return real_obj

  def handle_peers(self,children,context=[]):
    cnames = []
    cobjs = []
    for child in children:
      if type(child) == type(''):
        cnames.append(None)
        cobjs.append(child)
      else:
        (cname,cattr,cchildren) = child
        cobj = self.handle_element(child,context)
        if cobj is not None:
          cnames.append(cname)
          cobjs.append(cobj)
    return (cnames,cobjs)

  def make_object(self,klass,name,attr,cnames,cobjs):
    if issubclass(klass,WrapperElement):
      # take first non-character child element
      for obj in cobjs:
        if type(obj) != type(''):
          return obj
      try:
        return cobjs[0]
      except IndexError:
        # empty wrapper???
        pass
      return None
    try:
      if hasattr(klass,'update'):
        # method1: incremental feed
        real_obj = klass(name)
        for key,value in attr.items():
          real_obj.update(key,value)
        for key,value in zip(cnames,cobjs):
          real_obj.update(key,value)
        if hasattr(klass,'finalize'):
          real_obj.finalize()
      else:
        # method2: all passed to __init__
        real_obj = klass(name,attr,cobjs)
    except:
      raise ReaderException('failed instantiating %s%s' % (name,str(attr)),exc_info=True)
    return real_obj

  def lookup_class(self,name,context):
    """simple lookup, context ignored for now"""
    klass = self.classmap.get(name,self.defaultclass)
    if not klass:
      raise ReaderException('no classmap entry for %s in context %s' % (name,str(context)))
    return klass

#################################################################

class UniClassMapper(PipeStage):
  def __init__(self,classmap,log):
    PipeStage.__init__(self,log)
    #self.classmap = classmap
    #self.defaultclass = classmap.get('*')

  def feed(self,data):
    """No new objects are created on being fed unitables to this
       mapper, just stuff into our own queue for subsequent
       callback"""
    self.queue.append(data)

#################################################################
from xml.sax import expatreader, xmlreader, handler

class FrameLoader(PipeStage,handler.ContentHandler,handler.ErrorHandler):
  """given the text of an input frame, parse the XML into an
    intermediate structure
  """
  def __init__(self,log,autoattr=True):
    PipeStage.__init__(self,log)
    self.autoattr = autoattr  # autoconvert simple elements to attributes
    self._parser = expatreader.create_parser()
    self._parser.setContentHandler(self)
    self._parser.setErrorHandler(self)
    self.reset()

  def reset(self):
    self._parser.reset()
    self._result = None
    self._stack = None

  def feed(self,data):
    """given the text of a complete frame, parse XML into
      intermediate (name,attr={},children=[]) objects and,
      if successful, push base object onto queue."""
    self.reset()
    try:
      self._parser.feed(data)
      self._parser.close()
    except ReaderException, err:
      self.log.error('bad data frame, discarding %s bytes (%s)',str(len(data)),str(err))
      return
    for obj in self._result:
      if type(obj) == type(''):
        # ignore whitespace, report any other cruft
        if obj.strip() != '':
          self.log.warning('unexpected data in frame, discarding %s bytes: "%s"',str(len(obj)),obj)
      else:
        self.queue.append(obj)

  ####################################
  # parsing support methods

  def stack_push(self,name,attr):
    # ensure conversion from unicode
    if name:
      name = str(name)
    if attr:
      for key,val in attr.items():
        del attr[key]
        attr[str(key)] = str(val)
    element = (name,attr,[])
    self._stack.append(element)
    return element

  def stack_pop(self):
    return self._stack.pop()

  def stack_append(self,child):
    name,attr,children = self._stack[-1]
    if len(children) == 0:
      children.append(child)
      return
    # if last child is whitespace, replace it
    last = children[-1]
    if type(last) == type('') and not last.strip():
      children[-1] = child
    else:
      children.append(child)

  def stack_append_chars(self,child):
    # ensure conversion from unicode
    chars = str(child)
    name,attr,children = self._stack[-1]
    if len(children) == 0:
      children.append(chars)
      return
    # concatenate adjacent char data
    if type(children[-1]) == type(''):
      children[-1] += chars
      return
    # if mixed content do not keep whitespace   
    if chars.strip():
      children.append(chars)

  def try_autoattr(self,element):
    """try to conver child elements to attributes

      this only applies if element has no attributes
      and only to simple children that contain string data
      and only if the conversion has no attribute name conflicts
    """
    # reject if not an element
    if type(element) == type(''):
      return element
    name,attr,children = element
    # reject if already has attributes, or has no children
    if len(attr) > 0 or len(children) < 1:
      return element
    nattr = {}
    nchildren = []
    for child in children:
      # reject if mixed or character content
      if type(child) == type(''):
        return element
      cname,cattr,cchildren = child
      # reject if attribute name conflict
      if nattr.has_key(cname):
        return element
      # keep child element if complex
      if len(cattr) > 0 or len(cchildren) > 1:
        nchildren.append(child)
        continue
      # get value of child element
      if len(cchildren) == 1:
        cvalue = cchildren[0]
      else:
        cvalue = ''
      # keep child element if complex
      if type(cvalue) != type(''):
        nchildren.append(child)
        continue
      nattr[cname] = cvalue
    # if we got here, conversion is OK
    element = (name,nattr,nchildren)
    return element

  ####################################
  # ContentHandler methods
  def startDocument(self):
    #self.log.debug('startDocument')
    self._stack = []
    self.stack_push(None,None)

  def startElement(self,name,attrs):
    attrdict = dict(attrs.items())
    #self.log.debug('startElement %s %s',name,str(attrdict))
    self.stack_push(name,attrdict)

  def characters(self,content):
    #self.log.debug('characters %s',repr(content))
    self.stack_append_chars(content)

  def endElement(self,name):
    element = self.stack_pop()
    #self.log.debug('endElement %s: %s',name,pformat(element))
    if self.autoattr:
      element = self.try_autoattr(element)
      #self.log.debug('endElement(autoattr) %s: %s',name,pformat(element))
    self.stack_append(element)

  def endDocument(self):
    #self.log.debug('endDocument %s',pformat(self._stack))
    self._result = self.stack_pop()[2]
    self._stack = None

  ####################################
  # ErrorHandler methods
  def error(self,saxerr): return self._errorhandler('error',saxerr)
  def fatalError(self,saxerr): return self._errorhandler('fatal',saxerr)
  def warning(self,saxerr): return self._errorhandler('warning',saxerr)
  def _errorhandler(self,errtype,saxerr):
    #error = saxerr.getMessage()
    error = str(saxerr)
    raise ReaderException(error)

#################################################################
class UniTableLoader(FrameLoader):
  def __init__(self,log,autoattr=True):
    FrameLoader.__init__(self,log,autoattr)

  def feed(self,data):
    self.queue.append(UniTableHolder(data))

class UniTableHolder:
  def __init__(self,data):
    self.data = data

#################################################################

class SingleFileFramer(PipeStage):
  """read an entire file as a single frame

  """
  def __init__(self,handle,log,done_on_eof=True,header=None,sep=None,types=None, ffConvert=False):
    PipeStage.__init__(self,log)
    self.handle = handle
    self.done_on_eof = done_on_eof

  def read_more(self):
    if self.handle.closed:
      raise ReaderDone()
    out = self.handle.read()
    self.handle.close()
    self.queue.append(out)

  def feed(self,data):
    self.read_more()

#################################################################

_use_delegation = False
if _use_delegation:
  import xml_fifo_io2
  class MagicHeaderFramerDelegation(PipeStage):
    """identify frames from an input stream

      this class uses a MagicHeader token and the associated
      frame length, the XML content is not examined.
      this is a pass-through to the xml_fifo_io2 handler.
    """
    def __init__(self,handle,log,done_on_eof=True,header=None,sep=None,types=None):
      PipeStage.__init__(self,log)
      self.handle = handle
      self.delegate = xml_fifo_io2.receive_text
      self.done_on_eof = done_on_eof

    def read_more(self):
      """while delegating to xml_fifo_io2.receive_text,
        this should always result in a single frame read"""
      out = self.delegate(self.handle,logger=self.log)
      self.queue.append(out)

    def feed(self,data):
      self.read_more()

#################################################################
import struct
import math

class MagicHeaderFramer(PipeStage):
  """identify frames from an input stream

    this class uses a MagicHeader token and the associated
    frame length, the XML content is not examined.
    this clones code from xml_fifo_io2 with some adjustments.
  """
  MagicHeaderBegin = "\xF3\xFA\xE1\xE4"

  def __init__(self,handle,log,done_on_eof=False,header=None,sep=None,types=None, ffConvert=False):
    PipeStage.__init__(self,log)
    self.handle = handle
    self.done_on_eof = done_on_eof

  def read_more(self):
    """while delegating to xml_fifo_io2.receive_text,
      this should always result in a single frame read"""
    if self.handle.closed:
      raise ReaderExit('input source unexpectedly closed')
    out = self.receive_text()
    self.queue.append(out)

  def feed(self,data):
    self.read_more()

  ################################################
  # the following taken from xml_fifo_io.py

  def receive_text(self,max_size=65536):
    """Receive a length delimited blob of text from a file"""
    expected_size = struct.calcsize ('!i')
    good = False
    stream_err = False
    bytes_dropped = 0
    while True:
      # find the next magic header
      self.find_next_header()
      # read the size off the stream and unpack it
      size_struct = self.exact_size_read(expected_size)
      text_size, = struct.unpack('!i',size_struct)

      # if the size is too large, we're probably on an invalid header. 
      # Go back to the start and try to find the next valid header.
      if text_size > max_size:
        errstr =  "Length header (%d) " % text_size
        errstr += "exceeds the maximum length allowed "
        errstr += "(%d) by xml_fifo_io" % max_size
        self.log.error(errstr)

        errstr = "Attempting to find the start of the next message...."
        self.log.info(errstr)
        continue
      elif text_size <= 0:
        errstr =  "Got a negative or zero text size (%d) " % text_size
        errstr += "in xml_fifo_io"
        self.log.error(errstr)
        continue

      # read the block of text from the stream
      text = self.exact_size_read(text_size)

      # If we reach here, then we found a valid record, as far as we know.
      # Return it and let the reader verify the content.
      break

    if stream_err:
      errstr =  "Found next record in xml_fifo_io. "
      errstr += "Dropped %d bytes." % bytes_dropped
      self.log.error(errstr)

    return text

  def find_next_header(self):
    header = self.exact_size_read(4)
    stream_err = False
    bytes_dropped = 0
    while header != self.MagicHeaderBegin:
      if stream_err == False:
        errstr =  "Invalid header in xml_fifo_io. "
        errstr += "Got (%s) instead of (%s). " % (repr(header),repr(self.MagicHeaderBegin))
        errstr += "Skipping bytes to find the next record... "
        self.log.error(errstr)
      stream_err = True
      header = header[1:]
      while len(header) < len(self.MagicHeaderBegin):
        header += self.exact_size_read(1)
        bytes_dropped += 1

    if stream_err:
        errstr =  "Skipped %d bytes before finding a valid record." % bytes_dropped
        self.log.error(errstr)
    return True

  def exact_size_read(self,msglen):
    """read full requested length from file"""
    max_sleep = 20
    msg = ""
    zero_reads = 0
    while len(msg) != msglen:
      partial = self.handle.read(msglen - len(msg))
      if len(partial) == 0:
        if self.done_on_eof:
          raise ReaderDone()
        if zero_reads % 20 == 0:
          self.log.debug('zero read, waiting for input')
        zero_reads += 1
        sleep_time = 0.1 * (math.log(zero_reads + 1)/math.log(2))
        time.sleep (min((sleep_time,max_sleep)))
      else:
        msg = msg + partial
    return msg


#################################################################
class UniTableFramer(PipeStage):
  # Read 
  def __init__(self,handle,log,done_on_eof=False,header=None,sep=None,types=None,ffConvert=None, framing='EOF'):
    PipeStage.__init__(self,log)
    self.readThread = None
    self.handle = handle
    self.header = header
    self.sep = sep
    self.types = types
    self.ffConvert = ffConvert
    self.framing = framing
    if self.framing == 'EOF':
      self.chunksize = CHUNKSIZE    
    else:
      try:
        self.chunksize = int(self.framing)
      except:
        log.warning('Could not parse requested framing size. Using default value of %i\n'%CHUNKSIZE)
        self.chunksize = CHUNKSIZE
   #self.done_on_eof = done_on_eof

  def read_more(self):
    if self.handle.closed:
        raise ReaderExit('input source unexpectedly closed')
    out = self.receive_unitable()
    self.queue.append(out)

  def feed(self,data):
    self.read_more()

  def receive_unitable(self):
    _csvargs={}
    if self.types is not None:
      _csvargs['types'] = self.types
    if self.sep is not None:
      _csvargs['insep'] = self.sep
    if self.header is not None:
      _csvargs['header'] = self.header
    try:
      if self.header is None:
        if self.sep is None:
          #No special treatment needed
          if ((len(_csvargs) == 0) and (self.ffConvert is None)):
            u = UniTable()
            if self.framing != 'EOF':
              #New size-framed stream
              u.fromfile(self.handle, bufferFramed=True, chunksize=self.chunksize)
              d = u.get_csv_dialect()
              self.sep = d.delimiter
              self.header = self.sep.join(u.keys())
            else:
              # Traditional file-framed:
              u.fromfile(self.handle, bufferFramed=False, chunksize=self.chunksize)
            return u
          elif self.ffConvert is not None:  
            # Jonathan's clean solution:
            fields = self.ffConvert.fields
            return UniTable().from_fixed_width_file(self.handle, fields)          
          else:
            return UniTable().from_csv_file(self.handle,**_csvargs)
        else:
          u = UniTable()
          if self.framing != 'EOF':
            #New size-framed stream
            u.fromfile(self.handle, bufferFramed=True, insep = self.sep, chunksize=self.chunksize, **_csvargs)
            self.header = self.sep.join(u.keys())
          else:
            # Traditional file-framed:
            u.fromfile(self.handle, insep = self.sep, bufferFramed=False, chunksize=self.chunksize, **_csvargs)
          return u
          #return UniTable().from_csv_file(self.handle, insep = self.sep, **_csvargs)
      else:
        if self.framing != 'EOF':
          # A header exists so a prior read has been made.
          if self.sep is None:
            return UniTable().from_csv_file(self.handle, bufferFramed=True, chunksize=self.chunksize, header = self.header, **_csvargs)
          else:
            return UniTable().from_csv_file(self.handle, bufferFramed=True, chunksize=self.chunksize, header = self.header, insep = self.sep, **_csvargs)
        else:
            return UniTable().from_csv_file(self.handle, bufferFramed=False, **_csvargs)
    except Exception, inst:
      #print "Exception is: {0}".format(type(inst))
      #print inst.args
      #print inst
      return None

  #def find_next_header(self):
    #pass

  #def exact_size_read(self,msglen):
    #pass
  
#################################################################

class WrapperElement(object):
  """dummy element, just pass through first child"""

class TruncateElement(object):
  """dummy element, ignore this and all children"""

class NativeElement(list):
  """convenience class for mapping XML to native python

  each element is a native python list, with added
  'name' and 'attr' attributes, and some glue so that
  a nested structure of these can pretty-print itself"""

  def __new__(cls,name='',attr={},children=[]):
    obj = list.__new__(cls)
    return obj
  def __init__(self,name='',attr={},children=[]):
    list.__init__(self,children)
    self.name = name
    self.attr = attr
  def __repr__(self):
    keys = self.attr.keys()
    keys.sort()
    out = ['%s=%s' % (str(k),repr(self.attr[k])) for k in keys]
    out = ','.join(out)
    out = '%s(%s)' % (self.name,out)
    if len(self):
      sep1 = '\n|  '
      sep2 = '\n+--'
      out2 = [sep1.join(repr(x).split('\n')) for x in self]
      out2 = sep2.join(out2)
      out = '%s%s%s' % (out,sep2,out2)
    return out

#################################################################
#################################################################

class _doctest:
  '''
  Normal use of FileReader:

  >>> from StringIO import StringIO
  >>> xml_str = """<event>
  ... <TimeStamp>20050403020100</TimeStamp>
  ... <TimeStamp/>
  ... <TimeStamp value='20050403020100'/>
  ... <TimeStamp><TimeStamp/></TimeStamp>
  ... <TimeStamp></TimeStamp>
  ... </event>
  ... """
  >>> src = StringIO(str(xml_str))
  >>> freader = FileReader(source=src)
  >>> for obj in freader.read():
  ...   print obj
  event()
  +--TimeStamp()
  |  +--'20050403020100'
  +--TimeStamp()
  +--TimeStamp(value='20050403020100')
  +--TimeStamp(TimeStamp='')
  +--TimeStamp()

  Standalone use of ClassMapper, using ElementTree to synthesize
  the input (which may be coming from a database, etc):

  >>> from elementtree.ElementTree import Element
  >>> event = Element('event')
  >>> tmp = Element('TimeStamp')
  >>> tmp.text = '20050403020100'
  >>> event.append(tmp)
  >>> event.append(Element('TimeStamp'))
  >>> event.append(Element('TimeStamp',value='20050403020100'))
  >>> event.append(Element('TimeStamp',TimeStamp=''))
  >>> event.append(Element('TimeStamp'))
  >>> mapper = ClassMapper(classmap={'*':NativeElement},log=None)
  >>> mapper.feed_elemtree(event)
  >>> for obj in mapper:
  ...   print obj
  event()
  +--TimeStamp()
  |  +--'20050403020100'
  +--TimeStamp()
  +--TimeStamp(value='20050403020100')
  +--TimeStamp(TimeStamp='')
  +--TimeStamp()

  '''

if __name__ == "__main__":
  import doctest
  doctest.testmod()
