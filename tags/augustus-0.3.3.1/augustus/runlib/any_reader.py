"""\
This module implements robust, fault-tolerant reading
of an XML event stream.
"""

__copyright__ = """
Copyright (C) 2005-2009  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""

import sys, os, time, logging, traceback, datetime
from pprint import pformat
from augustus.kernel.unitable import UniTable

__all__ = ( 'Reader', 'FileReader',
            'WrapperElement', 'TruncateElement', 'NativeElement',
)


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
  def __init__(self,callback,classmap=None,source=None,logger=None,openwait=5,magicheader=True,unitable=False,autoattr=True,header=None,sep=None,types=None,ffConvert=None):
    self.callback = callback
    self.classmap = classmap or {'*':NativeElement}
    self.source = source or sys.stdin
    self.log = logger or logging.getLogger()
    self.openwait = openwait
    self.magicheader = magicheader
    self.unitable=unitable
    self.autoattr=autoattr
    self.header=header
    self.sep=sep
    self.types=types
    self.done_on_eof = True
    self.ffConvert = ffConvert

  def read_forever(self,timelimit=None):
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
    while True:
      if timelimit and datetime.datetime.now() >= timelimit:
        return
      try:
        self.feed_pipe(None,pipe)
      except ReaderContinue:
        continue
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
    if timelimit and not isinstance(timelimit,datetime.datetime):
      if isinstance(timelimit,datetime.timedelta):
        timelimit = datetime.datetime.now() + timelimit
      else:
        timelimit = datetime.datetime.now() + datetime.timedelta(seconds=timelimit)
    pipe = self.new_pipe()
    if timelimit and datetime.datetime.now() >= timelimit:
        return
    try:
        self.feed_pipe(None,pipe)
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
    return
  
  def feed_pipe(self,data,pipe):
    head = pipe[0]
    tail = pipe[1:]
    try:
      stage = head.__class__.__name__
      return_val = head.feed(data)
    except StandardError, err:
      raise ReaderException('event failed: %s: %s' % (stage,repr(data)),exc_info=True)
    # last stage of pipe is consumer, never has results
    for result in head:
      return_val = self.feed_pipe(result,tail)
    return return_val

  def new_pipe(self):
    src = self.open_source()
    if self.magicheader:
      Framer = MagicHeaderFramer
    elif (self.unitable or self.ffConvert):
      Framer = UniTableFramer
    else:
      Framer = SingleFileFramer
      self.done_on_eof = True
    framer = Framer(src,log=self.log,done_on_eof=self.done_on_eof,header=self.header,sep=self.sep,types=self.types,ffConvert=self.ffConvert)
    if self.unitable:
      loader=UniTableLoader(log=self.log)
      mapper = UniClassMapper(classmap=self.classmap,log=self.log)
    else:
      loader = FrameLoader(log=self.log,autoattr = self.autoattr)
      mapper = ClassMapper(classmap=self.classmap,log=self.log)
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
    if type(self.source) != type(''):
      return self.source
    if self.source == '-':
      return sys.stdin
    while True:
      try:
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
        if (data.data):
          #Store what's in data in a new variable
          uni = data
          #And now we can re-use the variable named data.
          #This let's the exception handling below output which row we failed on.
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
    """given a UniTable, instantiate according to classmap"""
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
    self.data=data

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
import struct, math

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
      if (text_size > max_size):
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

    if (stream_err):
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
  def __init__(self,handle,log,done_on_eof=False,header=None,sep=None,types=None,ffConvert=None):
    PipeStage.__init__(self,log)
    self.handle = handle
    self.header = header
    self.sep = sep
    self.types = types
    self.ffConvert = ffConvert
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
    try:
      if self.header is None:
        if self.sep is None:
          #No special treatment needed
          if ((len(_csvargs) ==0) and (self.ffConvert is None)):
            return UniTable().fromfile(self.handle)
          elif (self.ffConvert is not None):  
            # Jonathan's clean solution:
            fields = self.ffConvert.fields
            return UniTable().from_fixed_width_file(self.handle, fields)          
          else:
            return UniTable().from_csv_file(self.handle,**_csvargs)
        else:
          return UniTable().from_csv_file(self.handle, insep = self.sep, **_csvargs)
      else:
        if self.sep is None:
          return UniTable().from_csv_file(self.handle, header = self.header, **_csvargs)
        else:
          return UniTable().from_csv_file(self.handle, header = self.header, insep = self.sep, **_csvargs)
    except:
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
  >>> src = StringIO(xml_str)
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

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
