"""Support for input/output of a UniTable

"""

__copyright__ = """
Copyright (C) 2005-2008  Open Data ("Open Data" refers to
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


import os
import sys
import csv
from augustus.external import numpy as na
from augustus.external.numpy import char as nastr
from augustus.external.numpy import rec as narec
from augustus.external.numpy import ma as ma
import itertools as it
from cStringIO import StringIO

from asarray import asarray, import_asarray, get_format, export_string, \
                    pack_binary_mask, unpack_binary_mask, is_char_array

import logging
logging.basicConfig()
log = logging.getLogger('unitable')
#log.setLevel(1)

########################################################################

class TableMethods(object):
  """functions for various presentations and input/output of table data.

  While typically used with a UniTable, may also apply to anything supporting
  dict-like interface.  These are all class methods -- functions in a common
  namespace.

  This may be used as a mixin for any class with a dict-like interface
  supporting: keys(), values(), items().  The keys must be text strings and
  the values must be lists or vectors.
  """

  @classmethod
  def _process_file(self,filename,mode,func,*args,**kwargs):
    '''wrapper for universal file I/O calls'''
    needs_close = False
    if isinstance(filename,str):
      fd = file(filename,mode)
      needs_close = True
    elif filename in ('-',None):
      if 'r' in mode:
        fd = sys.stdin
      else:
        fd = sys.stdout
    else:
      fd = filename
    out = func(fd,*args,**kwargs)
    if needs_close: fd.close()
    return out

  @classmethod
  def _to_str(self,func,*args,**kwargs):
    '''wrapper for file output to string'''
    fd = StringIO()
    func(fd,*args,**kwargs)
    out = fd.getvalue()[:-1]
    fd.close()
    return out

  @classmethod
  def _from_str(self,data,func,*args,**kwargs):
    '''wrapper for file input from string'''
    fd = StringIO(data)
    out = func(fd,*args,**kwargs)
    fd.close()
    return out

  #######################################################
  # Autodetect input format

  def from_any_file(self,filename,**kwargs):
    '''read autodetected format from file'''
    return self._process_file(filename,'rb',self._from_any,**kwargs)
  fromfile = from_any_file

  def from_any_str(self,data):
    '''read autodetected format from string'''
    return self._from_str(data,self._from_any,**kwargs)

  def _from_any(self,fd,**kwargs):
    header = kwargs.get('header') or None
    if header is None:
      try:
        header = fd.readline()
      except KeyboardInterrupt:
        sys.exit(1)
    format = None
    try:
      out = self._from_nab(fd,header,**kwargs)
      format = 'bin'
    except TypeError:
      try:
        out = self._from_csv(fd,header,**kwargs)
        format = 'csv'
      except:
        raise
    return out

  def set_original_format(self,format): self._original_format = format
  def get_original_format(self): return getattr(self,'_original_format',None)

  #######################################################
  # binary I/O

  def to_nab_file(self,filename):
    '''write numnp binary file

    This is essentially a standard numpy record file with a text header
    prepended to identify the field names and types.
    '''
    return self._process_file(filename,'wb',self._to_nab)
  tofile = to_nab_file

  def to_nab_str(self):
    '''return numpy binary string'''
    return self._to_str(self._to_nab)

  def _to_nab(self,fd):
    masktype = []
    values = []
    for (m,v) in [pack_binary_mask(value) for value in self.values()]:
      if m is None: m = ''
      masktype.append(m)
      values.append(v)
    if masktype.count('') == len(masktype):
      masktype = None
    names = ','.join(self.keys())
    formats = ','.join(self.get_type_codes(values))
    self._write_nab_file(fd=fd,values=values,formats=formats,names=names,masktype=masktype)

  def _write_nab_file(self,fd,values,formats,names,masktype=None):
    bfh = BinaryFileHeader(formats=formats,names=names,masktype=masktype)
    fd.write(bfh.make_header()+'\n')
    if not len(values):
      return
    try:
      tmpdata = narec.array(values,formats=formats,names=names)
    except MemoryError:
      log.warning('got MemoryError, trying alternate write method')
      tmpdata = None
    except self.BufferError:
      log.info('reached memmap limit on system, using alternate write method')
      tmpdata = None
    if tmpdata is not None:
      tmpdata.tofile(fd)
      del tmpdata
      return
    # write using small groups to conserve memory 
    total = len(values[0])
    grpsize = max(1,min(1024*1024,total/16))
    for i in range(0,total,grpsize):
      valslice = [arr[i:i+grpsize] for arr in values]
      tmpdata = narec.array(valslice,formats=formats,names=names)
      tmpdata.tofile(fd)
      del tmpdata

  def get_type_codes(self,arrs=None):
    if arrs is None:
      arrs = self.values()
    return [get_format(arr) for arr in arrs]

  def from_nab_file(self,filename):
    '''read numpy binary file'''
    return self._process_file(filename,'rb',self._from_nab)

  def from_nab_str(self,data):
    '''read numpy binary string'''
    return self._from_str(data,self._from_nab)

  def _from_nab(self,fd,header=None,**kwargs):
    if header is None:
      header = fd.readline()
    bfh = BinaryFileHeader(header)
    args = {}
    for arg in ('names','formats'):
      args[arg] = bfh.get(arg)
    keys = bfh.get('names',split=1)
    self.set_original_format('bin')

    # read entire file
    #print 'Reading from NAB file'
    #print narec
    #print narec.fromfile
    #print char
    #print chararray
    #x=input()
    try:
      accum = narec.fromfile(fd,**args)
    except MemoryError:
      log.warning('got MemoryError, trying alternate read method')
      accum = None
    except self.BufferError:
      log.info('reached memmap limit on system, using alternate read method')
      accum = None
    except IOError:
      log.warning('got IOError, trying alternate read method')
      accum = None
      # reposition pointer, it could be anywhere
      fd.seek(len(header),0)
    if accum is None:
      # read using small groups to avoid buffer limit
      tmpdata = narec.fromfile(fd,shape=1,**args)
      accum = self._new_hook()
      for key in keys:
        accum[key] = tmpdata.field(key)
      recsize = sum(values.itemsize() for values in accum.values())
      bytes = os.path.getsize(fd.name) - fd.tell()
      assert bytes % recsize == 0, \
        'file header inconsistent with file size (%s bytes remain)' % (bytes % recsize)
      records = bytes/recsize
      while records:
        chunk = min(records,0x10000000/recsize)
        records -= chunk
        tmpdata = narec.fromfile(fd,shape=chunk,**args)
        tmptbl = self._new_hook()
        for key in keys:
          tmptbl[key] = tmpdata.field(key)
        accum.extend(tmptbl)

    # all data has now been read, handle any masked data encoding
    masktype = bfh.get('masktype',split=1)
    if not masktype:
      masktype = ['']*len(keys)
    for key,mask in zip(keys,masktype):
      if mask not in (None,''):
        self[key] = unpack_masked_array(accum.field(key))
      else:
        self[key] = accum.field(key).copy()
    return self


  #######################################################
  # CSV I/O

  class _default_csv_dialect(csv.Dialect):
    delimiter = '\t'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL

  def set_csv_dialect(self,dialect,**kwargs):
    self._csv_dialect = dialect
    for key,value in kwargs.items():
      if value is not None:
        setattr(self._csv_dialect,key,value)

  def get_csv_dialect(self):
    try:
      return self._csv_dialect
    except AttributeError:
      return self._default_csv_dialect


  def to_csv_file(self,filename,sep=None,with_headers=True,with_data=True):
    '''write CSV to file'''
    return self._process_file(filename,'wb',self._to_csv,sep=sep,with_headers=with_headers,with_data=with_data)

  def to_csv_str(self,sep=None,with_headers=True,with_data=True):
    '''write CSV to string'''
    return self._to_str(self._to_csv,sep=sep,with_headers=with_headers,with_data=with_data)

  def _to_csv(self,fd,sep=None,with_headers=True,with_data=True):
    dialect = self.get_csv_dialect()
    csv_args = {}
    if sep is not None:
      csv_args['delimiter'] = sep
    writer = csv.writer(fd,dialect=dialect,**csv_args)
    if with_headers:
      writer.writerow(self.keys())
    if with_data:
      values = [export_string(value) for value in self.values()]
      # Write an empty file if everything is None
      if [None]*len(self.values()) == values:
          writer.writerow('')
      else:
          lentbl = max([len(col) for col in values])
          for rownum in range(lentbl):
            out = [str(col[rownum]) for col in values]
            writer.writerow(out)


  def from_csv_file(self,filename,header=None,insep=None,**kwargs):
    '''read CSV from file'''
    return self._process_file(filename,'rb',self._from_csv,header=header,insep=insep,**kwargs)

  def from_csv_str(self,data,header=None,insep=None,**kwargs):
    '''read CSV from string'''
    return self._from_str(data,self._from_csv,header=header,insep=insep,**kwargs)

  def _from_csv(self, fd, header=None, **kwargs):
    if header is None:
      header = fd.readline()
    if not header:
      log.error('no input data')
      sys.exit(1)
    self.set_original_format('csv')
    insep = kwargs.get('insep')
    bufferFramed = kwargs.get('bufferFramed') or None
    dialect = csv.Sniffer().sniff(header,delimiters=insep)
    if insep and len(insep) == 1:
      # sniffer keeps making wrong choices esp with '|' char as delimiter
      # force correct choice if just one option
      if dialect.delimiter != insep:
        log.debug('Sniffer chose %r as delimiter, forcing %r',dialect.delimiter,insep)
        dialect.delimiter = insep
    if dialect.delimiter == header.rstrip():
      dialect.delimiter = chr(0)
    self.set_csv_dialect(dialect)
    keys = csv.reader(StringIO(header),dialect=dialect).next()
    self.setkeys(keys)
    # buffer holds individual values delivered to a unitable at
    # conclusion of reading from source and at discrete
    # intervals triggered by a fixed 'size'.
    trigger = kwargs.get('chunksize',10000000)
    # make it a nice round number, force it to be in units
    # of the number of items in one record, and put a limit on
    # how few records get read in before flushing the buffer.
    nRecordsMin = 2 # probably could safely set this to 1000 or so.
    trigger = len(keys)*max(1,(trigger/len(keys))/nRecordsMin)*nRecordsMin
    linecnt = 1
    expect_rowlen = len(keys)
    buffer = []
    if kwargs.get('broken_csv'):
      sep = dialect.delimiter
      for line in fd.readlines():
        row = line.rstrip('\r\n').split(sep)
        linecnt += 1
        if len(row) != expect_rowlen:
          row = self._csv_badrow(row,linecnt,expect_rowlen)
          if not row:
            continue
        buffer.extend(row)
        if len(buffer) >= trigger:
          self._consume_input_data(buffer,keys,is_final=False,**kwargs)
    else:
      getrows = csv.reader(fd,dialect=dialect)
      for row in getrows:
        linecnt += 1
        if len(row) != expect_rowlen:
          row = self._csv_badrow(row,linecnt,expect_rowlen)
          if not row:
            continue
        buffer.extend(row)
        if len(buffer) >= trigger:
          if not bufferFramed:
            self._consume_input_data(buffer,keys,is_final=False,**kwargs)
          else:
            self._consume_input_data(buffer,keys,is_final=True,**kwargs)
            return self 
    self._consume_input_data(buffer,keys,is_final=True,**kwargs)
    return self

  def _csv_badrow(self,row,linecnt,expect_rowlen):
    '''report bad CSV data, return fixed row or None to skip this row'''
    if not len(row) or (len(row) == 1 and row[0].strip() == ''):
      # ignore empty line
      return None
    msg = 'CSV line %s has %s fields (expected %s)' % (linecnt,len(row),expect_rowlen)
    delta = expect_rowlen - len(row)
    if delta < 0:
      log.error('%s: removing %s extra fields',msg,-delta)
      return row[:expect_rowlen]
    log.error('%s: padding %s missing fields',msg,delta)
    missing = [''] * delta
    return row + missing

  #######################################################
  # Fixed width I/O

  def from_fixed_width_file(self,filename,fields,**kwargs):
    return self._process_file(filename,'rb',self._from_fixed_width,fields,**kwargs)

  def _from_fixed_width(self,fd,*args,**kwargs):
    fields = args[0]
    keys = []
    widths = []
    for (key,value) in fields:
      keys.append(key)
      widths.append(value)

    self.setkeys(keys)

    trigger = kwargs.get('chunksize',10000000)
    # make it a nice round number
    trigger = len(keys)*max(1,(trigger/len(keys))/1000)*1000

    cr = kwargs.get('cr','true')
    
    buffer = []

    if(cr == 'true'):
      for line in fd.readlines():
        self._parse_line(line,widths,buffer )
        if len(buffer) >= trigger:
          self._consume_input_data(buffer,keys,is_final=False,**kwargs)
      self._consume_input_data(buffer,keys,is_final=True,**kwargs)
    else:
      rec_len = 0
      for(width) in widths:
        rec_len += width
      while True:
        line = fd.read(rec_len)
        if(line == "" or line == "\n" ):
          break
        self._parse_line(line,widths,buffer )
        if len(buffer) >= trigger:
            self._consume_input_data(buffer,keys,is_final=False,**kwargs)
      self._consume_input_data(buffer,keys,is_final=True,**kwargs)
      
    return self

  def _parse_line( self, line, widths, buffer ):
    offset = 0
    for(width) in widths:
      buffer.append(line[offset:offset+width])
      offset += width
  
  #######################################################
  # Process input data and insert into UniTable
  
  def _consume_input_data(self,buffer,keys,is_final,**kwargs):
    if not len(self):
      target = self
    else:
      target = self._new_hook()
    log.debug('consuming batch of %s lines',len(buffer)/len(keys))
    for i,key in enumerate(keys):
      log.debug('consuming key (%s/%s): %s',i+1,len(keys),key)
      target[key] = buffer[i::len(keys)]
    if target is not self:
      log.debug('appending batch')
      self.extend(target)
      del target
    buffer[:] = []
    if not is_final:
      log.info('read progress :: %8d lines',len(self))
    else:
      log.info('finished reading %8d lines',len(self))
      types = kwargs.get('types')
      if types:
        delim = kwargs.get('types_delim')
        if delim is None:
          delim = self._csv_dialect.delimiter
        typedic=dict(zip(self.keys(),types.split(delim)))
      for key,value in self.items():
        if types:
          nvalue = import_asarray(value,typedic[key]=='string')
        else:
          nvalue = import_asarray(value)
        if value is not nvalue:
          self[key] = nvalue
          msg = 'compacted'
        else:
          msg = 'unchanged'
        log.info('finished processing field (%s): %s',msg,key)
      log.info('finished compacting table')
      
  #######################################################
  # Pretty-print table methods

  def to_pptbl_file(self,filename,sep='|',xsep='+',xfill='-',method=str,text='right',rhead=0):
    '''write pretty-printed table to file'''
    return self._process_file(filename,'wb',self._to_pptbl,sep,xsep,xfill,method,text,rhead)

  def to_pptbl_str(self,sep='|',xsep='+',xfill='-',method=str,text='right',rhead=0):
    '''write pretty-printed table to string'''
    return self._to_str(self._to_pptbl,sep,xsep,xfill,method,text,rhead)

  def _to_pptbl(self,fd,sep='|',xsep='+',xfill='-',method=str,text='right',rhead=0):
    for line in self._iter_pptbl(sep=sep,xsep=xsep,xfill=xfill,method=method,text=text,rhead=rhead):
      fd.write(line+'\n')

  def _iter_pptbl(self,sep='|',xsep='+',xfill='-',method=str,text='right',rhead=0):
    sizes = []
    values = [export_string(value) for value in self.values()]
    for (name,col) in zip(self.keys(),values):
      #print name, type(values), repr(values)
      ################################################
      # this is a lookahead on entire dataset to find max
      # print size for each field - try to short circuit where possible
      namesize = len(method(name))
      fldsize = None
      try:
        if is_char_array(col):
          fldsize = col.maxLen()
        elif col is None:
          fldsize = len(method(None))
        else:
          natype = get_format(col)
          is_float = natype.startswith('Float')
          if not is_float:
            nasize = col.itemsize()
            if nasize*3 <= namesize:
              fldsize = namesize
            else:
              minmax = min(col),max(col)
              fldsize = max(len(prtfld) for prtfld in it.imap(method,minmax))
      except:
        pass

      if fldsize is None:
        # no shortcut found, convert entire column
        try:
          fldsize = max(len(prtfld) for prtfld in it.imap(method,col))
        except:
          fldsize = 0
      sizes.append(max(namesize,fldsize))

    xbar = xsep + xsep.join([w*xfill for w in sizes]) + xsep
    out = [name.center(w) for (w,name) in zip(sizes,self.keys())]
    headline = sep + sep.join(out) + sep

    if text == 'left':
      for i,col in enumerate(values):
        try:
          if is_char_array(col):
            sizes[i] = -sizes[i]
        except:
          pass
    formats = ['%%%ss' % s for s in sizes]

    cols = []
    for col in values:
      if col is None:
        col = [col]*len(self)
      cols.append(col)

    # finally, yield the result

    if not rhead:
      # if not repeating header, yield it first
      yield xbar
      yield headline
      yield xbar

    for rownum in range(len(self)):
      if rhead and rownum % rhead == 0:
        # repeat header at specified interval
        yield xbar
        yield headline
        yield xbar
      out = [(fmt % method(col[rownum])) for (fmt,col) in zip(formats,cols)]
      yield sep + sep.join(out) + sep
    yield xbar

  #######################################################
  # HTML output table methods

  def to_html_file(self,filename,tblattr=None,method=str):
    '''write HTML table to file'''
    return self._process_file(filename,'wb',self._to_html,tblattr,method)

  def to_html_str(self,tblattr=None,method=str):
    '''write HTML table to string'''
    return self._to_str(self._to_html,tblattr,method)

  def _to_html(self,fd,tblattr=None,method=str):
    tree = self._to_html_elementtree(tblattr,method)
    tree.write(fd)

  def _to_html_elementtree(self,tblattr=None,method=str):
    if tblattr is None:
      tblattr = {'border':'1'}
    from augustus.external.etree import Element, SubElement, ElementTree
    out = Element('table',**tblattr)
    out.text = out.tail = '\n'
    headings = SubElement(out,'tr')
    headings.tail = '\n'
    for key in self.keys():
      heading = SubElement(headings,'th')
      heading.text=method(key)
    values = [export_string(value) for value in self.values()]
    cols = []
    for col in values:
      if col is None:
        col = [col]*len(self)
      cols.append(col)
    for rownum in range(len(self)):
      datarow = SubElement(out,'tr')
      datarow.tail = '\n'
      for col in cols:
        datacell = SubElement(datarow,'td')
        datacell.text = method(col[rownum])
    return ElementTree(out)
      
  #######################################################
  # XML output methods

  def to_xml_file(self,filename,**kwargs):
    '''write XML table to file'''
    return self._process_file(filename,'wb',self._to_xml,**kwargs)

  def to_xml_str(self,**kwargs):
    '''write XML table to string'''
    return self._to_str(self._to_xml,**kwargs)

  def _to_xml(self,fd,**kwargs):
    tree = self._to_xml_elementtree(**kwargs)
    tree.write(fd)

  def _to_xml_elementtree(self,**kwargs):
    cfg = {
      'tbl_element':  'table',
      'row_element':  'row',
    }
    cfg.update(kwargs)
    from augustus.external.etree import Element, SubElement, ElementTree
    out = Element(cfg['tbl_element'])
    out.text = out.tail = '\n'
    values = [export_string(value) for value in self.values()]
    cols = []
    for col in values:
      if col is None:
        col = [col]*len(self)
      cols.append(col)
    row_element = cfg['row_element']
    keys = self.keys()
    for rownum in range(len(self)):
      datarow = SubElement(out,row_element)
      datarow.tail = '\n'
      for key,col in zip(keys,cols):
        datacell = SubElement(datarow,key)
        datacell.text = str(col[rownum])
    return ElementTree(out)
      
########################################################################
########################################################################

class BinaryFileHeader(object):
  """helper class to encode/decode header for binary storage

  >>> h = BinaryFileHeader('RecArray names=a,b formats=Int8,a3 junk=foobar')
  >>> print h
  RecArray formats=Int8,a3 names=a,b junk=foobar
  >>> print h.get('names',split=1)
  ['a', 'b']
  >>> BinaryFileHeader('RecArray names formats=Int8,a3 junk')
  Traceback (most recent call last):
  ...
  TypeError: invalid header format: 'RecArray names formats=Int8,a3 junk'

    
  """
  _magic = 'RecArray'
  _required = ('formats','names')
  _optional = ('masktype',)
  _fldsep = ','

  def __init__(self,header=None,**kwargs):
    self.data = {}
    if header is not None:
      self.from_header(header)
    for key,value in kwargs.items():
      self.set(key,value)

  def __str__(self): return self.make_header()
  def make_header(self):
    data = self.data.copy()
    out = [self._magic]
    for key in self._required:
      out.append('%s=%s' % (key,data.pop(key)))
    for key in self._optional:
      value = data.pop(key,None)
      if value is not None:
        out.append('%s=%s' % (key,value))
    # pass-thru unknowns
    for key in sorted(data.keys()):
      out.append('%s=%s' % (key,data.pop(key)))
    return ' '.join(out)

  def get(self,key,split=False):
    rawval = self.data.get(key)
    if not split or not rawval:
      return rawval
    return rawval.split(self._fldsep)

  def set(self,key,value):
    if value is None:
      if self.data.has_key(key):
        del self.data[key]
      return
    if isinstance(value,(tuple,list)):
      value = self._fldsep.join(value)
    self.data[key] = value

  def from_header(self,header):
    parts = header.rstrip().split()
    if not parts or parts[0] != self._magic:
      raise TypeError, 'unrecognized header: %r' % header
    self.data = data = {}
    try:
      for part in parts[1:]:
        key,val = part.split('=')
        data[key] = val
    except:
      raise TypeError, 'invalid header format: %r' % header

#################################################################

class SymbolIndex(object):
  """facilitate reading huge files by replacing select text fields
  with a numeric index into a symbol table which is stored separately.
  """
  def __init__(self,filename=None,keys=[]):
    self.keys = keys
    self.data = data = {'':0}  # try to pre-assign empty string value
    self.filename = filename
    if filename and os.path.exists(filename):
      from augustus.kernel.unitable import UniTable
      tbl = UniTable().fromfile(filename)
      for i,value in it.izip(tbl['index'],tbl['data']):
        data[value] = i
      del tbl

  def flush(self):
    if self.filename and len(self.data) > 1:
      from augustus.kernel.unitable import UniTable
      tbl = UniTable(keys=['index','data'])
      tmp = self.data.items()
      tbl['index'] = [x[1] for x in tmp]
      tbl['data'] = [x[0] for x in tmp]
      del tmp
      tbl.sort_on('index')
      tbl.to_csv_file(self.filename)
      del tbl
        
      

#################################################################

if __name__ == "__main__":
  import doctest
  flags =  doctest.NORMALIZE_WHITESPACE
  flags |= doctest.ELLIPSIS
  flags |= doctest.REPORT_ONLY_FIRST_FAILURE
  doctest.testmod(optionflags=flags)

