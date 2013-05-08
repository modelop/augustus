
"""Generic record writer

This is intended to provide a uniform interface for writing
formatted records to one or more destinations.  Each destination
may have its own formatting and interface requirements.

Destinations currently supported are "XML stream".
Other destination types may include SQL DB, logging records, and
an instant messaging interface.
"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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


import sys, struct, logging
from elementtree.ElementTree import Element, tostring

################################################################

class AnyWriter(object):
  """Collection of Writer objects to be invoked

  >>> map1 = ('ALERT',('FOO','apple'),('BAR','pear'))
  >>> map2 = ('WARN',('FRUIT',('APPLE','apple'),('PEAR','pear'),('ORANGE','orange')))

  >>> writer = AnyWriter()
  >>> writer.append(XMLWriter(map1))
  >>> writer.append(XMLWriter(map2))
  >>> print writer.format({'orange':2.34,'banana':1.23,'pear':9.99})
  <ALERT><FOO /><BAR>9.99</BAR></ALERT>
  <WARN><FRUIT><APPLE /><PEAR>9.99</PEAR><ORANGE>2.34</ORANGE></FRUIT></WARN>

  """

  def __init__(self,*args):
    self._writers = list(args)

  def append(self,arg):
    self._writers.append(arg)

  def format(self,obj):
    # this is probably only useful for the internal doctest run
    out = []
    for writer in self._writers:
      out.append(writer.format(obj))
    return '\n'.join(out)

  def write(self,obj):
    """Write a record using all registered Writers.
    The input to this call is a mapping object which
    will be formatted by all Writers as part of the writing
    process.
    """
    for writer in self._writers:
      writer.write(obj)

################################################################

class BaseWriter(object):
  """Base class for text writers

  Handles dest of type filename, file handle, logging call.
  Using default parameters, each record is written with a terminating
  newline.
  
  For applications that use, for example, a binary magic header
  followed by message size, and with no terminating newline,
  useful parameters may be:
    head = "\xF3\xFA\xE1\xE4"
    sizecode = '!i'
    tail = ''
  """
  def __init__(self,dest=None,logger=None,head='',sizecode='',tail='\n'):
    self.head = head
    self.sizecode = sizecode
    self.tail = tail
    self.logger = logger or logging.getLogger()
    self._fd = None
    if hasattr(dest,'write'):
      self._fd = dest
      self._write = dest.write
    elif callable(dest):
      self._write = dest
    elif dest is None:
      self._fd = sys.stdout
      self._write = self._fd.write
    else:
      self._fd = file(dest,'wb')
      self._write = self._fd.write

  def format(self,obj):
    # meant to be overridden
    return str(obj)

  def write(self,obj):
    try:
      body = self.format(obj)
    except:
      self.logger.error('unable to format: %r',obj)
      return
    if self.sizecode:
      bodysize = struct.pack(self.sizecode,len(body))
    else:
      bodysize = ''
    try:
      self._write(self.head+bodysize+body+self.tail)
      if self._fd:
        self._fd.flush()
    except:
      self.logger.error('unable to write: %r',body)
      return
    return body
    
################################################################

class XMLWriter(BaseWriter):
  """
  >>> defaults = {'apple':'green','pear':'bruised'}
  >>> elemap = ('ALERT',
  ...     ('FOO','apple'),
  ...     ('BAR','pear'),
  ...     ('RANGE',
  ...       ['LOWER','ground'],
  ...       ('UPPER','sky'),
  ...     ),
  ...   )
  >>> writer = XMLWriter(elemap,defaults)

  >>> print writer.format({})
  <ALERT><FOO>green</FOO><BAR>bruised</BAR><RANGE><LOWER /><UPPER /></RANGE></ALERT>

  >>> print writer.format({'apple':'red','ground':0.0,'sky':99999.99})
  <ALERT><FOO>red</FOO><BAR>bruised</BAR><RANGE><LOWER>0.0</LOWER><UPPER>99999.99</UPPER></RANGE></ALERT>


  """
  def __init__(self,elemap,defaults={},dest=None,logger=None,**kwargs):
    BaseWriter.__init__(self,dest=dest,logger=logger,**kwargs)
    self._elemap = elemap
    self._defaults = defaults

  def format(self,obj):
    root = self._format(self._elemap,obj)
    return tostring(root)

  def _format(self,elemap,obj):
    defaults = self._defaults
    out = Element(elemap[0])
    for subspec in elemap[1:]:
      if type(subspec) == type(''):
        tmp = obj.get(subspec,None)
        if tmp is None or str(tmp) == '':
          tmp = defaults.get(subspec,'')
        out.text = str(tmp)
        continue
      # here, subspec is really a recursive element map
      subelem = self._format(subspec,obj)
      out.append(subelem)
    return out
    

################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
