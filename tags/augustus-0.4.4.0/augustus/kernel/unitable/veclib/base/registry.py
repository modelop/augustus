
__copyright__ = """
Copyright (C) 2005-2007  Open Data ("Open Data" refers to
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


import inspect
import os
from pprint import pformat
from equiv import Equiv

########################################################################

class Registry(object):
  '''make Equiv group library from imported globals'''
  def __init__(self,globals=None):
    if globals is None: globals = __builtin__.globals()
    self.groups = {}
    self.equivs = {}
    for obj in globals.values():
      self.add_search(obj)

  def keys(self): return sorted(self.groups.keys())
  def items(self): return [(name,self[name]) for name in self.keys()]
  def publish(self): return dict(self.items())

  def get(self,key,default=None):
    try:
      return self[key]
    except KeyError:
      return default

  def __getitem__(self,key):
    group = self.groups[key]
    return group.export


  def add_search(self,obj):
    '''search object namespace for groups'''
    if inspect.isclass(obj):
      if issubclass(obj,Equiv) and len(obj._candidates()):
        self.add_group(obj)
    elif inspect.ismodule(obj):
      for name,subobj in inspect.getmembers(obj,inspect.isclass):
        self.add_search(subobj)

  def add_group(self,klass):
    '''add a group (Equiv class) to the registry'''
    group = klass()
    name = group._name()
    assert not self.groups.has_key(name), \
      'redefinition of %s: %r, previous: %r' % (name,inst,self.equivs[name])
    self.groups[name] = group
    
  def __repr__(self):
    return (os.linesep).join([repr(self.groups[name]) for name in self.keys()])

########################################################################
########################################################################

class Platform(object):
  """ Describes hardware platform and desired operational profile.
    
    This is the standard option container used to select appropriate
    function groups for a given machine and task.
  """
  def __init__(self,**kwargs):
    self._data = self.platform()
    self._data.update(kwargs)

  def platform(self):
    import sys
    import platform
    out = {}
    for name in ('maxint','byteorder'):
      out[name] = getattr(sys,name)
    for name in ('architecture','machine','platform','processor','release','system',
                  'uname','node','dist','libc_ver'):
      out[name] = getattr(platform,name)()
    return out

  def __str__(self):
    return pformat(self._data)


if __name__ == '__main__':
  print Platform()

