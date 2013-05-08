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

