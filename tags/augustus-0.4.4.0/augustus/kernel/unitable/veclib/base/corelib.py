"""
CoreLib: a common namespace for system libraries
"""

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

from inspect import isfunction
from anyarray import as_any_array, import_native

_core_imports = (
  'numpy',
# JP 2011-03-01: pygsl doesn't even have an Ubuntu package, though gsl can be obtained with
#                sudo apt-get install gsl-bin libgsl0-dev
# what is it needed for?
  'pygsl',
  'pygsl.rng',
  'pygsl.const',
  'pygsl.math',
)

########################################################################

def _load_mod_attrs(obj,*args):
  """load modules into the namespace of an object

  for example, makes the following assignments to obj:
    obj._modules    # list of top-level imported modules
    obj._failed     # list of module names that failed to import

    obj.numarray    # each module, by name
    obj.pygsl       

    obj.array       # each object from the first module
    obj.asarray     # etc...
  """
  prefix = 'augustus.external.'
  obj._modules = mods = []
  failed = []
  for arg in args:
    parts = arg.split('.',1)
    modpath = prefix+parts[0]
    if len(parts) == 1:
      fromlist = ['*']
    else:
      fromlist = [parts[1]]
    try:
      mod = __import__(modpath,globals(),{},fromlist)
    except ImportError:
      failed.append(arg)
      continue
    if mod not in mods:
      mods.append(mod)
      modname = mod.__name__
      if modname.startswith(prefix):
        modname = modname[len(prefix):]
      setattr(obj,modname,mod)
  if len(mods):
    for (key,value) in vars(mods[0]).items():
      if key[:2] == '__': continue
      if isfunction(value):
        value = staticmethod(value)
      setattr(obj,key,value)
  if failed:
    obj._failed = failed
    # let caller ignore this as needed
    raise ImportError, 'module import failed for: %s' % ', '.join(failed)
  return obj

########################################################################

def _load_veclib_attrs(obj,*args,**kwargs):
  """load attribute into a class object"""
  kwargs.update((arg.__name__,arg) for arg in args)
  for name,arg in kwargs.items():
    if isfunction(arg):
      arg = staticmethod(arg)
    setattr(obj,name,arg)
  return obj

########################################################################

def _make_vecmod(*args,**kwargs):
  """experimental: make a pseudo-module instead of a class"""
  import imp
  out = imp.new_module(kwargs.pop('__name__','veclib'))
  out.__doc__ = kwargs.pop('__doc__','veclib container')
  for (key,obj) in kwargs.items():
    setattr(out,key,obj)
  load_mod_attrs(out,*args)
  return out

########################################################################

class CoreLib(object):
  def __init__(self,*args,**kwargs):
    self(*args,**kwargs)
  @classmethod
  def __call__(cls,*args,**kwargs):
    kwargs.update((arg.__name__,arg) for arg in args)
    for name,arg in kwargs.items():
      if isfunction(arg): arg = staticmethod(arg)
      setattr(cls,name,arg)

_load_mod_attrs(CoreLib,*_core_imports)

####################################

corelib = CoreLib(as_any_array,import_native)

class VecLib(CoreLib):
  pass

########################################################################

if __name__ == '__main__':
  def abc(a,b): return a+b
  veclib = VecLib(abc,abc2=abc)
  veclib.abc3 = abc
  veclib(abc4=abc)
  assert abc(1,2) == 3
  assert veclib.abc4(3,0) == 3
  assert veclib.abc2(2,1) == 3
  assert veclib.abc(1,2) == 3
  assert veclib.abc3(2,1) == 3
  assert list(veclib.as_any_array((1,'xyz'))) == ['1','xyz']
  help(veclib)

