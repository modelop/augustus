"""These are wrappers to "vectorize" arbitrary python functions,
  making them compatible with UFuncs in most cases.

  >>> import operator as op
  >>> testargs = (0,1,asarray(2),asarray([3]),[4,5])
  >>> f = UFuncWrapper(op.add)
  >>> for a in testargs:
  ...   for b in testargs:
  ...     x = f(a,b)
  ...     y = na.add(a,b)
  ...     print a, '+', b, '==', x
  ...     assert na.all(na.equal(x,y))
  0 + 0 == 0
  0 + 1 == 1
  0 + 2 == 2
  0 + [3] == [3]
  0 + [4, 5] == [4 5]
  1 + 0 == 1
  1 + 1 == 2
  1 + 2 == 3
  1 + [3] == [4]
  1 + [4, 5] == [5 6]
  2 + 0 == 2
  2 + 1 == 3
  2 + 2 == 4
  2 + [3] == [5]
  2 + [4, 5] == [6 7]
  [3] + 0 == [3]
  [3] + 1 == [4]
  [3] + 2 == [5]
  [3] + [3] == [6]
  [3] + [4, 5] == [7 8]
  [4, 5] + 0 == [4 5]
  [4, 5] + 1 == [5 6]
  [4, 5] + 2 == [6 7]
  [4, 5] + [3] == [7 8]
  [4, 5] + [4, 5] == [ 8 10]

  >>> testargs = ('a','ab',asarray(['b']),asarray(('a','cd')))
  >>> f = make_ufunc(str.count)
  >>> for a in testargs:
  ...   for b in testargs:
  ...     x = f(a,b)
  ...     print a, 'COUNT', b, '==', x
  a COUNT a == 1
  a COUNT ab == 0
  a COUNT ['b'] == [0]
  a COUNT ['a' 'cd'] == [1 0]
  ab COUNT a == 1
  ab COUNT ab == 1
  ab COUNT ['b'] == [1]
  ab COUNT ['a' 'cd'] == [1 0]
  ['b'] COUNT a == [0]
  ['b'] COUNT ab == [0]
  ['b'] COUNT ['b'] == [1]
  ['b'] COUNT ['a' 'cd'] == [0 0]
  ['a' 'cd'] COUNT a == [1 0]
  ['a' 'cd'] COUNT ab == [0 0]
  ['a' 'cd'] COUNT ['b'] == [0 0]
  ['a' 'cd'] COUNT ['a' 'cd'] == [1 1]

  >>> @as_ufunc
  ... def func(arg1,arg2): return 100.0*arg1/(arg1+arg2)
  >>> print func(2,3), func([2,3,12],3)
  40.0 [ 40.  50.  80.]

  >>> @as_ufunc(arity=2)
  ... def func(arg1,arg2): return 100.0*arg1/(arg1+arg2)
  >>> print func(2,3), func([2,3,12],3)
  40.0 [ 40.  50.  80.]

  >>> class Func(object):
  ...  def __call__(self,arg1,arg2):
  ...   return 100.0*arg1/(arg1+arg2)
  >>> func = as_ufunc(Func())
  >>> print func(2,3), func([2,3,12],3)
  40.0 [ 40.  50.  80.]



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


import inspect
import itertools as it
import numpy as na
from numpy import char as nastr
from asarray import asarray, is_scalar, all_scalar

__all__ = ('make_ufunc','as_ufunc')

#################################################################

def make_ufunc(func,arity=None):
  '''wrap a python function to simulate a numpy UFunc'''
  return UFuncWrapper(func,arity=arity)

#################################################################

def as_ufunc(*args,**kwargs):
  '''decorator to apply make_ufunc'''
  if not len(kwargs) and len(args) == 1 and callable(args[0]):
    return make_ufunc(args[0])
  def deco(func):
    return make_ufunc(func,*args,**kwargs)
  return deco

#################################################################

class UFuncWrapper(object):
  def __init__(self,func,arity=None):
    if arity is None:
      arity = self._get_arity(func)
    self.arity = arity
    self._func = func
    try:
      self.func_name = func.__name__
    except:
      # FIXME
      self.func_name = func.__class__.__name__

  def __call__(self,*args):
    if all_scalar(args):
      # pure scalars just pass thru
      return self._func(*args)

    args = [asarray(arg) for arg in args]
    maxrank = max(arg.rank for arg in args)
    maxsize = max(na.size(arg) for arg in args)

    if maxsize == 1:
      # single call is sufficient
      args = [na.ravel(arg)[0] for arg in args]
      out = self._func(*args)
      if maxrank > 0:
        out = [out]
    else:
      # must iterate over args
      argiters = [self._argiter(arg) for arg in args]
      out = list(it.imap(self._func,*argiters))

    out = asarray(out)
    assert na.size(out) == maxsize, 'internal error'
    return out

  def _argiter(self,arg):
    """return appropriate fast iterable for arg"""
    if is_scalar(arg):
      return it.repeat(arg)
    if arg.rank == 0 or na.size(arg) == 1:
      return it.repeat(arg.flat[0])
    return iter(arg)

  @staticmethod
  def _get_arity(func):
    """determine length of mandatory args to function"""
    arity = 0
    if not inspect.isfunction(func):
      if inspect.ismethod(func):
        func = func.im_func
        arity = -1
      else:
        return None
    (args,varargs,varkw,defaults) = inspect.getargspec(func)
    if varargs is not None:
      return None
    arity += len(args)
    if defaults is not None:
      arity -= len(defaults)
    return arity

#################################################################
#################################################################

if __name__ == "__main__":
  import doctest
  flags =  doctest.NORMALIZE_WHITESPACE
  flags |= doctest.ELLIPSIS
  flags |= doctest.REPORT_ONLY_FIRST_FAILURE
  doctest.testmod(optionflags=flags)

#################################################################
