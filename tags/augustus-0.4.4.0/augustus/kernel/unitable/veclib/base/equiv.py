
""" Container for group of functions that are functionally equivalent.

  This facilitates checking correctness and performance comparisons.

  The 'itypes' attr is used for generating tests. By default all input
  arg types are assumed to allow any of (binary,int,float).
  Example argtypes:
    '?if'       : all parameters may be binary, int, or float.
    'i'         : only ints are allowed for any parameter
    ('?if','?') : first param is anything, second must be binary

  Normally, the output type(s) are determined by standard numerical
  type promotion, but sometimes the type is known apriori, and
  sometimes it contains non-primitive types and must be considered
  an 'object' vector.  Also, in the case of reduce methods, the output
  may always be a scalar.

  The 'samelen' flag indicates whether the in/out sizes are always the
  same (thus making the function result suitable in a unitable).
  The output size may be smaller for things like index arrays, unique
  value lists, reduce functions, etc.  In that case the output is
  useful as an intermediate value, but not as a unitable destination.

  It's recommended that each Equiv group have at least one 'naive'
  implementation.  That serves as documentation and as a correctness
  check against highly optimized functions that may be harder to
  understand.

####################################
    
nin = None          # number of input parameters
nout = None         # number of outputs
itypes = None       # expected input types (for generating tests)
otypes = None       # expected output types
samelen = None      # is output len same as input len?

ranking = None      # if present, orders func names from best to worst


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
import re
import os
from corelib import corelib
from tester import Test, Tester

########################################################################

class Equiv(object):
  """ Container for group of functions that are functionally equivalent.

  An Equiv group is created by defining functions in a subclass of this.
  All functions must accept the same argument types/counts and will be
  verified for consistency of results.

  The following attributes may be assigned to override the defaults:

  @name		the canonical name of the function (default is to convert
                class name as in 'DoThisNow' -> 'do_this_now')

  @ranking      ordered list of function names indicating generic selection
                preference (default is alphabetical list of all functions in
                class not starting with char '_')
 

  ####################################
      
  nin = None          # number of input parameters
  nout = None         # number of outputs
  itypes = None       # expected input types (for generating tests)
  otypes = None       # expected output types
  samelen = None      # is output len same as input len?

  ranking = None      # if present, orders func names from best to worst

  bench_sizes = None  # override sizes of random data for benchmarking

  ####################################

  _prep_testdata()    # may be overridden to preprocess test data to
                      # match intended operating environment


  """

  bench_sizes = (1,2,3,4,5,10,20,50,100,200,300,500,1000,5000,10000,50000,100000,500000,1000000)


  def __getattr__(self,key):
    #print 'getattr(%s)' % key
    if key[0] == '_':
      raise AttributeError, 'no attribute %r or %r' % (key[1:],key)
    genattr = getattr(self,'_'+key)
    out = genattr()
    setattr(self,key,out)
    return out

  def _group(self): return self.__class__.__name__
  def _name(self): return re.sub(r'([^A-Z_])([A-Z])',r'\1_\2',self.group).lower()
  def _itypes(self): return None

  def _verify(self): return Tester(self).verify
  def _benchmark(self): return Tester(self).benchmark
  def _tests(self): return ()

  def _check_result(self): return self._nullfunc_

  def _default(self): return self.ranking[0]
  def _ranking(self): return self._candidates()

  def _prep_testdata(self,*args,**kwargs): return (args,kwargs)

  @classmethod
  def _candidates(cls):
    getmembers = inspect.getmembers
    filter = inspect.isroutine
    return tuple(name for (name,func) in getmembers(cls,filter) if name[0] != '_')

  def _rawfuncs(self): return [getattr(self,name) for name in self.ranking]
  def _call(self): return getattr(self,self.default,self._nullfunc_)
  def __call__(self,*args,**kwargs): return self.call(*args,**kwargs)

  @staticmethod
  def _nullfunc_(*args,**kwargs):
    '''simulate identity function'''
    if len(args) == 1:
      return args[0]
    return args

  def _export(self): return self._decorate_(self.call)
  def _decorate_(self,func):
    try:
      # cannot do this for instancemethods
      func.__name__ = self.name
      func.__doc__ = ( os.linesep ).join([x for x in (func.__doc__,self.__doc__) if x])
    except:
      pass
    return func

  def __repr__(self):
    out = ['%-16s : %r' % (self.name,repr(self.__class__))]
    for (name,func) in zip(self.ranking,self.rawfuncs):
      out.append('%r' % func)
    return (os.linesep + ' ').join(out)


########################################################################
# Subclasses for the most common cases

class EquivUnary(Equiv):
  """Typical unary function that takes any numeric input"""
  nin,nout = 1,1

class EquivBinary(Equiv):
  """Typical binary function that takes any numeric input"""
  nin,nout = 2,1

class EquivMask(Equiv):
  """Binary function that requires a binary mask for second arg"""
  nin,nout = 2,1
  argtypes = (None,'?')

class EquivIndex(Equiv):
  """Binary function that requires an integer index for second arg"""
  nin,nout = 2,1
  argtypes = (None,'i')

########################################################################

########################################################################
########################################################################

if __name__ == '__main__':
  import itertools as it

  x = Test([1,2,3],[3,4,5],foo='bar') == [9,9,9]
  print x


  class Addition(EquivBinary):
    tests = (
      Test([1,2,3],[0,10,100]),
    )

    @staticmethod
    def infix(x,y): return x+y

    @staticmethod
    def gen_izip(x,y): return [(xx+yy) for (xx,yy) in it.izip(x,y)]

    @staticmethod
    def naive_zip(x,y):
      out = []
      for (xx,yy) in zip(x,y):
        out.append(xx+yy)
      return out

    @staticmethod
    def naive(x,y):
      out = []
      for i in range(len(x)):
        out.append(x[i]+y[i])
      return out

  a = Addition()
  print dir(a)
  print '-'*77
  print a
  b = a([1],[2])
  print a.__call__
  print 'a([1],[2]) ==', b

  print a.benchmark()
  #print help(a)


########################################################################
