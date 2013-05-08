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


"""Convenience functions for collecting options specs
  and providing a main() function"""

import optparse
from optparse import make_option
import os
from augustus.version import __version__

#################################################################

class OptionWrapper(object):
  """generic option container with both attr and dict interface
  
  >>> class MyOpt(OptionWrapper): m='myopt'
  >>> opt = MyOpt(a=1,b='qwerty')
  >>> print opt.a, opt.b, opt['a'], opt.m, opt['m']
  1 qwerty 1 myopt myopt
  >>> print dict(opt)
  {'a': 1, 'b': 'qwerty', 'm': 'myopt'}
  >>> print list(opt)
  [('a', 1), ('b', 'qwerty'), ('m', 'myopt')]

  """
  def __init__(self,*args,**kwargs):
    for other in args+(kwargs,):
      if isinstance(other,optparse.Values):
        other = other.__dict__
      self.__dict__.update(other)
  def __getitem__(self,key):
    return getattr(self,key)
  def __iter__(self):
    return iter((key,getattr(self,key)) for key in dir(self) if not key[:1] == '_')
  def __repr__(self):
    return str(dict(self))


#################################################################

class MainOpts(object):
  """mix-in class to help automate handling of options
    
    The class should have an option_list attribute containing
    Options suitable for use with optparse.
    All classes derived from this are candidates for
    harvest_options(), and then may be set from the command line
    when the default main() is used.

    This can also be used independent of Main(), just as a way
    of verbosely assigning options with defaults to class objects.
  """
  option_list = []
  def __init__(self,opt=None):
    if opt is None:
      # set default option values
      opt = OptionParser(self.option_list)
    self.opt = opt

#################################################################

def harvest_options(where=None):
  """find all option groups starting from a class or module"""
  search = []
  if where is None:
    search = globals().values()
  else:
    search.append(where)
    try:
      search.extend(vars(where).values())
    except AttributeError:
      pass
  out = []
  for thing in search:
    try:
      if issubclass(thing,MainOpts):
        out.append(thing.option_list)
    except TypeError:
      pass
  return out

#################################################################

stdopts = [
  make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
  make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
]

#################################################################

def mainparser(harvest=None,with_stdopts=True,**kwargs):
  """setup option parser"""
  from optparse import OptionParser

  kwargs.setdefault('usage','usage: %prog [options] [file(s)]')
  kwargs.setdefault('version','%prog ' + __version__)
  parser = OptionParser(**kwargs)
  parser.set_conflict_handler('resolve')
      
  if with_stdopts:
    parser.add_options(stdopts)

  if harvest is not None:
    for optlist in harvest_options(harvest):
      parser.add_options(optlist)

  return parser

#################################################################

def mainopts(harvest=None,args=None,with_stdopts=True,**kwargs):
  """setup and call option parser"""

  parser = mainparser(harvest=harvest,with_stdopts=with_stdopts,**kwargs)
  (opt,args) = parser.parse_args(args=args)
  return (opt,args)

#################################################################

class Main(object):
  """persistent container for top level program opts and args"""

  def __init__(self,opt=None,args=None):
    self.opt = opt
    self.args = args

  def __call__(self,klass=None,args=None):
    (opt,args) = mainopts(klass,args=args)
    self.__init__(opt,args)
    if klass is None:
      return None
    return klass(args,opt)

main = Main()

#################################################################

def mainclass(klass,args=None):
  """handle user command when run as top level program"""
  (opt,args) = mainopts(klass,args=args)
  return klass(args,opt)

#################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

  from warnings import filterwarnings
  filterwarnings('ignore',category=FutureWarning)

  # simple sanity test
  class Sample(MainOpts):
    option_list = [
      make_option('-v','--version',action='store_true',help="show version"),
      make_option('-s','--quiet',action='store_true',dest='verbose',help="be silent"),
      make_option('-i','--int',action='store',type='int',help="integer here"),
      make_option('-q','--quit',action='store_true',help="quit now"),
    ]
    def __init__(self,args,opt):
      MainOpts.__init__(self,opt)
      self.args = args
    def __str__(self):
      out = []
      out.append('opts: %s' % str(self.opt))
      out.append('args: %s' % str(self.args))
      return (os.linesep).join(out)
    
  out = main(klass=Sample)
  print out


#################################################################
#################################################################
