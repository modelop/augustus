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

import logging
from any_reader import Reader, FileReader
from any_reader import NativeElement, WrapperElement, TruncateElement
from StringIO import StringIO
from pprint import pformat
from augustus.version import __version__

# dummy classes for testing
class Z:
  def __init__(self):
    self.name = None
    self.attr = {}
    self.children = []
  def __repr__(self):
    klass = self.__class__.__name__
    k = self.attr.keys()
    k.sort()
    out = ['%s=%s' % (str(k),repr(self.attr[k])) for k in k]
    out = ','.join(out)
    out = '%s:%s(%s)' % (klass,self.name,out)
    if len(self.children):
      sep = '\n  '
      out2 = [sep.join(repr(x).split('\n')) for x in self.children]
      out2 = sep.join(out2)
      out = '%s\n[%s%s\n]' % (out,sep,out2)
    return out

class X(Z):
  def __init__(self,name,attr,children):
    self.name = name
    self.attr = attr
    self.children = children

class Y(Z):
  def __init__(self,name):
    self.name = name
    self.attr = {}
    self.children = []
    self._pool = []
  def update(self,key,value):
    self._pool.append((key,value))
  def finalize(self):
    for (key,value) in self._pool:
      if type(value) == type(''):
        self.attr[key] = value
      else:
        self.children.append(value)

class A(X): pass
class B(Y): pass
class C(X): pass
class D(Y): pass
class T(X): pass

classmap = {
  'mission':        A,
  'request':        B,
  'report':         C,
  'lob':            D,
  'TimeStamp':      T,
  'EVENT':          WrapperElement,
#  '*':              X,
}

#################################################################

class ReaderTester:
  def __init__(self,source,classmap=classmap,logger=None,magicheader=True,limit=None,timelimit=None):
    self.reader = Reader(callback=self.handle_event,classmap=classmap,source=source,logger=logger,magicheader=magicheader)
    self.limit = limit
    self.cnt = 0
    self.reader.read_forever(timelimit=timelimit)
    print '***** done'

  def handle_event(self,event):
    self.cnt += 1
    print '*****', event
    if self.limit is not None and self.cnt >= self.limit:
      raise StopIteration, 'reached max iterations: %s' % self.limit

class FileReaderTester:
  def __init__(self,source):
    self.freader = FileReader(source=source)
    for obj in self.read():
      print obj

  def read(self):
    return self.freader.read()

####################################

class DataDictionary:
  def __init__(self,name,attr,children):
    self.datatypes = {}
    for child in children:
      self.datatypes[child.attr.get('name')] = child.attr.get('dataType')
  def __repr__(self):
    return pformat(self.datatypes)

class PMMLTester:
  classmap = {
    'DataDictionary' : DataDictionary,
    'DataField' : NativeElement,
    'PMML' : WrapperElement,
    '*' : TruncateElement,
  }
  def __init__(self,source):
    self.freader = FileReader(classmap=self.classmap,source=source)
    for obj in self.read():
      print obj

  def read(self):
    return self.freader.read()

#################################################################

# selftest input
xml_str = """<event>
<TimeStamp>20050403020100</TimeStamp>
<TimeStamp/>
<TimeStamp value='20050403020100'/>
<TimeStamp><TimeStamp/></TimeStamp>
<TimeStamp></TimeStamp>
</event>
"""


#################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] [datafiles]'
  version = "%prog " + __version__

  option_list = [
    make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),
    make_option('-i','--input',default='event.fifo',help="input source (default event.fifo)"),
    make_option('-f','--filereader',action='store_true',help="use filereading mode"),
    make_option('-p','--pmml',action='store_true',help="read PMML file"),
    make_option('-s','--fiforeader',action='store_true',help="use fiforeading mode"),
    make_option('-t','--timelimit',metavar='SECS',help="limit fifo read time to SECS"),
  ]

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  loglevel = logging.ERROR - opt.verbose*10
  logging.getLogger().setLevel(loglevel)
  logger = logging
  logger.info('initializing default logger')

  if opt.timelimit:
    opt.timelimit = int(opt.timelimit)

  if not args: args = [opt.input]

  for arg in args:
    if opt.filereader:
      tester = FileReaderTester(source=arg)
    elif opt.pmml:
      tester = PMMLTester(source=arg)
    elif opt.fiforeader:
      tester = ReaderTester(source=arg,logger=logger,limit=5,timelimit=opt.timelimit)
    else:
      src = StringIO(str(xml_str))
      tester = ReaderTester(source=src,classmap=None,magicheader=False,limit=0)

if __name__ == "__main__":
    main()

