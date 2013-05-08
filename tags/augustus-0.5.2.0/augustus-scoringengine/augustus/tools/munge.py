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

"""Mangle a data stream for testing purposes
"""

import sys
import random
import time
from optparse import make_option
from mainopts import MainOpts,mainclass

#################################################################

class Munge(MainOpts):
  option_list = [
    make_option('-b','--binary',action='store_true',help="insert some binary data"),
    make_option('-d','--delete',action='store_true',help="delete some data"),
    make_option('-r','--replicate',action='store_true',help="replicate some data"),
    make_option('-p','--pauses',action='store_true',help="insert pauses (flush+sleep)"),
    make_option('-a','--all',action='store_true',help="use all munging techniques (default)"),
    make_option('-o','--out',action='store',default=sys.stdout,metavar="FILE",help="output destination (default stdout)"),
    make_option('-f','--factor',action='store',type='float',default=1.0,metavar="N",
      help="more (N>1.0) or less (N<1.0) aggressive munge rate"),
    make_option('-s','--seed',action='store',type='int',metavar="N",help="set seed value for reproducibility (0=random)"),
    make_option('-x','--repeat',action='store',type='int',default=1,metavar="N",
      help="loop over input N times (0=forever) (not with stdin!)"),
    make_option('-z','--gunzip',action='store_true',help='unzip input file(s) while reading (not with stdin)'),
  ]
  def __init__(self,sources,opt=None):
    MainOpts.__init__(self,opt)
    opt = self.opt
    if not (opt.binary or opt.delete or opt.replicate or opt.pauses):
      opt.all = True
    self.sources = sources
    if opt.seed:
      random.seed(opt.seed)
    if type(opt.out) == type(''):
      self.out = open(opt.out,'wb')
    else:
      self.out = opt.out
    cnt = 0
    while opt.repeat == 0 or cnt < opt.repeat:
      cnt += 1
      if self.opt.verbose and cnt > 1:
        print >>sys.stderr, '**************** starting repetition %s ****************' % cnt
      self.run()
    self.close()

  def run(self):
    if not self.opt.gunzip:
      import fileinput
      lineiter = fileinput.input(self.sources)
    else:
      lineiter = self._lineiter()

    buffer = ''
    buffsize = random.randrange(256,256*20)
    for line in lineiter:
      buffer = buffer + line
      if len(buffer) > buffsize:
        self.write(self.munge(buffer))
        buffer = ''
        buffsize = random.randrange(256,256*20)
    self.write(buffer)

  def _lineiter(self):
    import gzip
    for f in self.sources:
      fd = gzip.open(f,'rb')
      while True:
        line = fd.readline()
        if not line: break
        yield line
      fd.close()

  def close(self): return self.out.close()
  def flush(self): return self.out.flush()

  def maybe(self,optname,percent=10):
    if not getattr(self.opt,optname) and not self.opt.all:
      return False
    thresh = percent*self.opt.factor
    return random.uniform(0,100) < thresh

  def maybe_pause(self):
    if not self.maybe('pauses',10):
      return
    self.flush()
    n = min(random.expovariate(100),3.0)
    if self.opt.verbose > 1:
      print >>sys.stderr, 'sleeping %s seconds' % n
    time.sleep(n)

  def write(self,data):
    more = data[:]
    while len(more):
      if len(more) < 3:
        n = len(more)
      else:
        n = random.randrange(1,len(data))
        n = max(n,len(more))
      if self.opt.verbose > 2:
        print >>sys.stderr, 'writing %s bytes' % n
      self.out.write(more[:n])
      more = more[n:]
      self.maybe_pause()

  def munge(self,data):
    meth = (self.munge_delete,self.munge_replicate,self.munge_binary)
    how = random.choice(meth)
    return how(data)

  def munge_delete(self,data):
    if self.maybe('delete',5):
      i = random.randrange(1,len(data))
      j = random.randrange(i,len(data))
      data = data[:i] + data[j:]
      if self.opt.verbose:
        print >>sys.stderr, 'deleting %s bytes' % str(j-i)
    return data

  def munge_replicate(self,data):
    if self.maybe('replicate',3):
      i = random.randrange(1,len(data))
      j = random.randrange(i,len(data))
      k = random.randrange(0,len(data))
      data = data[:k] + data[i:j] + data[k:]
      if self.opt.verbose:
        print >>sys.stderr, 'replicating %s bytes' % str(j-i)
    return data

  def munge_binary(self,data):
    if self.maybe('binary',1):
      k = random.randrange(0,len(data))
      junk = self.some_binary()
      data = data[:k] + junk + data[k:]
      if self.opt.verbose:
        print >>sys.stderr, 'inserting %s bytes of binary junk' % len(junk)
    return data

  def some_binary(self):
    out = []
    for x in range(int(random.randrange(0,10*self.opt.factor))):
      out.append(chr(int(random.randrange(0,256))))
    return ''.join(out)


def main():
  return mainclass(Munge)


if __name__ == "__main__":
    main()
