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


import sys
import time
import inspect
import itertools as it
from augustus.kernel.unitable import UniTable
import augustus.external.numpy
getnan = lambda x: augustus.external.numpy.nonzero(augustus.external.numpy.isnan(x))
import augustus.const as AUGUSTUS_CONSTS
from corelib import corelib
as_any_array = corelib.as_any_array
narand = corelib.random
nastr = corelib.char
allclose = corelib.allclose
array_equiv = corelib.array_equiv
isnan = corelib.isnan


class Args(object):
  """A container for function call args

  >>> print Args(1,'foo',bar=None)
  Args(*args=(1, 'foo'), **kwargs={'bar': None})

  """
  def __init__(self,*args,**kwargs):
    self.args = args
    self.kwargs = kwargs
    iargs = list(inspect.getargvalues(inspect.currentframe()))[:]
    iargs[0] = iargs[0][1:]   # get rid of reference to self
    self._argfmt = inspect.formatargvalues(*iargs)
  def __str__(self): return self.arg_str('Args')
  def arg_str(self,funcname=''): return '%s%s' % (funcname,self._argfmt)
  def apply(self,func,prepfunc=None):
    if prepfunc is not None:
      out = prepfunc(*self.args,**self.kwargs)
      if isinstance(out,tuple):
        args,kwargs = out
      else:
        args,kwargs = out,self.kwargs
    else:
      args,kwargs = self.args,self.kwargs
    return func(*args,**kwargs)
  __call__ = apply

########################################################################

class Test(Args):
  """Test case specification

  >>> tests = (
  ...   Test(1,2) == 3,
  ...   Test([1],[2]) == [3],
  ...   Test(1,2) == 4,
  ... )
  >>> for t in tests:
  ...   print t
  Test(*args=(1, 2), **kwargs={}) == 3
  Test(*args=([1], [2]), **kwargs={}) == [3]
  Test(*args=(1, 2), **kwargs={}) == 4

  >>> for t in tests:
  ...   t.run(corelib.add)
  Traceback (most recent call last):
  ...
  AssertionError: test failed: <UFunc: 'add'>(*args=(1, 2), **kwargs={}) == 4 != 3

  >>> for t in tests:
  ...   t.run(corelib.subtract)
  Traceback (most recent call last):
  ...
  AssertionError: test failed: <UFunc: 'subtract'>(*args=(1, 2), **kwargs={}) == 3 != -1


  """
  def equals(self,expect):
    self._cmp = array_equiv
    self.expect = expect
    return self
  def allclose(self,expect):
    self._cmp = allclose
    self.expect = expect
    return self
  __eq__ = equals
  __pow__ = allclose

  def __str__(self): return self.test_str()
  def func_str(self,func):
    if not isinstance(func,str):
      try:
        func = func.__name__
      except:
        pass
    return self.arg_str(func)
  def test_str(self,func='Test'):
    return '%s == %r' % (self.func_str(func),self.expect)

  def run(self,func,prepfunc=None):
    result = self.apply(func,prepfunc)
    if self._cmp(result,self.expect): return
    try:
      if list(result) == list(as_any_array(self.expect)): return
    except:
      pass
    report = '%s != %r' % (self.test_str(func),result)
    assert False, 'test failed: ' + report
    

########################################################################

class Tester(object):
  """ Runs correctness and performance tests on an Equiv group.

  """

  NA = 0.0
  #NA = ieee.NAN

  def __init__(self,equiv,namesel=None):
    self.group = equiv
    testfuncs = [(func.__name__,func) for func in self.group.rawfuncs]
    if namesel:
      self.testfuncs = [(name,func) for (name,func) in testfuncs if name in namesel]
    else:
      self.testfuncs = testfuncs
      
# def Old_verify(self,size=16):
#   '''ensure that all functions in a group produce correct results'''
#   group = self.group
#   alldata = self.make_data(group.nin,group.itypes,size)
#   viewdata = UniTable()
#   for i,arg in enumerate(alldata):
#     viewdata['arg%s'%i] = arg
#   reference = None
#   for name,func in self.testfuncs:
#     timings,results = self.run_tests(func,alldata,[size])
#     for i,out in enumerate(results):
#       viewdata['%s_out%s'%(name,i)] = out
#     if reference is None:
#       reference = results
#     elif results is not None:
#       self.compare_data(reference,results,name)
#   return viewdata

  def too_long(self,secs=None,rate=None):
    if secs is not None and secs > 60:
      return True
    if rate is not None and rate < 100:
      return True
    return False

  def verify(self,size=16):
    '''run test set over all functions in a group'''
    group = self.group
    ntests = len(group.tests)
    nfuncs = len(group.rawfuncs)
    _t0 = time.time()
    for test in group.tests:
      for func in group.rawfuncs:
        test.run(func,group._prep_testdata)
    secs = time.time() - _t0
    return 'OK: ran %2d tests (%2d cases over %2d functions in %1.5f seconds)' \
      % (ntests*nfuncs,ntests,nfuncs,secs)
      

  def benchmark(self,sizes=None):
    performance = self.run_benchmark(sizes=sizes)
    keys = performance.keys()[1:]
    if not len(keys):
      return performance
    best = []
    for row in [performance[i] for i in range(len(performance))]:
      vals = row.fields(*keys)
      best_val,best_key = sorted(zip(vals,keys))[-1]
      best.append(best_key)
    performance['_best_'] = best
    for key in keys:
      tmp = performance[key]
      nanidx = getnan(tmp)[0]
      tmp = list(nastr.num2char(tmp/1000.0,'%9.2f'))
      for i in nanidx:
        tmp[i] = 'N/A'
      performance[key] = tmp
    return performance

  def benchmark_iter(self,sizes=None):
    for performance in self.run_benchmark_iter(sizes=sizes):
      keys = performance.keys()[1:]
      if not len(keys):
        yield performance
        continue
      best = []
      for row in [performance[i] for i in range(len(performance))]:
        vals = row.fields(*keys)
        best_val,best_key = sorted(zip(vals,keys))[-1]
        best.append(best_key)
      performance['_best_'] = best
      for key in keys:
        #performance[key] = nastr.num2char(performance[key]/1000.0,'%9.2f')
        tmp = performance[key]
        nanidx = getnan(tmp)[0]
        tmp = list(nastr.num2char(tmp/1000.0,'%9.2f'))
        for i in nanidx:
          tmp[i] = 'N/A'
        performance[key] = tmp
      yield performance

  def run_benchmark(self,sizes=None):
    '''return a table of runtimes for all functions for a range of data sizes'''
    group = self.group
    if sizes is None:
      sizes = group.bench_sizes
    performance = UniTable()
    performance['_n_'] = sizes
    rawdata = self.make_data(group.nin,group.itypes,max(sizes))
    out = group._prep_testdata(*rawdata)
    if isinstance(out,tuple):
      alldata,kwargs = out
    else:
      alldata,kwargs = out,{}
    reference = None
    for (name,func) in self.testfuncs:
      timings,results = self.run_tests(func,alldata,sizes,kwargs=kwargs)
      if len(timings) == len(performance):
        performance[name] = timings
      else:
        missing = len(performance)-len(timings)
        performance[name] = list(timings) + [self.NA]*missing
        results = list(results) + [None]*missing
      if reference is None:
        reference = results
      elif results is not None:
        self.compare_data(reference,results,name)
    return performance

  def run_benchmark_iter(self,sizes=None):
    '''return a table of runtimes for all functions for a range of data sizes'''
    group = self.group
    if sizes is None:
      sizes = group.bench_sizes
    rawdata = self.make_data(group.nin,group.itypes,max(sizes))
    out = group._prep_testdata(*rawdata)
    if isinstance(out,tuple):
      alldata,kwargs = out
    else:
      alldata,kwargs = out,{}
    names = [name for (name,func) in self.testfuncs]
    perfseen = UniTable(keys=['_n_']+names)
    skiplist = []
    for i,size in enumerate(sizes):
      perf = {'_n_':size}
      reference = None
      for name,func in self.testfuncs:
        if name in skiplist:
          perf[name] = self.NA
          continue
        timings,results = self.run_tests(func,alldata,[size],kwargs=kwargs)
        rate = timings[0]
        perf[name] = rate
        if self.too_long(rate=rate):
          skiplist.append(name)
        if reference is None:
          reference = results
        elif results is not None:
          self.compare_data(reference,results,name)
      perfseen.append(perf)
      yield perfseen.copy()

  def run_tests(self,func,alldata,sizes,kwargs={}):
    timings = []
    results = []
    for x,size in enumerate(sizes):
      args = []
      for data in alldata:
        a = len(sizes)-x-1
        arg = data[a:size+a].copy()
        args.append(arg.copy())
      _t0 = time.time()
      out = func(*args,**kwargs)
      _t1 = time.time()
      secs = _t1 - _t0
      rate = 1.0*size/secs
      timings.append(rate)
      results.append(out)
      if self.too_long(secs=secs,rate=rate):
        break
    return (timings,results)

  def make_data(self,nin,itypes,size):
    size = size * 2
    if itypes is None or isinstance(itypes,str):
      itypes = [itypes]
    while len(itypes) < nin:
      itypes.append(itypes[-1])
    out = []
    for i,itype in zip(range(nin),itypes):
      if itype is None or 'f' in itype:
        tmp = narand.normal(470.0,88.0,shape=size)
      elif '?' not in itype:
        tmp = narand.randint(0,size,shape=size)
      else:
        tmp = narand.randint(0,2,shape=size)
      idx = narand.randint(0,size,shape=size*2)
      tmp[idx] = 0
      out.append(tmp)
    return out

  def compare_data(self,arg1,arg2,funcname):
    if len(arg1) != len(arg2):
      msg = ['data length differs for func: %s' % funcname]
      msg.append('lens=(%s,%s)' % (len(arg1),len(arg2)))
      msg.append('types=(%s,%s)' % (type(arg1),type(arg2)))
      assert False, ', '.join(msg)
    for part1,part2 in zip(arg1,arg2):
      if part1 is None or part2 is None:
        continue
      if not allclose(part1,part2):
        idx = (part1 != part2).nonzero()
        assert not len(idx), 'output mismatch for %s, on %s elements, first,last = %s,%s' \
          % (funcname,len(idx),idx[0],idx[-1])
    return True
      

####################################

def testmod():
  """test all Equiv groups defined in the current module
  
  note: the '-i' option may be used to print intermediate results
        for long running benchmark tests.
  """
  from optparse import OptionParser, make_option
  usage = 'usage: %prog [options] [methods]'
  version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
  option_list = [
    make_option('-i','--itermode',action='store_true',help="show intermediate benchmark tables"),
  ]
  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  from registry import Registry
  mod = sys.modules.get('__main__')
  registry = Registry({'__main__':mod})
  return testall(registry,itermode=opt.itermode,namesel=args)

####################################

def testall(registry,itermode=False,namesel=None):
  """test all Equiv groups in given registry"""
  #print registry
  for name in registry.keys():
    group = registry.groups[name]
    tester = Tester(group)
    print 'verifying %16s ... ' % name,
    print tester.verify()
  print
  for name in registry.keys():
    group = registry.groups[name]
    tester = Tester(group,namesel=namesel)
    print 'benchmarking %16s ... ' % name
    if not itermode:
      print tester.benchmark()
    else:
      for out in tester.benchmark_iter():
        print out
        sys.stdout.flush()


if __name__ == '__main__':
  import doctest
  doctest.testmod()

