
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


import itertools as it
from base import corelib, EquivUnary, Test, as_any_array, as_num_array
subtract = corelib.subtract
sort = corelib.sort
arange = corelib.arange
cumsum = corelib.cumsum
groupby = it.groupby
izip = it.izip
takewhile = it.takewhile
dropwhile = it.dropwhile


########################################################################

class DictMap(EquivUnary):
  '''apply dictionary mapping to each element in array, retaining unmatched values

    >>> func = DictMap()
    >>> mapping = {1:100,2:200,3:300,'a':'aaa','b':'bbb'}
    >>> assert func([1,3,6,7,'a',9],mapping=mapping).tolist() == [100,300,6,7,'aaa',9]

  '''
  name = 'dict_map'
  ranking = ('naive_comp', 'naive_loop',)

  tests = (
    Test([1,3,6,7,7,9],mapping={1:100,2:200,3:300}) == [100,300,6,7,7,9],
    Test([1,3,6,7,'a',9],mapping={'1':100,'a':'aaa','3':300}) == [100,300,6,7,'aaa',9],
  )

  @staticmethod
  def naive_loop(arg,mapping={}):
    arg = as_any_array(arg)
    out = []
    for value in arg:
      out.append(mapping.get(value,value))
    return as_any_array(out)

  @staticmethod
  def naive_comp(arg,mapping={}):
    arg = as_any_array(arg)
    out = [mapping.get(value,value) for value in arg]
    return as_any_array(out)


########################################################################

class DictMapDefault(EquivUnary):
  '''apply dictionary mapping to each element in array, using default for unmatched

    >>> func = DictMapDefault()
    >>> mapping = {1:100,2:200,3:300,'a':'aaa','b':'bbb'}
    >>> assert func([1,3,6,7,'a',9],mapping=mapping).tolist() == [100,300,6,7,'aaa',9]

  '''
  name = 'dict_map_default'
  ranking = ('naive_comp', 'naive_loop',)

  tests = (
    Test([1,3,6,7,7,9],mapping={1:100,2:200,3:300}) == [100,300,0,0,0,0],
    Test([1,3,6,7,'a',9],mapping={'1':100,'a':'aaa','3':300}) == ['100','300','0','0','aaa','0'],
  )

  @staticmethod
  def naive_loop(arg,mapping={},default=0):
    arg = as_any_array(arg)
    out = []
    for value in arg:
      out.append(mapping.get(value,default))
    return as_any_array(out)

  @staticmethod
  def naive_comp(arg,mapping={},default=0):
    arg = as_any_array(arg)
    out = [mapping.get(value,default) for value in arg]
    return as_any_array(out)


########################################################################

class FuncMap(EquivUnary):
  '''apply function to each element in array

    >>> func = FuncMap()
    >>> mapping = {1:100,2:200,3:300,'a':'aaa','b':'bbb'}
    >>> assert func([1,3,6,7,7,9],func=mapping.get).tolist() == [100,300,6,7,7,9]

  '''
  name = 'func_map'
  ranking = ('naive_comp', 'naive_loop',)

  tests = (
    Test([1,3,6,7,7,9],func={1:100,2:200,3:300,6:6,7:7,9:9}.get) == [100,300,6,7,7,9],
  )

  @staticmethod
  def _default_func(arg): return arg

  @staticmethod
  def naive_loop(arg,func=None):
    func = func or FuncMap._default_func
    arg = as_any_array(arg)
    out = []
    for value in arg:
      out.append(func(value))
    return as_any_array(out)

  @staticmethod
  def naive_comp(arg,func=None):
    func = func or FuncMap._default_func
    arg = as_any_array(arg)
    out = [func(value) for value in arg]
    return as_any_array(out)


########################################################################

class GetShift(EquivUnary):
  '''get value of other column shifted by some offset
    (typically useful for previous value (or arbitrary offset into column)

    >>> func = GetShift()
    >>> assert func([1,3,6,7,7,9],filler=99).tolist() == [3,6,7,7,9,99]

  '''
  name = 'get_shift'
  ranking = ('smart','fast', 'naive_comp', 'naive_loop',)

  tests = (
    Test([0])                     == [0],
    Test([1,3,6,7,7,9])           == [0,1,3,6,7,7],
    Test([1,3,6,7,7,9],offset=3)  == [0,0,0,1,3,6,],
    Test([1,3,6,7,7,9],offset=-2) == [6,7,7,9,0,0],
    Test([1,3,6,7,7,9],filler=99) == [99,1,3,6,7,7],
  )

  @staticmethod
  def naive_loop(arg,offset=1,filler=0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    if offset < 0:
      cut = len(arg)+offset
      for i in xrange(cut):
        out[i] = arg[i-offset]
      for i in xrange(cut,len(arg)):
        out[i] = filler
    else:
      cut = offset
      for i in xrange(cut):
        out[i] = filler
      for i in xrange(cut,len(arg)):
        out[i] = arg[i-offset]
    return out

  @staticmethod
  def naive_comp(arg,offset=1,filler=0,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    if offset < 0:
      cut = len(arg)+offset
      out[:cut] = [arg[i] for i in xrange(-offset,len(arg))]
      out[cut:] = [filler]*(-offset)
    else:
      cut = offset
      out[:cut] = [filler]*(offset)
      out[cut:] = [arg[i] for i in xrange(len(arg)-cut)]
    return out

  @staticmethod
  def fast(arg,offset=1,filler=0,out=None):
    arg = as_num_array(arg)
    if offset < 0:
      cut = len(arg)+offset
      out1 = arg[-offset:len(arg)]
      out2 = (-offset)*[filler]
    else:
      cut = offset
      out1 = offset*[filler]
      out2 = arg[:len(arg)-cut]
    if not out:
      out = arg.new()
    out[:cut] = out1
    out[cut:] = out2
    return out

  @classmethod
  def smart(self,arg,offset=1,filler=0,out=None):
    if len(arg) < 100:
      return self.naive_loop(arg,offset=offset,filler=filler,out=out)
    return self.fast(arg,offset=offset,filler=filler,out=out)

  @staticmethod
  def _check_result(out,arg,**kwargs):
    offset = kwargs.get('offset',1)
    filler = kwargs.get('filler',0)
    if offset < 0:
      cut = len(arg)+offset
      for i in xrange(cut):
        assert out[i] == arg[i-offset]
      for i in xrange(cut,len(arg)):
        assert out[i] == filler
    else:
      cut = offset
      for i in xrange(cut):
        assert out[i] == filler
      for i in xrange(cut,len(arg)):
        assert out[i] == arg[i-offset]
    return True



########################################################################

class DeltaPrev(EquivUnary):
  '''difference from previous value

    >>> func = DeltaPrev()
    >>> assert func([1,3,6,7,7,9]).tolist() == [0,2,3,1,0,2]

  '''
  name = 'delta_prev'
  ranking = ('smart','fast', 'naive_comp', 'naive_loop',)

  tests = (
    Test([1,3,6,7,7,9]) == [0,2,3,1,0,2],
    Test([0,3,0,7,0,0]) == [0,3,-3,7,-7,0],
    Test([0])           == [0],
  )

  @staticmethod
  def naive_loop(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
      out[0] = 0
    for i in xrange(1,len(arg)):
      out[i] = arg[i] - arg[i-1]
    return out

  @staticmethod
  def naive_comp(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
      out[0] = 0
    out[1:] = [arg[i]-arg[i-1] for i in xrange(1,len(arg))]
    return out

  @staticmethod
  def fast(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
      out[0] = 0
    subtract(arg[1:],arg[:-1],out[1:])
    return out

  @classmethod
  def smart(self,arg,out=None):
    if len(arg) < 10:
      return self.naive_loop(arg,out)
    return self.fast(arg,out)

  @staticmethod
  def _check_result(out,arg,**kwargs):
    assert out[0] == 0
    for o,a1,a2 in izip(out[1:],arg[1:],arg[:-1]):
      assert o == a1-a2
    return True



#################################################################

class CarryForward(EquivUnary):
  '''fill empty values with previous non-empty value

    >>> func = CarryForward().fast
    >>> assert func([0,3,0,0,4,0]).tolist() == [0,3,3,3,4,4]

  '''
  name = 'carry_forward'
  ranking = ('naive_loop', 'naive_iter', 'array_idx')

  tests = (
    Test([0,3,0,0,4,0]) == [0,3,3,3,4,4],
    Test([0,0,0,0,0,0]) == [0,0,0,0,0,0],
    Test([0,0,0,0,0,5]) == [0,0,0,0,0,5],
    Test([9,0,0,0,0,0]) == [9,9,9,9,9,9],
  )

  @staticmethod
  def array_idx(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    idx = arg.nonzero()[0]
    try:
      first = idx[0]
    except IndexError:
      first = len(out)
    out[:first] = 0
    if not len(idx):
      return out
    a,b = it.tee(idx)
    b.next()
    for start,stop in izip(a,b):
      out[start:stop] = arg[start]
    last = idx[-1]
    out[last:] = arg[last]
    return out

  @staticmethod
  def naive_iter(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    last = 0
    for i,value in izip(it.count(),arg):
      if value != 0:
        out[i] = last = value
      else:
        out[i] = last
    return out

  @staticmethod
  def naive_loop(arg,out=None):
    arg = as_num_array(arg)
    if not out:
      out = arg.new()
    last = 0
    for i in xrange(len(arg)):
      if arg[i] != 0:
        last = arg[i]
      out[i] = last
    return out

########################################################################

class LookAheadIndex(EquivUnary):
  """Given
        1) a vector of sorted values that may contain duplicates and
        irregular spacing (like event timestamps, for example), and
        2) a numeric delta to add to the vector (to identify the
        future timestamp some fixed interval away),
      return an index vector into the original data vector such that
      for each current value, the index points to either
        1) the first occurrence of current value+delta, or
        2) if the data doesn't contain such value, the first occurrence
        of the lext lower value that does occur in the data.

    If delta==0, then the index points to the first/last occurrence of the
    current value in the data.

  """
  itypes = 'i'

  ranking = ('naive_loop',)
  ranking = ('missing1','naive_loop')
  bench_sizes = (1,2,3,4,5,10,20,50,100,200,300,500,1000,5000,
    #10000,
    #50000,
    #100000,
    #500000,
    #1000000,
  )


  tests = (
    Test([0,0,1,1,1,2,3,5,9],delta=0,first=True)	== [0,0,2,2,2,5,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=1,first=True)	== [2,2,5,5,5,6,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=2,first=True)	== [5,5,6,6,6,6,7,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=3,first=True)	== [6,6,6,6,6,7,7,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=4,first=True)	== [6,6,7,7,7,7,7,8,8],
    Test([0,0,1,1,1,2,3,5,9],delta=5,first=True)	== [7,7,7,7,7,7,7,8,8],

    Test([0,0,1,1,1,2,3,5,9],delta=0,first=False)	== [1,1,4,4,4,5,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=1,first=False)	== [4,4,5,5,5,6,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=2,first=False)	== [5,5,6,6,6,6,7,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=3,first=False)	== [6,6,6,6,6,7,7,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=4,first=False)	== [6,6,7,7,7,7,7,8,8],
    Test([0,0,1,1,1,2,3,5,9],delta=5,first=False)	== [7,7,7,7,7,7,7,8,8],

    Test([0,0,1,1,1,2,3,5,9],delta=-0,first=True)	== [0,0,2,2,2,5,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=-1,first=True)	== [0,0,0,0,0,2,5,6,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-2,first=True)	== [0,0,0,0,0,0,2,6,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-3,first=True)	== [0,0,0,0,0,0,0,5,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-4,first=True)	== [0,0,0,0,0,0,0,2,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-5,first=True)	== [0,0,0,0,0,0,0,0,6],

    Test([0,0,1,1,1,2,3,5,9],delta=-0,first=False)	== [1,1,4,4,4,5,6,7,8],
    Test([0,0,1,1,1,2,3,5,9],delta=-1,first=False)	== [1,1,1,1,1,4,5,6,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-2,first=False)	== [1,1,1,1,1,1,4,6,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-3,first=False)	== [1,1,1,1,1,1,1,5,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-4,first=False)	== [1,1,1,1,1,1,1,4,7],
    Test([0,0,1,1,1,2,3,5,9],delta=-5,first=False)	== [1,1,1,1,1,1,1,1,6],

    Test([0],delta=-1)			== [],
    Test([1,5],delta=-3)		== [0,0],
    Test([0])				== [],
    Test([1,5],delta=3)			== [0,1],
  )

  def _prep_testdata(self,*args,**kwargs):
    # benchmark for inputs that are already vectors
    # simplification for tests: dep == indep
    return [as_num_array(sorted(arg)) for arg in args]

  @staticmethod
  def missing1(arg,delta=1,first=False):
    # build answer lookup mapping each arg value to first index
    run_lens = [(k,len(list(g))) for k,g in groupby(arg)]
    keys = as_num_array([k for k,l in run_lens])
    lens = as_num_array([l for k,l in run_lens])
    ends = cumsum(lens)
    starts = ends - lens
    if first:
      answer = dict(izip(keys,starts))
    else:
      answer = dict(izip(keys,ends-1))
    # identify missing keys
    need = keys + delta
    needset = set(need)
    haveset = set(answer)
    fillset = needset.difference(haveset)
    fill = as_num_array(sorted(fillset))
    #
    #print
    #print 'haveset:', haveset
    #print 'need:', need
    #print 'fill:', fill
    #print 'answer1:', answer
    #
    minkey,maxkey = arg[0],arg[-1]
    #
    have_iter = iter(keys[-1::-1])
    fill_iter = iter(fill[-1::-1])
    thiskey = maxkey
    thisval = answer[thiskey]
    for fillkey in fill_iter:
    #  print 'fillkey:', fillkey
      if thiskey >= fillkey:
        try:
          thiskey = dropwhile(lambda x:x>=fillkey,have_iter).next()
        except StopIteration:
          thiskey = minkey
        thisval = answer[thiskey]
      answer[fillkey] = thisval
    #print 'answer2:', answer
    out = [answer[val+delta] for val in arg]
    return out


  @staticmethod
  def naive_loop(arg,delta=1,first=False):
    out = []
    for i,val in enumerate(arg):
      # find answer range
      target = val + delta
      jj = i
      if target > val:
        # look forward
        for j in xrange(i+1,len(arg)):
          if arg[j] > target:
            break
        else:
          j = len(arg)
        jj = j-1
        target = arg[jj]
      elif target < val:
        # look backward
        for j in xrange(i-1,-1,-1):
          if arg[j] <= target:
            break
        else:
          j = 0
        jj = j
        target = arg[jj]

      # find first or last answer within range
      if first:
        kk = 0
        for k in xrange(jj,-1,-1):
          if arg[k] != target:
            kk = k+1
            break
      else:
        kk = len(arg)-1
        for k in xrange(jj,len(arg)):
          if arg[k] != target:
            kk = k-1
            break
      out.append(kk)
    return out

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
