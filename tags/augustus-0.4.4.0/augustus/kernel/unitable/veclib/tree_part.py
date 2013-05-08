"""Partitioning of datasets, targeted for building trees.

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


import itertools as it
from base import corelib, Test, EquivUnary, EquivBinary, as_num_array

searchsorted = corelib.searchsorted
nonzero = corelib.nonzero
argsort = corelib.argsort
sort = corelib.sort
take = corelib.take
izip = it.izip

import segment
import gini
import histogram

midpoints_integer = segment.MidpointsInteger()
gini2 = gini.Gini2()
gini2_presorted = gini.Gini2Presorted()
gini2_counts = gini.Gini2Counts()
histo_tuple = histogram.HistoTuple()


########################################################################

class PartitionIntegerGini(EquivBinary):
  """Given a pair of dependent+independent integer vectors,
      consider all possible cutpoints in the independent vector, and
      return list of Gini scores for resulting splits in dependent.

  """
  itypes = ('i','i')

  ranking = ('isort','deltacnt2','deltacnt1','deltacnt0','presort','idxsel','masksel','naive')
  bench_sizes = (1,2,3,4,5,10,20,50,100,200,300,500,1000,5000,10000,
    # the following sizes take too long to include in the default benchmarking run
    #50000,
    #100000,
    #500000,
    #1000000,
  )

  tests = (
    Test([1,2])				== [0.0],
    Test([1,2,3])			** ([0.3333333]*2),
    Test([1,2,3,4])			** ([0.5]*3),
    Test([1,2,3,4,5])			** ([0.5999999]*4),
    Test([0,1,2,3,4])			** ([0.5999999]*4),
    Test([1,2,3,2,1])			** [0.2666666,0.4],
    Test([3,2,1,1,1,1,1,1,1,1,1,1,1,1])	** [0.071428571428571425,0.13186813186813176],
    Test([1,2,3,4],[6,7,6,8])		** [0.5,0.5],
    Test([1,2,3,4,5,6],[6,7,6,8,9,6])	** ([0.6666666]*3),
    Test([1,2,1,2,1,2],[6,7,8,8,9,9])	** [0.3999999,0.5,0.5],
    Test([1,2,1,2,1,2],[4,5,6,7,8,9])	** [0.3999999,0.5,0.4444444,0.5,0.3999999],
    Test([1,2,1,2,1,2],[4,5,6,7,8,9],cutpoints=[7,8])	** [0.4444444,0.5],
  )

  def _prep_testdata(self,*args,**kwargs):
    # benchmark for inputs that are already vectors
    # simplification for tests: dep == indep
    out = [as_num_array(arg) for arg in args]
    if len(out) == 1:
      out.append(out[0].copy())
    if not kwargs.get('dep_sorted'):
      idx = argsort(out[0])
      out = [take(vec,idx) for vec in out]
      kwargs['dep_sorted'] = True
    return (out,kwargs)

  @staticmethod
  def smart(dep,indep,cutpoints=None,**kwargs):
    # not needed unless high penalty for small datasets
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    if len(dep) < 100:
      return PartitionIntegerGini.naive(dep,indep,cutpoints=cutpoints,**kwargs)
    return PartitionIntegerGini.isort(dep,indep,cutpoints=cutpoints,**kwargs)

  @staticmethod
  def isort(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    if not len(cutpoints):
      return []
    # sort both vectors by *indep*
    idx = argsort(indep)
    dep = take(dep,idx)
    indep = take(indep,idx)
    #print len(dep),len(indep),len(cutpoints)
    cutidx = [0,0]
    for ival,isub in it.groupby(indep):
      ilen = len(list(isub))
      if ival < cutpoints[len(cutidx)-2]:
        cutidx[-1] += ilen
      else:
        if len(cutidx) > len(cutpoints):
          break
        cutidx.append(cutidx[-1]+ilen)
    assert len(cutidx)-1 == len(cutpoints), '%s != %s' % (len(cutidx)-1,len(cutpoints))
    out = []
    cnt2 = dict(histo_tuple(dep))
    cnt1 = dict.fromkeys(cnt2.keys(),0)
    for i1,i2 in izip(cutidx[:-1],cutidx[1:]):
      # update the counts from the last cut
      for d,cnt in histo_tuple(dep[i1:i2]):
        cnt1[d] += cnt
        cnt2[d] -= cnt
      # calculate results based on counts
      a1 = as_num_array([val for val in cnt1.itervalues() if val != 0])
      a2 = as_num_array([val for val in cnt2.itervalues() if val != 0])
      out.append(gini2_counts(a1,a2))
    assert len(out) == len(cutpoints), '%s != %s' % (len(out),len(cutpoints))
    return out

  @staticmethod
  def deltacnt2(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    # dictionary of counts in each dataset
    cnt2 = dict(histo_tuple(dep))
    cnt1 = dict.fromkeys(cnt2.keys(),0)
    lastmask = (indep != indep)
    for cut in cutpoints:
      mask = indep < cut
      # examine only the new values from the last cut
      maskdelta = mask & ~lastmask
      lastmask |= mask
      idxdelta = nonzero(maskdelta)[0]
      # update the counts from the last cut
      for d,cnt in histo_tuple(dep[idxdelta]):
        cnt1[d] += cnt
        cnt2[d] -= cnt
      # calculate results based on counts
      a1 = as_num_array([val for val in cnt1.itervalues() if val != 0])
      a2 = as_num_array([val for val in cnt2.itervalues() if val != 0])
      out.append(gini2_counts(a1,a2))
    return out

  @staticmethod
  def deltacnt1(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    # dictionary of counts in each dataset
    cnt2 = dict(histo_tuple(dep))
    cnt1 = dict.fromkeys(cnt2.keys(),0)
    lastmask = (indep != indep)
    for cut in cutpoints:
      mask = indep < cut
      # examine only the new values from the last cut
      maskdelta = mask & ~lastmask
      lastmask |= mask
      idxdelta = nonzero(maskdelta)[0]
      # update the counts from the last cut
      for d in dep[idxdelta]:
        cnt1[d] += 1
        cnt2[d] -= 1
      # calculate results based on counts
      a1 = as_num_array([val for val in cnt1.itervalues() if val != 0])
      a2 = as_num_array([val for val in cnt2.itervalues() if val != 0])
      out.append(gini2_counts(a1,a2))
    return out

  @staticmethod
  def deltacnt0(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    # get vector of counts in each dataset
    tmp = histo_tuple(dep)
    dep_keys = dict((x[0],i) for i,x in enumerate(tmp))
    cnt2 = as_num_array([x[1] for x in tmp])
    cnt1 = cnt2 - cnt2
    lastmask = (indep != indep)
    for cut in cutpoints:
      mask = indep < cut
      maskdelta = mask & ~lastmask
      lastmask |= mask
      for d in dep[maskdelta]:
        key = dep_keys[d]
        cnt1[key] += 1
        cnt2[key] -= 1
      a1 = cnt1[cnt1 != 0]
      a2 = cnt2[cnt2 != 0]
      out.append(gini2_counts(a1,a2))
    return out

  @staticmethod
  def presort(dep,indep,cutpoints=None,dep_sorted=False):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if not dep_sorted:
      idx = argsort(dep)
      dep = take(dep,idx)
      indep = take(indep,idx)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    for cut in cutpoints:
      mask = indep < cut
      i1 = nonzero(mask)[0]
      i2 = nonzero(~mask)[0]
      a1 = dep[i1]
      a2 = dep[i2]
      out.append(gini2_presorted(a1,a2))
    return out

  @staticmethod
  def idxsel(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    for cut in cutpoints:
      mask = indep < cut
      i1 = nonzero(mask)[0]
      i2 = nonzero(~mask)[0]
      a1 = dep[i1]
      a2 = dep[i2]
      out.append(gini2(a1,a2))
    return out

  @staticmethod
  def masksel(dep,indep,cutpoints=None,**kwargs):
    dep = as_num_array(dep)
    indep = as_num_array(indep)
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    for cut in cutpoints:
      mask = indep < cut
      a1 = dep[mask]
      a2 = dep[~mask]
      out.append(gini2(a1,a2))
    return out

  @staticmethod
  def naive(dep,indep,cutpoints=None,**kwargs):
    if cutpoints is None:
      cutpoints = midpoints_integer(indep)
    out = []
    for cut in cutpoints:
      a1 = [d for d,i in izip(dep,indep) if i < cut]
      a2 = [d for d,i in izip(dep,indep) if i >= cut]
      out.append(gini2(a1,a2))
    return out

    
########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
