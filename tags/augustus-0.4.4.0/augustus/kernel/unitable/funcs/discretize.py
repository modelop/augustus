
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


import numpy as na
asarray = na.asarray

class Discretize(object):
  """data binning function

  >>> b = [1,10,50]
  >>> v = [0,5,35,100]
  >>> data = [100,75,50,25,2,1,0]

  >>> disc = Discretize(b)
  >>> print disc(data)
  [3 3 2 2 1 0 0]

  >>> disc = Discretize(b,v)
  >>> print disc(data)
  [100 100  35  35   5   0   0]

  """
  def __init__(self,boundaries,values=None):
    assert len(boundaries), "at least one boundary is required"
    if values is None:
      self.boundaries = na.sort(boundaries)
      self.values = values
    else:
      assert len(boundaries)+1 == len(values), "len(values) must be len(boundaries)+1"
      lastvalue = values[-1]
      boundvalues = sorted(zip(boundaries,values))
      self.boundaries = asarray([x[0] for x in boundvalues])
      self.values = asarray([x[1] for x in boundvalues]+[lastvalue])

  def __call__(self,data):
    idx = na.searchsorted(self.boundaries,data)
    if self.values is None:
      return idx
    return self.values.take(idx)

#################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
