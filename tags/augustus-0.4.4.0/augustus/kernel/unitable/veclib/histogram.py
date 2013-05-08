"""Histogram extractions

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
groupby = it.groupby
from base import corelib, EquivUnary, Test, as_any_array

########################################################################

class HistoTuple(EquivUnary):
  '''Histogram returned as list of (value,count) tuples

  '''
  name = 'histo_tuple'
  ranking = ('iter_groupby','dict1')

  tests = (
    Test([1,2,3,4,1,2,3,1,1,1,9]) ==  [(1,5),(2,2),(3,2),(4,1),(9,1)],
    Test([1,1,1,1,1,1,1,1,1,1,1]) ==  [(1,11)],
  )

  @staticmethod
  def iter_groupby(arg):
    arg = as_any_array(arg)
    return sorted([(k,len(list(g))) for k,g in groupby(sorted(arg))])

  @staticmethod
  def dict1(arg,out=None):
    arg = as_any_array(arg)
    out = {}
    for val in arg:
        out[val] = out.get(val,0) + 1
    return sorted(out.items())

########################################################################

if __name__ == "__main__":
  from base import tester
  tester.testmod()

########################################################################
