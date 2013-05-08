"""Sample python usage of UniTable

  Lists top ten values for each field in a set of files.
  Works for enumeration files that contain a '_count_'
  field showing number of occurances of given record.
"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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
from itertools import izip
from augustus.kernel.unitable import UniTable

def top_ten(filenames):

  # track values for each field
  seen_fields = {}
  total_recs = 0

  # read each file in turn
  for filename in filenames:
    tbl = UniTable()
    tbl.fromfile(filename)

    keys = tbl.keys()[:]
    if '_count_' in keys:
      total_recs += tbl['_count_'].sum()
      keys.remove('_count_')
    else:
      total_recs += len(tbl)
      tbl['_count_'] = 1

    # read each column in turn
    for key in keys:
      seen_values = seen_fields.setdefault(key,{})

      # iterate over counts and values
      for cnt,value in izip(tbl['_count_'],tbl[key]):
        try:
          seen_values[value] += cnt
        except KeyError:
          seen_values[value] = cnt

  # report results
  for key,seen_values in seen_fields.items():

    # find top ten
    top_cnts = sorted(seen_values.values())
    cutoff = top_cnts[-10:][0]
    tmp = sorted([cnt,value] for (value,cnt) in seen_values.items() if cnt >= cutoff)
    top = reversed(tmp[-10:])

    # report
    print 'Field:', key
    for (cnt,value) in top:
      percent = 100.0*cnt/float(total_recs)
      print '\t(%8.5f%%) %r' % (percent,value)



top_ten(sys.argv[1:])

