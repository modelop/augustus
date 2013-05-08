"""Sample python usage of EvalTable: a UniTable with rules

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


import sys
from itertools import izip
from augustus.kernel.unitable import EvalTable, Rules

rules = Rules(
  "is_decline = ISO_Resp_Cd=='05'",
  "is_not_decline = is_decline==0",
  """something_not_found = (
                      POS_Entry_Mode_Not_Found.astype('Bool') |
                      POS_PIN_Entry_Cpbty_Not_Found.astype('Bool') |
                      POS_Cond_Cd_Not_Found.astype('Bool') |
                      POS_Terml_Typ_Not_Found.astype('Bool') |
                      POS_Terml_Entry_Cpbty_Not_Found.astype('Bool') |
                      MOTO_ECI_Not_found.astype('Bool') |
                      POS_Env_Cd_Not_Found.astype('Bool')
                      )""",
  """something_missing = (
                      Card_Acptr_ID_Missing.astype('Bool') |
                      Card_Acptr_Nm_Missing.astype('Bool') |
                      Card_Acptr_City_Missing.astype('Bool') |
                      Card_Acptr_Ctry_Missing.astype('Bool')
                      )""",
  """something_padded = (
                      Card_Acptr_ID_Padded.astype('Bool') |
                      Card_Acptr_Nm_Padded.astype('Bool') |
                      Card_Acptr_City_Padded.astype('Bool')
                      )""",
  "something_not_avail = something_not_found | something_missing",
  "something_nonstd = something_not_avail | something_padded",
  "is_standard = something_nonstd==0",

  "is_std_decline = is_standard & is_decline",
  "is_std_ok = is_standard & is_not_decline",
  "is_nonstd_decline = something_nonstd & is_decline",
  "is_nonstd_ok = something_nonstd & is_not_decline",

  "standard = _count_ * is_standard",
  "not_standard = _count_ * something_nonstd",
  "declines = _count_ * is_decline",
  "not_declines = _count_ * is_not_decline",
  "std_declines = _count_ * is_std_decline",
  "nonstd_declines = _count_ * is_nonstd_decline",
)

def demo(filenames):
  # read each file in turn
  for filename in filenames:
    tbl = EvalTable(rules)
    tbl.fromfile(filename)

    records = tbl['_count_'].sum()

    calc_sum = ('standard','not_standard','declines','not_declines','std_declines','nonstd_declines')
    cnt = {}
    for name in calc_sum:
      cnt[name] = tbl[name].sum()
    #sanity check
    assert cnt['declines'] + cnt['not_declines'] == records, \
      "%s + %s != %s" % (cnt['declines'],cnt['not_declines'],records)
    assert cnt['standard'] + cnt['not_standard'] == records, \
      "%s + %s != %s" % (cnt['standard'],cnt['not_standard'],records)

    out = []
    rpt_names = ('standard','declines','std_declines','nonstd_declines')
    for name in rpt_names:
      out.append('%s=%6.3f%%' % (name,100.0*cnt[name]/float(records)))

    #rej_names = ('std_declines','nonstd_declines')
    #for name in rej_names:
    #  out.append('ratio_%s=%6.3f%%' % (name,100.0*cnt[name]/float(cnt['declines'])))

    print '%14s (%8d records): %s' % (filename,records,' '.join(out))


demo(sys.argv[1:])

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
