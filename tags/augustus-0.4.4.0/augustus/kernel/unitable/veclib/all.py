"""
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


from base import corelib, VecLib, Registry

# import all definitions
modules = (
  'common','histogram','gini',
  'prob_dist','segment','pmml_models',
  'tree_part','sample','utils','window',
)

failed = []
loaded = {}
for mod in modules:
  try:
    loaded[mod] = __import__(mod,globals())
  except:
    failed.append(mod)

# register functions
registry = Registry(loaded)

# default library
veclib = VecLib(**registry.publish())


########################################################################

if __name__ == "__main__":
  if failed:
    print 'WARNING: failed to load modules: %s' % failed
    print
  from pprint import pprint
  pprint(registry.publish())
  print
  from base import tester
  tester.testall(registry)
