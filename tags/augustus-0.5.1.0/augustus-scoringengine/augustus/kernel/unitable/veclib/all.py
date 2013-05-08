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
