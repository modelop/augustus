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

"""
Library of pure vector functions designed to be interoperable with
numpy. Designed to provide access to all available functions in one
namespace. This should allow switching between core packages (eg. numarray
and scipy) reasonable transparently.  Also it allows for selecting
from equivalent packages according to needs (eg. different machine
arch, batch vs real-time processing, predominance of extremely
short or long vectors, and other considerations.

Typical use:

  from augustus.kernel.unitable.veclib import veclib as vec
  # vec now contains default function choices
  vec.asarray(...)   # includes numarray namespace
  vec.chgdetect(...) # and local functions

Future use (these names will likely change):

  from augustus.kernel.unitable.veclib import MakeLib, Platform
  platform = Platform(ramsize='3G')   # choose constraints and policy
  vec = MakeLib(platform)             # get custom veclib namespace

  from augustus.kernel.unitable.veclib import Tester
  tester = Tester(vec)
  tester.selftest()
  tester.list_functions()
  print tester.benchmark(funclist(...),sizes=(1,2,3,50,500))


"""

from all import registry, veclib, corelib, VecLib
from base import as_any_array, as_num_array, as_char_array, import_native

