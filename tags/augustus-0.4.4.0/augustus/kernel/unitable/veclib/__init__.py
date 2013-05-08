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


from all import registry, veclib, corelib, VecLib
from base import as_any_array, as_num_array, as_char_array, import_native

