#!/usr/bin/env python

"""run all unitable module doctests as a single suite
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
import unittest
import doctest
flags =  doctest.NORMALIZE_WHITESPACE
flags |= doctest.ELLIPSIS
#flags |= doctest.REPORT_ONLY_FIRST_FAILURE


modprefix = 'augustus.kernel.unitable'
testnames = ['asarray', 'storage', 'unitable', 'evaltbl', 'rules', 'wrappers']

testmods = ['.'.join([modprefix,name]) for name in testnames]

__import__(modprefix,None,None,testnames)

suite = unittest.TestSuite()
for mod in testmods:
  suite.addTest(doctest.DocTestSuite(mod,optionflags=flags))
runner = unittest.TextTestRunner()
runner.run(suite)

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
