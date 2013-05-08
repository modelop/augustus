__copyright__ = """
Copyright (C) 2006-2009  Open Data ("Open Data" refers to
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
import os
dn = os.path.dirname
# _basedir is directory of directory with the executing code.
_basedir = dn(os.path.abspath(dn(os.path.realpath(sys.argv[0]))))
# locate top of augustus source tree. This would not
# be necessary in usual deployment which would put it
# in 'correct' python module tree.
sys.path.insert(1,os.path.join(_basedir,'augustus'))
sys.path.insert(1,os.path.abspath(os.curdir))
del os,sys,dn,_basedir
__all__=()

