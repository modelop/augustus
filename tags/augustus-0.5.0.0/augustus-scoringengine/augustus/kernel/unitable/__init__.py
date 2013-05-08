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
The "UniTable" is an implementation of a conceptual "Universal Table"
that is at the core of the Augustus scoring engine.  The data
structure is analogous to an R frame: a table where the columns are
vectors of equal length, but may be of different types.  It is based
on the Python numpy package and the programming interface attempts
to maintain consistency with the style established therein.

The design goal was to create a very fast, efficient object for data
shaping, model building, and scoring, both in a batch and real-time
context.  The key features are:

- A file format that matches the native machine memory storage of the
  data.  This allows for memory-mapped access to the data, eliminating
  the need for data parsing or sequential reading. 

- Fast vector operations using any number of data columns.

- Support for demand driven, rule based calculations.  Derived columns
  can be defined in terms of operations on other columns, including other
  derived columns, and will be made available when referenced.

- The ability to invoke calculations in scalar or vector mode transparently.
  Thus, one set of rule definitions can be applied to an entire
  data set in batch mode, or to individual rows incoming as real-time
  events.

- The ability to handle huge real-time data rates by automatically
  switching to vector mode when behind, and scalar mode when keeping
  up with individual input events.

########################################################################
1. unitable tool

   "unitable" converts between CSV and binary data formats. The input
   format is autodetected and may be binary or any flavor of well-formed
   CSV.  Output formats are:

     unitable --csv	# CSV output, use --sep for custom field delim
     unitable --bin	# binary output
     unitable --tbl	# pretty printed table for viewing

   Future ouput formats may include HTML, and R frames.

   Note that there may or may not be space savings by converting from
   CSV to binary format, depending on the data.  The advantage of the
   binary format is in performance gains for subsequent processing.

########################################################################
2. unitable binary file format

   The unitable binary file contains a brief text header describing
   field names and formats, followed by raw binary data.  Each field
   is stored as a contiguous vector of native machine types.  When
   "reading" a binary file, the data is not read in the traditional
   sense.  Rather, a memory mapping is established to the disk location
   and data is made available on-demand.

   The performance gains are spectacular.  For example, one huge CSV
   file that takes an hour to read, parse, and store as internal lists
   takes about 45 seconds to "read" in binary format.

########################################################################
3. unitable python data structure

   See unitable.py for instructions
   and a large list of examples.  Typical usage is:

     from augustus.unitable import UniTable
     tbl = UniTable
     tbl.fromfile(filename)

   For simple needs, tbl can be considered a dictionary where each
   column in the table is stored as a separate list of values.
   
   For maximum benefit, it is necessary to understand the python
   numpy module. Each column is actually a numpy vector and
   supports a variety of vector operations.  Also, the UniTable
   supports operations on the entire table in the style of numpy
   records.
"""

from unitable import UniTable
from evaltbl import EvalTable
from rules import Rules, Rule

