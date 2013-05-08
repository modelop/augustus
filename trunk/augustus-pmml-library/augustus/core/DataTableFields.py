#!/usr/bin/env python

# Copyright (C) 2006-2013  Open Data ("Open Data" refers to
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

"""This module defines the DataTableFields class."""

import sys
import os

from augustus.core.defs import defs
from augustus.core.OrderedDict import OrderedDict

class DataTableFields(OrderedDict):
    """DataTableFields is a dictionary mapping field names to
    DataColumns.

    It can only be named"fields" or "output", depending on the
    role that it plays in its DataTable.
    """

    @property
    def name(self):
        return self._name

    def __init__(self, *args, **kwds):
        """Initializes a DataTableFields.

        @param *args, **kwds: Arguments passed to OrderedDict, the superclass of DataTableFields.
        """

        self._name = "fields"
        self._length = None
        super(DataTableFields, self).__init__(*args, **kwds)

    def __getitem__(self, name):
        """Get an existing field.

        @type name: string
        @param name: The name of the field to get.
        @raise LookupError: If a field named C{name} does not already exist, attempting to get it raises an error.
        """

        try:
            return super(DataTableFields, self).__getitem__(name)
        except KeyError:
            raise LookupError("Field \"%s\" does not exist in the DataTable; perhaps it was not provided as input or is defined later in the PMML document?" % name)

    def replaceField(self, name, dataColumn):
        """Replace an existing field.

        @type name: string
        @param name: The name of the field to replace.
        @type dataColumn: DataColumn
        @param dataColumn: The new data.
        @raise LookupError: If a field named C{name} does not already exist, attempting to replace it raises an error.
        """

        if name not in self:
            raise LookupError("Field \"%s\" does not exist in the DataTable; perhaps it was not provided as input or is defined later in the PMML document?" % name)
        super(DataTableFields, self).__setitem__(name, dataColumn)

    def __setitem__(self, name, dataColumn):
        """Add a field; one cannot use this method to shadow an existing field (see C{replaceField}).

        @type name: string
        @param name: The name of the new field.
        @type dataColumn: DataColumn
        @param dataColumn: The data.
        @raise LookupError: If a field named C{name} already exists, attempting to replace it raises an error.
        """

        if name in self:
            raise LookupError("Field \"%s\" already exists; it cannot be overshadowed by another field with the same name" % name)

        super(DataTableFields, self).__setitem__(name, dataColumn)

        try:  # ugly fix for pickle protocol=2 (http://bugs.python.org/issue826897)
            if self._length is None or len(dataColumn) < self._length:
                self._length = len(dataColumn)
        except AttributeError:
            return

        for dataColumn in self.values():
            if len(dataColumn) > self._length:
                dataColumn._data = dataColumn._data[:self._length]
                if dataColumn._mask is not None:
                    dataColumn._mask = dataColumn._mask[:self._length]
                    if not dataColumn._mask.any():
                        dataColumn._mask = None

    def __len__(self):
        """Get the length of the DataTableFields.

        @rtype: int
        @return: The number of rows in the DataTableFields.
        """

        if self._length is None:
            return 0
        else:
            return self._length

    def __repr__(self):
        return "<DataTable.%s %d rows; %s at 0x%x>" % (self.name, len(self), " ".join(["\"%s\"" % f for f in self]), id(self))

    def look(self, head=10, tail=10, restriction=None, stream=None, columnWidth=10, score=None):
        """An informative representation of the DataTableFields,
        intended for interactive use.

        Note: if C{head + tail} is greater or equal to the length of
        the table, all rows will be shown.  Otherwise, just the
        beginning and the end.

        @type head: int
        @param head: Number of rows to display from the beginning of the table.
        @type tail: int
        @param tail: Number of rows to display from the end of the table.
        @type restriction: list of strings or None
        @param restriction: If None, display all columns; otherwise, display only the specified columns.
        @type stream: file-like object or None
        @param stream: If None, print to C{sys.stdout}; otherwise, write to the specified stream.
        @type columnWidth: int or dict
        @param columnWidth: If C{columnWidth} is an integer, set the width of all columns to the specified number of characters.  If C{columnWidth} is a dictionary mapping column names (strings) to integers, set column widths on a per-column basis with C{columnWidth[None]} as a default.  If C{columnWidth[None]} is not defined, the default is 10 characters.
        @type score: DataColumn or None
        @param score: This is a means for the DataTable to insert its score into C{DataTableFields.look}.  It is not intended for end-users.
        """

        if stream is None:
            stream = sys.stdout

        if restriction is None:
            restriction = self.keys()

        if score is not None:
            restriction = restriction + [None]

        if not isinstance(columnWidth, dict):
            columnWidth = dict((fieldName, columnWidth) for fieldName in restriction)
        for fieldName in restriction:
            if fieldName not in columnWidth:
                columnWidth[fieldName] = columnWidth.get(None, 10)

        formatting = dict((fieldName, "%%-%d.%ds" % (columnWidth[fieldName], columnWidth[fieldName])) for fieldName in restriction)
        
        lastNumberLength = len(repr(len(self)))
        stream.write(("%%-%d.%ds" % (lastNumberLength, lastNumberLength)) % "#")
        stream.write(" | ")
        stream.write(" | ".join(formatting[fieldName] % (fieldName if fieldName is not None else "SCORE") for fieldName in restriction))
        stream.write(os.linesep)

        stream.write(("%%-%d.%ds" % (lastNumberLength, lastNumberLength)) % ("".join(["-"] * lastNumberLength)))
        stream.write("-+-")
        stream.write("-+-".join(formatting[fieldName] % "".join(["-"] * columnWidth[fieldName]) for fieldName in restriction))
        stream.write(os.linesep)

        head = range(0, min(head, len(self)))
        tail = range(max(0, len(self) - tail), len(self))

        if len(head) > 0 and len(tail) > 0 and head[-1] < tail[0] - 1:
            self._look(restriction, head, formatting, columnWidth, lastNumberLength, stream, score)
            stream.write("   ..." + os.linesep)
            self._look(restriction, tail, formatting, columnWidth, lastNumberLength, stream, score)
        else:
            self._look(restriction, range(len(self)), formatting, columnWidth, lastNumberLength, stream, score)

        stream.flush()

    def _look(self, restriction, rows, formatting, columnWidth, lastNumberLength, stream, score):
        """Used by look."""

        for row in rows:
            stream.write(("%%-%d.%ds" % (lastNumberLength, lastNumberLength)) % row)
            stream.write(" | ")

            values = []
            for fieldName in restriction:
                if fieldName is None:
                    dataColumn = score
                else:
                    dataColumn = self[fieldName]

                ellipsis = ""
                if isinstance(dataColumn, tuple):
                    dataColumn = dataColumn[0]
                    ellipsis = "..."

                value = dataColumn.fieldType.valueToString(dataColumn.data[row])
                if not isinstance(value, basestring):
                    value = repr(value)

                if len(value) > columnWidth[fieldName]:
                    try:
                        asfloat = float(value)
                    except ValueError:
                        if columnWidth[fieldName] > 3:
                            value = value[:(columnWidth[fieldName] - 3)] + "..."
                        else:
                            value = "#" * columnWidth[fieldName]
                    else:
                        good = False
                        for i in xrange(columnWidth[fieldName], 0, -1):
                            formatter = "%%.%dg" % i
                            smaller = formatter % asfloat
                            if len(smaller) <= columnWidth[fieldName]:
                                good = True
                                value = smaller
                                break

                        if not good:
                            value = "#" * columnWidth[fieldName]

                if dataColumn.mask is not None:
                    if dataColumn.mask[row] == defs.VALID:
                        pass
                    elif dataColumn.mask[row] == defs.MISSING:
                        value = "MISSING"
                    elif dataColumn.mask[row] == defs.INVALID:
                        value = "INVALID"
                    else:
                        value = "???"

                values.append(value + ellipsis)

            stream.write(" | ".join(formatting[fieldName] % value for fieldName, value in zip(restriction, values)))
            stream.write(os.linesep)
