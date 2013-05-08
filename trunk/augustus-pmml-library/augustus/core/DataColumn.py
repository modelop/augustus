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

"""This module defines the DataColumn class."""

import StringIO

import numpy

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP

class DataColumn(object):
    """DataColumn is an immutable, homogeneous type, one-dimensional
    masked dataset.

    Every PMML operation on a DataColumn copies or filters it with
    C{subDataColumn}.  For performance, this function returns a
    shallow copy when possible, so that the potentially large dataset
    is not needlessly copied.  Therefore, DataColumns must be treated
    as immutable to avoid surprising non-local side effects.

    For specific cases in which you know that it is safe to mutate an
    array (and there is some compelling reason to do so), the
    C{_unlock} and C{_lock} methods provide a back-door to making the
    array temporarily mutable.
    """

    # a DataColumn is immutable (unless temporarily unlocked)
    @property
    def fieldType(self):
        return self._fieldType

    @property
    def data(self):
        return self._data

    @property
    def mask(self):
        return self._mask

    def __init__(self, fieldType, data, mask):
        """Create a DataColumn.

        @type fieldType: FieldType
        @param fieldType: Provides an interpretation of the data in C{data}.
        @type data: 1d Numpy array
        @param data: The data.
        @type mask: 1d Numpy array of dtype defs.maskType, or None
        @param mask: If None, all data are treated as valid; otherwise, C{data} rows should only be considered usable if C{mask} is C{defs.VALID}; otherwise they are C{defs.MISSING} or C{defs.INVALID}, as specified by C{mask}.
        """

        self._fieldType = fieldType
        self._data = data
        self._mask = mask
        self._lock()

    def __repr__(self):
        return "<DataColumn (%s %s; %d rows) at 0x%x>" % (self._fieldType.optype, self._fieldType.dataType, len(self.data), id(self))

    def __len__(self):
        """Get the length of the DataColumn.

        @rtype: int
        @return: The number of rows in the DataColumn.
        """

        return len(self.data)

    def __getstate__(self):
        """Used by Pickle to serialize the DataColumn.

        The serialization format for Numpy arrays is its native NPY
        format.
        """

        if isinstance(self._data, tuple):
            data = []
            for d in self._data:
                datum = StringIO.StringIO()
                numpy.save(datum, d)
                datum.seek(0)
                data.append(datum)
        else:
            data = StringIO.StringIO()
            numpy.save(data, self._data)  # should be outside the NumpyInterface
            data.seek(0)

        if self._mask is None:
            mask = None
        elif isinstance(self._mask, tuple):
            mask = []
            for m in self._mask:
                maskum = StringIO.StringIO()
                numpy.save(maskum, m)
                maskum.seek(0)
                mask.append(maskum)
        else:
            mask = StringIO.StringIO()
            numpy.save(mask, self._mask)
            mask.seek(0)

        return {"fieldType": self._fieldType, "data": data, "mask": mask}

    def __setstate__(self, serialization):
        """Used by Pickle to unserialize the DataColumn.

        The serialization format for Numpy arrays is its native NPY
        format.
        """

        self._fieldType = serialization["fieldType"]

        if isinstance(serialization["data"], list):
            self._data = []
            for d in serialization["data"]:
                self._data.append(numpy.load(d))
            self._data = tuple(self._data)
        else:
            self._data = numpy.load(serialization["data"])  # should be outside the NumpyInterface

        if serialization["mask"] is None:
            self._mask = None
        elif isinstance(serialization["mask"], list):
            self._mask = []
            for m in serialization["mask"]:
                self._mask.append(numpy.load(m))
            self._mask = tuple(self._mask)
        else:
            self._mask = numpy.load(serialization["mask"])

        self._lock()
        
    def _lock(self):
        """Back-door to lock the DataColumn and make it immutable again."""

        if isinstance(self._data, tuple):
            for d in self._data:
                d.setflags(write=False)
        else:
            self._data.setflags(write=False)
        if isinstance(self._mask, tuple):
            for m in self._mask:
                m.setflags(write=False)
        elif self._mask is not None:
            self._mask.setflags(write=False)

    def _unlock(self):
        """Back-door to unlock the DataColumn and make it (temporarily?) mutable."""

        if isinstance(self._data, tuple):
            for d in self._data:
                d.setflags(write=True)
        else:
            self._data.setflags(write=True)
        if isinstance(self._mask, tuple):
            for m in self._mask:
                m.setflags(write=True)
        elif self._mask is not None:
            self._mask.setflags(write=True)

    def values(self):
        """Get a Python representation of all of the rows.

        Note that if you only want a few rows, C{value(i)} would not
        need to convert the entire DataColumn.

        @rtype: list of Python representation of the fieldType
        @return: A list of objects.
        """

        return self.fieldType.fromDataColumn(self)

    def value(self, i):
        """Get a Python representation of the value in index i.

        @type i: int
        @param i: The index of the row to convert.
        @rtype: Python representation of the fieldType
        @return: A single object, not a list.
        """

        return self.fieldType.valueToPython(self._data[i])

    def singleton(self):
        """Return the object in the first row.

        @rtype: Python representation of the fieldType
        @return: A single object, not a one-element list.
        """

        return self.value(0)

    def subDataColumn(self, selection=None):
        """Return or filter this DataColumn with C{selection}.

        If C{selection} is None, this function returns a shallow copy
        of the DataColumn.  It has a new Python C{id}, but the
        potentially large numerical array is not copied.  This
        function can therefore be used in performance-critical
        situtations.

        @type selection: 1d Numpy array of dtype bool, or None
        @param selection: If None, simply return the DataColumn; otherwise, use the boolean array to filter it.
        @rtype: DataColumn
        @return: A DataColumn of the same length or shorter.
        """

        if selection is None:
            return DataColumn(self._fieldType, self._data, self._mask)

        else:
            subData = self.data[selection]
            if self.mask is None:
                subMask = None
            else:
                subMask = self.mask[selection]

            if not isinstance(subData, NP.ndarray):
                subData = NP("array", [subData])
                if subMask != None:
                    subMask = NP("array", [subMask])

            return DataColumn(self._fieldType, subData, subMask)

    @staticmethod
    def mapAnyMissingInvalid(masks, missingTo=defs.MISSING, invalidTo=defs.INVALID):
        """Loop over several DataColumn masks and map rows with a
        MISSING or INVALID value in any of the masks to a specified
        value.

        @type masks: list of 1d Numpy arrays of dtype defs.maskType
        @param masks: The input masks.
        @type missingTo: defs.maskType
        @param missingTo: The specified output value for a row with MISSING in any mask.
        @type invalidTo: defs.maskType
        @param invalidTo: The specified output value for a row with INVALID in any mask.
        @rtype: 1d Numpy array of dtype defs.maskType
        @return: The output mask.
        """

        missing = None
        invalid = None
        for mask in masks:
            if mask is not None:
                if missing is None:
                    missing = NP(mask == defs.MISSING)
                else:
                    NP("logical_or", missing, NP(mask == defs.MISSING), missing)

                if invalid is None:
                    invalid = NP(mask == defs.INVALID)
                else:
                    NP("logical_or", invalid, NP(mask == defs.INVALID), invalid)

        if missing is None:
            return None

        missing -= NP("logical_and", missing, invalid)         # INVALID takes precedence over MISSING
        return NP(NP(missing * missingTo) + NP(invalid * invalidTo))
