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

"""This module defines the FakeFieldType class."""

from augustus.core.FieldType import FieldType

class FakeFieldType(FieldType):
    """FakeFieldType is a programmatic substitute for a FieldType.

    FieldTypes are used to describe all data in a DataTable, but they
    require PmmlBindings (DataFields or DerivedFields) to create them.
    Often, you need to build FieldTypes without reference to a PMML
    document (defining the output of a new model or transformation,
    for instance).  A FakeFieldType can be used anywhere a FieldType
    is expected, but it can be built programmatically.
    """

    def __init__(self, dataType, optype, values=None, intervals=None, isCyclic=False):
        """Initialize a FakeFieldType.

        @type dataType: string
        @param dataType: The dataType name as defined by PMML (e.g. "string", "double", "integer").
        @type optype: string
        @param optype: The optype name as defined by PMML ("categorical", "ordinal", "continuous").
        @type values: list of PmmlBinding or FakeFieldValue
        @param values: List of allowed or special values, as defined by PMML.
        @type intervals: list of PmmlBinding
        @param intervals: List of allowed intervals, as defined by PMML.
        @type isCyclic: bool
        @param isCyclic: Labels the field as cyclic; used by some parts of PMML.
        """

        self._dataType = dataType
        self._optype = optype
        if values is None:
            self._values = []
        else:
            self._values = values
        if intervals is None:
            self._intervals = []
        else:
            self._intervals = intervals
        self._isCyclic = isCyclic

        self._setup()

    def __getstate__(self):
        """Used by Pickle to serialize the PmmlBinding."""

        return {"dataType": self._dataType, "optype": self._optype, "values": self._values, "intervals": self._intervals, "isCyclic": self._isCyclic}

    def __setstate__(self, serialization):
        """Used by Pickle to unserialize the PmmlBinding."""

        self._dataType = serialization["dataType"]
        self._optype = serialization["optype"]
        self._values = serialization["values"]
        self._intervals = serialization["intervals"]
        self._isCyclic = serialization["isCyclic"]
        self._setup()

    @property
    def dataType(self):
        return self._dataType

    @property
    def optype(self):
        return self._optype

    @property
    def values(self):
        return self._values

    @property
    def intervals(self):
        return self._intervals

    @property
    def isCyclic(self):
        return self._isCyclic
