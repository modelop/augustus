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

"""This module defines the DataTable class."""

import re

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.DataTableFields import DataTableFields
from augustus.core.DataTablePlots import DataTablePlots
from augustus.core.DataTableState import DataTableState
from augustus.core.FieldType import FieldType
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.OrderedDict import OrderedDict
from augustus.core.DataColumn import DataColumn

class DataTable(object):
    """DataTable holds all of the inputs and outputs of the PMML
    evaluation.

    A DataTable is the user's way of interacting with a PMML machine.
    The C{fields} member represents a lexical namespace of all
    currently defined data fields.  Derived fields are added to the
    namespace as they are encountered.  If a derived field is defined
    in a nested scope (e.g. a LocalTransformation of a model), then
    that field is added to the local namespace and would not be
    visible from the top level.  PMML defines OutputFields to emit
    results; these appear in the DataTable's C{output} member.

    Some elements of the PMML machine accumulate data and need to keep
    track of their state.  The C{state} member of the DataTable stores
    this metadata as key-value pairs.

    Plots are another form of output from a (non-standard) PMML
    machine.  Since plots are never intended for subsequent
    calculations, the C{plot} namespace is global.

    Some PMML methods define an overall score that may or may not be
    associated with a field.  This goes into the C{score} member.

    @type fields: DataTableFields
    @param fields: Maps field names to DataColumns.
    @type state: DataTableState
    @param state: Key-value store for persistent state of the PMML machine.
    @type plots: DataTablePlots
    @param plots: Maps plot names to SVG output (global in scope).
    @type output: DataTableFields
    @param output: Maps the result of PMML OutputFields to DataColumns.  These might duplicate the C{fields}.
    @type score: DataColumn or None
    @param score: If the PMML defines a score, this provides access to it; otherwise, it is None.  It might duplicate a C{field} or an C{output}.
    """

    @classmethod
    def singleton(self, inputData, inputMask=None, inputState=None):
        """Create a single-row DataTable for event-based processes.

        This static method is to the DataTable constructor, but it
        creates a DataTable with only one row and it uses the Python
        data type of the C{inputData} to define a type, rather than an
        explicit C{context}.

        @type inputData: dict-like mapping from strings to single values (not lists)
        @param inputData: A single data record.
        @type inputMask: dict-like mapping from strings to single C{defs.maskType} values (not lists), or None
        @param inputMask: A single mask.
        @type inputState: DataTableState or None
        @param inputState: Initial state of the DataTable.  To continue a previous calculation, use the C{dataTable.state} from the previous calculation.
        """

        dataColumns = OrderedDict()
        for fieldName in sorted(inputData.keys()):
            value = inputData[fieldName]

            if isinstance(value, basestring):
                fieldType = FakeFieldType("string", "continuous")
            elif isinstance(value, float):
                fieldType = FakeFieldType("double", "continuous")
            elif isinstance(value, int):
                fieldType = FakeFieldType("integer", "continuous")
            elif isinstance(value, bool):
                fieldType = FakeFieldType("boolean", "continuous")

            # TODO: PMML date types (when passed a datetype.datetype object)

            else:
                fieldType = FakeFieldType("object", "any")

            data = NP("empty", 1, dtype=fieldType.dtype)
            data[0] = value

            if inputMask is None or inputMask.get(fieldName) is None:
                mask = None
            else:
                mask = NP("empty", 1, dtype=defs.maskType)
                mask[0] = inputMask.get(fieldName)

            dataColumns[fieldName] = DataColumn(fieldType, data, mask)

        dataTable = DataTable.__new__(DataTable)
        dataTable._configure(dataColumns, inputState)
        return dataTable

    @classmethod
    def buildManually(self, fieldTypes, internalArrays, internalMasks=None, inputState=None):
        """Create a DataTable from pre-built Numpy arrays filled with
        internal values rather than user-friendly values.  For experts
        only.

        @type fieldTypes: dict of str to FieldTypes
        @param fieldTypes: Maps field names to their FieldType.
        @type internalArrays: dict of str to 1d Numpy arrays.
        @param internalArrays: Maps field names to the internal data.
        @type internalMasks: dict of str to 1d Numpy arrays, or None
        @param internalMasks: Maps field names to the masks, or None for no masks.
        @type inputState: DataTableState or None
        @param inputState: Initial state of the DataTable.  To continue a previous calculation, use the C{dataTable.state} from the previous calculation.
        @raise ValueError: If the C{fieldTypes}, C{internalArrays}, or C{internalMasks} have different field names, this function raises an error.
        """

        if internalMasks is None:
            internalMasks = dict((x, None) for x in internalArrays)

        if set(fieldTypes) != set(internalArrays) or set(fieldTypes) != set(internalMasks):
            raise ValueError("Mismatch between fieldType names, internalArray names, or internalMasks names")

        dataColumns = {}
        for name in sorted(fieldTypes):
            dataColumns[name] = DataColumn(fieldTypes[name], internalArrays[name], internalMasks[name])

        dataTable = DataTable.__new__(DataTable)
        dataTable._configure(dataColumns, inputState)
        return dataTable

    def __init__(self, context, inputData, inputMask=None, inputState=None):
        """Create a DataTable from a type-context, input data,
        possible input masks, and possible input states.

        For maximum flexibility, very few assumptions are made about
        the format of C{inputData}.  It need only have a structure
        that is equivalent to a dictionary mapping strings (field
        names) to lists of values (data columns).  Numpy
        U{record arrays<http://docs.scipy.org/doc/numpy/user/basics.rec.html>},
        U{NpzFiles <http://docs.scipy.org/doc/numpy/reference/generated/numpy.savez.html>},
        and U{Pandas data frames<http://pandas.pydata.org/>}
        effectively present their data in this format because::

            inputData[fieldName]

        yields a column of values.  Regardless of the input type,
        these values are then interpreted by the C{context} to set
        their PMML type.

        The length of the resulting DataTable is equal to the length
        of the shortest DataColumn.  Generally, one should use
        equal-length arrays to build a DataTable.

        @type context: PmmlBinding, FieldType, string, dict, or None
        @param context: If a rooted PmmlBinding, use the PMML's DataDictionary to interpret C{inputData}.  If a FieldType, use that FieldType to interpret all fields.  If a string, use that dataType (e.g. "integer", "dateDaysSince[1960]") to interpret all fields.  If a dictionary from field names to FieldTypes or dataType strings, use them on a per-field basis.  Otherwise, assume a FieldType from the Numpy C{dtype}.  The last option only works if all C{inputData} columns are Numpy arrays.
        @type inputData: any dict-like mapping from strings to lists
        @param inputData: Maps field names (strings) to columns of data (lists or Numpy arrays) that are interpreted by C{context}.
        @type inputMask: dict-like mapping from strings to lists of bool, or None
        @param inputMask: If None, missing data are identified by C{NaN} values in the C{inputData} (Pandas convention).  Otherwise, C{NaN} or a True value in the corresponding {inputMask} would label a data item as MISSING.
        @type inputState: DataTableState or None
        @param inputState: Initial state of the DataTable.  To continue a previous calculation, use the C{dataTable.state} from the previous calculation.
        @raise TypeError: If the C{inputData} columns are not Numpy arrays and a C{context} is not given, this method raises an error.
        """

        if isinstance(context, PmmlBinding) and len(context.xpath("ancestor-or-self::pmml:PMML")) != 0:
            # get types from PMML
            dataColumns = OrderedDict()
            for fieldName, fieldDefinition in context.fieldContext().items():
                fieldType = FieldType(fieldDefinition)

                try:
                    dataField = inputData[fieldName]
                except KeyError:
                    dataField = None
                else:
                    try:
                        maskField = inputMask[fieldName]
                    except (KeyError, TypeError):
                        maskField = None

                if dataField is not None:
                    dataColumns[fieldName] = fieldType.toDataColumn(dataField, maskField)

        else:
            if not isinstance(context, dict):
                context = dict((x, context) for x in inputData)

            if all(isinstance(x, FieldType) for x in context.values()):
                # FieldTypes provided explicitly
                dataColumns = OrderedDict()
                for fieldName in sorted(context.keys()):
                    data = inputData[fieldName]
                    if inputMask is None:
                        mask = None
                    else:
                        mask = inputMask[fieldName]

                    dataColumns[fieldName] = context[fieldName].toDataColumn(data, mask)

            elif all(isinstance(x, basestring) for x in context.values()):
                # FieldTypes provided by dataType name
                dataColumns = OrderedDict()
                for fieldName in sorted(context.keys()):
                    data = inputData[fieldName]
                    if inputMask is None:
                        mask = None
                    else:
                        mask = inputMask[fieldName]

                    if context[fieldName] == "string":
                        fieldType = FakeFieldType(context[fieldName], "categorical")
                    else:
                        fieldType = FakeFieldType(context[fieldName], "continuous")
                    dataColumns[fieldName] = fieldType.toDataColumn(data, mask)

            elif all(isinstance(inputData[x], NP.ndarray) for x in inputData.keys()):
                # FieldTypes provided by NumPy types
                dataColumns = OrderedDict()
                for fieldName in sorted(context.keys()):
                    data = inputData[fieldName]
                    if inputMask is None:
                        mask = None
                    else:
                        mask = inputMask[fieldName]

                    if data.dtype in (NP.object, NP.object0, NP.object_, NP.str, NP.str_, NP.string0, NP.string_) or re.match("\|S[0-9]+", str(data.dtype)) is not None:
                        fieldType = FakeFieldType("string", "categorical")
                    elif data.dtype in (NP.int, NP.int0, NP.int8, NP.int16, NP.int32, NP.int64, NP.int_, NP.integer):
                        fieldType = FakeFieldType("integer", "continuous")
                    elif data.dtype in (NP.float, NP.__getattr__("float16", noneIfMissing=True), NP.float32):
                        fieldType = FakeFieldType("float", "continuous")
                    elif data.dtype in (NP.float64, NP.float128, NP.float_, NP.double):
                        fieldType = FakeFieldType("double", "continuous")
                    elif data.dtype in (NP.bool, NP.bool8, NP.bool_):
                        fieldType = FakeFieldType("boolean", "continuous")
                    else:
                        raise TypeError("Unrecognized NumPy dtype: %r" % data.dtype)

                    dataColumns[fieldName] = fieldType.toDataColumn(data, mask)

            else:
                raise TypeError("Context must be PMML (anchored by a <PMML> ancestor), a dictionary of FieldType objects, dataType strings, or inputData must consist entirely of NumPy arrays")

        self._configure(dataColumns, inputState)

    def _configure(self, dataColumns, inputState):
        """Used by all methods that create DataTables."""

        self.fields = DataTableFields()
        for fieldName, dataColumn in dataColumns.items():
            self.fields[fieldName] = dataColumn

        self.state = DataTableState()
        if inputState is not None:
            self.state = inputState

        self.plots = DataTablePlots()
        self.output = DataTableFields()
        self.output._name = "output"
        self.score = None

    def __len__(self):
        """Get the length of the DataTable.

        @rtype: int
        @return: The number of rows in the DataTable.
        """

        return len(self.fields)

    def subTable(self, selection=None):
        """Return or filter this DataTable with C{selection}.

        This is used to filter data in segmented models, decision
        trees, rulesets, lexical scopes of nested models, etc.  The
        following DataTable attributes are copied into the sub-table:
          - C{fields} because local field names shouldn't appear in
            their parent namespace.  DataColumns associated to field
            names are duplicated with C{subDataColumn}, which merely
            references the immutable data if it is not being filtered.
          - C{output} because outputs are merged as nested algorithms
            pop the stack to return a result.
          - C{score} for the same reason.

        The following DataTable attributes are merely referenced in
        the sub-table:
          - C{state} because the DataTableState has only one key
            namespace
          - C{plots} so that generated plots are not hidden by nested
            namespaces

        @type selection: 1d Numpy array of dtype bool, or None
        @param selection: If None, create a DataTable of the same length; otherwise, use the boolean array to filter it.
        @rtype: DataTable
        @return: A table of the same length or shorter.
        """

        table = self.__class__.__new__(self.__class__)

        # COPY, do not reference, the fields so that local field names don't appear in their parent namespaces
        # (the large data content of the arrays are referenced if unchanged, and treated as immutable for safety)
        table.fields = DataTableFields()
        for fieldName, dataColumn in self.fields.items():
            table.fields[fieldName] = dataColumn.subDataColumn(selection)

        # REFERENCE, do not copy, the state so that a single table accumulates
        table.state = self.state

        # REFERENCE, do not copy, the plots so that a single table accumulates
        table.plots = self.plots

        # create a NEW output, since these are merged as subTables pop
        table.output = DataTableFields()

        # create a NEW score, since these are merged as subTables pop
        table.score = None

        return table

    def __repr__(self):
        return "<DataTable at 0x%x>" % id(self)

    def look(self, head=10, tail=10, restriction=None, stream=None, columnWidth=10):
        """An informative representation of the DataTable, intended
        for interactive use.

        If the DataTable has any C{output}, this method presents a
        table of the C{output}.  Otherwise, it presents the C{fields}.
        For more control, use::

            dataTable.output.look()
            dataTable.fields.look()

        If a C{score} exists, it is presented in its own column,
        possibly duplicating a field if the PMML outputs to a field
        and the global score.

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
        """

        if len(self.output.keys()) > 0:
            self.output.look(head, tail, restriction, stream, columnWidth, self.score)
        else:
            self.fields.look(head, tail, restriction, stream, columnWidth, self.score)
