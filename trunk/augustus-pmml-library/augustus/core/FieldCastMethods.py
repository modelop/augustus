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

"""This module defines the FieldCastMethods class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP

class FieldCastMethods(object):
    """FieldCastMethods is a bag of functions used to cast a
    DataColumn as a new FieldType or to apply
    invalidValueTreatment/mapMissingTo procedures."""

    @staticmethod
    def cast(fieldType, dataColumn):
        """Cast a DataColumn as a new FieldType.

        If the given C{fieldType} is equal to the
        C{dataColumn.fieldType} (C{dataType}, C{optype}, C{values},
        C{intervals}, and C{isCyclic} are equal), then this operation
        is a pass-through.  Also if both C{fieldType} and
        C{dataColumn.fieldType} are ordinal strings, this operation is
        a pass-through (to avoid disrupting the order of the strings).

        @type fieldType: FieldType
        @param fieldType: The new FieldType.
        @type dataColumn: DataColumn
        @param dataColumn: The original DataColumn.
        @rtype: DataColumn
        @return: The same DataColumn or a new DataColumn.
        """

        if dataColumn.fieldType == fieldType:
            return dataColumn

        elif fieldType.dataType == "string" and fieldType.optype == "ordinal" and dataColumn.fieldType.dataType == "string" and dataColumn.fieldType.optype == "ordinal":
            # if you re-cast ordinal an string, every value would be invalid because PMML can only define ordinal values in a DataField
            return dataColumn

        else:
            fieldType._newValuesAllowed = True
            return fieldType.toDataColumn(dataColumn.values(), dataColumn.mask)

    @staticmethod
    def applyInvalidValueTreatment(mask, invalidValueTreatment, overwrite=False):
        """Replace INVALID values with MISSING if invalidValueTreatment is "asMissing".

        This function does not modify the original data (unless
        C{overwrite} is True), but it returns a substitute.  Example
        use::

            mask = dataColumn.mask
            mask = FieldCastMethods.applyInvalidValueTreatment(mask, pmml.get("invalidValueTreatment"))
            return DataColumn(dataColumn.fieldType, dataColumn.data, mask)

        It can also be used in conjunction with other FieldCastMethods.

        @type mask: 1d Numpy array of dtype defs.maskType, or None
        @param mask: The mask.
        @type invalidValueTreatment: string
        @param invalidValueTreatment: One of "returnInvalid", "asIs", "asMissing"; only "asMissing" has an effect.
        @type overwrite: bool
        @param overwrite: If True, temporarily unlike and overwrite the original mask.
        @rtype: 1d Numpy array of dtype defs.maskType
        @return: The new mask.
        """

        if mask is None: return mask

        if invalidValueTreatment == "asMissing":
            if overwrite:
                mask.setflags(write=True)
            else:
                mask = NP("copy", mask)
                mask.setflags(write=True)
            mask[NP(mask == defs.INVALID)] = defs.MISSING

        return mask

    @staticmethod
    def applyMapMissingTo(fieldType, data, mask, mapMissingTo, overwrite=False):
        """Replace MISSING values with a given substitute.

        This function does not modify the original data (unless
        C{overwrite} is True), but it returns a substitute.  Example
        use::

            data, mask = dataColumn.data, dataColumn.mask
            data, mask = FieldCastMethods.applyMapMissingTo(dataColumn.fieldType, data, mask, "-999")
            return DataColumn(dataColumn.fieldType, data, mask)

        It can also be used in conjunction with other FieldCastMethods.

        @type fieldType: FieldType
        @param fieldType: The data fieldType (to interpret C{mapMissingTo}).
        @type data: 1d Numpy array
        @param data: The data.
        @type mask: 1d Numpy array of dtype defs.maskType, or None
        @param mask: The mask.
        @type mapMissingTo: string
        @param mapMissingTo: The replacement value, represented as a string (e.g. directly from a PMML attribute).
        @type overwrite: bool
        @param overwrite: If True, temporarily unlike and overwrite the original mask.
        @rtype: 2-tuple of 1d Numpy arrays
        @return: The new data and mask.
        """

        if mask is None: return data, mask

        if mapMissingTo is not None:
            selection = NP(mask == defs.MISSING)
            try:
                mappedValue = fieldType.stringToValue(mapMissingTo)
            except ValueError as err:
                raise defs.PmmlValidationError("mapMissingTo string \"%s\" cannot be cast as %r: %s" % (mapMissingTo, fieldType, str(err)))

            if overwrite:
                data.setflags(write=True)
                mask.setflags(write=True)
            else:
                data = NP("copy", data)
                mask = NP("copy", mask)

            data[selection] = mappedValue
            mask[selection] = defs.VALID

            if not mask.any():
                mask = None

        return data, mask

    @staticmethod
    def outliersAsMissing(mask, originalMask, selection, overwrite=False):
        """Label all rows specified by a selection as MISSING.

        This function does not modify the original mask (unless
        C{overwrite} is True), but it returns a substitute.  Example
        use::

            mask = dataColumn.mask
            mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, dataColumn.data < MINIMUM_CUT)
            mask = FieldCastMethods.outliersAsMissing(mask, dataColumn.mask, dataColumn.data > MAXIMUM_CUT)
            return DataColumn(dataColumn.fieldType, dataColumn.data, mask)

        It can also be used in conjunction with other FieldCastMethods.

        @type mask: 1d Numpy array of type defs.maskType, or None
        @param mask: The mask to be updated.
        @type originalMask: 1d Numpy array of type defs.maskType, or None
        @param originalMask: The original mask.
        @type selection: 1d Numpy array of bool
        @param selection: The rows to label as MISSING.
        @type overwrite: bool
        @param overwrite: If True, temporarily unlock and overwrite the original mask.
        @rtype: 1d Numpy array of type defs.maskType
        @return: The new mask.
        """

        if mask is None:
            mask = selection * defs.MISSING

        elif mask is originalMask:
            NP("logical_and", selection, NP(mask == defs.VALID), selection)
            if overwrite:
                mask.setflags(write=True)
            else:
                mask = NP("copy", mask)
                mask.setflags(write=True)
            mask[selection] = defs.MISSING

        else:
            NP("logical_and", selection, NP(mask == defs.VALID), selection)
            mask[selection] = defs.MISSING

        return mask
