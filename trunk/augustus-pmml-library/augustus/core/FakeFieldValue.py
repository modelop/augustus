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

"""This module defines the FakeFieldValue class."""

class FakeFieldValue(object):
    """FakeFieldValue is a programmatic substitute for a PMML <Value>
    element, usually used in a FakeFieldType.
    """

    def __init__(self, value, displayValue=None, property="valid"):
        """Initialize as FakeFieldValue.

        @type value: string
        @param value: The value in string form (as it would appear in a PMML document).
        @type displayValue: string
        @param displayValue: The displayValue, as defined by PMML.
        @type property: string
        @param property: The value's property, as defined by PMML ("valid", "invalid", "missing").
        """

        self._value = value
        self._displayValue = displayValue
        self._property = property

    def __getitem__(self, attribute):
        """Same as get(attribute)."""

        return self.get(attribute)

    def get(self, attribute, default=None):
        """Simulates the get method of a PmmlBinding for <Value> tagnames.

        @type attribute: string
        @param attribute: The attribute to get.
        @type default: string or None
        @param default: The result when C{attribute} is not one of "value", "displayValue", or "property".
        @rtype: string or None
        @return: The simulated attribute value or None if the attribute does not exist.
        """

        if attribute == "value":
            return self._value

        elif attribute == "displayValue":
            if self._displayValue is None:
                return default
            else:
                return self._displayValue

        elif attribute == "property":
            return self._property
        else:
            return None
