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

"""This module defines the PlotFormula class."""

from augustus.core.defs import defs
from augustus.core.NumpyInterface import NP
from augustus.core.PmmlBinding import PmmlBinding
from augustus.pmml.odg.Formula import Formula

class PlotFormula(PmmlBinding):
    """Wraps a text-based formula with a role, so that a plotting
    element knows what to do with it.

    PMML content:

      - A string that can be parsed with Formula.

    PMML attributes:

      - role: a string specified by the enclosing PMML element.

    See the source code for the full XSD.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="PlotFormula">
        <xs:complexType mixed="true">
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
            <xs:attribute name="role" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    def evaluate(self, dataTable, functionTable, performanceTable):
        """Evaluate the formula, given input data (most likely a grid
        of sample points) and a function table.

        @type dataTable: DataTable
        @param dataTable: Contains the data to plot.
        @type functionTable: FunctionTable
        @param functionTable: Defines functions that may be used to transform data for plotting.
        @type performanceTable: PerformanceTable
        @param performanceTable: Measures and records performance (time and memory consumption) of the drawing process.
        @rtype: DataColumn
        @return: The result of the expression as a DataColumn.
        """

        parsed = Formula.parse(self.text)
        return parsed.evaluate(dataTable, functionTable, performanceTable)
