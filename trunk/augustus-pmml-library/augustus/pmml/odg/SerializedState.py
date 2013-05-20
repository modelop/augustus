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

"""This module defines the SerializedState class."""

import json
import StringIO
import base64
import zlib

import numpy
from lxml.etree import _Element

from augustus.core.defs import defs
from augustus.core.PmmlBinding import PmmlBinding
from augustus.core.DataTable import DataTable
from augustus.core.DataTableState import DataTableState
from augustus.core.FakeFieldType import FakeFieldType

class SerializedState(PmmlBinding):
    """SerializedState embeds the content of a DataTableState in PMML.

    Each key of the DataTableState is stored as a separate JSON
    object, with Numpy arrays in Base64-NPY format.
    """

    xsd = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="SerializedState">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded" />
                <xs:element ref="SerializedStateKey" minOccurs="0" maxOccurs="unbounded" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""

    xsdAppend = ["""<xs:element name="SerializedStateKey" xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:complexType>
        <xs:complexContent mixed="true">
            <xs:restriction base="xs:anyType">
                <xs:sequence>
                    <xs:any processContents="skip" minOccurs="0" maxOccurs="1"/>
                </xs:sequence>
                <xs:attribute name="key" type="xs:string" use="required" />
            </xs:restriction>
        </xs:complexContent>
    </xs:complexType>
</xs:element>
"""]

    binaryCompression = 1

    class CustomJsonEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, numpy.ndarray):
                data = StringIO.StringIO()
                numpy.save(data, obj)
                data = data.getvalue()

                if self.binaryCompression is not None:
                    data = zlib.compress(data, self.binaryCompression)

                data = base64.b64encode(data)

                return {"__ndarray__": True, "data": data, "compression": (self.binaryCompression or 0)}
            else:
                return super(SerializedStateJSON, self).default(obj)

    @staticmethod
    def customJsonDecoder(obj):
        if obj.get("__ndarray__") is True and "data" in obj:
            data = base64.b64decode(obj["data"])

            if obj.get("compression") > 0:
                data = zlib.decompress(data)

            data = StringIO.StringIO(data)
            return numpy.load(data)
        else:
            return obj

    @classmethod
    def serializeState(cls, modelLoader, dataTableState, restriction=None):
        """Save the contents of a DataTableState as a PMML fragment.

        @type modelLoader: ModelLoader
        @param modelLoader: The ModelLoader that will be used to construct SerializedState and SerializedStateKey elements.
        @type dataTableState: DataTableState
        @param dataTableState: The execution state to turn into PMML.
        @type restriction: list of strings or None
        @param restriction: If None, save all keys; if a list, save only the keys in the list.
        @rtype: SerializedState
        @return: An in-memory SerializedState object that can be embedded in a PMML document.
        """

        E = modelLoader.elementMaker()
        output = E.SerializedState()
                
        cls.CustomJsonEncoder.binaryCompression = cls.binaryCompression

        for key, value in dataTableState.iteritems():
            if restriction is None or key in restriction:
                item = E.SerializedStateKey(key=key)

                if isinstance(value, _Element):
                    item.append(value)
                else:
                    item.text = json.dumps(value, cls=cls.CustomJsonEncoder)

                output.append(item)

        return output

    def unserializeState(self):
        """Load the contents of this SerializedState into a new DataTableState.

        @rtype: DataTableState
        @return: The DataTableState that can be used as an input to new calculations.
        """

        output = DataTableState()

        for serializedStateKey in self.childrenOfTag("SerializedStateKey"):
            if len(serializedStateKey) == 1 and serializedStateKey.text is None:
                value = serializedStateKey.getchildren()[0]

            elif len(serializedStateKey) == 0 and serializedStateKey.text is not None:
                value = json.loads(serializedStateKey.text, object_hook=self.customJsonDecoder)

            else:
                raise defs.PmmlValidationError("SerializedStateKey must contain either an XML tree xor JSON text")

            output[serializedStateKey["key"]] = value

        return output

    def emptyDataTable(self):
        """Construct an empty DataTable from the serialized DataTableFields and DataTableState.

        @rtype: DataTable
        @return: An empty DataTable, suitable for PmmlPlotContent.prepare.
        """

        context = {}
        inputData = {}
        inputState = self.unserializeState()

        for name, value in inputState.iteritems():
            if name.endswith(".context"):
                for fieldName, (dataType, optype) in value.iteritems():
                    context[fieldName] = FakeFieldType(dataType, optype)
                    inputData[fieldName] = []

        return DataTable(context, inputData, inputState=inputState)
