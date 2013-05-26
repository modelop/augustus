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

"""This module defines the AvroDataTableStream class."""

import glob
import json

from augustus.core.DataTable import DataTable
from augustus.core.DataTableState import DataTableState
from augustus.core.FieldType import FieldType
from augustus.core.FakeFieldType import FakeFieldType
from augustus.core.FakeFieldValue import FakeFieldValue

try:
    from augustus.dataio.avrostream import InputStream
except ImportError:
    InputStream = None

class AvroDataTableStream(object):
    def _setupMaps(self, fieldType):
        fieldType._stringToValue = {}
        fieldType._valueToString = {}
        fieldType._displayValue = {}
        for index, valueObject in enumerate(fieldType.values):
            string = valueObject.get("value")
            fieldType._stringToValue[string] = index
            fieldType._valueToString[index] = string
            fieldType._displayValue[string] = valueObject.get("displayValue", string)

    def __init__(self, fileNames, namesToFieldTypes=None, namesToAvroPaths=None, inputState=None, chunkSize=1000000):
        if InputStream is None:
            raise RuntimeError("The optional augustus.avrostream module is required for \"AvroDataTableStream\" but it hasn't been installed;%sRecommendation: re-build Augustus with \"python setup.py install --with-avrostream\"" % (os.linesep, os.linesep))

        self.fileNames = glob.glob(fileNames)
        if len(self.fileNames) == 0:
            raise IOError("No files matched the fileName pattern \"%s\"" % fileNames)

        self.schema = None
        for fileName in self.fileNames:
            inputStream = InputStream()
            inputStream.start(fileName, 0, {}, {})
            schema = json.loads(inputStream.schema())
            if self.schema is not None and schema != self.schema:
                raise ValueError("these files do not all have the same schema")
            self.schema = schema

        if self.schema["type"] != "record":
            raise TypeError("Top level of schema must describe a record, not %r" % self.schema)

        if namesToFieldTypes is None:
            namesToFieldTypes = dict((x["name"], None) for x in self.schema["fields"])

        if isinstance(namesToFieldTypes, (list, tuple)):
            namesToFieldTypes = dict((x, None) for x in namesToFieldTypes)

        if namesToAvroPaths is None:
            self.namesToAvroPaths = {}
            for name in namesToFieldTypes:
                self.namesToAvroPaths[name] = (name,)
        else:
            self.namesToAvroPaths = dict(namesToAvroPaths)
            for path in self.namesToAvroPaths:
                if isinstance(path, basestring):
                    self.namesToAvroPaths = (path,)

        self.namesToFieldTypes = dict(namesToFieldTypes)
        for name, fieldType in namesToFieldTypes.items():
            schemaObject = self.schema
            path = self.namesToAvroPaths[name]

            for pathname in path:
                if schemaObject["type"] != "record":
                    raise LookupError("path %r not found in the schema" % path)
                
                fieldNames = [x["name"] for x in schemaObject["fields"]]
                if pathname not in fieldNames:
                    raise LookupError("path %r not found in the schema" % path)

                schemaObject, = (x for x in schemaObject["fields"] if x["name"] == pathname)

            avroType = schemaObject["type"]
            if isinstance(avroType, dict):
                avroType = avroType["type"]

            if avroType == "enum":
                values = [FakeFieldValue(x) for x in schemaObject["type"]["symbols"]]
            else:
                values = []

            if fieldType == "string":
                self.namesToFieldTypes[name] = FakeFieldType("string", "continuous")
            elif fieldType == "categorical":
                self.namesToFieldTypes[name] = FakeFieldType("string", "categorical", values=values)
                self._setupMaps(self.namesToFieldTypes[name])
            elif fieldType == "ordinal":
                self.namesToFieldTypes[name] = FakeFieldType("string", "ordinal", values=values)
                self._setupMaps(self.namesToFieldTypes[name])
            elif isinstance(fieldType, basestring):
                self.namesToFieldTypes[name] = FakeFieldType(fieldType, "continuous")
            elif fieldType is None:
                if avroType in ("null", "record", "array", "map", "fixed"):
                    self.namesToFieldTypes[name] = FakeFieldType("object", "any")
                elif avroType in ("int", "long"):
                    self.namesToFieldTypes[name] = FakeFieldType("integer", "continuous")
                elif avroType == "float":
                    self.namesToFieldTypes[name] = FakeFieldType("float", "continuous")
                elif avroType == "double":
                    self.namesToFieldTypes[name] = FakeFieldType("double", "continuous")
                elif avroType in ("bytes", "string"):
                    self.namesToFieldTypes[name] = FakeFieldType("string", "continuous")
                elif avroType == "enum":
                    self.namesToFieldTypes[name] = FakeFieldType("string", "categorical", values=values)
                    self._setupMaps(self.namesToFieldTypes[name])
                else:
                    raise TypeError("Unrecognized Avro type: %s" % avroType)

            fieldType = self.namesToFieldTypes[name]
            if not isinstance(fieldType, FieldType):
                raise TypeError("namesToFieldTypes must map to FieldTypes")

            if fieldType in ("float", "boolean", "date", "time", "dateTime", "dateDaysSince[0]", "dateDaysSince[1960]", "dateDaysSince[1970]", "dateDaysSince[1980]", "timeSeconds", "dateTimeSecondsSince[0]", "dateTimeSecondsSince[1960]", "dateTimeSecondsSince[1970]", "dateTimeSecondsSince[1980]"):
                raise NotImplementedError

            if fieldType.dataType == "object":
                raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType == "string":
                if fieldType.optype == "continuous":
                    if avroType not in ("string", "bytes"):
                        raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))
                elif fieldType.optype == "categorical":
                    if avroType != "enum":
                        raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))
                elif fieldType.optype == "ordinal":
                    if avroType != "enum":
                        raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType in ("integer", "dateDaysSince[0]", "dateDaysSince[1960]", "dateDaysSince[1970]", "dateDaysSince[1980]", "timeSeconds", "dateTimeSecondsSince[0]", "dateTimeSecondsSince[1960]", "dateTimeSecondsSince[1970]", "dateTimeSecondsSince[1980]"):
                if avroType not in ("int", "long"):
                    raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType == "float":
                if avroType != "float":
                    raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType == "double":
                if avroType != "double":
                    raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType == "boolean":
                if avroType != "boolean":
                    raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

            elif fieldType.dataType in ("date", "time", "dateTime"):
                if avroType != "string":
                    raise TypeError("PMML type %r and Avro type \"%s\" are incompatible" % (fieldType, avroType))

        self.inputState = inputState
        self.chunkSize = chunkSize

    def _removeUnicode(self, obj):
        if isinstance(obj, unicode):
            return obj.encode("utf-8")
        elif isinstance(obj, dict):
            return dict((self._removeUnicode(x), self._removeUnicode(y)) for x, y in obj.items())
        elif isinstance(obj, (list, tuple)):
            return [self._removeUnicode(x) for x in obj]
        else:
            return obj

    def __iter__(self):
        types = {}
        for name, fieldType in self.namesToFieldTypes.items():
            name = self._removeUnicode(name)

            if fieldType.dataType == "string":
                if fieldType.optype == "continuous":
                    types[name] = "string"
                elif fieldType.optype in ("categorical", "ordinal"):
                    types[name] = "category"
            elif fieldType.dataType in ("integer", "dateDaysSince[0]", "dateDaysSince[1960]", "dateDaysSince[1970]", "dateDaysSince[1980]", "timeSeconds", "dateTimeSecondsSince[0]", "dateTimeSecondsSince[1960]", "dateTimeSecondsSince[1970]", "dateTimeSecondsSince[1980]"):
                types[name] = "integer"
            elif fieldType.dataType == "double":
                types[name] = "double"

            if name not in types:
                raise TypeError("Cannot match %r to an extraction type" % fieldType)

        if len(types) == 0:
            raise TypeError("At least one field must be selected")

        namesToAvroPaths = self._removeUnicode(self.namesToAvroPaths)

        if self.inputState is None:
            self.inputState = DataTableState()

        for fileName in self.fileNames:
            inputStream = InputStream()

            inputStream.start(fileName, self.chunkSize, namesToAvroPaths, types)
            while True:
                arrays = inputStream.next()
                yield DataTable.buildManually(self.namesToFieldTypes, arrays, inputState=self.inputState)
                if len(arrays.values()[0]) < self.chunkSize:
                    break
