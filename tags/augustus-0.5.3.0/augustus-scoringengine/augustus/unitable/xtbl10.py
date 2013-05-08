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

from augustus.core.python3transition import *

from augustus.core.xmlbase import XMLValidationError, load_xsdElement
import augustus.core.xmlbase as xmlbase

class XTBL(xmlbase.XML):
    topTag = "XTBL"
    xsdType = {}
    xsdGroup = {}
    classMap = {}
    
class root(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="XTBL">
        <xs:complexType>
            <xs:sequence>
                <xs:element ref="MetaData" minOccurs="0" />
                <xs:element ref="DataDictionary" />
                <xs:element ref="Pages" />
                <xs:element ref="SeekFooter" />
            </xs:sequence>
            <xs:attribute name="version" type="xs:string" use="required" />
        </xs:complexType>
    </xs:element>
    """)

    tag = "XTBL"

    def __init__(self, fields, types, metadata={}, fieldMetadatas={}):
        self.attrib = {"version": "1.0"}
        if len(metadata) > 0:
            self.children = [MetaData(metadata), DataDictionary(fields, types, fieldMetadatas), Pages(), SeekFooter(0)]
        else:
            self.children = [DataDictionary(fields, types, fieldMetadatas), Pages(), SeekFooter(0)]
        
    def post_validate(self):
        if self.attrib["version"] != "1.0":
            raise XMLValidationError("XTBL version in this file is \"%s\" but this is a XTBL 1.0 interpreter" % self.attrib["version"])

        fields = set()
        for dataField in self.child(DataDictionary).matches(DataField):
            fields.add(dataField.attrib["name"])

        for page in self.child(Pages).matches(Page):
            fields2 = set()
            for pageFieldOffset in page.matches(PageFieldOffset):
                fields2.add(pageFieldOffset.attrib["name"])

            if fields != fields2:
                raise XMLValidationError("PageFieldOffset fields (%s) do not match DataDictionary fields (%s)" % (fields2, fields))

XTBL.classMap["XTBL"] = root

class MetaData(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="MetaData">
      <xs:complexType>
        <xs:sequence>
          <xs:element ref="MetaDataItem" minOccurs="0" maxOccurs="unbounded" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    """)

    tag = "MetaData"

    def __init__(self, metadata):
        self.attrib = {}
        self.children = []
        keys = metadata.keys()
        keys.sort()
        for key in keys:
            self.children.append(MetaDataItem(key, metadata[key]))

XTBL.classMap["MetaData"] = MetaData

class MetaDataItem(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="MetaDataItem">
      <xs:complexType>
        <xs:attribute name="key" type="xs:string" use="required" />
        <xs:attribute name="value" type="xs:string" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    tag = "MetaDataItem"

    def __init__(self, key, value):
        self.attrib = {"key": key, "value": value}
        self.children = []

XTBL.classMap["MetaDataItem"] = MetaDataItem

class DataDictionary(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="DataDictionary">
      <xs:complexType>
        <xs:sequence>
          <xs:element ref="DataField" minOccurs="1" maxOccurs="unbounded" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    """)

    tag = "DataDictionary"

    def __init__(self, fields, types, fieldMetadatas={}):
        self.attrib = {}
        self.children = []
        for field in fields:
            self.children.append(DataField(field, types[field], fieldMetadatas.get(field, {})))

    def __eq__(self, other):
        return self.children == other.children

XTBL.classMap["DataDictionary"] = DataDictionary

class DataField(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="DataField">
      <xs:complexType>
        <xs:sequence>
          <xs:element ref="MetaDataItem" minOccurs="0" maxOccurs="unbounded" />
          <xs:element ref="MapInvalid" minOccurs="0" maxOccurs="unbounded" />
          <xs:element ref="MapMissing" minOccurs="0" maxOccurs="unbounded" />
          <xs:element ref="LookupTable" minOccurs="0" />
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required" />
        <xs:attribute name="dtype" type="xs:string" use="required" />
        <xs:attribute name="type" use="required">
          <xs:simpleType>
            <xs:restriction base="xs:string">
              <xs:enumeration value="category" />
              <xs:enumeration value="string" />
              <xs:enumeration value="object" />
              <xs:enumeration value="integer" />
              <xs:enumeration value="int64" />
              <xs:enumeration value="float" />
              <xs:enumeration value="double" />
            </xs:restriction>
          </xs:simpleType>
        </xs:attribute>
      </xs:complexType>
    </xs:element>
    """)

    tag = "DataField"

    def __init__(self, field, type, metadata={}):
        self.attrib = {"name": field, "type": type}
        self.children = []

        keys = metadata.keys()
        if not isinstance(keys, list): keys = list(keys)
        keys.sort()
        for key in keys:
            self.children.append(MetaDataItem(key, metadata[key]))

    def post_validate(self):
        if self.attrib["type"] == "category":
            self.lookup = self.child(LookupTable, exception=False)
            if self.lookup is None:
                raise XMLValidationError("If type is \"category\", a LookupTable must be provided.")
        else:
            if self.exists(LookupTable):
                raise XMLValidationError("If type is not \"category\", a LookupTable must not be provided.")
            self.lookup = None

        self.mapInvalid = self.matches(MapInvalid)
        self.mapMissing = self.matches(MapMissing)

        for mapping in self.mapInvalid + self.mapMissing:
            if self.attrib["type"] == "integer":
                mapping.setType(int)

            elif self.attrib["type"] == "int64":
                mapping.setType(long)

            elif self.attrib["type"] in ("float", "double"):
                mapping.setType(float)

        if len(self.mapInvalid) == 0: self.isInvalid = lambda value: False
        if len(self.mapMissing) == 0: self.isMissing = lambda value: False

    def isInvalid(self, value):
        for mapping in self.mapInvalid:
            if mapping(value):
                return True
        return False

    def isMissing(self, value):
        for mapping in self.mapMissing:
            if mapping(value):
                return True
        return False

    def __eq__(self, other):
        return self.attrib == other.attrib and \
               set(self.mapInvalid) == set(other.mapInvalid) and \
               set(self.mapMissing) == set(other.mapMissing) and \
               self.lookup == other.lookup

XTBL.classMap["DataField"] = DataField

class MapInvalid(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="MapInvalid">
      <xs:complexType>
        <xs:attribute name="value" type="xs:string" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    def setType(self, cast):
        self.attrib["value"] = cast(self.attrib["value"])

    def isInvalid(self, value):
        return (value == self.attrib["value"])

    def __eq__(self, other):
        return self.attrib["value"] == other.attrib["value"]

XTBL.classMap["MapInvalid"] = MapInvalid

class MapMissing(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="MapMissing">
      <xs:complexType>
        <xs:attribute name="value" type="xs:string" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    def setType(self, cast):
        self.attrib["value"] = cast(self.attrib["value"])

    def isMissing(self, value):
        return (value == self.attrib["value"])

    def __eq__(self, other):
        return self.attrib["value"] == other.attrib["value"]

XTBL.classMap["MapMissing"] = MapMissing

class LookupTable(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="LookupTable">
      <xs:complexType mixed="true">
        <xs:attribute name="delimiter" type="xs:string" use="required" />
        <xs:attribute name="n" type="xs:nonNegativeInteger" use="optional" />
      </xs:complexType>
    </xs:element>
    """)

    tag = "LookupTable"

    def __init__(self, n_to_v={}, delimiter=None):
        if delimiter is None: delimiter = chr(0x007f)
        self.attrib = {"delimiter": delimiter, "n": len(n_to_v)}
        self.children = []
        self.n_to_v = n_to_v

    def serialize(self):
        self.attrib["n"] = len(self.n_to_v)

        keys = self.n_to_v.keys()
        if not isinstance(keys, list): keys = list(keys)
        if len(keys) != 0:
            keys.sort()
            if keys[0] != 0 or keys[-1] != len(keys) - 1:
                raise TypeError("LookupTable.n_to_v keys must be range(len(n_to_v))")

        self.children = [xmlbase.XMLText(self.attrib["delimiter"].join([self.n_to_v[k] for k in keys]))]
        del self.n_to_v

    def post_validate(self):
        xmlText = []
        xmlChildren = []
        for child in self.children:
            if isinstance(child, xmlbase.XMLText):
                xmlText.append(child)
            else:
                xmlChildren.append(child)

        if len(xmlText) == 0 and hasattr(self, "n_to_v"):
            pass

        elif not hasattr(self, "n_to_v"):
            self.n_to_v = dict([(i, x) for i, x in enumerate("".join([str(x) for x in xmlText]).split(self.attrib["delimiter"]))])
            self.children = xmlChildren

        if "n" in self.attrib:
            if self.attrib["n"] != len(self.n_to_v):
                raise XMLValidationError("LookupTable[\"n\"] (%d) should be equal to len(LookupTable.n_to_v) (%d)" % (self.attrib["n"], len(self.n_to_v)))
        else:
            self.attrib["n"] = len(self.n_to_v)

    def __eq__(self, other):
        if hasattr(self, "n_to_v") and hasattr(other, "n_to_v"):
            return self.n_to_v == other.n_to_v
        else:
            return self.content() == other.content()

XTBL.classMap["LookupTable"] = LookupTable

class Pages(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="Pages">
      <xs:complexType>
        <xs:sequence>
          <xs:element ref="Page" minOccurs="1" maxOccurs="unbounded" />
        </xs:sequence>
      </xs:complexType>
    </xs:element>
    """)

    tag = "Pages"

    def __init__(self, pages=None):
        if pages is None: pages = []

        self.attrib = {}
        self.children = pages

XTBL.classMap["Pages"] = Pages

class Page(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="Page">
      <xs:complexType>
        <xs:sequence>
          <xs:element ref="PageFieldOffset" minOccurs="1" maxOccurs="unbounded" />
        </xs:sequence>
        <xs:attribute name="length" type="xs:nonNegativeInteger" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    tag = "Page"

    def __init__(self, length, pageFieldOffsets=None):
        if pageFieldOffsets is None: pageFieldOffsets = []

        self.attrib = {"length": length}
        self.children = pageFieldOffsets

XTBL.classMap["Page"] = Page

class PageFieldOffset(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="PageFieldOffset">
      <xs:complexType>
        <xs:attribute name="name" type="xs:string" use="required" />
        <xs:attribute name="byteOffset" type="xs:nonNegativeInteger" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    tag = "PageFieldOffset"

    def __init__(self, name, byteOffset):
        self.attrib = {"name": name, "byteOffset": byteOffset}
        self.children = []

XTBL.classMap["PageFieldOffset"] = PageFieldOffset

class SeekFooter(XTBL):
    xsd = load_xsdElement(XTBL, """
    <xs:element name="SeekFooter">
      <xs:complexType>
        <xs:attribute name="byteOffset" type="xs:nonNegativeInteger" use="required" />
      </xs:complexType>
    </xs:element>
    """)

    tag = "SeekFooter"

    def __init__(self, name, byteOffset=0):
        self.attrib = {"byteOffset": byteOffset}
        self.children = []

XTBL.classMap["SeekFooter"] = SeekFooter
