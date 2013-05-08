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

import random
import math
import string

import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
from augustus.core.xmlbase import XMLValidationError, load_xsdType, load_xsdGroup, load_xsdElement

class PmmlSplit(xmlbase.XML):
    topTag = "PmmlSplit"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            pmmlName = (pmmlName for pmmlName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("PmmlSplit class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, pmmlName, *children, **attrib)

class root(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="PmmlSplit">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="FileInput"/>
        <xs:choice>
          <xs:element ref="LoadBalanceSplit"/>
          <xs:element ref="LogicalSplit"/>
        </xs:choice>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  """)

    def evaluate(self):
        # evaluate the subelement with index=1, whatever that is (check XSD!)
        self.child(which=1).evaluate(self.child(FileInput).data)

PmmlSplit.classMap["PmmlSplit"] = root

class FileInput(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="FileInput">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.data = xmlbase.loadfile(self["fileName"], pmml.X_ODG_PMML, validation=False)

PmmlSplit.classMap["FileInput"] = FileInput

class LoadBalanceSplit(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="LoadBalanceSplit">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="FileOutput" minOccurs="1" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="how" default="sequential" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="sequential"/>
            <xs:enumeration value="random"/>
            <xs:enumeration value="textBalanced"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "how" not in self.attrib:
            self["how"] = "random"

        self.numFiles = len(self.matches(FileOutput))

    def evaluate(self, data):
        segmentation = data.descendant(pmml.Segmentation, exception=False)
        if segmentation is None:
            raise RuntimeError("PMML file has no <Segmentation> block; cannot split!")

        # pull out the segments and drop them from the file
        segments = segmentation.matches(pmml.Segment)
        segmentation.children = []

        outputSegments = []
        if self["how"] == "sequential":
            stepSize = int(math.ceil(len(segments)/float(self.numFiles)))
            for i in xrange(self.numFiles):
                outputSegments.append(segments[(i)*stepSize:(i+1)*stepSize])

        elif self["how"] == "random":
            random.shuffle(segments)
            stepSize = int(math.ceil(len(segments)/float(self.numFiles)))
            for i in xrange(self.numFiles):
                outputSegments.append(segments[(i)*stepSize:(i+1)*stepSize])

        elif self["how"] == "textBalanced":
            raise NotImplementedError("how == textBalanced not implemented yet")

        for i, fileOutput in enumerate(self.matches(FileOutput)):
            output = data.copy()  # copied without the segments
            output.descendant(pmml.Segmentation).children = outputSegments[i]
            output.write(fileOutput["fileName"])

PmmlSplit.classMap["LoadBalanceSplit"] = LoadBalanceSplit

class LogicalSplit(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="LogicalSplit">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Contains" minOccurs="1" maxOccurs="unbounded"/>
        <xs:element ref="FileOutput" minOccurs="0" maxOccurs="1"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.fileOutput = self.child(FileOutput, exception=False)

    def evaluate(self, data):
        segmentation = data.descendant(pmml.Segmentation, exception=False)
        if segmentation is None:
            raise RuntimeError("PMML file has no <Segmentation> block; cannot split!")

        # pull out the segments and drop them from the file
        segments = segmentation.matches(pmml.Segment)
        segmentation.children = []

        for contains in self.matches(Contains):
            matched = []
            nonMatched = []

            for segment in segments:
                if contains.evaluate(segment.child(pmml.nonExtension)):  # assume first nonExtension is the predicate
                    matched.append(segment)
                else:
                    nonMatched.append(segment)

            output = data.copy()  # copied without the segments
            output.descendant(pmml.Segmentation).children = matched
            output.write(contains.fileOutput["fileName"])

            # next search will use only the remainder
            segments = nonMatched

        if self.fileOutput is not None:
            output = data.copy()  # copied without the segments
            output.descendant(pmml.Segmentation).children = segments  # whatever's left
            output.write(self.fileOutput["fileName"])

PmmlSplit.classMap["LogicalSplit"] = LogicalSplit

class FileOutput(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="FileOutput">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

PmmlSplit.classMap["FileOutput"] = FileOutput

class Contains(PmmlSplit):
    xsd = load_xsdElement(PmmlSplit, """
  <xs:element name="Contains">
    <xs:complexType>
      <xs:sequence>
        <xs:choice>
          <xs:element ref="SimplePredicate"/>
          <xs:element ref="CompoundPredicate"/>
          <xs:element ref="SimpleSetPredicate"/>
          <xs:element ref="True"/>
          <xs:element ref="False"/>
        </xs:choice>
        <xs:element ref="FileOutput"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.fileOutput = self.child(FileOutput)

    def evaluate(self, predicate):
        myPredicate = self.child()

        if myPredicate.same(predicate):
            return True

        for i, elem in predicate.walk():
            if isinstance(elem, (pmml.SimplePredicate, pmml.CompoundPredicate, pmml.SimpleSetPredicate, pmml.pmmlTrue, pmml.pmmlFalse)):
                if myPredicate.same(elem):
                    return True

        return False

PmmlSplit.classMap["Contains"] = Contains

# just inherit these from PMML's specification

class SimplePredicate(PmmlSplit, pmml.SimplePredicate):
    def post_validate(self):
        pass

    def same(self, predicate):
        if self.tag != predicate.tag: return False
        if self["field"] != predicate["field"]: return False
        if self["operator"] != predicate["operator"]: return False
        if "value" in self.attrib and "value" not in predicate.attrib: return False
        if "value" not in self.attrib and "value" in predicate.attrib: return False
        if "value" not in self.attrib and "value" not in predicate.attrib: return True
        if self["value"] != predicate["value"]: return False
        return True

PmmlSplit.classMap["SimplePredicate"] = SimplePredicate

def _byhash(a, b):
    return cmp(a.hashValue, b.hashValue)

class CompoundPredicate(PmmlSplit, pmml.CompoundPredicate):
    def post_validate(self):
        pass

    def same(self, predicate):
        if self.tag != predicate.tag: return False
        if self["booleanOperator"] != predicate["booleanOperator"]: return False

        if not hasattr(self, "checklist"):
            self.checklist = []
            for child in self:
                child.hashValue = hash(repr(sorted((child.attrib).items())))
                self.checklist.append(child)
            self.checklist.sort(_byhash)

        if not hasattr(predicate, "checklist"):
            predicate.checklist = []
            for child in predicate:
                child.hashValue = hash(repr(sorted((child.attrib).items())))
                predicate.checklist.append(child)
            predicate.checklist.sort(_byhash)

        if len(self.checklist) != len(predicate.checklist): return False

        for myChild, theirChild in zip(self.checklist, predicate.checklist):
            if not myChild.same(theirChild):
                return False
        return True

PmmlSplit.classMap["CompoundPredicate"] = CompoundPredicate

class SimpleSetPredicate(PmmlSplit, pmml.SimpleSetPredicate):
    def post_validate(self):
        pass

    def same(self, predicate):
        if self.tag != predicate.tag: return False
        if self["field"] != predicate["field"]: return False
        if self["booleanOperator"] != predicate["booleanOperator"]: return False

        myArray = self.child(pmml.Array)
        theirArray = predicate.child(pmml.Array)
        if myArray["type"] != theirArray["type"]: return False
        if arrayValues(myArray) != arrayValues(theirArray): return False

        return True

PmmlSplit.classMap["SimpleSetPredicate"] = SimpleSetPredicate

class Array(PmmlSplit, pmml.Array):
    def post_validate(self):
        pass

def arrayValues(array):
    if not hasattr(array, "converted"):
        xmlText = []
        for child in array.children:
            if isinstance(child, xmlbase.XMLText):
                xmlText.append(child)
        textExpected = xmlbase.XSDList("Array", "xs:string")
        array.converted = textExpected.validateText("".join([str(x) for x in xmlText]).lstrip(string.whitespace).rstrip(string.whitespace))
    return array.converted
            
PmmlSplit.classMap["Array"] = Array

class pmmlTrue(PmmlSplit, pmml.pmmlTrue):
    def post_validate(self):
        pass

    def same(self, predicate):
        return self.tag == predicate.tag

PmmlSplit.classMap["True"] = pmmlTrue

class pmmlFalse(PmmlSplit, pmml.pmmlFalse):
    def post_validate(self):
        pass

    def same(self, predicate):
        return self.tag == predicate.tag

PmmlSplit.classMap["False"] = pmmlFalse

class Extension(PmmlSplit, pmml.Extension):
    def post_validate(self):
        pass

PmmlSplit.classMap["Extension"] = Extension
