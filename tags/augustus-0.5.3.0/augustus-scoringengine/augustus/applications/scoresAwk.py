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

import sys

import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
from augustus.core.xmlbase import XMLValidationError, load_xsdType, load_xsdGroup, load_xsdElement
from augustus.core.scoresfile import ScoresFile
from augustus.core.defs import Atom, NameSpace

globalVariables = NameSpace()

class ScoresAwk(xmlbase.XML):
    topTag = "ScoresAwk"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            pmmlName = (pmmlName for pmmlName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("ScoresAwk class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, pmmlName, *children, **attrib)

class root(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="ScoresAwk">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="FileInput"/>
        <xs:element ref="PythonFunction" minOccurs="1" maxOccurs="unbounded"/>
        <xs:choice>
          <xs:element ref="FileOutput"/>
          <xs:element ref="StandardOutput"/>
        </xs:choice>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.output = self.child(lambda x: isinstance(x, (FileOutput, StandardOutput)))

    def write(self, out, f):
        if isinstance(out, basestring):
            self.output.file.write(out)

        elif isinstance(out, xmlbase.XML):
            self.output.file.write(out.xml())
            self.output.file.write("\n")

        elif out is None:
            pass

        else:
            if not isinstance(out, (list, tuple)):
                try:
                    out = list(out)
                except TypeError:
                    raise RuntimeError("Unrecognized output from function %s: %s" % (f, out))

                for o in out: self.write(o, f)
                
    def evaluate(self):
        begins = self.matches(lambda x: isinstance(x, PythonFunction) and x.condition is PythonFunction.BEGIN)
        ends = self.matches(lambda x: isinstance(x, PythonFunction) and x.condition is PythonFunction.END)
        others = self.matches(lambda x: isinstance(x, PythonFunction) and x.condition is not PythonFunction.BEGIN and x.condition is not PythonFunction.END)

        for f in begins:
            out = f.begin()
            if out is not None:
                self.write(out, f)

        for event in self.child("FileInput").file:
            for f in others:
                out = f.evaluate(event)
                if isinstance(out, (list, tuple)):
                    for o in out:
                        if o is not None:
                            self.write(o, f)
                elif out is not None:
                    self.write(out, f)

        for f in ends:
            out = f.end()
            if out is not None:
                self.write(out, f)

ScoresAwk.classMap["ScoresAwk"] = root

class FileInput(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="FileInput">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Context" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="CastAttribute" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="CastContent" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="fileName" type="xs:string" use="required"/>
      <xs:attribute name="excludeTag" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        context = {}
        for c in self.matches(Context):
            context.update(c.context)

        self.castAttribute = {}
        for c in self.matches(CastAttribute):
            self.castAttribute[c["tag"] + "." + c["attribute"]] = eval(c["type"], context)

        self.castContent = {}
        for c in self.matches(CastContent):
            self.castContent[c["tag"]] = eval(c["type"], context)
            
        if "excludeTag" not in self.attrib:
            self["excludeTag"] = None

        self.file = ScoresFile(self["fileName"], excludeTag=self["excludeTag"], attributeCast=self.castAttribute, contentCast=self.castContent)

ScoresAwk.classMap["FileInput"] = FileInput

class CastAttribute(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="CastAttribute">
    <xs:complexType>
      <xs:attribute name="tag" type="xs:string" use="required"/>
      <xs:attribute name="attribute" type="xs:string" use="required"/>
      <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

ScoresAwk.classMap["CastAttribute"] = CastAttribute

class CastContent(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="CastContent">
    <xs:complexType>
      <xs:attribute name="tag" type="xs:string" use="required"/>
      <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

ScoresAwk.classMap["CastContent"] = CastContent

class Context(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="Context">
    <xs:complexType>
      <xs:attribute name="library" type="xs:string" use="required"/>
      <xs:attribute name="as" type="xs:string" use="optional"/>
      <xs:attribute name="path" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "as" not in self.attrib:
            self.attrib["as"] = self.attrib["library"]

        haspath = ("path" in self.attrib and self.attrib["path"] not in sys.path)
        if haspath: sys.path.append(self.attrib["path"])

        try:
            exec("import %s as tmp" % self.attrib["library"])
        except ImportError, err:
            raise XMLValidationError("Context element could not load library \"%s\"" % self.attrib["library"])

        if haspath: sys.path.remove(self.attrib["path"])

        if self.attrib["as"] == "*":
            self.context = tmp.__dict__
        else:
            self.context = {self.attrib["as"]: tmp}

ScoresAwk.classMap["Context"] = Context

class PythonFunction(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="PythonFunction">
    <xs:complexType>
      <xs:sequence>
          <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
      </xs:sequence>
      <xs:attribute name="condition" type="xs:string" use="optional"/>
      <xs:attribute name="action" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

    BEGIN = Atom("Begin")
    EVENT = Atom("Event")
    END = Atom("End")

    def post_validate(self):
        context = {"g": globalVariables}
        for c in self.matches(Context):
            context.update(c.context)

        cdatas = [i for i in self.children if isinstance(i, xmlbase.XMLCDATA)]
        if len(cdatas) != 1:
            raise XMLValidationError("A PythonFunction object must contain exactly one CDATA")

        theCode = "".join(cdatas[0].text).lstrip().rstrip()

        ## CAREFUL: evaluates whatever you give it!
        try:
            exec theCode in context
        except SyntaxError, err:
            raise XMLValidationError("PythonFunction could not be evaluated: %s" % str(err))

        if "condition" in self.attrib:
            if self["condition"] == "BEGIN":
                self.condition = self.BEGIN

            elif self["condition"] == "END":
                self.condition = self.END

            else:
                try:
                    self.condition = context[self["condition"]]
                    if not callable(self.condition):
                        raise KeyError
                except KeyError:
                    raise XMLValidationError("PythonFunction does not contain a condition function called \"%s\"" % self["condition"])

        else:
            self.condition = self.EVENT

        try:
            self.action = context[self["action"]]
            if not callable(self.action):
                raise KeyError
        except KeyError:
            raise XMLValidationError("PythonFunction does not contain an action function called \"%s\"" % self["action"])

    def begin(self):
        if self.condition is self.BEGIN:
            return self.action()

    def evaluate(self, event):
        if self.condition is self.EVENT:
            result = True
        else:
            result = self.condition(event)

        if result is True:
            return self.action(event)

        elif result is False:
            return None

        else:
            if not isinstance(result, (list, tuple)):
                try:
                    result = list(result)
                except TypeError:
                    raise RuntimeError("A PythonFunction's condition must return True, False, or a list of objects to act upon; result of %s is %s" % (self.condition, result))

            output = []
            for r in result:
                output.append(self.action(r))
            return output
                
    def end(self):
        if self.condition is self.END:
            return self.action()

ScoresAwk.classMap["PythonFunction"] = PythonFunction

class FileOutput(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="FileOutput">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
      <xs:attribute name="append" type="xs:boolean" default="false" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "append" not in self.attrib:
            self["append"] = False
        
        self.file = file(self["fileName"], "a" if self["append"] else "w")

ScoresAwk.classMap["FileOutput"] = FileOutput

class StandardOutput(ScoresAwk):
    xsd = load_xsdElement(ScoresAwk, """
  <xs:element name="StandardOutput">
    <xs:complexType/>
  </xs:element>
  """)

    def post_validate(self):
        self.file = sys.stdout

ScoresAwk.classMap["StandardOutput"] = StandardOutput
