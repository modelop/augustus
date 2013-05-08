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
import os
import string
import re
import math

import augustus.core.xmlbase as xmlbase
import augustus.core.pmml41 as pmml
from augustus.core.xmlbase import XMLValidationError, load_xsdType, load_xsdGroup, load_xsdElement
from augustus.core.defs import Atom, NameSpace

globalVariables = NameSpace()

class PmmlSed(xmlbase.XML):
    topTag = "PmmlSed"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            pmmlName = (pmmlName for pmmlName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("PmmlSed class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, pmmlName, *children, **attrib)

class root(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="PmmlSed">
    <xs:complexType>
      <xs:sequence>
        <xs:choice>
          <xs:element ref="PythonFunction"/>
          <xs:sequence minOccurs="1" maxOccurs="unbounded">
            <xs:element ref="Pattern"/>
            <xs:choice>
              <xs:element ref="Replacement"/>
              <xs:element ref="Insert"/>
              <xs:element ref="Append"/>
            </xs:choice>
          </xs:sequence>
        </xs:choice>
        <xs:choice>
          <xs:element ref="FileInput"/>
          <xs:element ref="StandardInput"/>
        </xs:choice>
        <xs:choice>
          <xs:element ref="FileOutput"/>
          <xs:element ref="StandardOutput"/>
        </xs:choice>
      </xs:sequence>
      <xs:attribute name="mode" use="optional" default="replaceAll">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="replaceAll"/>
            <xs:enumeration value="replaceFirst"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "mode" not in self.attrib:
            self["mode"] = "replaceAll"

        if self.exists(PythonFunction):
            self.transformation = self.child(PythonFunction)
        else:
            self.patterns = self.matches(Pattern)
            self.replacements = self.matches(Replacement)
            if len(self.patterns) != len(self.replacements):
                raise Exception("why didn't this fail validation???")

        if self.exists(FileInput):
            self.input = self.child(FileInput)
        else:
            self.input = self.child(StandardInput)

        if self.exists(FileOutput):
            self.output = self.child(FileOutput)
        else:
            self.output = self.child(StandardOutput)

    def evaluate(self):
        if self.exists(PythonFunction):
            if self.transformation.begin is not None:
                self.transformation.begin()

            output = self.transformation.evaluate(self.input.data)

            if self.transformation.end is not None:
                self.transformation.end()

        else:
            output = self.input.data
            for i in xrange(len(self.patterns)):
                output = self.patternReplacement(output, i)
            if isinstance(output, (list, tuple)):
                raise RuntimeError("Top-level replacement must be a single element")

        self.output.write(output)

    def patternReplacement(self, pmmlSnippet, i):
        matchedVariables, namedGroups = self.patterns[i].evaluate(pmmlSnippet)
        if matchedVariables is None:
            oldChildren = getattr(pmmlSnippet, "children", None)

            if oldChildren is not None:
                newChildren = []

                for child in pmmlSnippet.children:
                    out = self.patternReplacement(child, i)
                    if isinstance(out, (tuple, list)):
                        newChildren.extend(out)
                    else:
                        newChildren.append(out)

                pmmlSnippet.children = newChildren

            return pmmlSnippet

        else:
            return self.replacements[i].evaluate(pmmlSnippet, matchedVariables, namedGroups)
            
PmmlSed.classMap["PmmlSed"] = root

class Context(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
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

PmmlSed.classMap["Context"] = Context

class PythonFunction(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="PythonFunction">
    <xs:complexType>
      <xs:sequence>
          <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
      </xs:sequence>
      <xs:attribute name="name" type="xs:string" use="required"/>
      <xs:attribute name="begin" type="xs:string" use="optional"/>
      <xs:attribute name="end" type="xs:string" use="optional"/>
      <xs:attribute name="deepestNode" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
  """)

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

        try:
            self.func = context[self["name"]]
            if not callable(self.func):
                raise KeyError
        except KeyError:
            raise XMLValidationError("PythonFunction does not contain a function called \"%s\"" % self["name"])

        if "begin" in self.attrib:
            try:
                self.begin = context[self["begin"]]
                if not callable(self.begin):
                    raise KeyError
            except KeyError:
                raise XMLValidationError("PythonFunction does not contain a function called \"%s\"" % self["begin"])
        else:
            self.begin = None

        if "end" in self.attrib:
            try:
                self.end = context[self["end"]]
                if not callable(self.end):
                    raise KeyError
            except KeyError:
                raise XMLValidationError("PythonFunction does not contain a function called \"%s\"" % self["end"])
        else:
            self.end = None

        self.deepestNode = self.attrib.get("deepestNode", None)

    def evaluate(self, pmmlSnippet):
        output = self.func(pmmlSnippet)

        descend = (hasattr(pmmlSnippet, "tag") and pmmlSnippet.tag != self.deepestNode)
        if output is None:
            if hasattr(pmmlSnippet, "children"):
                newChildren = []

                for child in pmmlSnippet.children:
                    if descend:
                        out = self.evaluate(child)
                    else:
                        out = child

                    if isinstance(out, (tuple, list)):
                        newChildren.extend(out)
                    else:
                        newChildren.append(out)

                pmmlSnippet.children = newChildren

            return pmmlSnippet

        else:
            return output

PmmlSed.classMap["PythonFunction"] = PythonFunction

class NamedGroupMatch(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="NamedGroupMatch">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="required" />
      <xs:attribute name="maxMatch" type="xs:nonNegativeInteger" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

PmmlSed.classMap["NamedGroupMatch"] = NamedGroupMatch

class Pattern(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="Pattern">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:any minOccurs="1" maxOccurs="1" processContents="skip" />
          </xs:sequence> 
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
  """)

    regex = re.compile("^/(.*)/([ilmsux]*)$")

    def flags(self, f):
        output = 0
        if "i" in f or "I" in f: output += re.I
        if "l" in f or "L" in f: output += re.L
        if "m" in f or "M" in f: output += re.M
        if "s" in f or "S" in f: output += re.S
        if "u" in f or "U" in f: output += re.U
        if "x" in f or "X" in f: output += re.X
        return output

    def match(self, pattern, target, variables, namedGroups):
        if isinstance(pattern, xmlbase.XMLSpecial) and isinstance(target, xmlbase.XMLSpecial):
            return pattern == target

        elif isinstance(pattern, xmlbase.XMLSpecial) and not isinstance(target, xmlbase.XMLSpecial):
            return False

        elif not isinstance(pattern, xmlbase.XMLSpecial) and isinstance(target, xmlbase.XMLSpecial):
            return False

        if pattern.tag != target.tag:
            return False

        newVariables = {}

        for key, value in pattern.attrib.items():
            if key not in target.attrib:
                return False

            if value[0] == "{" and value[-1] == "}":
                newVariables[value[1:-1]] = target[key]

            else:
                m = re.match(self.regex, value)
                if m is not None:
                    patty, flags = m.groups()
                    if re.match(patty, str(target[key]), self.flags(flags)) is None:
                        return False

                else:
                    if value != str(target[key]):
                        return False

        patternIndex = 0
        targetIndex = 0

        while patternIndex < len(pattern.children) and targetIndex < len(target.children):
            done = False

            while isinstance(pattern[patternIndex], xmlbase.XMLSpecial) and not isinstance(pattern[patternIndex], xmlbase.XMLText):
                patternIndex += 1
                if patternIndex >= len(pattern.children):
                    done = True
                    break

            while isinstance(target[targetIndex], xmlbase.XMLSpecial) and not isinstance(target[targetIndex], xmlbase.XMLText):
                targetIndex += 1
                if targetIndex >= len(target.children):
                    done = True
                    break

            if done: break

            pat = pattern[patternIndex]
            tar = target[targetIndex]

            if isinstance(pat, xmlbase.XMLText) and not isinstance(tar, xmlbase.XMLText):
                return False

            elif not isinstance(pat, xmlbase.XMLText) and isinstance(tar, xmlbase.XMLText):
                return False

            elif isinstance(pat, xmlbase.XMLText) and isinstance(tar, xmlbase.XMLText):
                pattext = "".join(pat.text).lstrip(string.whitespace).rstrip(string.whitespace)
                tartext = "".join(tar.text).lstrip(string.whitespace).rstrip(string.whitespace)

                if pattext[0] == "{" and pattext[-1] == "}":
                    newVariables[pattext[1:-1]] = tartext

                else:
                    m = re.match(self.regex, pattext)
                    if m is not None:
                        patty, flags = m.groups()
                        if re.match(patty, tartext, self.flags(flags)) is None:
                            return False

                    else:
                        if pattext != tartext:
                            return False

            elif isinstance(pat, NamedGroupMatch):
                if pat["name"] not in namedGroups:
                    namedGroups[pat["name"]] = []

                namedGroups[pat["name"]].append(tar)

                if "maxMatch" in pat.attrib and len(namedGroups[pat["name"]]) >= pat["maxMatch"]:
                    patternIndex += 1
                targetIndex += 1

            else:
                output = self.match(pat, tar, newVariables, namedGroups)
                if output == False:
                    return False

                patternIndex += 1
                targetIndex += 1

        variables.update(newVariables)
        return True

    def evaluate(self, pmmlSnippet):
        variables = {}
        namedGroups = {}
        if self.match(self.child(), pmmlSnippet, variables, namedGroups):
            return variables, namedGroups
        else:
            return None, None

PmmlSed.classMap["Pattern"] = Pattern

class Replacement(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="Replacement">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
            <xs:any minOccurs="0" maxOccurs="unbounded" processContents="skip" />
          </xs:sequence>
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.context = {}
        for child in self.matches(Context):
            self.context.update(child.context)

    def construct(self, pattern, variables, namedGroups):
        if isinstance(pattern, xmlbase.XMLText):
            text = "".join(pattern.text).lstrip(string.whitespace).rstrip(string.whitespace)
            if text[0] == "{" and text[-1] == "}":
                ## CAREFUL: another string evaluation as Python code!
                text = str(eval(text[1:-1], variables))
            return xmlbase.XMLText(text)

        elif isinstance(pattern, xmlbase.XMLSpecial):
            return pattern.copy()

        elif isinstance(pattern, NamedGroupMatch):
            return [i.copy() for i in namedGroups[pattern["name"]]]

        else:
            pmmlClass = pmml.X_ODG_PMML.classMap.get(pattern.tag, None)
            if pmmlClass is None:
                output = xmlbase.XML.__new__(xmlbase.XML)
            else:
                output = pmmlClass.__new__(pmmlClass)

            output.tag = pattern.tag
            output.attrib = {}
            output.children = []

            for key, value in pattern.attrib.items():
                if value[0] == "{" and value[-1] == "}":
                    ## CAREFUL: another string evaluation as Python code!
                    output[key] = str(eval(value[1:-1], variables))
                else:
                    output[key] = value

            for child in pattern.children:
                construction = self.construct(child, variables, namedGroups)
                if isinstance(construction, list):
                    output.children.extend(construction)
                else:
                    output.children.append(construction)

            return output

    def evaluate(self, pmmlSnippet, matchedVariables, namedGroups):
        variables = dict(self.context)
        variables.update(matchedVariables)

        output = []
        for child in self.children:
            if not isinstance(child, Context):
                construction = self.construct(child, variables, namedGroups)
                if isinstance(construction, list):
                    output.extend(construction)
                else:
                    output.append(construction)

        if len(output) == 1:
            return output[0]

        else:
            return output

PmmlSed.classMap["Replacement"] = Replacement

# Insert is a subclass of Replacement because most of the methods are the same
class Insert(Replacement):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="Insert">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
            <xs:any minOccurs="0" maxOccurs="unbounded" processContents="skip" />
          </xs:sequence>
          <xs:attribute name="at" type="xs:string" use="required"/>
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        values = self["at"].split(",")
        try:
            values = map(int, values)
        except ValueError:
            raise XMLValidationError("Insert's 'at' parameter must be an integer or a comma-separated list of integers, not \"%s\"" % self["at"])

        self.treeindex = values[:-1]
        self.index = values[-1]

    def evaluate(self, pmmlSnippet, matchedVariables, namedGroups):
        variables = dict(self.context)
        variables.update(matchedVariables)

        insertion = []
        for child in self.children:
            if not isinstance(child, Context):
                construction = self.construct(child, variables, namedGroups)
                if isinstance(construction, list):
                    insertion.extend(construction)
                else:
                    insertion.append(construction)

        if len(self.treeindex) > 0:
            pmmlSnippet = pmmlSnippet[self.treeindex]

        insertion.reverse()
        for item in insertion:
            pmmlSnippet.children.insert(index, item)

        return pmmlSnippet
        
PmmlSed.classMap["Insert"] = Insert

# Append is a subclass of Replacement because most of the methods are the same
class Append(Replacement):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="Append">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
            <xs:any minOccurs="0" maxOccurs="unbounded" processContents="skip" />
          </xs:sequence>
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
  """)

    def evaluate(self, pmmlSnippet, matchedVariables, namedGroups):
        variables = dict(self.context)
        variables.update(matchedVariables)

        insertion = []
        for child in self.children:
            if not isinstance(child, Context):
                construction = self.construct(child, variables, namedGroups)
                if isinstance(construction, list):
                    insertion.extend(construction)
                else:
                    insertion.append(construction)

        for item in insertion:
            pmmlSnippet.children.append(item)

        return pmmlSnippet
        
PmmlSed.classMap["Append"] = Append

class FileInput(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="FileInput">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
      <xs:attribute name="validate" type="xs:boolean" default="true" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "validate" not in self.attrib:
            self["validate"] = True

        try:
            self.data = xmlbase.loadfile(self["fileName"], pmml.X_ODG_PMML, validation=self["validate"])
        except XMLValidationError, err:
            raise RuntimeError("PMML file %s failed validation: %s" % (self["fileName"], str(err)))

PmmlSed.classMap["FileInput"] = FileInput

class StandardInput(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="StandardInput">
    <xs:complexType>
      <xs:attribute name="validate" type="xs:boolean" default="true" use="optional" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if "validate" not in self.attrib:
            self["validate"] = True

        try:
            self.data = xmlbase.load(sys.stdin.read(), pmml.X_ODG_PMML, validation=self["validate"])
        except XMLValidationError, err:
            raise RuntimeError("StandardInput PMML failed validation: %s" % str(err))

PmmlSed.classMap["StandardInput"] = StandardInput

class FileOutput(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="FileOutput">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
      <xs:attribute name="validate" type="xs:boolean" default="true" use="optional" />
      <xs:attribute name="indent" type="xs:string" default="    " use="optional" />
      <xs:attribute name="linesep" type="xs:string" default="%s" use="optional" />
    </xs:complexType>
  </xs:element>
  """ % os.linesep)

    def post_validate(self):
        if "validate" not in self.attrib:
            self["validate"] = True

        if "indent" not in self.attrib:
            self["indent"] = "    "

        if "linesep" not in self.attrib:
            self["linesep"] = os.linesep

    def write(self, pmmlFile):
        pmmlFile.write(self["fileName"], indent=self["indent"], linesep=self["linesep"])

        if self["validate"]:
            pmmlFile.validate(exception=True)

PmmlSed.classMap["FileOutput"] = FileOutput

class StandardOutput(PmmlSed):
    xsd = load_xsdElement(PmmlSed, """
  <xs:element name="StandardOutput">
    <xs:complexType>
      <xs:attribute name="validate" type="xs:boolean" default="true" use="optional" />
      <xs:attribute name="indent" type="xs:string" default="    " use="optional" />
      <xs:attribute name="linesep" type="xs:string" default="%s" use="optional" />
    </xs:complexType>
  </xs:element>
  """ % os.linesep)

    def post_validate(self):
        if "validate" not in self.attrib:
            self["validate"] = True

        if "indent" not in self.attrib:
            self["indent"] = "    "

        if "linesep" not in self.attrib:
            self["linesep"] = os.linesep

    def write(self, pmmlFile):
        print pmmlFile.xml(indent=self["indent"], linesep=self["linesep"])

        if self["validate"]:
            pmmlFile.validate(exception=True)

PmmlSed.classMap["StandardOutput"] = StandardOutput
