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

import augustus.core.xmlbase as xmlbase
from augustus.core.xmlbase import XMLValidationError, load_xsdElement

import augustus.core.config
import augustus.engine.mainloop
import augustus.applications.pmmlSed
import augustus.applications.pmmlSplit
import augustus.applications.scoresAwk

class Workflow(xmlbase.XML):
    topTag = "Workflow"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            pmmlName = (pmmlName for pmmlName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("Workflow class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, pmmlName, *children, **attrib)

class root(Workflow):
    xsd = load_xsdElement(Workflow, """
  <xs:element name="Workflow">
    <xs:complexType>
      <xs:sequence>
        <xs:choice minOccurs="1" maxOccurs="unbounded">
          <xs:element ref="AugustusConfiguration"/>
          <xs:element ref="AugustusConfigurationFromFile"/>
          <xs:element ref="PmmlSed"/>
          <xs:element ref="PmmlSedFromFile"/>
          <xs:element ref="PmmlSplit"/>
          <xs:element ref="PmmlSplitFromFile"/>
          <xs:element ref="ScoresAwk"/>
          <xs:element ref="ScoresAwkFromFile"/>
        </xs:choice>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  """)

    def evaluate(self):
        for child in self:
            if child.tag == "AugustusConfiguration":
                augustus.engine.mainloop.main(child)

            elif child.tag == "AugustusConfigurationFromFile":
                augustus.engine.mainloop.main(child.config)

            elif child.tag == "PmmlSed":
                child.evaluate()

            elif child.tag == "PmmlSedFromFile":
                child.config.evaluate()

            elif child.tag == "PmmlSplit":
                child.evaluate()

            elif child.tag == "PmmlSplitFromFile":
                child.config.evaluate()

            elif child.tag == "ScoresAwk":
                child.evaluate()

            elif child.tag == "ScoresAwkFromFile":
                child.config.evaluate()

Workflow.classMap["Workflow"] = root

Workflow.classMap["AugustusConfiguration"] = augustus.core.config.AugustusConfiguration
augustus.core.config.AugustusConfiguration.embeddedBase = augustus.core.config.Config

class AugustusConfigurationFromFile(Workflow):
    xsd = load_xsdElement(Workflow, """
  <xs:element name="AugustusConfigurationFromFile">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.config = xmlbase.loadfile(self["fileName"], augustus.core.config.Config, lineNumbers=True)

Workflow.classMap["AugustusConfigurationFromFile"] = AugustusConfigurationFromFile

Workflow.classMap["PmmlSed"] = augustus.applications.pmmlSed.root
augustus.applications.pmmlSed.root.embeddedBase = augustus.applications.pmmlSed.PmmlSed

class PmmlSedFromFile(Workflow):
    xsd = load_xsdElement(Workflow, """
  <xs:element name="PmmlSedFromFile">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.config = xmlbase.loadfile(self["fileName"], augustus.applications.pmmlSed.root, lineNumbers=True)

Workflow.classMap["PmmlSedFromFile"] = PmmlSedFromFile

Workflow.classMap["PmmlSplit"] = augustus.applications.pmmlSplit.root
augustus.applications.pmmlSplit.root.embeddedBase = augustus.applications.pmmlSplit.PmmlSplit

class PmmlSplitFromFile(Workflow):
    xsd = load_xsdElement(Workflow, """
  <xs:element name="PmmlSplitFromFile">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.config = xmlbase.loadfile(self["fileName"], augustus.applications.pmmlSplit.root, lineNumbers=True)

Workflow.classMap["PmmlSplitFromFile"] = PmmlSplitFromFile

Workflow.classMap["ScoresAwk"] = augustus.applications.scoresAwk.root
augustus.applications.scoresAwk.root.embeddedBase = augustus.applications.scoresAwk.ScoresAwk

class ScoresAwkFromFile(Workflow):
    xsd = load_xsdElement(Workflow, """
  <xs:element name="ScoresAwkFromFile">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required" />
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        self.config = xmlbase.loadfile(self["fileName"], augustus.applications.scoresAwk.root, lineNumbers=True)

Workflow.classMap["ScoresAwkFromFile"] = ScoresAwkFromFile
