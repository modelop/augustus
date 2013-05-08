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

"""Define an XSD for the ProducerConsumer configuration file."""

from sys import version_info
if version_info < (2, 6): from sets import Set as set
import sys
import re
import code
import StringIO
import os
from xml.sax.saxutils import escape, quoteattr

try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json = None

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    from functools import reduce
except ImportError:
    pass

# local includes
from augustus.core.xmlbase import load_xsdElement
from augustus.core.xmlbase import load_xsdGroup
from augustus.core.xmlbase import load_xsdType
from augustus.core.defs import Atom, NameSpace, JSONEncoderWithNameSpace
import augustus.core.xmlbase as xmlbase

#################################################### ConfigurationError
class ConfigurationError(xmlbase.XMLValidationError): pass  # Any configuration error

# base class for all Config
class Config(xmlbase.XML):
    topTag = "AugustusConfiguration"
    xsdType = {}
    xsdGroup = {}
    classMap = {}

    def __init__(self, *children, **attrib):
        # reverse-lookup the classMap
        try:
            configName = (configName for configName, pythonObj in self.classMap.items() if pythonObj == self.__class__).next()
        except StopIteration:
            raise Exception("Config class is missing from the classMap (programmer error)")
        xmlbase.XML.__init__(self, configName, *children, **attrib)

########################################################### Config types

Config.xsdType["LogSetup"] = load_xsdType("""
  <xs:complexType name="LogSetup">
    <xs:sequence>
      <xs:choice>
        <xs:element ref="ToLogFile"/>
        <xs:element ref="ToStandardError"/>
        <xs:element ref="ToStandardOut"/>
      </xs:choice>
      <xs:element minOccurs="0" maxOccurs="unbounded" ref="DifferentLevel"/>
    </xs:sequence>
    <xs:attribute name="level" use="optional">
      <!-- Note:
           For the root logger, level defaults to ERROR.
           For the metadata logger, level defaults to DEBUG.
           For the metadata logger, the only options are DEBUG,
           INFO, or no logging...
      -->
      <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:enumeration value="DEBUG"/>
          <xs:enumeration value="INFO"/>
          <xs:enumeration value="WARNING"/>
          <xs:enumeration value="ERROR"/>
        </xs:restriction>
      </xs:simpleType>
    </xs:attribute>
    <xs:attribute
      name="formatString" type="xs:string"
      default="%(created)012.0f\t%(asctime)s\t%(levelname)s\t%(message)s"
      use="optional"/>
    <xs:attribute
      name="dateFmt" type="xs:string" default="%Y-%m-%dT%H:%M:%S"
      use="optional"/>
  </xs:complexType>
""")

Config.xsdType["WithTagAttributeType"] = load_xsdType("""
  <xs:complexType name="WithTagAttributeType">
    <xs:attribute name="name" type="xs:string" use="required"/>
  </xs:complexType>
""")

Config.xsdType["NullType"] = load_xsdType("""
  <xs:complexType name="NullType">
  </xs:complexType>
""")


######################################################## Config elements

class AugustusConfiguration(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="AugustusConfiguration">
    <xs:complexType>
      <xs:all>
        <xs:element ref="Logging" minOccurs="0"/>
        <xs:element ref="Metadata" minOccurs="0"/>
        <xs:element ref="AggregationSettings" minOccurs="0"/>
        <xs:element ref="EventSettings" minOccurs="0"/>
        <xs:element ref="ModelInput" minOccurs="0"/>
        <xs:element ref="DataInput" minOccurs="0"/>
        <xs:element ref="ConsumerBlending" minOccurs="0"/>
        <xs:element ref="Output" minOccurs="0"/>
        <xs:element ref="ModelSetup" minOccurs="0"/>
        <xs:element ref="ModelVerification" minOccurs="0"/>
        <xs:element ref="CustomProcessing" minOccurs="0"/>
      </xs:all>
      <xs:attribute name="randomSeed" type="xs:integer" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        """ Complain if neither producing nor consuming a model."""

        if self.exists(DataInput) and not self.exists(ModelSetup) and not self.exists(Output):
            raise ConfigurationError("If \"DataInput\" is set, and not producing a model, the \"Output\" must also be present to identify a file / location to write the output scores...")

        if not self.exists(ModelSetup) and not self.exists(Output):
            raise ConfigurationError("At least one of \"Output\" or \"ModelSetup\"should be present, or else Augustus isn't going to make either a new model or any output scores...")

Config.classMap["AugustusConfiguration"] = AugustusConfiguration

class Logging(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Logging" type="LogSetup"/>
""")

Config.classMap["Logging"] = Logging

class DifferentLevel(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="DifferentLevel">
    <xs:complexType>
      <xs:attribute name="stage" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="initialization"/>
            <xs:enumeration value="verification"/>
            <xs:enumeration value="eventloop"/>
            <xs:enumeration value="segment"/>
            <xs:enumeration value="produce"/>
            <xs:enumeration value="shutdown"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="level" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="DEBUG"/>
            <xs:enumeration value="INFO"/>
            <xs:enumeration value="WARNING"/>
            <xs:enumeration value="ERROR"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="segment" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
  """)

    def post_validate(self):
        if self.attrib["stage"] == "segment" and "segment" not in self.attrib:
            raise ConfigurationError("If stage == 'segment', a segment name must be provided")

        if self.attrib["stage"] != "segment" and "segment" in self.attrib:
            raise ConfigurationError("If stage != 'segment', a segment name must not be provided")

Config.classMap["DifferentLevel"] = DifferentLevel

class Metadata(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Metadata" type="LogSetup"/>
""")
    def post_validate(self):
        """ Restrict the logging levels to DEBUG or INFO."""
        if "level" in self.attrib:
            if self["level"] not in ("DEBUG", "INFO"):
                raise ConfigurationError("The only two allowed logging levels for Metadata are 'DEBUG' and 'INFO'")

Config.classMap["Metadata"] = Metadata

class FileRotateBySize(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="FileRotateBySize">
    <xs:complexType>
      <xs:attribute name="mode" default="a" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="a"/>
            <xs:enumeration value="w"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute
        name="maxBytes" default="0" type="xs:integer" use="optional"/>
      <xs:attribute
        name="backupCount" default="0" type="xs:integer" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["FileRotateBySize"] = FileRotateBySize

class FileRotateByTime(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="FileRotateByTime">
    <xs:complexType>
      <xs:attribute name="when" default="H" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="H"/><!-- Hours -->
            <xs:enumeration value="D"/><!-- Days -->
            <xs:enumeration value="midnight"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute
        name="interval" default="1" type="xs:integer" use="optional"/>
      <xs:attribute
        name="backupCount" default="0" type="xs:integer" use="optional"/>
      <xs:attribute
        name="utc" default="0" type="xs:boolean" use="optional"/>
        <!-- if true, use local time, otherwise use UTC -->
    </xs:complexType>
  </xs:element>
""")

Config.classMap["FileRotateByTime"] = FileRotateByTime

class ModelInput(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ModelInput">
    <xs:complexType>
<!--      <xs:choice> -->
<!--        <xs:element ref="FromFile"/>  -->
<!--        <xs:element ref="FromFifo"/>  PMML model from a FIFO pipe?  What?!?  -->
<!--      </xs:choice> -->
      <xs:attribute name="fileLocation" type="xs:string" use="required" />
      <xs:attribute name="maturityThreshold" type="xs:nonNegativeInteger" use="optional" default="0"/>
      <xs:attribute name="selectmode" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="lastAlphabetic"/>
            <xs:enumeration value="mostRecent"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ModelInput"] = ModelInput

class DataInput(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="DataInput">
    <!-- DataInput is ignored if the ProducerConsumer
      is instructed to only create a model. Then the training
      data referenced in the model build instructions are used. -->
    <xs:complexType>
      <xs:choice>
        <xs:element ref="FromFile" />
        <xs:element ref="FromStandardIn" />
        <xs:element ref="FromHTTP" />
        <xs:element ref="Interactive"/>
      </xs:choice>
<!--      <xs:all>                                     -->
<!--        <xs:element ref="ReadOnce" minOccurs="0"/> -->
<!--        <xs:choice>                                -->
<!--          <xs:element ref="Interactive"/>          -->
<!--          <xs:element ref="FromFile"/>             -->
<!--          <xs:element ref="FromCSVFile"/>          -->
<!--          <xs:element ref="FromFifo"/>             -->
<!--          <xs:element ref="FromFixedRecordFile"/>  -->
<!--          <xs:element ref="FromStandardIn"/>       -->
<!--          <xs:element ref="FromHTTP"/>             -->
<!--        </xs:choice>                               -->
<!--      </xs:all>                                    -->
    </xs:complexType>
  </xs:element>
""")

Config.classMap["DataInput"] = DataInput

# class ReadOnce(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="ReadOnce" type="NullType"/>
# """)

# Config.classMap["ReadOnce"] = ReadOnce

class AggregationSettings(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="AggregationSettings">
    <xs:complexType>
      <xs:attribute name="score" type="xs:boolean" use="optional"/>
      <xs:attribute name="output" type="xs:boolean" use="optional"/>
      <xs:attribute name="atEnd" type="xs:boolean" default="true" use="optional"/>
      <xs:attribute name="eventNumberInterval" type="xs:integer" default="-1" use="optional"/>
      <xs:attribute name="fieldValueInterval" type="xs:integer" default="-1" use="optional"/>
      <xs:attribute name="field" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        self.attrib["score"] = self.attrib.get("score", False)
        self.attrib["output"] = self.attrib.get("output", False)

        if "atEnd" not in self.attrib:
            self["atEnd"] = True

        if "eventNumberInterval" not in self.attrib:
            self["eventNumberInterval"] = -1

        if "fieldValueInterval" not in self.attrib:
            self["fieldValueInterval"] = -1

        if self["fieldValueInterval"] >= 0:
            if "field" not in self.attrib:
                raise ConfigurationError("If \"fieldValueInterval\" is non-negative, then the \"field\" must be specified.")

Config.classMap["AggregationSettings"] = AggregationSettings


class EventSettings(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="EventSettings">
    <xs:complexType>
      <xs:attribute name="score" type="xs:boolean" use="optional"/>
      <xs:attribute name="output" type="xs:boolean" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        self.attrib["score"] = self.attrib.get("score", False)
        self.attrib["output"] = self.attrib.get("output", False)

Config.classMap["EventSettings"] = EventSettings


class Output(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Output">
    <xs:complexType>
      <xs:all>
        <xs:choice>
          <xs:element ref="ToFile"/>
          <xs:element ref="ToStandardError"/>
          <xs:element ref="ToStandardOut"/>
<!--          <xs:element ref="ToHTTP"/>       -->
        </xs:choice>
        <xs:element ref="ReportTag" minOccurs="0"/>
        <xs:element ref="EventTag" minOccurs="0"/>
      </xs:all>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        self.destination = self.child()

Config.classMap["Output"] = Output

class Interactive(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Interactive" type="NullType"/>
""")

Config.classMap["Interactive"] = Interactive

class ReportTag(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ReportTag">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ReportTag"] = ReportTag

class ModelSetup(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ModelSetup">
    <xs:complexType>
      <xs:all>
        <xs:element ref="ProducerAlgorithm" minOccurs="0"/>
        <xs:element ref="Serialization" minOccurs="0"/>
        <xs:element ref="ProducerBlending" minOccurs="0"/>
        <xs:element ref="ZeroVarianceHandling" minOccurs="0"/>
        <!-- if omitted, the default will be to use
             ZeroVarianceHandling with method="exception" -->
        <xs:choice>
          <xs:element ref="SegmentationSchema" minOccurs="0"/>
          <xs:element ref="ExternalSegmentationSchema" minOccurs="0"/>
        </xs:choice>
      </xs:all>
      <xs:attribute name="outputFilename" type="xs:string" use="optional"/>
      <xs:attribute name="mode" use="optional" default="replaceExisting">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="lockExisting"/>
            <xs:enumeration value="replaceExisting"/>
            <!-- replace: create statistics from just the new data  -->
            <xs:enumeration value="updateExisting"/>
            <!-- update: modify statistics with the new data  -->
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="updateEvery" use="optional" default="event">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="event"/>
            <xs:enumeration value="aggregate"/>
            <xs:enumeration value="both"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        if self.exists(ZeroVarianceHandling):
            raise NotImplementedError("Zero variance handling options are not yet implemented")

        if "mode" in self.attrib and self["mode"] == "lockExisting":
            if not self.exists(ProducerBlending, maxdepth=1):
                if not self.exists(SegmentationSchema, maxdepth=1):
                    raise ConfigurationError("Model 'mode' is 'lockExisting', but there are no new segments defined and a 'ProducerBlending' configuration exists; nothing is going to change in the model at all...\nPlease omit the entire \"ModelSetup\" element if you want this behavior, or else add segmentation or modify the \"ModelSetup\" 'mode' attribute.")

Config.classMap["ModelSetup"] = ModelSetup

class ProducerAlgorithm(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ProducerAlgorithm">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Parameter" minOccurs="0" maxOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="model" type="xs:string" use="required"/>
      <xs:attribute name="algorithm" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""")

    available = {"BaselineModel": ["streaming", "hold", "pass"],
                 "ClusteringModel": ["streaming", "kmeans"],
                 "TreeModel": ["streaming", "iterative", "c45", "cart"],
                 "RuleSetModel": ["streaming", "iterative", "c45", "cart"],
                 "RegressionModel": ["streaming"],
                 "NaiveBayesModel": ["streaming"],
                 }

    def post_validate(self):
        if self.attrib["model"] not in self.available.keys():
            raise ConfigurationError("Unrecognized model \"%s\" (%s are available)" % (self.attrib["model"], self.available.keys()))

        if self.attrib["algorithm"] not in self.available[self.attrib["model"]]:
            raise ConfigurationError("Unrecognized algorithm \"%s\" for model %s (%s are available)" % (self.attrib["algorithm"], self.attrib["model"], self.available[self.attrib["model"]]))

        self.parameters = {}
        for param in self.matches(Parameter):
            self.parameters[param.attrib["name"]] = param.attrib["value"]

Config.classMap["ProducerAlgorithm"] = ProducerAlgorithm

class Parameter(Config): 	 
    xsd = load_xsdElement(Config, """
  <xs:element name="Parameter">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="required"/>
      <xs:attribute name="value" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""") 	 
	  	 
Config.classMap["Parameter"] = Parameter

producerAlgorithmDefaults = {
    "BaselineModel": ProducerAlgorithm(model="BaselineModel", algorithm="streaming"),
    "ClusteringModel": ProducerAlgorithm(model="ClusteringModel", algorithm="streaming"),
    "TreeModel": ProducerAlgorithm(model="TreeModel", algorithm="streaming"),
    "RuleSetModel": ProducerAlgorithm(model="RuleSetModel", algorithm="streaming"),
    "RegressionModel": ProducerAlgorithm(model="RegressionModel", algorithm="streaming"),
    "NaiveBayesModel": ProducerAlgorithm(model="NaiveBayesModel", algorithm="streaming"),
    }

class Serialization(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Serialization">
    <xs:complexType>
      <xs:attribute
        name="writeFrequency" type="xs:integer" use="optional"/>
        <xs:attribute name="frequencyUnits" use="optional">
          <xs:simpleType>
            <xs:restriction base="xs:string">
              <xs:enumeration value="M"/>
              <xs:enumeration value="H"/>
              <xs:enumeration value="d"/>
              <xs:enumeration value="observations"/>
            </xs:restriction>
          </xs:simpleType>
        </xs:attribute>
      <xs:attribute name="storage" default="asPMML" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="asPickle"/>
            <xs:enumeration value="asPMML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        if "writeFrequency" in self.attrib:
            if not "frequencyUnits" in self.attrib:
                raise ConfigurationError("\"writeFrequency\" and \"frequencyUnits\" must be both be present together")
        elif "frequencyUnits" in self.attrib:
            raise ConfigurationError("\"writeFrequency\" and \"frequencyUnits\" must be both be present together")

Config.classMap["Serialization"] = Serialization

class ProducerBlending(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ProducerBlending">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="MaturityThreshold" minOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="method" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="computerTimeWindowSeconds"/>
            <xs:enumeration value="eventTimeWindow"/>
            <xs:enumeration value="exponential"/>
            <xs:enumeration value="unweighted"/>
            <xs:enumeration value="window"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="alpha" type="xs:double" use="optional"/>
      <xs:attribute name="timeFieldName" type="xs:string" use="optional"/>
      <xs:attribute name="windowLag" type="xs:integer" use="optional"/>
      <xs:attribute name="windowSize" type="xs:integer" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        # FIXME: some of these attributes, like "timeFieldName", are
        # always optional, which breaks the logic of this check

        need_template = "Need \"%s\" with %s blending"
        no_template = "No \"%s\" attribute allowed when using \"%s\""

        if "computerTimeWindowSeconds" in self.attrib:
            raise NotImplementedError("ProducerBlending 'computerTimeWindowSeconds' has not been implemented.")

        needs = {}
        needs["computerTimeWindowSeconds"] = set(["windowSize"])
        needs["eventTimeWindow"] = set(["windowSize"])
        needs["exponential"] = set(["alpha"])
        needs["unweighted"] = set()
        needs["window"] = set(["windowSize"])
        options = set(self.attrib)
        if "method" in self.attrib:
            options.remove("method")
            method = self["method"]
        else:
             method = "unweighted"
        for need in needs[method] - options:
            raise ConfigurationError(need_template % (need, method))
        if "windowLag" in options and method not in ("unweighted", "exponential"):
            options.remove("windowLag")
        for no in options - needs[method]:
            if no != "timeFieldName":
                raise ConfigurationError(no_template % (no, method))

Config.classMap["ProducerBlending"] = ProducerBlending

class ConsumerBlending(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ConsumerBlending">
    <xs:complexType>
      <xs:attribute name="method" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="computerTimeWindowSeconds"/>
            <xs:enumeration value="eventTimeWindow"/>
            <xs:enumeration value="exponential"/>
            <xs:enumeration value="unweighted"/>
            <xs:enumeration value="window"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="alpha" type="xs:double" use="optional"/>
      <xs:attribute name="timeFieldName" type="xs:string" use="optional"/>
      <xs:attribute name="windowLag" type="xs:integer" use="optional"/>
      <xs:attribute name="windowSize" type="xs:integer" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        # FIXME: some of these attributes, like "timeFieldName", are
        # always optional, which breaks the logic of this check

        need_template = "Need \"%s\" with %s blending"
        no_template = "No \"%s\" attribute allowed when using \"%s\""

        if "computerTimeWindowSeconds" in self.attrib:
            raise NotImplementedError("ConsumerBlending 'computerTimeWindowSeconds' has not been implemented.")

        needs = {}
        needs["computerTimeWindowSeconds"] = set(["windowSize"])
        needs["eventTimeWindow"] = set(["timeFieldName", "windowSize"])
        needs["exponential"] = set(["alpha"])
        needs["unweighted"] = set()
        needs["window"] = set(["windowSize"])
        options = set(self.attrib)
        if "method" in self.attrib:
            options.remove("method")
            method = self["method"]
        else:
             method = "unweighted"
        for need in needs[method] - options:
            raise ConfigurationError(need_template % (need, method))
        if "windowLag" in options and method not in ("unweighted", "exponential"):
            options.remove("windowLag")
        for no in options - needs[method]:
            if no != "timeFieldName":
                raise ConfigurationError(no_template % (no, method))

Config.classMap["ConsumerBlending"] = ConsumerBlending

class MaturityThreshold(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="MaturityThreshold">
    <xs:complexType>
      <xs:attribute name="threshold" type="xs:integer" use="required"/>
      <xs:attribute name="lockingThreshold" type="xs:integer" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["MaturityThreshold"] = MaturityThreshold

class ZeroVarianceHandling(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ZeroVarianceHandling">
    <xs:complexType>
      <xs:attribute name="method" default="exception" use="optional">
        <xs:simpleType>
        <xs:restriction base="xs:string">
          <xs:enumeration value="exception"/>
          <xs:enumeration value="quiet"/>
          <xs:enumeration value="varianceDefault"/>
          <xs:enumeration value="interpolateZeroVarianceEstimate"/>
        </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute
        name="value" type="xs:double" use="optional"/>
        <!-- only use "value" with method="VarianceDefault";
        it sets the default variance -->
      <xs:attribute
        name="resolution" type="xs:double" use="optional"/>
        <!-- only use "resolution" with
        method="InterpolateZeroVarianceEstimate";
        it sets the initial variance, to be shrunk with data
        size, by presuming the reason zero variance was
        observed was because the resolution of the data
        (e.g. integers) was bigger than the variance
        (which, say, equals 0.5) -->
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        if "value" in self.attrib:
            if "method" in self.attrib and not self["method"]=="varianceDefault":
                    raise ConfigurationError("Can only use \"value\" with \"varianceDefault\"")
        if "resolution" in self.attrib:
            if "method" in self.attrib and not self["method"]=="interpolateZeroVarianceEstimate":
                    raise ConfigurationError("Can only use \"resolution\" with \"interpolateZeroVarianceEstimate\"")

Config.classMap["ZeroVarianceHandling"] = ZeroVarianceHandling

class SegmentationSchema(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="SegmentationSchema">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="BlacklistedSegments" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="SpecificSegments" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="GenericSegment" minOccurs="0"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""")

    def _everCollides(self, itemsA, rangesA, itemsB, rangesB):
        if len(itemsA.intersection(itemsB)) > 0:
            return True
        else:
            for item in itemsA:
                item = float(item)
                for rng in rangesB:
                    low = rng[0]
                    high = rng[1]
                    closure = rng[2]
                    if low < item and item < high:
                        return True
                    elif low == item and closure.startswith('c'):
                        return True
                    elif high == item and closure.beginswith('d'):
                        return True
            for item in itemsB:
                item = float(item)
                for rng in rangesA:
                    low = rng[0]
                    high = rng[1]
                    closure = rng[2]
                    if low < item and item < high:
                        return True
                    elif low == item and closure.startswith('c'):
                        return True
                    elif high == item and closure.beginswith('d'):
                        return True
        return False


    def post_validate(self):
        """Check that there are no collisions in the list of SpecificSegments."""
        # A collision exists if any of the SpecificSegments's set of fields
        # can be totally exhausted before or at the same time as any other
        # SpecificSegment's set of fields.
        self.whiteList = None
        self.blackList = None
        self.generic = None
        specifics = self.matches(SpecificSegments, maxdepth=1)
        N = len(specifics)
        for i in xrange(N):
            current = specifics[i].fields
            for j in xrange(N):
                if i != j:
                    compare = specifics[j].fields
                    def anyCollide(field, y):
                        return self._everCollides(current[field].items, current[field].ranges, compare[field].items, compare[field].ranges) and y

                    keysIntersection = set(current.keys()) & set(compare.keys())
                    collides = reduce(anyCollide, keysIntersection, False)
                    if collides:
                        # If along every dimension in the intersection
                        # there is a collision, the segments collide.
                        raise ConfigurationError("Segments described in different \"SpecificSegments\" sections overlap.")

        if N > 0:
            self.whiteList = [item.fields for item in specifics]
        blackListed = self.matches(BlacklistedSegments, maxdepth=1)
        if blackListed:
            self.blackList = [item.fields for item in blackListed]
        generic = self.child(GenericSegment, exception=False)
        if generic:
            self.generic = [generic.fields]

Config.classMap["SegmentationSchema"] = SegmentationSchema

# Note: this is a subclass of SegmentationSchema.  After loading a SegmentationSchema, it acts like one.
class ExternalSegmentationSchema(SegmentationSchema):
    xsd = load_xsdElement(Config, """
  <xs:element name="ExternalSegmentationSchema">
    <xs:complexType>
      <xs:attribute name="fileName" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        try:
            external = xmlbase.loadfile(self.attrib["fileName"], Config)
        except IOError, err:
            raise ConfigurationError("External segmentation schema file \"%s\" could not be opened: %s" % (self.attrib["fileName"], str(err)))

        self.tag = external.tag
        self.children = external.children
        self.attrib = external.attrib

        SegmentationSchema.post_validate(self)

Config.classMap["ExternalSegmentationSchema"] = ExternalSegmentationSchema

class ToLogFile(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ToLogFile">
    <xs:complexType>
      <xs:sequence>
        <xs:choice minOccurs="0">
          <xs:element ref="FileRotateBySize"/>
          <xs:element ref="FileRotateByTime"/>
        </xs:choice>
      </xs:sequence>
      <xs:attribute name="name" type="xs:string" use="required"/>
      <xs:attribute name="overwrite" type="xs:boolean" default="false" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ToLogFile"] = ToLogFile

class ToFile(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ToFile">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" use="required"/>
      <xs:attribute name="type" default="XML" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="JSON"/>
            <xs:enumeration value="XML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="overwrite" type="xs:boolean" default="false" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        outputType = self.attrib.get("type", "XML")
        if outputType == "JSON":
            raise NotImplementedError("The \"JSON\" output format is not yet implemented")

Config.classMap["ToFile"] = ToFile

# class ToHTTP(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="ToHTTP">
#     <xs:complexType>
#       <xs:attribute name="host" type="xs:string" use="required"/>
#       <xs:attribute name="port" type="xs:string" use="optional"/>
#       <xs:attribute name="url" type="xs:anyURI" use="required"/>
#       <xs:attribute name="type" default="XML" use="optional">
#         <xs:simpleType>
#           <xs:restriction base="xs:string">
#             <xs:enumeration value="JSON"/>
#             <xs:enumeration value="XML"/>
#           </xs:restriction>
#         </xs:simpleType>
#       </xs:attribute>
#     </xs:complexType>
#   </xs:element>
# """)

# Config.classMap["ToHTTP"] = ToHTTP

class ToStandardError(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ToStandardError">
    <xs:complexType>
      <xs:attribute name="type" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="JSON"/>
            <xs:enumeration value="XML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ToStandardError"] = ToStandardError

class ToStandardOut(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ToStandardOut">
    <xs:complexType>
      <xs:attribute name="type" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="JSON"/>
            <xs:enumeration value="XML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ToStandardOut"] = ToStandardOut

# class FromCSVFile(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="FromCSVFile">
#     <xs:complexType>
#       <xs:attribute name="name" type="xs:string" use="required"/>
#       <xs:attribute name="header" type="xs:string" use="optional"/>
#       <xs:attribute name="sep" type="xs:string" use="optional"/>
#       <xs:attribute name="framing" type="xs:string" use="optional"/>
#     </xs:complexType>
#   </xs:element>
# """)

# Config.classMap["FromCSVFile"] = FromCSVFile

# class FromFifo(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="FromFifo">
#     <xs:complexType>
#       <xs:attribute name="type" default="XML" use="optional">
#         <xs:simpleType>
#           <xs:restriction base="xs:string">
#             <xs:enumeration value="CSV"/>
#             <xs:enumeration value="UniTable"/>
#             <xs:enumeration value="XML"/>
#           </xs:restriction>
#         </xs:simpleType>
#       </xs:attribute>
#       <xs:attribute name="name" type="xs:string" use="required"/>
#       <xs:attribute name="header" type="xs:string" use="optional"/>
#     </xs:complexType>
#   </xs:element>
# """)

#     def post_validate(self):
#         if "type" not in self.attrib or self["type"]=="XML":
#             if "header" in self.attrib:
#                 raise ConfigurationError("An XML file is incompatible with a CSV header string")

# Config.classMap["FromFifo"] = FromFifo

# class FromFile(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="FromFile">
#     <xs:complexType>
#       <xs:attribute name="type" default="XML" use="optional">
#         <xs:simpleType>
#           <xs:restriction base="xs:string">
#             <xs:enumeration value="CSV"/>
#             <xs:enumeration value="UniTable"/>
#             <xs:enumeration value="XML"/>
#           </xs:restriction>
#         </xs:simpleType>
#       </xs:attribute>
#       <xs:attribute name="name" type="xs:string" use="required"/>
#       <xs:attribute name="selectmode" use="optional">
#         <xs:simpleType>
#           <xs:restriction base="xs:string">
#             <xs:enumeration value="lastAlphabetic"/>
#             <xs:enumeration value="mostRecent"/>
#           </xs:restriction>
#         </xs:simpleType>
#       </xs:attribute>
#     </xs:complexType>
#   </xs:element>
# """)

# Config.classMap["FromFile"] = FromFile

# class FromFixedRecordFile(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="FromFixedRecordFile">
#     <xs:complexType>
#       <xs:sequence>
#         <xs:element ref="RecordField" maxOccurs="unbounded"/>
#       </xs:sequence>
#       <xs:attribute name="name" type="xs:string" use="required"/>
#       <xs:attribute name="cr" type="xs:string" use="optional"/>
#     </xs:complexType>
#   </xs:element>
# """)

# Config.classMap["FromFixedRecordFile"] = FromFixedRecordFile

# class RecordField(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="RecordField">
#     <xs:complexType>
#       <xs:attribute name="name" type="xs:string" use="required"/>
#       <xs:attribute name="length" type="xs:integer" use="required"/>
#     </xs:complexType>
#   </xs:element>
# """)

# Config.classMap["RecordField"] = RecordField

class FromStandardIn(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="FromStandardIn">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Parameter" minOccurs="0" maxOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="format" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="CSV"/>
            <xs:enumeration value="XML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        self.parameters = {}
        for p in self.matches(Parameter):
            self.parameters[p.attrib["name"]] = p.attrib["value"]

Config.classMap["FromStandardIn"] = FromStandardIn

class FromFile(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="FromFile">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Parameter" minOccurs="0" maxOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="fileLocation" type="xs:string" use="required"/>
      <xs:attribute name="format" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="XTBL"/>
            <xs:enumeration value="CSV"/>
            <xs:enumeration value="XML"/>
            <xs:enumeration value="NAB"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        if "format" not in self.attrib:
            if self.attrib["fileLocation"][-5:].upper() == ".XTBL":
                self.attrib["format"] = "XTBL"
            elif self.attrib["fileLocation"][-4:].upper() == ".CSV":
                self.attrib["format"] = "CSV"
            elif self.attrib["fileLocation"][-4:].upper() == ".XML":
                self.attrib["format"] = "XML"
            elif self.attrib["fileLocation"][-4:].upper() == ".NAB":
                self.attrib["format"] = "NAB"
            else:
                raise ConfigurationError("Unrecognized file extension in \"%s\"" % self.attrib["fileLocation"])

        # TODO: add some ability to sort the files

        self.parameters = {}
        for p in self.matches(Parameter):
            self.parameters[p.attrib["name"]] = p.attrib["value"]

Config.classMap["FromFile"] = FromFile

class FromHTTP(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="FromHTTP">
    <xs:complexType>
      <xs:attribute name="host" type="xs:string" use="optional" default=""/>
      <xs:attribute name="port" type="xs:integer" use="required"/>
      <xs:attribute name="respond" type="xs:boolean" use="optional" default="true"/>
      <xs:attribute name="checkAddress" type="xs:boolean" use="optional" default="true"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["FromHTTP"] = FromHTTP

# class FromStandardIn(Config):
#     xsd = load_xsdElement(Config, """
#   <xs:element name="FromStandardIn" type="NullType"/>
# """)

# Config.classMap["FromStandardIn"] = FromStandardIn

class EventTag(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="EventTag">
    <xs:complexType>
      <xs:attribute name="name" type="xs:string" 
        default="Event" use="optional"/>
      <xs:attribute name="pseudoName" type="xs:string"
        default="pseudoEvent" use="optional"/>
   </xs:complexType>
  </xs:element>
""")

Config.classMap["EventTag"] = EventTag

class AllSegments(object):
    """Subclassed by SpecificSegments, BlacklistedSegments, GenericSegment.

    Provides methods to check whether new additions collide with
    existing entries to the segment definitions, and to find a match
    """
    class Inf(Atom):
        def __gt__(self, other): return True
        def __ge__(self, other): return True
        def __lt__(self, other): return False
        def __le__(self, other): return other is self or False
    INF = Inf("Inf")

    class NegInf(Atom):
        def __gt__(self, other): return False
        def __ge__(self, other): return other is self or False
        def __lt__(self, other): return True
        def __le__(self, other): return True
    NEGINF = NegInf("NegInf")


    def _addItems(self, field, include_items, exclude_items):
        if not hasattr(self, "fields"):
            self.fields = {}
        if field not in self.fields:
            self.fields[field] = NameSpace(
                include_items=include_items,
                exclude_items=exclude_items,
                ranges=[],
                partitions=[])
        else:
            self._checkItemsCollision(field, include_items)
            self.fields[field].include_items.update(include_items)
            self.fields[field].exclude_items.extend(exclude_items)

    def _addRanges(self, field, ranges):
        if not hasattr(self, "fields"):
            self.fields = {}
        if field not in self.fields:
            self.fields[field] = NameSpace(include_items=set(), exclude_items=[], ranges=ranges, partitions=[])
        else:
            self._checkRangesCollision(field, ranges)
            self.fields[field].ranges.extend(ranges)
            self.fields[field].ranges.sort()

    def _checkItemsCollision(self, field, include_items):
        if self.fields[field].include_items == None or include_items == None:
                raise ConfigurationError("An \"EnumeratedDimension\" for field %s exists with no \"Selection\" identified, which is interpreted as a command to auto-generate segments for that field. No further specification can be made for that field" % field)
        partitions = self.fields[field].partitions

    def _checkRangesCollision(self, field, ranges):
        if self.fields[field].include_items == None:
            raise ConfigurationError("An \"EnumeratedDimension\" for field %s exists with no \"Selection\" identified, which is interpreted as a command to auto-generate segments for that field. No further specification can be made for that field" % field)

    def _createPartitions(self):
        for key, field in self.fields.iteritems():
            field.ranges.sort
            field.partitions = []
            lookupStart = 0
            for i, (left, right, closure, divisions) in enumerate(field.ranges):
                if left is AllSegments.NEGINF or right is AllSegments.INF:
                    field.partitions.append((left, right, closure))
                else:
                    step = (right - left) / divisions
                    finish = divisions - 1
                    field.partitions.extend([(
                        j * step + left,
                        (j+1) * step + left if not j == finish else right,
                        closure)
                        for j in range(divisions)])

                newTuple = (left, right, closure, divisions, lookupStart)
                field.ranges[i] = newTuple
                lookupStart += divisions


class SpecificSegments(Config, AllSegments):
    xsd = load_xsdElement(Config, """
  <xs:element name="SpecificSegments">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="EnumeratedDimension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="PartitionedDimension" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        for element in self.matches(EnumeratedDimension):
            if len(element.children) == 0:
                raise ConfigurationError("Must specify a selection for \"EnumeratedDimension\" in \"SpecificSegments\"")
            self._addItems(element["field"], element.include_items, element.exclude_items)
        for element in self.matches(PartitionedDimension):
            self._addRanges(element["field"], element.ranges)
        self._createPartitions()

Config.classMap["SpecificSegments"] = SpecificSegments

class BlacklistedSegments(Config, AllSegments):
    xsd = load_xsdElement(Config, """
  <xs:element name="BlacklistedSegments">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="EnumeratedDimension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="PartitionedDimension" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        for element in self.matches(EnumeratedDimension):
            if len(element.children)==0:
                raise ConfigurationError("Must specify a selection for \"EnumeratedDimension\" in \"BlacklistedSegments\"")
            self._addItems(element["field"], element.include_items, element.exclude_items)
        for element in self.matches(PartitionedDimension):
            self._addRanges(element["field"], element.ranges)
        self._createPartitions()

Config.classMap["BlacklistedSegments"] = BlacklistedSegments

class GenericSegment(Config, AllSegments):
    xsd = load_xsdElement(Config, """
  <xs:element name="GenericSegment">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="EnumeratedDimension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="PartitionedDimension" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        emptyDimensions = 0
        for element in self.matches(EnumeratedDimension):
            if len(element.matches(Selection)) == 0:
                emptyDimensions += 1
            self._addItems(element["field"], element.include_items, element.exclude_items)

        if emptyDimensions == 0:
            raise ConfigurationError("Must have at least one \"EnumeratedDimension\" section empty when in \"GenericSegment\"")
        for element in self.matches(PartitionedDimension):
            self._addRanges(element["field"], element.ranges)
        self._createPartitions()

Config.classMap["GenericSegment"] = GenericSegment

class EnumeratedDimension(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="EnumeratedDimension">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Selection" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="field" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        includeList = self.matches(lambda x: isinstance(x, Selection) and x.attrib.get("operator","equal") == "equal")
        if len(includeList):
            self.include_items = set([selection.attrib['value'] for selection in includeList])
        else:
            self.include_items = None

        excludeList = self.matches(lambda x: isinstance(x, Selection) and x.attrib.get("operator","equal") == "notEqual")
        self.exclude_items = [selection.attrib['value'] for selection in excludeList]

Config.classMap["EnumeratedDimension"] = EnumeratedDimension

class Selection(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Selection">
    <xs:complexType>
      <xs:attribute name="value" type="xs:string" use="required"/>
      <xs:attribute name="operator" default="equal" use="optional">
      <!-- if 'operator' is notEqual, only one Selection can exist. -->
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="equal"/>
            <xs:enumeration value="notEqual"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["Selection"] = Selection

class PartitionedDimension(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="PartitionedDimension">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Partition" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="field" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
""")

    def _addRangeTuple(self, partition):
        bLow = partition._getWithDefault('low', AllSegments.NEGINF)
        bHigh =  partition._getWithDefault('high', AllSegments.INF)
        bClosure =  partition._getWithDefault('closure', 'openClosed')
        bDivisions =  partition._getWithDefault('divisions', 1)
        for aLow, aHigh, aClosure, x in self.ranges:
            # check if the beginning of new is in any of the old items
            try:
                if (aLow < bLow and bLow < aHigh) or\
                    (bLow < aLow and aLow < bHigh):
                    # (abab, abba) or (baba, baab)
                    raise ConfigurationError
                if aHigh == bLow and\
                    aClosure.endswith('d') and bClosure.startswith('c'):
                    # aa][bb
                    raise ConfigurationError
                if bHigh == aLow and\
                    bClosure.endswith('d') and aClosure.startswith('c'):
                    # bb][aa
                    raise ConfigurationError
            except ConfigurationError:
                raise ConfigurationError("Collision in \"PartitionedDimension\": the partition with low, high values of %s, %s overlaps another partition's range" % (str(bLow), str(bHigh)))
        self.ranges.append((bLow, bHigh, bClosure, bDivisions))

    def post_validate(self):
        definitionList = self.matches(Partition)
        if len(definitionList) == 0:
            raise ConfigurationError("Must specify at least one partition in \"PartitionedDimension\"")
        self.ranges = []
        for definition in definitionList:
            self._addRangeTuple(definition)

Config.classMap["PartitionedDimension"] = PartitionedDimension

class Partition(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Partition">
    <xs:complexType>
      <xs:attribute name="low" type="xs:double" use="optional"/>
      <xs:attribute name="high" type="xs:double" use="optional"/>
      <xs:attribute name="divisions" type="xs:integer" use="optional"/>
      <!-- if 'divisions is present:
         * Closure cannot be closedClosed
         * Closure cannot be openOpen
         * both 'low' and 'high' must be present. -->
      <xs:attribute name="closure" default="openClosed" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="closedClosed"/>
            <xs:enumeration value="closedOpen"/>
            <xs:enumeration value="openClosed"/>
            <xs:enumeration value="openOpen"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
    </xs:complexType>
  </xs:element>
""")

    def _getWithDefault(self, key, default):
        tmp = default if key not in self.attrib else self[key]
        if tmp not in (AllSegments.NEGINF, AllSegments.INF):
            try:
                if key in ('low', 'high'):
                    tmp = float(tmp)
                if key == 'divisions':
                    tmp = int(tmp)
            except ValueError:
                raise ConfigurationError("In \"Partition\", the low and high values must be convertible to floats (or absent), and the divisions must be convertible to an integer")
        return tmp

    def post_validate(self):
        if 'low' in self.attrib and 'high' in self.attrib and\
            self['low'] > self['high']:

            raise ConfigurationError("'low' value in \"Partition\" must be less than 'high' value in \"Partition\"")

        if 'divisions' in self.attrib:
            if 'divisions' <= 0:
                raise ConfigurationError("'divisions' in \"Partition\" must be one or greater")
            if 'divisions' > 1:
                if 'low' not in self.attrib or 'high' not in self.attrib:
                    raise ConfigurationError("A \"Partition\" with 'divisions' must have both the 'low' and 'high' bound defined")
                if 'closure' in self.attrib and\
                    self['closure'] in ('closedClosed', 'openOpen'):

                    raise ConfigurationError("A \"Partition\" with 'divisions' cannot use 'closure' values of 'closedClosed' (ranges would overlap) or 'openOpen' (ranges would exclude their boundary)")
        else:
            if 'low' not in self.attrib and 'closure' in self.attrib:
                if self['closure'].startswith('c'):
                    raise ConfigurationError("A \"Partition\" that is unbounded on the low end cannot use 'closure' values of 'closedClosed' or 'closedOpen'")
            if 'high' not in self.attrib:
                if 'closure' not in self.attrib:
                    raise ConfigurationError("A \"Partition\" that is unbounded on the high end cannot use 'closure' values of 'openClosed', which is the default...")
                elif self['closure'].endswith('d'):
                    raise ConfigurationError("A \"Partition\" that is unbounded on the high end cannot use 'closure' values of 'closedClosed' or 'openClosed'")

Config.classMap["Partition"] = Partition

class AlternateDistribution(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="AlternateDistribution">
    <xs:complexType>
      <xs:choice>
        <xs:element ref="Distribution"/>
        <xs:element ref="MeanShift"/>
      </xs:choice>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["AlternateDistribution"] = AlternateDistribution

class MeanShift(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="MeanShift">
    <xs:complexType>
      <xs:attribute name="sigmas" type="xs:double" use="required"/>
      <!-- alternate distribution is the baseline with mean
           multiplied by "sigmas" -->
    </xs:complexType>
  </xs:element>
""")

Config.classMap["MeanShift"] = MeanShift

class Distribution(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="Distribution">
    <xs:complexType>
      <xs:attribute name="dist" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="gaussian"/>
            <xs:enumeration value="poisson"/>
            <xs:enumeration value="exponential"/>
            <xs:enumeration value="uniform"/>
            <xs:enumeration value="discrete"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="file" type="xs:string" use="required"/>
      <xs:attribute name="type" default="XML" use="optional">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="CSV"/>
            <xs:enumeration value="UniTable"/>
            <xs:enumeration value="XML"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="header" type="xs:string" use="optional"/>
      <xs:attribute name="sep" type="xs:string" use="optional"/>
      <xs:attribute name="types" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

Config.classMap["Distribution"] = Distribution

class ModelVerification(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="ModelVerification">
    <xs:complexType>
      <xs:attribute name="checkModel" type="xs:boolean" default="true" use="optional" />
      <xs:attribute name="checkSegments" type="xs:boolean" default="true" use="optional" />
      <xs:attribute name="onFailures" default="halt">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="halt"/>
            <xs:enumeration value="report"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="reportInScores" type="xs:boolean" default="false" use="optional" />
    </xs:complexType>
  </xs:element>
""")

Config.classMap["ModelVerification"] = ModelVerification

class CustomProcessing(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="CustomProcessing">
    <xs:complexType>
      <xs:sequence>
          <xs:element minOccurs="1" maxOccurs="1" ref="PersistentStorage"/>
          <xs:element minOccurs="0" maxOccurs="unbounded" ref="Context"/>
      </xs:sequence>
      <xs:attribute name="action" type="xs:string" use="required"/>
      <xs:attribute name="begin" type="xs:string" use="optional"/>
      <xs:attribute name="end" type="xs:string" use="optional"/>
      <xs:attribute name="exception" type="xs:string" use="optional"/>
    </xs:complexType>
  </xs:element>
""")

    def post_validate(self):
        cdatas = [i for i in self.children if isinstance(i, xmlbase.XMLCDATA)]
        if len(cdatas) != 1:
            raise ConfigurationError("A CustomProcessing object must contain exactly one CDATA")
        self.code = "".join(cdatas[0].text).lstrip().rstrip()

    class Output(StringIO.StringIO):
        _eventNumber = "begin"

        def xmlopen(self, tag="Event", attrib=None):
            if not hasattr(self, "_tagsToClose"):
                self._tagsToClose = []
            self._tagsToClose.append(tag)
            if tag == "Event" and attrib is None:
                attrib = {"number": self._eventNumber}
            elif attrib is None:
                attrib = {}
            if len(attrib) > 0:
                a = " ".join([""] + ["%s=%s" % (name, quoteattr(str(value))) for name, value in attrib.items()])
            else:
                a = ""
            self.write("<%s%s>" % (str(tag), a))

        def xmlfield(self, name, value, attrib={}):
            if len(attrib) > 0:
                a = " ".join([""] + ["%s=%s" % (n, quoteattr(str(v))) for n, v in attrib.items()])
            else:
                a = ""
            if value == "" or value is None:
                self.write("<%s%s/>" % (str(name), a))
            else:
                self.write("<%s%s>%s</%s>" % (str(name), a, str(value), str(name)))

        def xmlclose(self):
            if not hasattr(self, "_tagsToClose"):
                self._tagsToClose = []
            if len(self._tagsToClose) > 0:
                self.write("</%s>" % self._tagsToClose.pop())

        def xmlcloseall(self):
            if not hasattr(self, "_tagsToClose"):
                self._tagsToClose = []
            lastReturn = (len(self._tagsToClose) > 0)
            while len(self._tagsToClose) > 0:
                self.write("</%s>" % self._tagsToClose.pop())
            if lastReturn:
                self.write(os.linesep)

    def initialize(self, pmmlDocument, pmmlModel, constants, allSegments, atoms, logger, metadata, consumerUpdateScheme, producerUpdateScheme):
        self.breakPoints = True
        self.allSegments = allSegments
        self.constants = constants

        self.persistentStorage = self.child(PersistentStorage)
        self.persistentStorage.read()

        self.globalVariables = NameSpace()

        self.context = {"pmmlDocument": pmmlDocument,
                        "pmmlModel": pmmlModel,
                        "allSegments": self.allSegments,
                        "db": self.persistentStorage.db[None],
                        "const": self.constants,
                        "g": self.globalVariables,
                        "logger": logger,
                        "metadata": metadata,
                        "consumerUpdateScheme": consumerUpdateScheme,
                        "producerUpdateScheme": producerUpdateScheme,
                        "breakpoint": self.breakpoint,
                        }
        self.context.update(atoms)
        for c in self.matches(Context):
            self.context.update(c.context)
        
        if self.code is not None:
            self.code += os.linesep + os.linesep + "None" # In Python 2.6 (only), an indented comment on the last line can cause SyntaxError

            ## CAREFUL: evaluates whatever you give it!
            try:
                exec self.code in self.context
            except SyntaxError, err:
                raise SyntaxError("CustomProcessing could not be evaluated: %s" % str(err))

            try:
                self.actionFunction = self.context[self.attrib["action"]]
                if not callable(self.actionFunction):
                    raise KeyError
            except KeyError:
                raise RuntimeError("CustomProcessing does not contain an action function named \"%s\"" % self.attrib["begin"])

            if "begin" in self.attrib:
                try:
                    self.beginFunction = self.context[self.attrib["begin"]]
                    if not callable(self.beginFunction):
                        raise KeyError
                except KeyError:
                    raise RuntimeError("CustomProcessing does not contain a begin function named \"%s\"" % self.attrib["begin"])
            else:
                self.beginFunction = None

            if "end" in self.attrib:
                try:
                    self.endFunction = self.context[self.attrib["end"]]
                    if not callable(self.endFunction):
                        raise KeyError
                except KeyError:
                    raise RuntimeError("CustomProcessing does not contain an end function named \"%s\"" % self.attrib["end"])
            else:
                self.endFunction = None

            if "exception" in self.attrib:
                try:
                    self.exceptionFunction = self.context[self.attrib["exception"]]
                    if not callable(self.exceptionFunction):
                        raise KeyError
                except KeyError:
                    raise RuntimeError("CustomProcessing does not contain an exception function named \"%s\"" % self.attrib["exception"])
            else:
                self.exceptionFunction = None

        else:
            del self.context["breakpoint"]
            self.context = NameSpace(**self.context)

    def doBegin(self):
        self.context["output"] = self.Output()

        if self.code is not None:
            if self.beginFunction is not None:
                ## CAREFUL: evaluates whatever you give it!
                exec self.beginFunction.func_code in self.context
        else:
            self.callbackClass.begin(self.context)

        output = self.context["output"]
        del self.context["output"]
        if isinstance(output, self.Output):
            output.xmlcloseall()
            return output.getvalue()
        else:
            return None

    def doEvent(self, syncNumber, eventNumber, get, matchingSegments):
        self.context["syncNumber"] = syncNumber
        self.context["eventNumber"] = eventNumber
        self.context["get"] = get
        self.context["segments"] = matchingSegments
        self.context["output"] = self.Output()
        self.context["output"]._eventNumber = eventNumber

        for segment in matchingSegments:
            segment.get = segment.pmmlModel.dataContext.get
            segment.score = (lambda s: (lambda: s.consumer.score(syncNumber, s.get)))(segment)
            segment.update = (lambda s: (lambda: s.producer.update(syncNumber, s.get)))(segment)

        if self.code is not None:
            ## CAREFUL: evaluates whatever you give it!
            exec self.actionFunction.func_code in self.context
        else:
            self.callbackClass.action(self.context)

        output = self.context["output"]

        for segment in matchingSegments:
            del segment.get
            del segment.score
            del segment.update

        del self.context["syncNumber"]
        del self.context["eventNumber"]
        del self.context["get"]
        del self.context["segments"]
        del self.context["output"]

        if isinstance(output, self.Output):
            output.xmlcloseall()
            return output.getvalue()
        else:
            return None

    def doEnd(self):
        self.context["output"] = self.Output()
        self.context["output"]._eventNumber = "end"

        if self.code is not None:
            if self.endFunction is not None:
                ## CAREFUL: evaluates whatever you give it!
                exec self.endFunction.func_code in self.context
        else:
            self.callbackClass.end(self.context)
        self.persistentStorage.write()

        output = self.context["output"]
        del self.context["output"]
        if isinstance(output, StringIO.StringIO):
            output.xmlcloseall()
            return output.getvalue()
        else:
            return None

    def doException(self):
        if self.code is not None:
            if self.exceptionFunction is not None:
                ## CAREFUL: evaluates whatever you give it!
                exec self.exceptionFunction.func_code in self.context
        else:
            self.callbackClass.exception(self.context)
        self.persistentStorage.write()

    def breakpoint(self, label="(unlabeled)", variables={}):
        if self.breakPoints:
            ## CAREFUL: evaluates whatever you give it!
            context = dict(self.context)
            context.update(variables)
            interactiveConsole = code.InteractiveConsole(context)

            # interactiveConsole.interact("Stopping at breakpoint \"%s\"; press ctrl-D when done.")
            print "Breakpoint \"%s\": press ctrl-D to exit this breakpoint, ctrl-C to skip all breakpoints." % label
            done = False
            while not done:
                try:
                    exec code.compile_command(interactiveConsole.raw_input(">>> ")) in context

                except EOFError:
                    done = True

                except KeyboardInterrupt:
                    done = True
                    self.breakPoints = False

                except Exception, err:
                    print "%s: %s" % (err.__class__.__name__, str(err))

            return context

        else:
            return {}

Config.classMap["CustomProcessing"] = CustomProcessing

class Context(Config):
    xsd = load_xsdElement(Config, """
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
            raise ConfigurationError("Context element could not load library \"%s\"" % self.attrib["library"])

        if haspath: sys.path.remove(self.attrib["path"])

        if self.attrib["as"] == "*":
            self.context = tmp.__dict__
        else:
            self.context = {self.attrib["as"]: tmp}

Config.classMap["Context"] = Context

class PersistentStorage(Config):
    xsd = load_xsdElement(Config, """
  <xs:element name="PersistentStorage">
    <xs:complexType>
      <xs:attribute name="connect" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  """)

    protocols = ["json", "pickle"]

    def post_validate(self):
        if self.attrib["connect"] == "":
            self.protocol = None
            self.address = None
            return

        m = re.match("([a-zA-Z]+)://(.*)", self.attrib["connect"])
        if m is None:
            raise ConfigurationError("Connect string must be PROTOCOL://ADDRESS, not \"%s\"" % self.attrib["connect"])
        self.protocol, self.address = m.groups()
        self.protocol = self.protocol.lower()

        if self.protocol not in self.protocols:
            raise ConfigurationError("Protocol \"%s\" not recognized; must be one of %s" % (self.protocol, self.protocols))

    def read(self):
        if self.protocol is None:
            self.db = {None: NameSpace()}
            return

        # check the file-based protocols
        if self.protocol in ("json", "pickle",):
            directory = os.path.sep.join((self.address).split(os.path.sep)[:-1])
            if directory == "": directory = "."

            if not os.path.exists(directory):
                raise IOError("No directory named \"%s\" in which to save as %s" % (directory, self.protocol))

        if os.path.exists(self.address):
            if self.protocol == "json":
                if json is None: raise NotImplementedError("The json library was not found on your system.")

                thejson = json.load(file(self.address))
                self.db = {}

                try:
                    self.db[None] = NameSpace(**thejson["Global"])
                    for name, value in thejson["Segments"].items():
                        self.db[name] = NameSpace(**value)
                except KeyError, TypeError:
                    raise IOError("PersistentStorage json file is incorrectly formatted")

            elif self.protocol == "pickle":
                self.db = pickle.load(file(self.address))

        else:
            self.db = {None: NameSpace()}

    def write(self):
        if self.protocol is None: return

        if self.protocol == "json":
            if json is None: raise NotImplementedError("The json library was not found on your system.")

            dbglobal = self.db[None]
            dbsegments = dict(self.db)
            del dbsegments[None]

            json.dump({"Global": dbglobal, "Segments": dbsegments}, file(self.address, "w"), cls=JSONEncoderWithNameSpace, sort_keys=True)

        elif self.protocol == "pickle":
            pickle.dump(self.db, file(self.address, "w"))
        
Config.classMap["PersistentStorage"] = PersistentStorage
