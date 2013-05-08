""""""


__copyright__ = """
Copyright (C) 2006-2009  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""



import xml.sax
from pmmlModels import *
import sys

pmmlClassMap = {
    #models
    "PMML":pmmlPMML,
    "BaselineModel":pmmlBaselineModel,
    "TreeModel":pmmlTreeModel,
    "NaiveBayesModel":pmmlNaiveBayesModel,
    "NaiveBayes":pmmlNaiveBayes,
    "Regression":pmmlRegression,
    "RegressionModel":pmmlRegressionModel,
    
    #Model elements
    "MiningField":pmmlMiningField,
    "MiningSchema":pmmlMiningSchema,
    "OutputField":pmmlOutputField,
    "Output":pmmlOutput,
    "Counts":pmmlCounts,
    "Quantile":pmmlQuantile,
    "NumericInfo":pmmlNumericInfo,
    "Array":pmmlArray,
    "DiscrStats":pmmlDiscrStats,
    "ContStats":pmmlContStats,
    "UnivariateStats":pmmlUnivariateStats,
    "ModelStats":pmmlModelStats,
    "TargetValue":pmmlTargetValue,
    "Target":pmmlTarget,
    "Targets":pmmlTargets,
    "VerificationField":pmmlVerificationField,
    "VerificationFields":pmmlVerificationFields,
    "ModelVerification":pmmlModelVerification,
    "FieldValueCount":pmmlFieldValueCount,
    "FieldValueCounts":pmmlFieldValueCounts,
    "CountTable":pmmlCountTable,
    "NormalizedCountTable":pmmlNormalizedCountTable,
    "HistogramTable":pmmlHistogramTable,
    "UniformDistribution":pmmlUniformDistribution,
    "PoissonDistribution":pmmlPoissonDistribution,
    "GaussianDistribution":pmmlGaussianDistribution,
    "AnyDistribution":pmmlAnyDistribution,
    "Alternate":pmmlAlternate,
    "Baseline":pmmlBaseline,
    "TestDistributions":pmmlTestDistributions,
    "SimplePredicate":pmmlSimplePredicate,
    "CompoundPredicate":pmmlCompoundPredicate,
    "SimpleSetPredicate":pmmlSimpleSetPredicate,
    "True":pmmlTrue,
    "False":pmmlFalse,
    "Partition":pmmlPartition,
    "PartitionFieldStats":pmmlPartitionFieldStats,
    "ScoreDistribution":pmmlScoreDistribution,
    "Node":pmmlNode,
    "BayesInputs":pmmlBayesInputs,
    "BayesOutput":pmmlBayesOutput,
    "BayesInput":pmmlBayesInput,
    "RegressionTable":pmmlRegressionTable,
    "NumericPredictor":pmmlNumericPredictor,
    "PredictorTerm":pmmlPredictorTerm,
    "CategoricalPredictor":pmmlCategoricalPredictor,
    
    #Extensions
    "Skip":extensionSkip,
    "Segments":pmmlSegments,
    "RegularSegment":pmmlRegularSegment,
    "ExplicitSegment":pmmlExplicitSegment,
    "ValueCount":pmmlValueCount,
    "DiscreteDistribution":pmmlDiscreteDistribution,
    
    #Elements
    "Extension":pmmlExtension,
    "Application":pmmlApplication,
    "Annotation":pmmlAnnotation,
    "Timestamp":pmmlTimestamp,
    "Header":pmmlHeader,
    "MiningBuildTask":pmmlMiningBuildTask,
    "Interval":pmmlInterval,
    "Value":pmmlValue,
    "DataField":pmmlDataField,
    "TableLocator":pmmlTableLocator,
    "row":pmmlrow,
    "InlineTable":pmmlInlineTable,
    "ChildParent":pmmlChildParent,
    "Taxonomy":pmmlTaxonomy,
    "DataDictionary":pmmlDataDictionary,
    "Constant":pmmlConstant,
    "FieldRef":pmmlFieldRef,
    "LinearNorm":pmmlLinearNorm,
    "NormContinuous":pmmlNormContinuous,
    "NormDiscrete":pmmlNormDiscrete,
    "DiscretizeBin":pmmlDiscretizeBin,
    "Discretize":pmmlDiscretize,
    "FieldColumnPair":pmmlFieldColumnPair,
    "MapValues":pmmlMapValues,
    "Apply":pmmlApply,
    "Aggregate":pmmlAggregate,
    "ParameterField":pmmlParameterField,
    "DefineFunction":pmmlDefineFunction,
    "DerivedField":pmmlDerivedField,
    "TransformationDictionary":pmmlTransformationDictionary,
    "EventCount":pmmlEventCount,
    "LocalTransformations":pmmlLocalTransformations,
    "PairCounts":pmmlPairCounts,
    "TargetValueCounts":pmmlTargetValueCounts,
    "TargetValueCount":pmmlTargetValueCount,
    
    #strictly for use to process raw xml
    "*":pmmlXMLElement}

class xmlElement:
  """"""
  def __init__(self, name, attributes):
    """"""
    self.name = name
    self.attributes = attributes
    self.children = []
    self.__finished = False
  
  def addChild(self, child):
    """"""
    self.children.append(child)
  
  def __repr__(self):
    """"""
    """allows the elements to print themselves"""
    out = "<" + self.name
    
    for name in self.attributes:
      if self.attributes[name]:
        out += " " + name + '="' + self.attributes[name] + '"'
    
    #close opening tag
    out += ">"
    
    cur = len(out)
    
    #print the children
    for aChild in self.__children:
      out += aChild.__repr__()
    
    if cur != len(out):
      #closing tag
      out += "</" + self.name + ">"
    else:
      #empty element
      out = out[:len(out) - 1] + " />"
    
    return out

class pmmlElementHandler(xml.sax.handler.ContentHandler):
  """"""
  def __init__(self):
    """"""
    self.__list = []
    self.__buffer = []
  
  def startElement(self, name, attributes):
    """"""
    self.flushBuffer()
    newAtts = {}
    name = name.encode('ascii', 'xmlcharrefreplace')
    for key in attributes.keys():
      newAtts[str(key)] = str(attributes[key])
    self.__list.append(xmlElement(name, newAtts))
  
  def characters(self, data):
    """"""
    self.__buffer.append(data)
  
  def endElement(self, name):
    """"""
    self.flushBuffer()
    
    try:
      current = self.__list[len(self.__list) - 1]
      if current.name in pmmlClassMap:
        element = pmmlClassMap[current.name](current.name, current.attributes, current.children)
      else:
        element = pmmlClassMap["*"](current.name, current.attributes, current.children)
      
      if len(self.__list) > 1:
        self.__list[len(self.__list) - 2].addChild(element)
      else:
        self.root = element
      
      self.__list.pop()
    except pmmlError, err:
      print "--------PMML Reader Error--------\n" + str(err)
      sys.exit(1)
  
  def flushBuffer(self):
    """"""
    text = ''.join(self.__buffer)
    if len(self.__list) > 0 and len(text) > 0 and not text.isspace():
      self.__list[len(self.__list) - 1].addChild(pmmlString(text.encode('ascii', 'xmlcharrefreplace')))
    
    self.__buffer = []

class pmmlReader:
  """"""
  def __init__(self):
    """"""
    self.__handler = pmmlElementHandler()
    self.__parser = xml.sax.make_parser()
    self.__parser.setContentHandler(self.__handler)
  
  def parse(self, filename):
    """"""
    self.__parser.parse(filename)
    self.root = self.__handler.root
