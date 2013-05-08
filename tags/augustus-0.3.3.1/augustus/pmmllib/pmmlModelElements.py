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


from pmmlElements import *

#This file defines PMML elements utilized within models

#for each element, the same functions are defined that are
#described at the top of the pmmlElements.py file.

#############################################################################
#elements that belong to models in general
class pmmlMiningField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name","usageType","optype","importance","outliers",\
    "lowValue","highValue","missingValueReplacement","missingValueTreatment",\
    "invalidValueTreatment"]
    requiredAttributes = ["name"]
    pmmlElement.__init__(self, "MiningField", myChild, attributeNames, attributes, requiredAttributes)
  
  def __setMissingValue(self, convert):
    """"""
    self.__missing = self.getAttribute("missingValueReplacement")
    if not self.__missing is None and convert:
      self.__missing = convert(self.__missing)
  
  def dependencies(self):
    return []
  
  def code(self, get, indent=""):
    name = self.getAttribute("name")
    dataType = self.getAttribute("dataType")
    missing = self.getAttribute("missingValueReplacement")
    if missing:
      ending = indent + "if " + name + """ is None:
""" + indent + "  " + name + " = '" + missing + """'
"""
    return indent + name + " = get('" + name + "')\n"
  
  def column(self, get, call):
    name = self.getAttribute("name")
    return execColumn(name, self.code(get), "MiningField:  " + name, localDict={"pmmlString":pmmlString,"pmmlErrorStrings":pmmlErrorStrings,"NULL":NULL,"pmmlError":pmmlError,"fieldMissing":fieldMissing,"get":get})
  
  def initialize(self, get, convert):
    """"""
    #remember where to get the value
    self.__get = get
    self.__field = self.getAttribute("name")
    #store the missing value
    self.__setMissingValue(convert)
  
  def defaultOnly(self, convert=None):
    """"""
    self.__setMissingValue(convert)
    self.get = self.getMissing
  
  def getMissing(self):
    """"""
    return self.__missing
  
  def get(self):
    """"""
    value = self.__get(self.__field)
    if value is None:
      return self.__missing
    return value

class pmmlMiningSchema(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "MiningSchema:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MiningSchema:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "MiningSchema", myChild)
    self.__cache = {}
  
  def __add(self, name, needed, dependencies):
    if not name in needed:
      needed.append(name)
      if name in dependencies:
        for child in dependencies[name]:
          self.__add(child, needed, dependencies)

  def getPredicted(self):
    predicted = []
    fields = self.getChildrenOfType(pmmlMiningField)
    for field in fields:
      if (field.getAttribute("usageType")=='predicted'):        
        predicted.append(field.getAttribute("name"))
    return predicted

  def limit(self, dataInput, outputFields):
    dependencies = dataInput.dependencies()
    fields = self.getChildrenOfType(pmmlMiningField)
    needed = []
    directFields = []
    for field in fields:
      name = field.getAttribute("name")
      #record which fields are required to be processed
      self.__add(name, needed, dependencies)
      #if the field is already in the dataInput, simply add the correct code
      if name in dataInput:
        directFields.append(name)
      #add the correct column
      else:
        pass
    for field in outputFields:
      self.__add(field, needed, dependencies)
    return directFields
  
  def dictionary(self, get):
    """This function is currently not used."""
    dictionary.__init__(self, self.getChildrenOfType(pmmlMiningField), get)

class pmmlOutputField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["name","displayName","optype","dataType","targetField",\
          "feature","value"]
    requiredAttributes = ["name"]
    pmmlElement.__init__(self, "OutputField", myChild, attributeNames, attributes, requiredAttributes)

class pmmlOutput(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlOutputField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Output:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Output:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Output", myChild)

class pmmlCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["totalFreq","missingFreq","invalidFreq"]
    requiredAttributes = ["totalFreq"]
    pmmlElement.__init__(self, "Counts", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargetValueCount(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [0]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TargetValueCount:  " + pmmlErrorStrings.elements
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["value","count"]
    requiredAttributes = ["value","count"]
    pmmlElement.__init__(self, "TargetValueCount", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargetValueCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValueCount]
    maximums = [None, None]
    minimums = [0, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TargetValueCounts:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence([pmmlExtension,pmmlTargetValueCount], minimums, maximums,children)    
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "TargetValueCounts", myChild, attributeNames, attributes, requiredAttributes)

class pmmlPairCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValueCounts]
    maximums = [None, 1]
    minimums = [0, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "PairCounts:  " + pmmlErrorStrings.elements
    #myChild = pmmlList([pmmlExtension, pmmlTargetValueCounts], children)
    myChild = pmmlSequence([pmmlExtension,pmmlTargetValueCounts], minimums, maximums,children)    
    attributeNames = ["value"]
    requiredAttributes = ["value"]
    pmmlElement.__init__(self, "PairCounts", myChild, attributeNames, attributes, requiredAttributes)


class pmmlQuantile(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["quantileLimit","quantileValue"]
    requiredAttributes = ["quantileLimit","quantileValue"]
    pmmlElement.__init__(self, "Quantile", myChild, attributeNames, attributes, requiredAttributes)

class pmmlNumericInfo(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlQuantile]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NumericInfo:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["minimum","maximum","mean","standardDeviation","median","interQuartileRange"]
    pmmlElement.__init__(self, "NumericInfo", myChild, attributeNames, attributes)

class pmmlArray(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    (extras, children) = pmmlSequence.formatChildren([pmmlString], [1], children)
    if extras != []:
      raise pmmlError, "Array:  " + pmmlErrorStrings.elements
    if len(children) != 1:
      raise pmmlError, "Array:  " + pmmlErrorStrings.elements
    myChild = pmmlString(children[0])
    pmmlElement.__init__(self, "Array", myChild, ["n","type"], attributes, ["type"], whiteSpaceImportant=True)

class pmmlDiscrStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 2]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "DiscrStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modalValue"]
    pmmlElement.__init__(self, "DiscrStats", myChild, attributeNames, attributes)

class pmmlContStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlInterval, pmmlArray]
    maximums = [None, None, 3]
    minimums = [None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ContStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["totalValuesSum","totalSquaresSum"]
    pmmlElement.__init__(self, "ContStats", myChild, attributeNames, attributes)

class pmmlUnivariateStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlCounts, pmmlNumericInfo, pmmlDiscrStats, pmmlContStats]
    maximums = [None, 1, 1, 1, 1]
    minimums = [None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "UnivariateStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field"]
    pmmlElement.__init__(self, "UnivariateStats", myChild, attributeNames, attributes)

class pmmlModelStats(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlUnivariateStats]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "ModelStats:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "ModelStats", myChild)

class pmmlTargetValue(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["value","rawDataValue","priorProbability","defaultValue"]
    pmmlElement.__init__(self, "TargetValue", myChild, attributeNames, attributes)

class pmmlTarget(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTargetValue]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Target:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field","optype","castInteger","min","max","rescaleConstant","rescaleFactor"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self, "Target", myChild, attributeNames, attributes, requiredAttributes)

class pmmlTargets(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlTarget]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Targets:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Targets:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Targets", myChild)

class pmmlVerificationField(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["field","column","precision","zeroThreshold"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self, "VerificationField", myChild, attributeNames, attributes, requiredAttributes)

class pmmlVerificationFields(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlVerificationField]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "VerificationFields:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "VerificationFields:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "VerificationFields", myChild)

class pmmlModelVerification(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlVerificationFields, pmmlInlineTable]
    maximums = [None, 1, 1]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ModelVerification:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["recordCount","fieldCount"]
    pmmlElement.__init__(self, "ModelVerification", myChild, attributeNames, attributes)

class pmmlRegularSegment(pmmlEmptyElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    if children != []:
      raise pmmlError, "RegularSegment:  " + pmmlErrorStrings.noElements
    pmmlEmptyElement.__init__(self, "RegularSegment", ["field","low","high"], attributes,["field","low","high"])

class pmmlExplicitSegment(pmmlEmptyElement):
  """"""
  def __init__(self, name="", attributes={}, children=[],whiteSpaceImportant=False):
    """"""
    if children != []:
      raise pmmlError, "ExplicitSegment:  " + pmmlErrorStrings.noElements
    pmmlEmptyElement.__init__(self, "ExplicitSegment", ["field","value"], attributes,["field","value"],whiteSpaceImportant)

class pmmlSegments(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExplicitSegment, pmmlRegularSegment]
    myChild = pmmlBoundedList(types, None, None, children)
    pmmlElement.__init__(self, "Segments", myChild)

#############################################################################
#elements that belong to specific models
