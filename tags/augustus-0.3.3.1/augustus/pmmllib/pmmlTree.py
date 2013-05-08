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


from pmmlModelElements import *

#This file defines PMML elements utilized within tree models

#for each element, the same functions are defined that are
#described at the top of the pmmlElements.py file.

class pmmlSimplePredicate(pmmlElement):
  """SimplePredicate element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "SimplePredicate:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field", "operator", "value"]
    requiredAttributes = ["field", "operator"]
    pmmlElement.__init__(self,"SimplePredicate", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput):
    """Initialize the predicate for scoring."""
    field = self.getAttribute("field")
    self.__datatype = dataInput.datatype(field)
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    field = self.getAttribute("field")
    op = self.getAttribute("operator")
    if op == "equal":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) == value
    elif op == "notEqual":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) != value
    elif op == "lessThan":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) < value
    elif op == "lessOrEqual":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) <= value
    elif op == "greaterThan":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) > value
    elif op == "greaterOrEqual":
      value = pmmlString.convert(self.getAttribute("value"), self.__datatype)
      return get(field) >= value
    elif op != "isMissing":
      return get(field) is None or get(field) is NULL
    elif op == "inNotMissing":
      return get(field) is not None and get(field) is not NULL
    else:
      #We really shouldn't get here.
      #This implies that there is an error in the pmml.
      return False

class pmmlCompoundPredicate(pmmlElement):
  """CompoundPredicate element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC]
    maximums = [None, None]
    minimums = [None, 2]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "CompoundPredicate:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["booleanOperator"]
    requiredAttributes = ["booleanOperator"]
    pmmlElement.__init__(self,"CompoundPredicate", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput):
    """Initialize the predicate for scoring."""
    pass
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    return False

class pmmlSimpleSetPredicate(pmmlElement):
  """SimpleSetPredicate element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlArray]
    maximums = [None, 1]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "SimpleSetPredicate:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field", "booleanOperator"]
    requiredAttributes = ["field", "booleanOperator"]
    pmmlElement.__init__(self,"SimpleSetPredicate", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput):
    """Initialize the predicate for scoring."""
    pass
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    return False

class pmmlTrue(pmmlElement):
  """True element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "True:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self,"True", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput):
    """Initialize the predicate for scoring."""
    pass
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    return True

class pmmlFalse(pmmlElement):
  """False element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "False:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self,"False", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput):
    """Initialize the predicate for scoring."""
    pass
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    return False

class choiceSC(pmmlChoice):
  """Predicate group"""
  __types = [pmmlSimplePredicate, pmmlCompoundPredicate, pmmlSimpleSetPredicate, pmmlTrue, pmmlFalse]
  __maximums = [1, 1, 1, 1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1, 1, 1, 1]
    pmmlChoice.__init__(self, choiceSC.__types, minimums, choiceSC.__maximums, instances)

  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceSC.__types, choiceSC.__maximums, children)

class pmmlPartition(pmmlElement):
  """Partition element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlPartitionFieldStats]
    maximums = [None, None]
    minimums = [None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Partition:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["name", "size"]
    requiredAttributes = ["name"]
    pmmlElement.__init__(self,"Partition", myChild, attributeNames, attributes, requiredAttributes)

class pmmlPartitionFieldStats(pmmlElement):
  """PartitionFieldStats element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlCounts, pmmlNumericInfo, pmmlArray]
    maximums = [None, 1, 1, 1]
    minimums = [None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "PartitionFieldStats:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self,"PartitionFieldStats", myChild, attributeNames, attributes, requiredAttributes)

class pmmlScoreDistribution(pmmlElement):
  """ScoreDistribution element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "ScoreDistribution:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["value", "recordCount", "confidence"]
    requiredAttributes = ["value", "recordCount"]
    pmmlElement.__init__(self,"ScoreDistribution", myChild, attributeNames, attributes, requiredAttributes)

class sequencePS(pmmlSequence):
  """Partition/ScoreDistirbution/Node sequence that is embedded in a choice in Node."""
  # pmmlNode is appended to the given types after it is defined below
  __types = [pmmlPartition, pmmlScoreDistribution]
  
  @staticmethod
  def add(aType):
    """Adds a type to the sequencePS.__types list.  Should only be used for the pmmlNode selfreferential loop."""
    sequencePS.__types.append(aType)
  
  __maximums = [1, None, None]
  def __init__(self, children):
    """"""
    minimums = [None, None, None]
    pmmlSequence.__init__(self, sequencePS.__types, minimums, sequencePS.__maximums, children)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlSequence.formatChildren(sequencePS.__types, sequencePS.__maximums, children)

class pmmlEmbeddedModel(pmmlElement):
  """EmbeddedModel element which can used in tree models.  Not implemented."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "EmbeddedModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self,"EmbeddedModel", myChild, attributeNames, attributes, requiredAttributes)

class choicePE(pmmlChoice):
  """Partition/ScoreDistirbution/Node EmbeddedModel choice in the Node lement"""
  __types = [sequencePS, pmmlEmbeddedModel]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choicePE.__types, minimums, choicePE.__maximums, instances)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choicePE.__types, choicePE.__maximums, children)

class pmmlNode(pmmlElement):
  """Node element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC, choicePE, pmmlSegments]
    maximums = [None, 1, 1, 1]
    minimums = [None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Node:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["id", "score", "recordCount", "defaultChild"]
    requiredAttributes = []
    pmmlElement.__init__(self,"Node", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput, directFields, localTransDict=None, root = True):
    """Initialiaze the node for scoring."""
    #Get our predicate and any children.
    self.predicate = self.getChildrenOfType(choiceSC)[0]
    self.predicate.initialize(dataInput)
    self.children = self.getChildrenOfType(choicePE)
    
    #Initialiaze our children
    for child in self.children:
      child.initialize(dataInput, directFields, localTransDict, False)
    
    #Everything else is only for the root node to do.
    if not root:
      self.__root = False
      return
    
    self.__root = True
    
    self.__directFields = directFields
    self.__getMining = dataInput.__getitem__
    #Assign update function.  If we have a localTransformations element use that otherwise do nothing.
    if localTransDict is None:
      self.update = self.__doNothing
      self.get = self.__getMining
      self.revert = self.__doNothing
      self.lastUpdate = self.__doNothing
    else:
      self.update = localTransDict.update
      self.__getLocalTrans = localTransDict.__getitem__
      self.get = self.__getLocal
      self.update = localTransDict.update
      self.revert = localTransDict.revert
      self.lastUpdate = localTransDict.lastUpdate
      self.limit = localTransDict.limit
      self.force = localTransDict.force
    
    #Assign match function.
    self.match = self.__matchLocal
    
    #Set up rules for segmentation
    #get keys for faster sorting
    rules = self.getRestrictions()
    ruleFields = rules.keys()
    fields = []
    values = []
    for field in directFields:
      if field in ruleFields:
        rule = rules[field]
        if isinstance(rule, str):
          fields.append(field)
          values.append(rule)
    #record the rules that will still need to be checked
    self.__orig = rules
    self.__rules = {}
    for field in rules:
      if not field in fields:
        self.__rules[field] = rules[field]
    
    return (tuple(fields), tuple(values))
  
  def score(self, get = None):
    """Return a score from the tree."""
    #Intentionally overwrite the default paramater
    if get is None:
      get = self.get
    
    #print self.predicate
    if self.predicate.checkTF(get):
      #We're at the right node
      if self.children != []:
        #We have child nodes so see what they return
        for child in self.children:
          score = child.score(get)
          if score:
            #We got a valid score so return it, otherwise go to the next child
            if self.__root:
              #Return the list that the producer and consumer expect
              return [(score, False, self.__orig, None)]
            else:
              return score
      else:
        if self.__root:
          #Return the list that the producer and consumer expect
          return [(self.getAttribute("score"), False, self.__orig, None)]
        else:
          return self.getAttribute("score")
    else:
      #Not me, return None
      return None
  
  def getRestrictions(self):
    """Parse segmentation information.
    Coppied from pmmlTestDistributions."""
    restrictions = {}
    segments = self.getChildrenOfType(pmmlSegments)
    if len(segments) == 1:
      segments = segments[0]
      segments = segments.getChildren()
      for segment in segments:
        if isinstance(segment, pmmlExplicitSegment):
          restrictions[segment.getAttribute("field")] = segment.getAttribute("value")
        else:
          restrictions[segment.getAttribute("field")] = (float(segment.getAttribute("low")), float(segment.getAttribute("high")))
    return restrictions
  
  def __matchLocal(self):
    """Copied from pmmlTestDistributions."""
    met = True
    for field in self.__rules:
      rule = self.__rules[field]
      value = self.get(field)
      if value is NULL:
        met = False
        break
      if isinstance(rule, str):
        if str(value) != rule:
          met = False
          break
      elif value < rule[0] or value > rule[1]:
        met = False
        break
    return met
  
  def __getLocal(self, field):
    """Copied from pmmlTestDistributions."""
    if field in self.__directFields:
      return self.__getMining(field)
    return self.__getLocalTrans(field)
  
  def __doNothing(self):
    """This function does nothing and seems to be more efficient at doing nothing than an lambda function is."""
    pass

sequencePS.add(pmmlNode)
