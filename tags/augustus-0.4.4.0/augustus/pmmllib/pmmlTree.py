""""""


__copyright__ = """
Copyright (C) 2006-2010  Open Data ("Open Data" refers to
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

from pmmlBases import pmmlError
from pmmlModelElements import *
import os
from itertools import izip

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
  
  def initialize(self, dataInput, localTrans=None):
    """Initialize the predicate for scoring."""
    self.__field = self.getAttribute("field")
    self.__datatype = dataInput.datatype(self.__field)
    if self.__field is None:
      raise ValueError("Simple Predicate is being built without a field to test!" + os.linesep)
    if self.__datatype is None:
      if localTrans is None:
        # Have to get a datatype from *somewhere* die with prejudice.
        raise pmmlError("Predicate is being built off of field %s with no datatype info%s"%(self.__field, os.linesep))
      else:
        self.__datatype = localTrans.datatype(self.__field)
    self.__op = self.getAttribute("operator")
    self.__value = self.getAttribute("value")
    self.__converted_value = pmmlString.convert(self.__value, self.__datatype)
    return self.getSegment()
  
  def getSegment(self):
    op = self.__op
    if op == "equal":
      return (self.__field,), (self.__value,)
    elif op == "notEqual":
      return (self.__field,), ()
    elif op == "lessThan":
      return (self.__field,), ((None, self.__value),)
    elif op == "lessOrEqual":
      return (self.__field,), ((None, self.__value),)
    elif op == "greaterThan":
      return (self.__field,), ((self.__value, None),)
    elif op == "greaterOrEqual":
      return (self.__field,), ((self.__value, None),)
    elif op == "isMissing":
      return (self.__field,), ()
    elif op == "inNotMissing":
      return (self.__field,), ()
    else:
      #We really shouldn't get here.
      #This implies that there is an error in the pmml.
      #TODO: Change this to logging
      print "Operator %s is not a valid value for an operator in a simple predicate" % op
      # This may not make sense since we do not know the operator
      return (self.__field,), ()
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    field = self.__field
    op = self.__op
    if op == "equal":
      value = self.__converted_value
      return get(field) == value
    elif op == "notEqual":
      value = self.__converted_value
      return get(field) != value
    elif op == "lessThan":
      value = self.__converted_value
      return get(field) < value
    elif op == "lessOrEqual":
      value = self.__converted_value
      return get(field) <= value
    elif op == "greaterThan":
      value = self.__converted_value
      return get(field) > value
    elif op == "greaterOrEqual":
      value = self.__converted_value
      return get(field) >= value
    elif op == "isMissing":
      return get(field) is None or get(field) is NULL
    elif op == "inNotMissing":
      return get(field) is not None and get(field) is not NULL
    else:
      #We really shouldn't get here.
      #This implies that there is an error in the pmml.
      #TODO: Change this to logging
      print "Operator %s is not a valid value for an operator in a simple predicate" % op
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
  
  def initialize(self, dataInput, localTrans=None):
    """Initialize the predicate for scoring."""
    if self.getAttribute("booleanOperator") != "and":
      print 'Warning: only booleanOperator="and" is currently supported with CompoundPredicate.  We are ignoring your choice.'
    self.__predicates = self.getChildrenOfType(choiceSC)
    for pred in self.__predicates:
      pred.initialize(dataInput, localTrans)
    return self.getSegment()
  
  def getSegment(self):
    fields = []
    values = []
    reg_fields = {}
    #Get segmentation information from our children
    for pred in self.__predicates:
      fs, vs = pred.getSegment()
      for f, v in izip(fs, vs):
        if isinstance(v, str):
          fields.append(f)
          values.append(v)
        else:
          #"regular" segmentation.
          if f in reg_fields:
            #The max is because predicates of x > 1 and x > 3 can only be satisfied if x > max(1, 3)
            v_min = reg_fields[f][0] if v[0] is None else max(v[0], reg_fields[f][0])
            
            #Because min of None and anything is None we need to avoid both arguments being None
            if v[1] is None:
              v_max = reg_fields[f][1]
            elif reg_fields[f][1] is None:
              v_max = v[1]
            else:
              v_max = min(v[1], reg_fields[f][1])
            
            reg_fields[f] = (v_min, v_max)
          else:
            reg_fields[f] = v
    
    #Now add regular segmentation info to our list
    if reg_fields:
      for f,v in reg_fields.iteritems():
        fields.append(f)
        values.append(v)
    
    
    if fields:
      return tuple(fields), tuple(values)
    else:
      return (None,), (None,)
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    #Assuming booleanOperator == "and"
    for pred in self.__predicates:
      if not pred.checkTF(get):
        #This one returned false so the and fails
        return False
    #All true
    return True

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
  
  def initialize(self, dataInput, localTrans=None):
    """Initialize the predicate for scoring."""
    pass
  
  def getSegment(self):
    return (None,), (None,)
  
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
  
  def initialize(self, dataInput, localTrans=None):
    """Initialize the predicate for scoring."""
    return self.getSegment()
  
  def getSegment(self):
    return (None,), (None,)
  
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
  
  def initialize(self, dataInput, localTrans=None):
    """Initialize the predicate for scoring."""
    return self.getSegment()
  
  def getSegment(self):
    return (None,), (None,)
  
  def checkTF(self, get):
    """This function checks to see if the predicate evaluates to true or false."""
    return False

class choiceSC(pmmlChoice):
  """Predicate group"""
  types = [pmmlSimplePredicate, pmmlCompoundPredicate, pmmlSimpleSetPredicate, pmmlTrue, pmmlFalse]
  __maximums = [1, 1, 1, 1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1, 1, 1, 1]
    pmmlChoice.__init__(self, choiceSC.types, minimums, choiceSC.__maximums, instances)

  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceSC.types, choiceSC.__maximums, children)

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
  types = [sequencePS, pmmlEmbeddedModel]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choicePE.types, minimums, choicePE.__maximums, instances)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choicePE.types, choicePE.__maximums, children)

class pmmlNode(pmmlElement):
  """Node element used in tree models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC, choicePE]
    maximums = [None, 1, 1]
    minimums = [None, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Node:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["id", "score", "recordCount", "defaultChild"]
    requiredAttributes = []
    pmmlElement.__init__(self,"Node", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, get, dataInput, localTransDict=None, segment = None, root = True):
    """Initialiaze the node for scoring."""
    #Get our predicate and any children.
    self.predicate = self.getChildrenOfType(choiceSC)[0]
    self.predicate.initialize(dataInput, localTransDict)
    self.children = self.getChildrenOfType(pmmlNode)
    
    #Initialiaze our child nodes
    for child in self.children:
      child.initialize(get, dataInput, localTransDict, root=False)
    
    #Everything else is only for the root node to do.
    if not root:
      self.__root = False
      return
    
    self.__root = True
    self.get = get
    
    if segment:
      self.segment = segment
    else:
      self.segment = {}
  
  def score(self, get = None):
    """Return a score from the tree."""
    #Intentionally overwrite the default paramater
    if get is None:
      get = self.get
    
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
              return [(score, False, self.segment, None)]
            else:
              return score
      else:
        if self.__root:
          #Return the list that the producer and consumer expect
          return [(self.getAttribute("score"), False, self.segment, None)]
        else:
          return self.getAttribute("score")
    else:
      #Not me, return None
      return None

sequencePS.add(pmmlNode)
