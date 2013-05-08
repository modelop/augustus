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


from pmmlModelElements import *
from pmmlTree import *

#This file defines PMML elements utilized within ruleset models

#for each element, the same functions are defined that are
#described at the top of the pmmlElements.py file.

class pmmlRuleSelectionMethod(pmmlElement):
  """RuleSelectionMethod element used in ruleset models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension]
    maximums = [None]
    minimums = [None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RuleSelectionMethod:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["criterion"]
    requiredAttributes = ["criterion"]
    pmmlElement.__init__(self,"RuleSelectionMethod", myChild, attributeNames, attributes, requiredAttributes)

class pmmlSimpleRule(pmmlElement):
  """SimpleRule element used in ruleset models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC, pmmlScoreDistribution]
    maximums = [None, 1, None]
    minimums = [None, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "SimpleRule:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["id", "score", "recordCount", "nbCorrect", "confidence", "weight"]
    requiredAttributes = ["score"]
    pmmlElement.__init__(self,"SimpleRule", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, get, dataInput, localTransDict=None):
    """Initialiaze the rule for scoring."""
    #Get our predicate and initialize it.
    self.predicate = self.getChildrenOfType(choiceSC)[0]
    self.predicate.initialize(dataInput, localTransDict)
    self.get = get
  
  def score(self):
    """Return a score from the rule."""
    
    if self.predicate.checkTF(self.get):
      #We're at the right node
      return self.getAttribute("score")
    else:
      #Not me, return None
      return None

class pmmlCompoundRule(pmmlElement):
  """CompundRule element used in ruleset models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC, choiceRule]
    maximums = [None, 1, None]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "CompoundRule:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self,"CompoundRule", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, get, dataInput, localTransDict=None):
    """Initialiaze the rule for scoring."""
    #Get our predicate and any children.
    self.predicate = self.getChildrenOfType(choiceSC)[0]
    self.predicate.initialize(dataInput, localTransDict)
    self.children = self.getChildrenOfType(choiceRule)
    
    #Initialiaze our children
    for child in self.children:
      child.initialize(get, dataInput, localTransDict)
    
    self.get = get
  
  def score(self):
    """Return a score from the rules."""
    
    if self.predicate.checkTF(self.get):
      #We're at the right node
      for child in self.children:
        score = child.score()
        if score is not None:
          #First hit is the only Rule Selection method we have implemented:
          #We got a valid score so return it, otherwise go to the next child
          return score
      else:
        #No child returned a score
        return None
    else:
      #Not me, return None
      return None

class choiceRule(pmmlChoice):
  """Rule group"""
  types = [pmmlSimpleRule, pmmlCompoundRule]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choiceRule.types, minimums, choiceRule.__maximums, instances)

  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceRule.types, choiceRule.__maximums, children)

class pmmlRuleSet(pmmlElement):
  """RuleSet element used in ruleset models."""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlRuleSelectionMethod, pmmlScoreDistribution, choiceRule]
    maximums = [None, None, None, None]
    minimums = [None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RuleSet:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["recordCount", "nbCorrect", "defaultScore", "defaultConfidence"]
    requiredAttributes = []
    pmmlElement.__init__(self,"RuleSet", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, get, dataInput, localTransDict=None, segment = None):
    """Initialiaze the ruleset for scoring."""
    
    #Get our children.
    self.children = self.getChildrenOfType(choiceRule)
    
    #Initialiaze our children
    for child in self.children:
      child.initialize(get, dataInput, localTransDict)
    
    self.get = get
    
    self.__default_score = self.getAttribute("defaultScore")
    
    if segment:
      self.segment = segment
    else:
      self.segment = {}
  
  def score(self):
    """Return a score from the ruleset."""
    
    """Everything from here down was coppied from the tree node class"""
    if self.children != []:
      #We have child nodes so see what they return
      for child in self.children:
        score = child.score()
        if score is not None:
          #We got a valid score so return it, otherwise go to the next child
          #This part will need to change to implement a rule selection method other than first hit
          
          #Return the list that the producer and consumer expect
          return [(score, False, self.segment, None)]
      else:
        #No children returned a score so return the default score if we have one.
        if self.__default_score is not None:
          return [(self.__default_score, False, self.segment, None)]
        else:
          return []
    else:
      #No children so return the default score if we have one.
      if self.__default_score is not None:
        return [(self.__default_score, False, self.segment, None)]
      else:
        return []
