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
from pmmlModelElements import *
from numpy import *
from string import atof

class pmmlBayesOutput(pmmlElement):
  """"""
  def __init__(self, name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlTargetValueCounts]
    maximums = [None,1]
    minimums = [None,1]
    attributeNames = ["fieldName"]
    requiredAttributes=["fieldName"]
    if attributes == {}:
      raise pmmlError, "NaiveBayesOutput:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BayesOutput:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesOutput", myChild, attributeNames, attributes, requiredAttributes)

class pmmlBayesInputs(pmmlElement):
  """"""
  def __init__(self, name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlBayesInput]
    maximums = [None,None]
    minimums = [None,1]
    if attributes !={}:
      raise pmmlError, "NaiveBayesInputs:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BayesInputs:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesInputs", myChild)

class pmmlBayesInput(pmmlElement):
  """"""
  def __init__(self, name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlDerivedField,pmmlPairCounts]
    maximums = [None, 1, None]
    minimums = [None, None, 1]
    if attributes =={}:
      raise pmmlError, "NaiveBayesInput:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    attributeNames = ["fieldName"]
    requiredAttributes=["fieldName"]
    if extras != []:
      raise pmmlError, "NaiveBayesInput:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesInput", myChild, attributeNames, attributes, requiredAttributes)

class NaiveBayes:
  """Helper class that wraps up the pmmlBayesInputs and pmmlBayesOutput into the logical unit that they are."""
  def __init__(self, inputs, output, threshold):
    """"""
    self.inputs = inputs
    self.output = output
    self.threshold = float(threshold)

  def initialize(self, get, dataInput, localTransDict=None, segment = None):
    """"""
    self.get = get
    self.__set = self.makeFunction()
    
    if segment:
      self.segment = segment
    else:
      self.segment = {}

  @staticmethod
  def __pValue(current,pValues):
    """"""
    try:
      reversed=[(tval, p) for (p,tval) in pValues[tuple(current)]]
    except KeyError:
      return None
    reversed.sort()
    return reversed[-1][1]

  def _xcombine(self,*seqin):
    '''returns a generator which returns combinations of argument sequences
for example xcombine((1,2),(3,4)) returns a generator; calling the next()
method on the generator will return [1,3], [1,4], [2,3], [2,4] and
StopIteration exception.  This will not create the whole list of 
combinations in memory at once. Thanks to the folks at:
http://code.activestate.com/recipes/users/2007923/ '''
    def rloop(seqin,comb):
      '''recursive looping function'''
      if seqin:                   # any more sequences to process?
        for item in seqin[0]:
          newcomb=comb+[item]     # add next item to current combination
          # call rloop w/ remaining seqs, newcomb
          for item in rloop(seqin[1:],newcomb):
            yield item          # seqs and newcomb
      else:                           # processing last sequence
        yield comb                  # comb finished, add to list
    return rloop(seqin,[])

  def makeFunction(self):
    """"""
    set = None
    self.__ancillary = {}
    self._countsData = {}
    otherfields = []
    inputPairs = {}
    inputElements = {}
    missingValueStrategy = None
    missingValue = object()
    targetValues=[]
    #outputs = self.getChildrenOfType(pmmlBayesOutput)[0]
    outputs = self.output
    for o in outputs.getChildrenOfType(pmmlTargetValueCounts):
      for t in o.getChildrenOfType(pmmlTargetValueCount):
        targetValues.append(t.getAttribute('value'))
        self._countsData[(None,None,t.getAttribute('value'))]=int(atof(t.getAttribute('count')))    

    self.targetfield = outputs.getAttribute("fieldName")
    #catch the case where the field name is missing
    if self.targetfield == None:
      raise pmmlError, "NaiveBayes:  " + pmmlErrorStrings.fieldMissing + repr(self)

    #inputs = self.getChildrenOfType(pmmlBayesInputs)[0]
    inputs = self.inputs

    for i in inputs.getChildrenOfType(pmmlBayesInput):
      otherfields.append(i.getAttribute('fieldName'))
      inputfieldname = i.getAttribute('fieldName')
      inputElements[inputfieldname]=[]
      for j in i.getChildrenOfType(pmmlPairCounts):
        inputfieldvalue = j.getAttribute('value')
        inputElements[inputfieldname].append(inputfieldvalue)
        for k in j.getChildrenOfType(pmmlTargetValueCounts)[0].getChildrenOfType(pmmlTargetValueCount):
          cnt = int(atof(k.getAttribute('count')))
          if (cnt == 0):
            cnt = self.threshold * self._countsData[(None, None, k.getAttribute('value'))]
          self._countsData[(inputfieldname,inputfieldvalue,k.getAttribute('value'))] = cnt
          self._countsData[(inputfieldname,missingValue,k.getAttribute('value'))] = self._countsData[(None, None, k.getAttribute('value'))]
      if (not missingValueStrategy):
       # Hooks for missing values. Assume all fields can have them for now
       # and that they will get encoded as None.
       inputElements[inputfieldname].append(missingValue)
     # Calculate pvalues for all possible inputs
    pValues={}
    inputvalues=[g for g in [inputElements[f] for f in otherfields]]
    for inp in self._xcombine(*inputvalues):
      LF={}
      for target in targetValues:
        inputs=[]
        for (f,v) in zip(otherfields,inp):
          if (f,v,target) in self._countsData.keys():
            try:
              inputs.append(float(self._countsData[(f,v,target)]))
            except:
              inputs = [float(self._countsData[(f,v,target)])]
          else:	
            try:
              inputs.append(0.0)
            except:
              inputs = [0.0]
        #try:
        #  inputs=[float(self._countsData[t]) for t in [(f,v,target) for (f,v) in zip(otherfields,inp)]]        
        #except:
        #   pass
        try:
          LF[target]=prod(array(inputs))/(self._countsData[(None,None,target)])**(len(inputs)-1)
        except:
          pass
      norm=sum(LF.values())
      pValues[tuple(inp)]=[(t,LF[t]/norm) for t in LF.keys()]
    function = lambda current : NaiveBayes.__pValue(current,pValues)
    if function:
       set = [0, otherfields, function, self.__ancillary]
    return set

  def score(self):
    """"""
    # inputs to update for this count:
    values = [self.get(j) for j in self.__set[1]]
    # _countsData is kept per segment for later use by Producer.
    # May move this to producer to keep track of.
    scoredFieldValues=[]
    for mykey in zip(self.__set[1],values):
     countKeys = tuple([mykey[0],mykey[1],self.get(self.targetfield)])
     self._countsData[countKeys]+=1.0
     scoredFieldValues.append(countKeys)
    self._countsData[(None,None,self.get(self.targetfield))]+=1.0
    scoredFieldValues.append((None,None,self.get(self.targetfield)))
    # ====================================
    ancillary=self.__set[3]
    ancillary["scoredKeys"] = scoredFieldValues
    answer = []
    if not isinstance(values, list):
      values = [values]
    if not values is NULL and not values is None:
      temp = self.__set[2](values)
      if not temp is None:
        self.__set[0] = temp
        #answer.append((temp, True, {}, ancillary, scoredFieldValues))
        answer.append((temp, True, self.segment, ancillary))
    return answer
