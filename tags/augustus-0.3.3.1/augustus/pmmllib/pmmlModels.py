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
from pmmlBaseline import *
from pmmlNaiveBayes import *
from pmmlRegression import *
from pmmlTree import *
from augustus.kernel.unitable import *
#############################################################################
#specific models
class pmmlBaselineModel(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlTargets, pmmlLocalTransformations, pmmlTestDistributions, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, None, None]
    minimums = [None, 1, None, None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BaselineModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName","functionName","algorithmName"]
    requiredAttributes = ["functionName"]
    pmmlElement.__init__(self, "BaselineModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput, outputFields, batch):
    """"""
    self.dataInput = dataInput
    fields = dataInput.keys()
    self.__output = outputFields
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    directFields = miningSchema.limit(dataInput, outputFields)
    directFields.sort()
    tests = self.getChildrenOfType(pmmlTestDistributions)
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    lookup = {}
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.newInit()
      aggregates = localTrans.aggregates()
    for test in tests:
      if localTrans != []:
        temp = test.newInit(dataInput, directFields, localTrans.dictionary(dataInput.__getitem__))
        #test.limit(nonAggregates)
        test.force(aggregates)
      else:
        temp = test.newInit(dataInput, directFields)
      if temp:
        (fields, values) = temp
        if fields in lookup:
          valueDict = lookup[fields]
          if values in valueDict:
            valueDict[values].append(test)
          else:
            valueDict[values] = [test]
        else:
          lookup[fields] = {values:[test]}
    self.__lookup = lookup
    return directFields
  
  def batchEvent(self):
    """"""
    self.dataInput.update()
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for test in valueDict[values]:
            test.update()
            if not test.match():
              test.revert()
  
  def batchScore(self):
    """"""
    output = []
    for fieldList in self.__lookup:
      valueDict = self.__lookup[fieldList]
      for values in valueDict:
        for test in valueDict[values]:
          test.lastUpdate()
          results = test.score()
          for result in results:
            (score, threshold, rules, ancillary) = result
            if not score is None:
              temp = [test.get(field) for field in self.__output]
              if not NULL in temp:
                temp.extend([score, threshold, rules, ancillary])
                output.append(temp)
    return output
  
  def score(self):
    """"""
    self.dataInput.update()
    output = []
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for test in valueDict[values]:
            test.update()
            if test.match():
              results = test.score()
              for result in results:
                (score, threshold, rules, ancillary) = result
                if not score is None:
                  temp = [test.get(field) for field in self.__output]
                  if not NULL in temp:
                    temp.extend([score, threshold, rules, ancillary])
                    output.append(temp)
            else:
              test.revert()
    return output

class pmmlMiningModel(pmmlElement):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlTargets, pmmlLocalTransformations, pmmlNaiveBayes, pmmlTestDistributions, pmmlRegression, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, None, None, None, 1, None]
    minimums = [None, 1, None, None, None, None, 1, 1, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MiningModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "threshold", "functionName","algorithmName"]
    requiredAttributes = ["threshold", "functionName"]
    pmmlElement.__init__(self, "MiningModel", myChild, attributeNames, attributes, requiredAttributes)

  def initialize(self, dataInput, outputFields, batch):
    """"""
    pass

class pmmlRegressionModel(pmmlElement):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlTargets, pmmlLocalTransformations, pmmlRegression, pmmlModelVerification, pmmlExtension]
    maximums = [None, None, None, None, None, None, None, None, None]
    minimums = [None, 1, None, None, None, None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RegressionModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "functionName","algorithmName","modelType","targetFieldName","normalizationMethod"]
    requiredAttributes = ["functionName"]
    pmmlElement.__init__(self, "RegressionModel", myChild, attributeNames, attributes, requiredAttributes)

  def initialize(self, dataInput, outputFields, batch):
    """"""
    self.dataInput = dataInput
    fields = dataInput.keys()
    self.__output = outputFields
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    self.predictedField  = miningSchema.getPredicted()[0]
    directFields = miningSchema.limit(dataInput, outputFields)
    directFields.sort()
    regressions = self.getChildrenOfType(pmmlRegression)
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    lookup = {}
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.newInit()
      aggregates = localTrans.aggregates()
    for regression in regressions:
      if localTrans != []:
        temp = regression.initialize(dataInput, directFields, self.predictedField, localTrans.dictionary(dataInput.__getitem__))
        #regression.limit(nonAggregates)
        regression.force(aggregates)
      else:
        temp = regression.initialize(dataInput, directFields, self.predictedField)
      # the following enables overlapping explicit segments.
      # lookup maps a set of segmenting fields to another
      # dictionary (valueDict) that associates each set of segment
      # values with matching lists of regressions.
      if temp:
        (fields, values) = temp
        if fields in lookup:
          valueDict = lookup[fields]
          if values in valueDict:
            valueDict[values].append(regression)
          else:
            valueDict[values] = [regression]
        else:
          lookup[fields] = {values:[regression]}
    self.vars, self.powers = regressions[0].getPredictors()
    self.__lookup = lookup
    return directFields

  def batchEvent(self):
    """
    Update data. Lookup regressions based on
    explicit segmentation. Among these, perform
    local updates (including transformations).
    For each matching segment, append the segment
    identification and the field values for the
    regression (dependent and independent variables,
    not the interactions).
    
    """
    self.dataInput.update()
    updatedsegs=[]
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for regressionSegment in valueDict[values]:
            rules, rowtoscore = regressionSegment.update()
            if not regressionSegment.match():
              regressionSegment.revert()
            else:
              updatedsegs.append((rules, rowtoscore))
    return updatedsegs

  def batchScore(self,datamatrices,requestedInteractions):
    """
    Input is a dictionary mapping segment rule sets
    to data matrices.
    """
    output = []
    for fieldList in self.__lookup:
      valueDict = self.__lookup[fieldList]
      for values in valueDict:
        for regressionSegment in valueDict[values]: 
          # just picks up data for each segment matching explicit segments.
          # need to pick out regular segments too.
          regressionSegment.lastUpdate()
          # need to get the specific rule for this segment.
          segmentid = regressionSegment.getSortedRestrictions()
          datamatrix = datamatrices[segmentid]
          dependent = datamatrix.pop(self.predictedField)
          results = regressionSegment.score(datamatrix)
          for result in results:
            (score, threshold, rules, ancillary) = result
            ancillary['dependent']=dependent
            if not score is None:
              temp = [regressionSegment.get(field) for field in self.__output]
              if not NULL in temp:
                temp.extend([score, threshold, rules, ancillary])
                output.append(temp)
    return output
  
  def score(self):
    """"""
    self.dataInput.update()
    output = []
    requestedInteractions = []
    vars, pows = self.getChildrenOfType(pmmlRegression)[0].getPredictors()
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for regressionSegment in valueDict[values]:
            rules, rowtoscore = regressionSegment.update()
            if regressionSegment.match():         
              toscore = EvalTable(keys = self.vars)
              toscore.append(rowtoscore)
              dependent = toscore.pop(self.predictedField)
              results = regressionSegment.score(toscore) # should return score and matrices
              for result in results:
                (score, threshold, rules, ancillary) = result
                if not score is None:
                  temp = [regressionSegment.get(field) for field in self.__output]
                  if not NULL in temp:
                    # Note that score is the result of a matrix multiplication.
                    # This may not be appropriate for a batch score.
                    temp.extend([score[0,0], threshold, rules, ancillary])
                    output.append(temp)
            else:
              regressionSegment.revert()
    return output

class pmmlTreeModel(pmmlElement):
  """Copied from pmmlBaselineModel."""
  def __init__(self, name="", attributes={}, children=[]):
    """Copied from pmmlBaselineModel."""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlTargets, pmmlLocalTransformations, pmmlNode, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, None, 1, None]
    minimums = [None, 1, None, None, None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TreeModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName","functionName","algorithmName","missingValueStrategy","missingValuePenalty","noTrueChildStrategy","splitCharacteristic"]
    requiredAttributes = ["functionName"]
    pmmlElement.__init__(self, "TreeModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def initialize(self, dataInput, outputFields, batch):
    """Copied from pmmlBaselineModel."""
    self.dataInput = dataInput
    fields = dataInput.keys()
    self.__output = outputFields
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    directFields = miningSchema.limit(dataInput, outputFields)
    directFields.sort()
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    lookup = {}
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.newInit()
      aggregates = localTrans.aggregates()
    for node in self.getChildrenOfType(pmmlNode):
      if localTrans != []:
        temp = node.initialize(dataInput, directFields, localTrans.dictionary(dataInput.__getitem__))
        #node.limit(nonAggregates)
        node.force(aggregates)
      else:
        temp = node.initialize(dataInput, directFields)
      if temp:
        (fields, values) = temp
        if fields in lookup:
          valueDict = lookup[fields]
          if values in valueDict:
            valueDict[values].append(node)
          else:
            valueDict[values] = [node]
        else:
          lookup[fields] = {values:[node]}
    self.__lookup = lookup
    return directFields
  
  def batchEvent(self):
    """Copied from pmmlBaselineModel."""
    self.dataInput.update()
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for test in valueDict[values]:
            test.update()
            if not test.match():
              test.revert()
  
  def batchScore(self):
    """Copied from pmmlBaselineModel."""
    output = []
    for fieldList in self.__lookup:
      valueDict = self.__lookup[fieldList]
      for values in valueDict:
        for test in valueDict[values]:
          test.lastUpdate()
          results = test.score()
          for result in results:
            (score, threshold, rules, ancillary) = result
            if not score is None:
              temp = [test.get(field) for field in self.__output]
              if not NULL in temp:
                temp.extend([score, threshold, rules, ancillary])
                output.append(temp)
    return output
  
  def score(self):
    """Copied from pmmlBaselineModel."""
    self.dataInput.update()
    output = []
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for test in valueDict[values]:
            test.update()
            if test.match():
              results = test.score()
              for result in results:
                (score, threshold, rules, ancillary) = result
                if not score is None:
                  temp = [test.get(field) for field in self.__output]
                  if not NULL in temp:
                    temp.extend([score, threshold, rules, ancillary])
                    output.append(temp)
            else:
              test.revert()
    return output


class pmmlNaiveBayesModel(pmmlElement):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlTargets, pmmlLocalTransformations, pmmlNaiveBayes, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, None, 1, None]
    minimums = [None, 1, None, None, None, None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NaiveBayesModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "threshold", "functionName","algorithmName"]
    requiredAttributes = ["threshold", "functionName"]
    pmmlElement.__init__(self, "NaiveBayesModel", myChild, attributeNames, attributes, requiredAttributes)

  def initialize(self, dataInput, outputFields, batch):
    """"""
    self.dataInput = dataInput
    fields = dataInput.keys()
    self.__output = outputFields
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    directFields = miningSchema.limit(dataInput, outputFields)
    directFields.sort()
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    lookup = {}
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.newInit()
      aggregates = localTrans.aggregates()
    for bayes in self.getChildrenOfType(pmmlNaiveBayes):
      if localTrans != []:
        temp = bayes.initialize(dataInput, directFields, self.getAttribute("threshold"), localTrans.dictionary(dataInput.__getitem__))
        #bayes.limit(nonAggregates)
        bayes.force(aggregates)
      else:
        temp = bayes.initialize(dataInput, directFields, self.getAttribute("threshold"))
      if temp:
        (fields, values) = temp
        if fields in lookup:
          valueDict = lookup[fields]
          if values in valueDict:
            valueDict[values].append(bayes)
          else:
            valueDict[values] = [bayes]
        else:
          lookup[fields] = {values:[bayes]}
    self.__lookup = lookup
    return directFields
  
  def batchEvent(self):
    """"""
    self.dataInput.update()
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for bayesSegment in valueDict[values]:
            bayesSegment.update()
            if not bayesSegment.match():
              bayesSegment.revert()
  
  def batchScore(self):
    """"""
    output = []
    valuecounts={}
    for fieldList in self.__lookup:
      valueDict = self.__lookup[fieldList]
      for values in valueDict:
        for bayesSegment in valueDict[values]:
          valuecounts[bayesSegment]={}
          bayesSegment.lastUpdate()
          results = bayesSegment.score()
          for result in results:
            (score, threshold, rules, ancillary, keys) = result
            for k in keys:
              try:
                valuecounts[k]+=1
              except:
                valuecounts[k]=1
            if not score is None:
              temp = [bayesSegment.get(field) for field in self.__output]
              if not NULL in temp:
                temp.extend([score, threshold, rules, ancillary])
                output.append(temp)
    #output.append(valuecounts)
    return output
  
  def score(self):
    """"""
    self.dataInput.update()
    output = []
    rules = {}
    keys = {}
    for fieldList in self.__lookup:
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        valueDict = self.__lookup[fieldList]
        if values in valueDict:
          for bayesSegment in valueDict[values]:
            bayesSegment.update()
            if bayesSegment.match():
              results = bayesSegment.score() # should return score and matrices
              for result in results:
                (score, threshold, rules, ancillary, keys) = result
                if not score is None:
                  temp = [bayesSegment.get(field) for field in self.__output]
                  if not NULL in temp:
                    temp.extend([score, threshold, rules, ancillary])
                    output.append(temp)
            else:
              bayesSegment.revert()
    self.scoredKeys = keys
    #output.append(keys)
    return output

#############################################################################
#container that allows one to choose whichever model they would like that is
#defined above
class pmmlModels(pmmlChoice):
  """"""
  __types = [pmmlBaselineModel, 
             pmmlTreeModel, 
             pmmlNaiveBayesModel, 
             pmmlRegressionModel, 
             pmmlMiningModel]
  __maximums = [1, 1, 1, 1, 1]
  def __init__(self, children):
    """"""
    minimums = [1, 1, 1, 1, 1]
    pmmlChoice.__init__(self, pmmlModels.__types, minimums, pmmlModels.__maximums, children)
  
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(pmmlModels.__types, pmmlModels.__maximums, children)
  formatChildren = staticmethod(formatChildren)

#############################################################################
#top level element
class pmmlPMML(pmmlElement):	
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlHeader, pmmlMiningBuildTask, pmmlDataDictionary, pmmlTransformationDictionary, pmmlModels, pmmlExtension]
    maximums = [1, 1, 1, 1, None, None]
    minimums = [None, None, 1, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "PMML:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "PMML", myChild, ["version"], attributes, ["version"])
    self.DataDictionary = self.getChildrenOfType(pmmlDataDictionary)[0]
  
  def initialize(self, get, outputFields=[], batch=False):
    """"""
    model = self.getChildrenOfType(pmmlModels)[0]
    dataDict = self.getChildrenOfType(pmmlDataDictionary)[0]
    dataInput = dataDict.dictionary(get)
    transDict = self.getChildrenOfType(pmmlTransformationDictionary)
    if transDict:
      transDict = transDict[0]
      dataInput.extend(transDict.columns(dataInput.__getitem__))
    model.initialize(dataInput, outputFields, batch)
