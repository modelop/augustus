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
from pmmlBaseline import *
from pmmlNaiveBayes import *
from pmmlRegression import *
from pmmlRuleset import *
from pmmlTree import *
from pmmlClusteringModel import *
from augustus.kernel.unitable import *

#######################################################################
#generic model

class genericModel:
  def __init__(self):
    """All initial values should be set in a derived classes __init__."""
    self.metadata = None
  
  def initialize(self, dataInput, transDict, outputFields, segment = None):
    """This will init the model and its child elements."""
    
    #Store our data input and output fields for later use.
    self.dataInput = dataInput
    self.__output = outputFields
    #Get the mining schema so that we can tell the transformation dictionary and local transformations where they are supposed to get data from.  We'll initialize it later once everything else is ready.
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    self.get = miningSchema.get
    
    #If we have a transformation dictionary, initialize it.
    if transDict:
      dataInput.extend(transDict.columns(miningSchema.td_get))
      #I'd like this to be a part of extend but I can't figure out quite how to stuff all the data together.
      dataInput.add_data_types(transDict.datatypes())
    
    #If we have a local transformations, initialize it.  While we're at it we'll assign the update function.
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.initialize()
      aggregates = localTrans.aggregates()
      localTrans = localTrans.dictionary(self.get)
      #localTrans.limit(nonAggregates)
      localTrans.force(aggregates)
      self.update = localTrans.update
      self.lastUpdate = localTrans.lastUpdate
    else:
      localTrans = None
      self.update = self.__doNothing
      self.lastUpdate = self.__doNothing
    
    #Initialize the mining schema
    miningSchema.initialize(dataInput, localTrans)
    
    #Initialize the model specific elements.
    model = self.getModelInstance()
    if model:
      model.initialize(self.get, dataInput, localTrans, segment)
      self.__model = model
  
  def __doNothing(self):
    """This function does nothing and seems to be more efficient at doing nothing than an lambda function is."""
    pass
  
  def batchEvent(self):
    """Update the model but don't return a score."""
    
    #Update the data fields and transformations
    self.dataInput.update()
    self.update()
  
  def batchScore(self):
    """Update the aggregate fields and return a score."""
    
    output = []
    #Update the aggregate fields
    self.lastUpdate()
    results = self.__model.score()
    for result in results:
      (score, threshold, rules, ancillary) = result
      if not score is None:
        temp = [self.get(field) for field in self.__output]
        if not NULL in temp:
          temp.extend([score, threshold, rules, ancillary])
          output.append(temp)
    return output
  
  def score(self):
    """Score a row of data."""
    
    #Update the data fields and transformations
    self.dataInput.update()
    self.update()
    
    output = []
    if self.metadata is not None:
      self.metadata['Number of Scores Calculated']+=1
      startScore = datetime.datetime.now()
    results = self.__model.score()
    for result in results:
      (score, threshold, rules, ancillary) = result
      if not score is None:
        temp = [self.get(field) for field in self.__output]
        if not NULL in temp:
          temp.extend([score, threshold, rules, ancillary])
          output.append(temp)
    return output

  def enableMetaDataCollection(self):
    self.metadata = {}

  def getMetaData(self):
    # Distinguish 'reported' metadata (strings) from
    # 'collected' data in case one wants to do multiple
    # collect/report sequences.
    if self.metadata is not None:
      reportedMetaData = {}
      for k in self.metadata.keys():
        reportedMetaData[k] = str(self.metadata[k])
    return [' : '.join((k,v)) for (k,v) in reportedMetaData.items()]

#######################################################################
#specific models
class pmmlMiningModel(pmmlElement):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlSegmentation, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, 1, 1, None]
    minimums = [None, 1, None, None, None, None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "MiningModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "functionName", "algorithmName"]
    requiredAttributes = ["functionName"]
    pmmlElement.__init__(self, "MiningModel", myChild, attributeNames, attributes, requiredAttributes)
    self.metadata = None

  def initialize(self, dataInput, transDict, outputFields, collectMetaData=False):
    """This will init the model and its child elements."""
    print 'Initializing Mining Model'
    if collectMetaData:
      # This is a neccessary option in order to have metadata initialized for segments.
      self.enableMetaDataCollection()
    self.dataInput = dataInput
    self.__output = outputFields
    
    #Get the mining schema so that we can tell the transformation dictionary and local transformations where they are supposed to get data from.  We'll initialize it later once everything else is ready.
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    self.get = miningSchema.get
    
    #If we have a transformation dictionary, initialize it.
    if transDict:
      dataInput.extend(transDict.columns(miningSchema.td_get))
      #I'd like this to be a part of extend but I can't figure out quite how to stuff all the data together.
      dataInput.add_data_types(transDict.datatypes())
    
    #If we have a local transformations, initialize it.  While we're at it we'll assign the update function.
    localTrans = self.getChildrenOfType(pmmlLocalTransformations)
    if localTrans != []:
      localTrans = localTrans[0]
      localTrans.initialize()
      aggregates = localTrans.aggregates()
      localTrans = localTrans.dictionary(self.get)
      #localTrans.limit(nonAggregates)
      localTrans.force(aggregates)
      self.update = localTrans.update
      self.lastUpdate = localTrans.lastUpdate
    else:
      localTrans = None
      self.update = self.__doNothing
      self.lastUpdate = self.__doNothing
    
    #Initialize the mining schema
    miningSchema.initialize(dataInput, localTrans)
    
    #Now init the segments
    #This part assumes that we are using selectAll
    segs = {} #The keys will be the fields needed to match a segment
    segmentation = self.getChildrenOfType(pmmlSegmentation)[0]
    segments = segmentation.getChildrenOfType(pmmlSegment)
    for segment in segments:
      temp = segment.initialize(self.get, dataInput)
      if self.metadata is not None:
        segment.enableMetaDataCollection()
        self.metadata['Number of Segments'] += 1
      if temp:
        (fields, values) = temp
        if fields in segs:
          valueDict = segs[fields]
          if values in valueDict:
            valueDict[values].append(segment)
          else:
            valueDict[values] = [segment]
        else:
          segs[fields] = {values:[segment]}
    self.segs = segs
  
  def __doNothing(self):
    """This function does nothing and seems to be more efficient at doing nothing than an lambda function is."""
    pass

  def batchEvent(self):
    """Update the model but don't return a score."""
    
    #Update the data fields and transformations
    self.dataInput.update()
    self.update()
    
    for fieldList in self.segs:
      #Get the values we need to see if we match a segment
      values = tuple([str(self.get(field)) for field in fieldList])
      if not NULL in values:
        #Find the segments that might match
        valueDict = self.segs[fieldList]
        if values in valueDict:
          for segment in valueDict[values]:
            if segment.match():
              #Update the segment
              segment.update()

  def batchScore(self):
    """Update the aggregate fields and return a score from every segment."""
    
    output = []
    #Update our aggregate fields
    self.lastUpdate()
    #Walk through all the segments
    for fieldList in self.segs:
      valueDict = self.segs[fieldList]
      for values in valueDict:
        for segment in valueDict[values]:
          #Update the aggregate fields in the segment and return a score
          segment.lastUpdate()
          results = segment.batchScore()
          for result in results:
            (score, threshold, rules, ancillary) = result
            if not score is None:
              temp = [segment.get(field) for field in self.__output]
              if not NULL in temp:
                temp.extend([score, threshold, rules, ancillary])
                output.append(temp)
    return output

  def score(self):
    """Score a row of data."""
    
    #Update the data fields and transformations
    self.dataInput.update()
    self.update()
    
    output = []
    for fieldList in self.segs:
      #Get the values we need to see if we match a segment
      values = tuple([str(self.dataInput[field]) for field in fieldList])
      if not NULL in values:
        #Find the segments that might match
        valueDict = self.segs[fieldList]
        if values in valueDict:
          segments = valueDict[values]
        elif () in valueDict:
          #This makes regular segments work
          segments = valueDict[()]
        else:
          segments = []
        
        for segment in segments:
          if segment.match():
            #We matched so update and score
            if segment.metadata is not None:
              segment.metadata['Number of Times matched'] += 1
            segment.update()
            results = segment.score()
            for result in results:
              (score, threshold, rules, ancillary) = result
              if not score is None:
                temp = [segment.get(field) for field in self.__output]
                if not NULL in temp:
                  temp.extend([score, threshold, rules, ancillary])
                  output.append(temp)
    return output

  def enableMetaDataCollection(self):
    if self.metadata is None:
      self.metadata={'Number of Segments':0}

  def getMetaData(self):
    # Distinguish 'reported' metadata (strings) from
    # 'collected' data in case one wants to do multiple
    # collect/report sequences.
    reportedMetaData = {}
    miningmodelMetaData = []
    if self.metadata is not None:
      for k in self.metadata.keys():
        reportedMetaData[k] = str(self.metadata[k])
      miningmodelMetaData = [' : '.join((k,v)) for (k,v) in reportedMetaData.items()]
      segmentation = self.getChildrenOfType(pmmlSegmentation)[0]
      segments = segmentation.getChildrenOfType(pmmlSegment)
      for segment in segments:
        miningmodelMetaData += [30*'>']
        segmentMetaData = segment.getMetaData()
        miningmodelMetaData += segmentMetaData
        miningmodelMetaData += [30*'>']
    return miningmodelMetaData

class pmmlBaselineModel(pmmlElement, genericModel):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlTestDistributions, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, 1, 1, None]
    minimums = [None, 1, None, None, None, None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "BaselineModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName","functionName","algorithmName"]
    requiredAttributes = ["functionName"]
    genericModel.__init__(self)
    pmmlElement.__init__(self, "BaselineModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def getModelInstance(self):
    """Return the instance of the underlying model element."""
    model = self.getChildrenOfType(pmmlTestDistributions)
    if model != []:
      return model[0]
    else:
      return None

class pmmlRegressionModel(pmmlElement, genericModel):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlRegressionTable, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, None, 1, None]
    minimums = [None, 1, None, None, None, None, None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RegressionModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "functionName","algorithmName","modelType","targetFieldName","normalizationMethod"]
    requiredAttributes = ["functionName"]
    genericModel.__init__(self)
    pmmlElement.__init__(self, "RegressionModel", myChild, attributeNames, attributes, requiredAttributes)

  def getModelInstance(self):
    """Return the instance of the underlying model element.
    
    In our case the model is comprised of multiple RegressionTable elements and so we'll create an instance of Regression to bind them together in a logical unit."""
    tables = self.getChildrenOfType(pmmlRegressionTable)
    miningSchema = self.getChildrenOfType(pmmlMiningSchema)[0]
    predictedField = miningSchema.getPredicted()[0]
    return Regression(tables, predictedField)

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

class pmmlTreeModel(pmmlElement, genericModel):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlNode, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, 1, 1, None]
    minimums = [None, 1, None, None, None, None, None, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TreeModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName","functionName","algorithmName","missingValueStrategy","missingValuePenalty","noTrueChildStrategy","splitCharacteristic"]
    requiredAttributes = ["functionName"]
    genericModel.__init__(self)
    pmmlElement.__init__(self, "TreeModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def getModelInstance(self):
    """Return the instance of the underlying model element."""
    model = self.getChildrenOfType(pmmlNode)
    if model != []:
      return model[0]
    else:
      return None
  
class pmmlNaiveBayesModel(pmmlElement, genericModel):
  """"""
  def __init__(self,name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlBayesInputs, pmmlBayesOutput, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, 1, 1, 1, None]
    minimums = [None, 1, None, None, None, None, None, 1, 1, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NaiveBayesModel: " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName", "threshold", "functionName","algorithmName"]
    requiredAttributes = ["threshold", "functionName"]
    genericModel.__init__(self)
    pmmlElement.__init__(self, "NaiveBayesModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def getModelInstance(self):
    """Return the instance of the underlying model element.
    
    In our case the model is comprised of the BayesInputs element and the BayesOutput element and so we'll create an instance of NaiveBayes to bind them together in a logical unit."""
    inputs = self.getChildrenOfType(pmmlBayesInputs)[0]
    outputs = self.getChildrenOfType(pmmlBayesOutput)[0]
    return NaiveBayes(inputs, outputs, self.getAttribute("threshold"))

class pmmlRuleSetModel(pmmlElement, genericModel):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlMiningSchema, pmmlOutput, pmmlModelStats, pmmlModelExplanation, pmmlTargets, pmmlLocalTransformations, pmmlRuleSet, pmmlModelVerification, pmmlExtension]
    maximums = [None, 1, 1, 1, 1, 1, 1, 1, 1, None]
    minimums = [None, 1, None, None, None, None, None, None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "RuleSetModel:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["modelName","functionName","algorithmName"]
    requiredAttributes = ["functionName"]
    genericModel.__init__(self)
    pmmlElement.__init__(self, "RuleSetModel", myChild, attributeNames, attributes, requiredAttributes)
  
  def getModelInstance(self):
    """Return the instance of the underlying model element."""
    model = self.getChildrenOfType(pmmlRuleSet)
    if model != []:
      return model[0]
    else:
      return None

pmmlElementByXSD(r"""
  <xs:element name="ClusteringModel">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="MiningSchema"/>
        <xs:element ref="Output" minOccurs="0" />
        <xs:element ref="ModelStats" minOccurs="0"/>
        <xs:element ref="ModelExplanation" minOccurs="0"/>
        <xs:element ref="LocalTransformations" minOccurs="0" />
        <xs:element ref="ComparisonMeasure"/>
        <xs:element ref="ClusteringField" minOccurs="0" maxOccurs="unbounded"/>
        <xs:element ref="MissingValueWeights" minOccurs="0"/>
        <xs:element ref="Cluster" maxOccurs="unbounded"/>
        <xs:element ref="ModelVerification" minOccurs="0"/>
        <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
      </xs:sequence>
      <xs:attribute name="modelName" type="xs:string" use="optional"/>
      <xs:attribute name="functionName" type="MINING-FUNCTION" use="required" />
      <xs:attribute name="algorithmName" type="xs:string" use="optional"/>
      <xs:attribute name="modelClass" use="required">
        <xs:simpleType>
          <xs:restriction base="xs:string">
            <xs:enumeration value="centerBased"/>
            <xs:enumeration value="distributionBased"/>
          </xs:restriction>
        </xs:simpleType>
      </xs:attribute>
      <xs:attribute name="numberOfClusters" type="INT-NUMBER" use="required"/>
    </xs:complexType>
  </xs:element>
""", globals(), locals())
pmmlClusteringModel.getModelInstance = lambda self: pmmlClustering(self)

#######################################################################

class pmmlModels(pmmlChoice):
  """Container class that allows one to choose whichever model they would like that is defined above."""
  types = [pmmlBaselineModel, 
             pmmlTreeModel, 
             pmmlNaiveBayesModel, 
             pmmlRegressionModel, 
             pmmlRuleSetModel, 
             pmmlMiningModel,
             pmmlClusteringModel,
           ]
  __maximums = [1, 1, 1, 1, 1, 1, 1]
  def __init__(self, children):
    """"""
    minimums = [1, 1, 1, 1, 1, 1, 1]
    pmmlChoice.__init__(self, pmmlModels.types, minimums, pmmlModels.__maximums, children)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(pmmlModels.types, pmmlModels.__maximums, children)

class pmmlEmbedableModels(pmmlChoice):
  """Container class that defines which models are allowed in a pmmlSegment."""
  types = [pmmlBaselineModel, 
           pmmlTreeModel, 
           pmmlNaiveBayesModel, 
           pmmlRegressionModel, 
           pmmlRuleSetModel,
           pmmlClusteringModel,
           ]
  __maximums = [1, 1, 1, 1, 1, 1]
  def __init__(self, children):
    """"""
    minimums = [1, 1, 1, 1, 1, 1]
    pmmlChoice.__init__(self, pmmlEmbedableModels.types, minimums, pmmlEmbedableModels.__maximums, children)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(pmmlEmbedableModels.types, pmmlEmbedableModels.__maximums, children)

#######################################################################
#Segmentation elements
#Note these are here instead of in pmmlModelElements because this avoids a selfreferential loop.

class pmmlSegment(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceSC, pmmlEmbedableModels]
    maximums = [None, 1, 1]
    minimums = [None, 1, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "pmmlSegment:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["id", "weight"]
    requiredAttributes = []
    pmmlElement.__init__(self, "Segment", myChild, attributeNames, attributes, requiredAttributes)

  def initialize(self, get, dataInput):
    """Initialize the segment for scoring."""
    #__matchGet is used for matching because it doesn't have the LocalTransformations that are inside of the model.
    self.__matchGet = get
    self.predicate = self.getChildrenOfType(choiceSC)[0]
    #self.__fields, self.__values = self.predicate.initialize(dataInput)
    fs, vs = self.predicate.initialize(dataInput)
    fields = []
    values = []
    segment = {}
    #for field, value in zip(self.__fields, self.__values):
    for field, value in zip(fs, vs):
      segment[field] = value
      if isinstance(value, str):
        fields.append(field)
        values.append(value)
    self.__fields = tuple(fields)
    self.__values = tuple(values)
    
    model = self.getChildrenOfType(pmmlEmbedableModels)[0]
    model.initialize(dataInput, None, [], segment)
    
    self.get = model.get
    self.update = model.update
    self.lastUpdate = model.lastUpdate
    self.score = model.score
    self.batchScore = model.batchScore
    self.metadata = None    

    return self.__fields, self.__values
  
  def getSegment(self):
    return zip(self.__fields, self.__values)

  def match(self):
    """Return true if the data maches this segment."""
    return self.predicate.checkTF(self.__matchGet)

  def enableMetaDataCollection(self):
    self.metadata={'Predicate':self.predicate,
     'Number of Times matched':0}

  def getMetaData(self):
    reportedMetaData = {}
    if self.metadata is not None:
      for k in self.metadata.keys():
        reportedMetaData[k] = str(self.metadata[k]).rstrip('\n')
      segmentMetaData = [' : '.join((k,v)) for (k,v) in reportedMetaData.items()]
    else:
      segmentMetaData = []
    return segmentMetaData


class pmmlSegmentation(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlLocalTransformations, pmmlSegment]
    maximums = [None, 1, None]
    minimums = [None, None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "pmmlSegmentation:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["multipleModelMethod"]
    requiredAttributes = ["multipleModelMethod"]
    pmmlElement.__init__(self, "Segmentation", myChild, attributeNames, attributes, requiredAttributes)
    #NOTE: We don't usually do this but this will probably save potential headaches later.
    if self.getChildrenOfType(pmmlLocalTransformations) != []:
      print "You really should know that we don't support LocalTransformations inside of Segmentation.  Please move this to the MiningModel."

#######################################################################
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
  
  def initialize(self, get, outputFields=[], metadata=None):
    """"""
    model = self.getChildrenOfType(pmmlModels)[0]
    dataDict = self.getChildrenOfType(pmmlDataDictionary)[0]
    dataInput = dataDict.dictionary(get)
    transDict = self.getChildrenOfType(pmmlTransformationDictionary)
    modelMetaData = False
    if transDict:
      transDict = transDict[0]
      #dataInput.extend(transDict.columns(dataInput.__getitem__))
    else:
      transDict = None
    if metadata is not None:
      modelMetaData = metadata.veryverbose()
    model.initialize(dataInput, transDict, outputFields, modelMetaData)
