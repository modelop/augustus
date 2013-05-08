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
    minimums = [0,1]
    attributeNames = ["fieldName"]
    requiredAttributes=["fieldName"]
    if attributes == {}:
      raise pmmlError, "NaiveBayesOutput:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NaiveBayesOutput:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesOutput", myChild, attributeNames, attributes, requiredAttributes)

class pmmlBayesInputs(pmmlElement):
  """"""
  def __init__(self, name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlBayesInput]
    maximums = [None,None]
    minimums = [0,1]
    if attributes !={}:
      raise pmmlError, "NaiveBayesInputs:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NaiveBayesInputs:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesInputs", myChild)

class pmmlBayesInput(pmmlElement):
  """"""
  def __init__(self, name="",attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlDerivedField,pmmlPairCounts]
    maximums = [None, 1, None]
    minimums = [0, 0, 1]
    if attributes =={}:
      raise pmmlError, "NaiveBayesInput:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    attributeNames = ["fieldName"]
    requiredAttributes=["fieldName"]
    if extras != []:
      raise pmmlError, "NaiveBayesInput:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "BayesInput", myChild, attributeNames, attributes, requiredAttributes)

##
# NaiveBayes is the current moral replacement for the TestDistribution inside
# BaselineModels. That is, it captures all of the data that can vary on a per-
# segment basis.
# It is anticipated that future specs for PMML will use it as an "embedded" 
# element with either a MiningModel or MiningModel+child segments structure. 
# Compare to relationship between TreeModel and DecisionTree.
#
##
class pmmlNaiveBayes(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension,pmmlBayesInputs, pmmlBayesOutput,pmmlSegments]
    maximums = [None, None, 1, 1]
    minimums = [None, 1, 1, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "NaiveBayes:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = []
    requiredAttributes = []
    pmmlElement.__init__(self, "NaiveBayes", myChild, attributeNames, attributes, requiredAttributes)

  def tupelize(rules):
    """"""
    keys = rules.keys()
    keys.sort()
    temp = [(key,rules[key]) for key in keys]
    return tuple(temp)
  tupelize = staticmethod(tupelize)

  def initialize(self, dataInput, directFields, threshold, localTransDict=None):
    self.threshold = float(threshold)
    self.__set = self.makeFunction()
    self.__directFields = directFields
    #if the function will never return a score
    if not self.__set:
      return None
    self.__getMining = dataInput.__getitem__
    #set up for no localTransformations
    if localTransDict is None:
      self.get = self.__getMining
      #self.update = self.__doNothing
      testStatistic = self.getAttribute("testStatistic")
      if testStatistic == "dDist":
        self.__localTransDictUpdate=self.__doNothing
        self.update = self.__update
      else:
        self.update = self.__doNothing
      #The next line was broken with regular segments
      #self.match = self.__returnTrue
      self.match = self.__matchLocal
      self.revert = self.__doNothing
      self.lastUpdate = self.__doNothing
    #set up for localTransformations
    else:
      self.__getLocalTrans = localTransDict.__getitem__
      self.get = self.__getLocal
      #self.update = localTransDict.update
      testStatistic = self.getAttribute("testStatistic")
      if testStatistic == "dDist":
        self.__localTransDictUpdate=localTransDict.update
        self.update = self.__update
      else:
        self.update = localTransDict.update
      self.match = self.__matchLocal
      self.revert = localTransDict.revert
      self.lastUpdate = localTransDict.lastUpdate
      self.limit = localTransDict.limit
      self.force = localTransDict.force
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
    #check to see if we should skip creating a score n times
    extensions = self.getChildrenOfType(pmmlExtension)
    self.__number = None
    if extensions:
      for extension in extensions:
        skip = extension.getChildrenOfType(extensionSkip)
        if skip:
          skip = skip[0]
          self.__number = long(skip.getAttribute("number"))
          self.__cnt = 0
          break
    #return the keys needed for quicker sorting
    return (tuple(fields), tuple(values))
  
  def __threshold(value, standard):
    """"""
    return (value, value >= standard)
  __threshold = staticmethod(__threshold)

  def __belowthreshold(value, standard):
    """"""
    return (value, value <= standard)
  __belowthreshold = staticmethod(__belowthreshold)
  
  def getRestrictions(self):
    """"""
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
  
  def __pValue(current,pValues):
    """"""
    try:
      reversed=[(tval, p) for (p,tval) in pValues[tuple(current)]]
    except KeyError:
      return None
    reversed.sort()
    return reversed[-1][1]    
  __pValue = staticmethod(__pValue)

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
    outputs = self.getChildrenOfType(pmmlBayesOutput)[0]
    for o in outputs.getChildrenOfType(pmmlTargetValueCounts):
      for t in o.getChildrenOfType(pmmlTargetValueCount):
        targetValues.append(t.getAttribute('value'))
        self._countsData[(None,None,t.getAttribute('value'))]=int(atof(t.getAttribute('count')))    

    self.targetfield = outputs.getAttribute("fieldName")
    #catch the case where the field name is missing
    if self.targetfield == None:
      raise pmmlError, "NaiveBayes:  " + pmmlErrorStrings.fieldMissing + repr(self)


    inputs = self.getChildrenOfType(pmmlBayesInputs)[0]

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
    function = lambda current : pmmlNaiveBayes.__pValue(current,pValues)
    if function:
       set = [0, otherfields, function, self.__ancillary]
    return set

  def __getLocal(self, field):
    """"""
    if field in self.__directFields:
      return self.__getMining(field)
    return self.__getLocalTrans(field)
  
  def __matchLocal(self):
    """"""
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
    answer = []
    if not isinstance(values, list):
      values = [values]
    if not values is NULL and not values is None:
      temp = self.__set[2](values)
      if not temp is None:
        self.__set[0] = temp
        if self.__number:
          self.__number -= 1
        else:
          answer.append((temp, True, self.__orig, ancillary, scoredFieldValues))
    return answer
  
  def __update(self):
    """"""
    self.__localTransDictUpdate()
    nvalue = str(self.get(self.__set[1]))
    if self.__weightField is None:
      nwght=1
    else:
      nwght=self.get(self.__weightField)
    # keep the value from weight field for use by Producer.add_results
    self.__ancillary["weightVal"]=nwght
    cnts=self.__ddCounts
    if self.__conditionField is None:
      # counts are indexed by just the test field value
      self.__ancillary["conditionVal"]=None
      # update frequency 
      if nvalue not in cnts:
        cnts[nvalue]=0
      cnts[nvalue]+=nwght
    else:
      # counts are indexed by condition field value and test field value
      cfval = self.get(self.__conditionField)
      self.__ancillary["conditionVal"]=cfval
      #print "test.update: updating conditional freq c=",cfval
      if cfval not in cnts:
        cnts[cfval]={}
      cnts=cnts[cfval]
      # update conditional count vector
      if nvalue not in cnts:
        cnts[nvalue]=0
      cnts[nvalue]+=nwght

  def __doNothing(self):
    """"""
    pass
  
  def __returnTrue(self):
    """"""
    return True
