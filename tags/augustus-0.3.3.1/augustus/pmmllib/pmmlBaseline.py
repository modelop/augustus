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
import math

#This file defines PMML elements utilized within baseline models

#for each element, the same functions are defined that are
#described at the top of the pmmlElements.py file.

class pmmlFieldValueCount(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["field","value","count"]
    requiredAttributes = ["field","count"]
    pmmlElement.__init__(self, "FieldValueCount", myChild, attributeNames, attributes, requiredAttributes)

class pmmlFieldValueCounts(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, pmmlFieldValueCount]
    maximums = [None, None]
    minimums = [None, 1]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "FieldValueCounts:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field","value"]
    requiredAttributes = ["field"]
    pmmlElement.__init__(self, "FieldValueCounts", myChild, attributeNames, attributes, requiredAttributes)

class sequenceFE(pmmlSequence):
  """"""
  types = [pmmlExtension, pmmlFieldValueCounts]
  maximums = [None, None]
  def __init__(self, children):
    """"""
    minimums = [None, 1]
    pmmlSequence.__init__(self, sequenceFE.types, minimums, sequenceFE.maximums, children)
  
  @staticmethod  
  def formatChildren(children):
    """"""
    return pmmlSequence.formatChildren(sequenceFE.types, sequenceFE.maximums, children)

class pmmlCountTable(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    if attributes != {}:
      raise pmmlError, "CountTable:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(sequenceFE.types, sequenceFE.maximums, children)
    if extras != []:
      raise pmmlError, "CountTable:  " + pmmlErrorStrings.elements
    myChild = sequenceFE(children)
    pmmlElement.__init__(self, "CountTable", myChild)

class pmmlNormalizedCountTable(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    if attributes != {}:
      raise pmmlError, "NormalizedCountTable:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(sequenceFE.types, sequenceFE.maximums, children)
    if extras != []:
      raise pmmlError, "NormalizedCountTable:  " + pmmlErrorStrings.elements
    myChild = sequenceFE(children)
    pmmlElement.__init__(self, "NormalizedCountTable", myChild)

class pmmlHistogramTable(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    if attributes != {}:
      raise pmmlError, "HistogramTable:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(sequenceFE.types, sequenceFE.maximums, children)
    if extras != []:
      raise pmmlError, "HistogramTable:  " + pmmlErrorStrings.elements
    myChild = sequenceFE(children)
    pmmlElement.__init__(self, "HistogramTable", myChild)

class choiceCN(pmmlChoice):
  """"""
  __types = [pmmlCountTable, pmmlNormalizedCountTable, pmmlHistogramTable]
  __maximums = [1, 1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1, 1]
    
    pmmlChoice.__init__(self, choiceCN.__types, minimums, choiceCN.__maximums, instances)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceCN.__types, choiceCN.__maximums, children)

class pmmlUniformDistribution(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["lower","upper"]
    requiredAttributes = ["lower","upper"]
    pmmlElement.__init__(self, "UniformDistribution", myChild, attributeNames, attributes, requiredAttributes)
  
  @staticmethod
  def __pdf(value, upper, lower):
    """"""
    if upper != lower and value >= lower and value <= upper:
      return 1/(upper - lower)
    return 0
  
  def pdf(self):
    """"""
    return lambda x : pmmlUniformDistribution.__pdf(x, float(self.getAttribute("upper")), float(self.getAttribute("lower")))

class pmmlPoissonDistribution(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["mean"]
    requiredAttributes = ["mean"]
    pmmlElement.__init__(self, "PoissonDistribution", myChild, attributeNames, attributes, requiredAttributes)
  
  @staticmethod
  def __fact(num):
    """"""
    num = int(num)
    if num == 1 or num == 0:
      return 1
    return reduce(lambda a,b : a * b, range(1,num+1))
  
  def __pdf(value, mean):
    """"""
    if value >= 0 and int(value) == value:
      return (math.exp(-mean) * pow(mean, value)) / pmmlPoissonDistribution.__fact(value)
    return 0
  __pdf = staticmethod(__pdf)
  
  def pdf(self):
    """"""
    return lambda x : pmmlPoissonDistribution.__pdf(x, float(self.getAttribute("mean")))

class pmmlGaussianDistribution(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["mean", "variance"]
    requiredAttributes = ["mean", "variance"]
    pmmlElement.__init__(self, "GaussianDistribution", myChild, attributeNames, attributes, requiredAttributes)
  
  @staticmethod
  def __pdf(value, mean, coef, expCoef):
    """"""
    return coef * math.exp((value - mean)**2*expCoef)
  
  def pdf(self):
    """"""
    var = float(self.getAttribute("variance"))
    stdDev = math.sqrt(var)
    mean = float(self.getAttribute("mean"))
    if stdDev == 0:
      return lambda x : None
    coef = 1/(stdDev * math.sqrt(2 * math.pi))
    expCoef = -1/(2 * var)
    return lambda x : pmmlGaussianDistribution.__pdf(x, mean, coef, expCoef)

class pmmlAnyDistribution(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["mean", "variance"]
    requiredAttributes = ["mean", "variance"]
    pmmlElement.__init__(self, "AnyDistribution", myChild, attributeNames, attributes, requiredAttributes)

# stuff to support DiscreteDistribution 
class pmmlValueCount(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    myChild = pmmlList([pmmlExtension], children)
    attributeNames = ["value","count"]
    requiredAttributes = ["value","count"]
    pmmlElement.__init__(self, "ValueCount", myChild, attributeNames, attributes, requiredAttributes)

class sequenceVC(pmmlSequence):
  """"""
  types = [pmmlExtension, pmmlValueCount]
  maximums = [None, None]
  def __init__(self, children):
    """"""
    minimums = [None, None]
    pmmlSequence.__init__(self, sequenceVC.types, minimums, sequenceVC.maximums, children)
    
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlSequence.formatChildren(sequenceVC.types, sequenceVC.maximums, children)

class pmmlDiscreteDistribution(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    (extras, children) = pmmlSequence.formatChildren(sequenceVC.types, sequenceVC.maximums, children)
    if extras != []:
      raise pmmlError, "DiscreteDistribution:  " + pmmlErrorStrings.elements
    myChild = sequenceVC(children)
    attributeNames = ["sample"]
    requiredAttributes = ["sample"]
    pmmlElement.__init__(self, "DiscreteDistribution", myChild, attributeNames, attributes, requiredAttributes)

class choiceAG(pmmlChoice):
  """"""
  __types = [pmmlAnyDistribution, pmmlGaussianDistribution, pmmlPoissonDistribution, pmmlUniformDistribution, pmmlDiscreteDistribution]
  __maximums = [1, 1, 1, 1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1, 1, 1, 1]
    
    pmmlChoice.__init__(self, choiceAG.__types, minimums, choiceAG.__maximums, instances)

  @staticmethod    
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceAG.__types, choiceAG.__maximums, children)

class choiceAC(pmmlChoice):
  """"""
  __types = [choiceAG, choiceCN]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    pmmlChoice.__init__(self, choiceAC.__types, minimums, choiceAC.__maximums, instances)

  @staticmethod    
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceAC.__types, choiceAC.__maximums, children)

class pmmlAlternate(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceAC]
    maximums = [None, None]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Alternate:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Alternate:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Alternate", myChild)

class choiceA(pmmlChoice):
  """"""
  __types = [pmmlAlternate]
  __maximums = [1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1]
    
    pmmlChoice.__init__(self, choiceA.__types, minimums, choiceA.__maximums, instances)
  
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceA.__types, choiceA.__maximums, children)

class pmmlBaseline(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, choiceAC]
    maximums = [None, 1]
    minimums = [None, 1]
    if attributes != {}:
      raise pmmlError, "Baseline:  " + pmmlErrorStrings.noAttributes
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "Baseline:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    pmmlElement.__init__(self, "Baseline", myChild)

class choiceBA(pmmlChoice):
  """"""
  __types = [pmmlBaseline, choiceAC]
  __maximums = [1, 1]
  def __init__(self, instances=[]):
    """"""
    minimums = [1, 1]
    
    pmmlChoice.__init__(self, choiceBA.__types, minimums, choiceBA.__maximums, instances)

  @staticmethod    
  def formatChildren(children):
    """"""
    return pmmlChoice.formatChildren(choiceBA.__types, choiceBA.__maximums, children)

class sequenceBA(pmmlSequence):
  """"""
  __types = [choiceBA, choiceA]
  __maximums = [1, None]
  def __init__(self, children):
    """"""
    minimums = [1, None]
    
    pmmlSequence.__init__(self, sequenceBA.__types, minimums, sequenceBA.__maximums, children)
    
  @staticmethod
  def formatChildren(children):
    """"""
    return pmmlSequence.formatChildren(sequenceBA.__types, sequenceBA.__maximums, children)

class extensionSkip(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    if children != []:
      raise pmmlError, "Skip:  " + pmmlErrorStrings.noElements
    pmmlElement.__init__(self, "Skip", pmmlList([],[]), ["number"],attributes,["number"])

class windowList(list):
  """defines a list of a maximum length
  when items are appended to the list beyond what it can handle,
    the first item entered is removed and the new item appended"""
  class __iterator:
    """"""
    def __init__(self, windowList):
      self.__length = len(windowList)
      self.__cur = -1
      self.__list = windowList
    
    def next(self):
      """"""
      self.__cur += 1
      if self.__cur >= self.__length:
        raise StopIteration
      return self.__list[self.__cur]
    
    def __iter__(self):
      """"""
      return self
  
  class __reverseIterator:
    """"""
    def __init__(self, windowList):
      """"""
      self.__cur = len(windowList)
      self.__list = windowList
    
    def next(self):
      """"""
      self.__cur -= 1
      if self.__cur < 0:
        raise StopIteration
      return self.__list[self.__cur]
    
    def __iter__(self):
      """"""
      return self
  
  def __init__(self, windowSize, *items):
    """"""
    self.__windowSize = windowSize
    self.__len = 0
    self.__first = 0
    list.__init__(self)
    list.extend(self, range(windowSize))
    for item in items:
      self.append(item)
  
  def append(self, item):
    """"""
    #find out where to put the next item and increment the size, if necessary
    if self.__len == self.__windowSize:
      next = self.__first
      #increment where the first item is located
      self.__first += 1
      if self.__first == self.__windowSize:
        self.__first = 0
    else:
      next = self.__first + self.__len
      if next >= self.__windowSize:
        next -= self.__windowSize
      self.__len += 1
    list.__setitem__(self, next, item)
  
  def expiring(self):
    """"""
    #return the item that will be dropped with append(), if any
    if self.__len == self.__windowSize:
      rval = list.__getitem__(self, self.__first)
    else:
      rval = None
    return rval
  
  def extend(self, items):
    """"""
    for item in items:
      self.append(item)
  
  def __len__(self):
    """"""
    return self.__len
  
  def __convertKey(self, key):
    """"""
    if key >= self.__len:
      raise IndexError
    index = self.__first + key
    if key < 0:
      index += self.__len
    if index >= self.__windowSize:
      index -= self.__windowSize
    return index
  
  def __getitem__(self, key):
    """"""
    return list.__getitem__(self, self.__convertKey(key))
  
  def __delitem__(self, key):
    """"""
    index = self.__convertKey(key)
    last = self.__convertKey(self.__len - 1)
    #move everything to the end of the list or the last entry up one
    while index + 1 != self.__windowSize and index + 1 != last:
      list.__setitem__(self, index, list.__getitem__(self, index + 1))
      index += 1
    #copy the last entry
    if index + 1 == last:
      list.__setitem__(self, index, list.__getitem__(self, index + 1))
    #need to start at the beginning and continue moving
    else:
      list.__setitem__(self, index, list.__getitem__(self, 0))
      index = 0
      while index != last:
        list.__setitem__(self, index, list.__getitem__(self, index + 1))
        index += 1
    #remove the reference to the last entry
    list.__setitem__(self, last, None)
    self.__len -= 1
  
  def __setitem__(self, key, value):
    """"""
    return list.__setitem__(self, self.__convertKey(key), value)
  
  def __iter__(self):
    """"""
    return windowList.__iterator(self)
  
  def reverseIterator(self):
    """"""
    return windowList.__reverseIterator(self)

class pmmlTestDistributions(pmmlElement):
  """"""
  def __init__(self, name="", attributes={}, children=[]):
    """"""
    types = [pmmlExtension, sequenceBA, pmmlSegments]
    maximums = [None, None, 1]
    minimums = [None, None, None]
    (extras, children) = pmmlSequence.formatChildren(types, maximums, children)
    if extras != []:
      raise pmmlError, "TestDistributions:  " + pmmlErrorStrings.elements
    myChild = pmmlSequence(types, minimums, maximums, children)
    attributeNames = ["field","testStatistic","testType","threshold","resetValue", "windowSize", "weightField", "conditionField","normalizationScheme"]
    requiredAttributes = ["testType","threshold"]
    pmmlElement.__init__(self, "TestDistributions", myChild, attributeNames, attributes, requiredAttributes)
  
  def __glr(self, current, mean, frac):
    """"""
    self.__glrList.append((current - mean)**2)
    maximum = self.__glrList[len(self.__glrList) - 1]
    sum = 0
    cnt = 0
    for entry in self.__glrList.reverseIterator():
      sum += entry
      cnt += 1
      avg = sum / cnt
      if avg > maximum:
        maximum = avg
    return maximum * frac

  @staticmethod  
  def __cusum(last, current, function, resetValue):
    """"""
    try:
      return max(resetValue, last + function(current))
    except:
      return None
  
  @staticmethod
  def __zValue(current, mean, stdDev):
    """"""
    return (current - mean) / stdDev
  
  def __dDist(self,current, sample, nulldist,ndsumsq):
    """"""
    if sample==0:
      return current
    else:
      self.__ancillary["maxDistCat"]="None"
      self.__ancillary["maxDistCatVal"]=0.0
      self.__ancillary["maxDistCondVal"]="None"
      self.__ancillary["distribution"]=[]
      maxscore=0.0
      #print "__dDist called with nulldist=", nulldist, "sumsq=",ndsumsq
      cnts=self.__ddCounts
      cndfld=self.__conditionField
      #print "_dDist : conditionfield=",cndfld

      if not cndfld is None:
        # Unclear how to handle windows on conditional count vectors, so punt
        if not self.__windowSize == 0:
          raise pmmlError, "Baseline dDist: cannot window conditional counts"
        # counts are broken up by value of conditionField
        # score each vector of counts against baseline and return max
        #print "__dDist with conditionField:", cndfld
        for cndval in cnts:
          cvec=cnts[cndval]
          if (self.__normalizationScheme=='Independent'):
            [tmpscore,maxcat,maxcontrib]=self.__cvscore2(cvec,nulldist,ndsumsq)
          else:
            [tmpscore,maxcat,maxcontrib]=self.__cvscore(cvec,nulldist,ndsumsq)
          #print "       cndval=:", cndval, " score=",tmpscore
          if tmpscore > maxscore:
            maxscore=tmpscore
            self.__ancillary["maxDistCat"]=maxcat
            self.__ancillary["maxDistCatVal"]=maxcontrib
            self.__ancillary["maxDistCondVal"]=cndval
          self.__ancillary["distribution"]=''.join(["<BIN index='"+i[0]+"' val='"+str(i[1])+"'/>" for i in cnts.items()])
          #self.__ancillary["distribution"]=cvec
        return maxscore

      # if scoring window, adjust counts for data moving out of window
      if not self.__windowSize == 0:
        nwght=self.__ancillary["weightVal"];
        # find the element being dropped and decrement its freq in Counts
        ovalue = self.__ddList.expiring()
        owght = self.__ddWghts.expiring()
        if not ovalue is None:
          cnts[ovalue]-=owght
        # put the current value in the windowList __dDlist
        self.__ddList.append(current)
        self.__ddWghts.append(nwght)

      # compute distance between current discrete distribution and baseline
      if (self.__normalizationScheme=='Independent'):
        [maxscore,maxcat,maxcontrib]=self.__cvscore2(cnts,nulldist,ndsumsq)
      else:
        [maxscore,maxcat,maxcontrib]=self.__cvscore(cnts,nulldist,ndsumsq)
      #print "__dDist sc=",maxscore,maxcat,maxcontrib
      #print "__dDist ns=",ns,mxcat,mxcontrib
      self.__ancillary["maxDistCat"]=maxcat
      self.__ancillary["maxDistCatVal"]=maxcontrib
      self.__ancillary["distribution"]=''.join(["<BIN index='"+i[0]+"' val='"+str(i[1])+"'/>" for i in cnts.items()])
      return maxscore

  @staticmethod
  def __cvscore(cv,nd,ndss):
    # Let v and w denote the count vectors from the nulldist
    # current window of field values. Then d = <v,w>/((<v,v>+<w,w>)/2)
    maxcat="None"
    maxcontrib=0.0
    wdsumsq=0
    innerprod=0.0
    for cat in cv:
      v=cv[cat]
      wdsumsq+=v*v
      if cat in nd:
         ipcontrib = nd[cat]*v
         innerprod += ipcontrib
         if ipcontrib > maxcontrib:
            maxcontrib=ipcontrib
            maxcat=cat
    ipscale=(ndss+wdsumsq)/2.0
    score=float(innerprod)/ipscale
    maxcontrib=maxcontrib/ipscale
    return [score, maxcat, maxcontrib]

  @staticmethod
  def __cvscore2(cv,nd,ndss):
    # Let v and w denote the count vectors from the nulldist
    # current window of field values. Then d = <v,w>/((sqrt(<v,v>)sqrt(<w,w>)))
    maxcat="None"
    maxcontrib=0.0
    wdsumsq=0
    innerprod=0.0
    for cat in cv:
      v=cv[cat]
      wdsumsq+=v*v
      if cat in nd:
         ipcontrib = nd[cat]*v
         innerprod += ipcontrib
         if ipcontrib > maxcontrib:
            maxcontrib=ipcontrib
            maxcat=cat
    ipscale=math.sqrt(max(ndss*wdsumsq,0.))
    try:
      score=float(innerprod)/ipscale
    except:
      score=1.0
    try:
      Maxcontrib=maxcontrib/ipscale
    except:
      Maxcontrib=maxcontrib
    return [score, maxcat, maxcontrib]
  
  @staticmethod
  def __threshold(value, standard):
    """"""
    return (value, value >= standard)

  @staticmethod
  def __belowthreshold(value, standard):
    """"""
    return (value, value <= standard)
  
  def getRestrictions(self):
    """Parse segmentation information."""
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
  
  def makeFunction(self):
    """"""
    set = None
    testType = self.getAttribute("testType")
    #the function is testing whether or not you break a threshold
    if testType == "threshold":
      function = None
      threshold = None
      self.__ancillary = {}
      testStatistic = self.getAttribute("testStatistic")
      field = self.getAttribute("field")
      #catch the case where the field name is missing
      if field == None:
        raise pmmlError, "TestDistributions:  " + pmmlErrorStrings.fieldMissing + repr(self)
      baseline = self.getChildrenOfType(pmmlBaseline)[0]
      baselineDist = baseline.getChildrenOfType(choiceAG)[0]
      #use the cusum function
      if testStatistic == "CUSUM":
        baselinePDF = baselineDist.pdf()
        alternateList = self.getChildrenOfType(pmmlAlternate)
        if len(alternateList) != 1:
          raise pmmlError, "TestDistributions:  " + pmmlErrorStrings.alternateMissing + repr(self)
        alternateDist = alternateList[0].getChildrenOfType(choiceAG)[0]
        alternatePDF = alternateDist.pdf()
        if alternatePDF(0) != None and baselinePDF(0) != None:
          ratio = lambda x : math.log10(alternatePDF(x)/baselinePDF(x))
          resetValue = self.getAttribute("resetValue")
          if resetValue != None:
            resetValue = float(resetValue)
          else:
            resetValue = 0
          function = lambda last, current : pmmlTestDistributions.__cusum(last, current, ratio, resetValue)
          threshold = lambda last, current : pmmlTestDistributions.__threshold(function(last, current), float(self.getAttribute("threshold")))
      #use the z value algorithm
      elif testStatistic == "zValue":
        mean = float(baselineDist.getAttribute("mean"))
        var = float(baselineDist.getAttribute("variance"))
        stdDev = math.sqrt(var)
        if stdDev != 0:
          function = lambda last, current : pmmlTestDistributions.__zValue(current, mean, stdDev)
          threshold = lambda last, current : pmmlTestDistributions.__threshold(function(last, current), float(self.getAttribute("threshold")))
      #use the discrete distribution algorithm
      elif testStatistic == "dDist":
        sample = float(baselineDist.getAttribute("sample"))
        window = self.getAttribute("windowSize")
        if window == None:
          self.__windowSize=0
        else:
          self.__windowSize=int(window)
          self.__ddList = windowList(self.__windowSize)
          self.__ddWghts = windowList(self.__windowSize)
        self.__ddCounts = {}
        self.__weightField=self.getAttribute("weightField")
        self.__normalizationScheme=self.getAttribute("normalizationScheme")
        self.__conditionField=self.getAttribute("conditionField")
        nulldist={}
        ndsumsq=0
        if sample != 0:
          valuecounts=baselineDist.getChildrenOfType(pmmlValueCount)
          for vc in valuecounts:
            v=vc.getAttribute("value")
            c=float(vc.getAttribute("count"))
            nulldist[v]=c
            ndsumsq+=c*c
          #print "makeFunction nulldist=",nulldist," ndsumsq=",ndsumsq
        function = lambda last, current : self.__dDist(current, sample, nulldist,ndsumsq)
        threshold = lambda last, current : pmmlTestDistributions.__belowthreshold(function(last, current), float(self.getAttribute("threshold")))
      #use the glr algorithm
      elif testStatistic == "GLR":
        mean = float(baselineDist.getAttribute("mean"))
        var = float(baselineDist.getAttribute("variance"))
        self.__windowSize = int(self.getAttribute("windowSize"))
        if var != 0:
          self.__glrList = windowList(self.__windowSize)
          frac = 1/(2*var)
          self.num = 0
          self.sumSquares = 0
          function = lambda last, current : self.__glr(current, mean, frac)
          threshold = lambda last, current : pmmlTestDistributions.__threshold(function(last, current), float(self.getAttribute("threshold")))
      if function and threshold:
        set = [0, field, threshold, self.__ancillary]
    return set
  
  def newInit(self, dataInput, directFields, localTransDict=None):
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
    values = self.get(self.__set[1])
    ancillary=self.__set[3]
    answer = []
    if not isinstance(values, list):
      values = [values]
    for value in values:
      if not value is NULL and not value is None:
        (temp, brokeThreshold) = self.__set[2](self.__set[0], value)
        if not temp is None:
          self.__set[0] = temp
          if self.__number:
            self.__number -= 1
          else:
            answer.append((temp, brokeThreshold, self.__orig, ancillary))
    return answer
  
  def __update(self):
    """"""
    #print "test.update: set=",self.__set
    self.__localTransDictUpdate()
    #No need to check on every event anymore
    #testStatistic = self.getAttribute("testStatistic")
    #if testStatistic == "dDist":
    nvalue = str(self.get(self.__set[1]))
    # print "test.update(): nvalue=",nvalue," set=", self.__set
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
    """This function does nothing and seems to be more efficient at doing nothing than a lambda function is."""
    pass
  
  def __returnTrue(self):
    """This function return true and seems to be more efficient at returning true than a lambda function is."""
    return True
