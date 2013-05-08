#!/usr/bin/env python

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


#vector based table class
import augustus.kernel.unitable.unitable as uni
#timer classes
import augustus.runlib.timer as timer
#xml tools
from augustus.external.etree import ElementTree as ET
#easy way to make and use program options
from optparse import OptionParser, make_option
#used to automatically input pmml into the necessary classes
from augustus.pmmllib.pmmlReader import *
#allows reading of multiple-root element xml files
import augustus.runlib.any_reader as anyReader
#allows easy log creation
import logging
#directory manipulations
import os
# for stream handles
from sys import stdout
# time handling for logging info
import datetime
#Needed to deep copy mining schemas and local transformations
import copy
# Constants
import augustus.const as AUGUSTUS_CONSTS
from numpy import array, cumprod
import gc
class metadataLogger(dict):
  """ 
   Container for options guiding metadata production
  """
  def __init__(self, logger):
    self.log = logger
    # Dictionary of metadata collected by different objects.
    self.isEnabled = False
    self.collected = {} 
    

  def enableMetaDataCollection(self):
    self['Total Producer Time'] = datetime.timedelta()
    self['Time Calculating Model'] = datetime.timedelta(0,0,0)
    self['Time Outputting Model'] = datetime.timedelta(0,0,0)
    self['Time Running _score'] = datetime.timedelta(0,0,0)
    self['Time Creating PMML Tests'] = datetime.timedelta(0,0,0)
    self['Time Processing Baseline Data'] = datetime.timedelta(0,0,0)
    self['Time Reading Data'] = datetime.timedelta(0,0,0)
    self['Calls to Score']=0
    self['Number of Requested Segments'] = 0
    self['Number of Segments Filled'] = 0
    self['Number of Explicitly Segmented Fields'] = 0
    self['Number of Requested Explicit Segment Values'] = []
    self['Number of Regularly Segmented Fields'] = 0
    self['Number of Requested Regular Segment Intervals'] = 0
    self['Unpopulated Segments'] = []
    self.isEnabled = True

  def getMetaData(self):
    self['Number of Requested Segments'] = numpy.prod(self['Number of Requested Explicit Segment Values'])
    self['Number of Requested Explicit Segment Values'] = numpy.sum(self['Number of Requested Explicit Segment Values'])
    self['Unpopulated Segments'] = (os.linesep + os.linesep + ' ').join(self['Unpopulated Segments'])
    for k in self.keys():
      self[k] = str(self[k])
    reportOrder = self.items()
    reportOrder.sort()
    return [' : '.join((k,v)) for (k,v) in reportOrder]

  def report(self):
    for reporter in self.collected.keys():
      self.log.info(10*' '+'%s%s'%(reporter,os.linesep))
      for msg in self.collected[reporter]:
        self.log.info('%s'%msg)


def sigmaInterpolate(N, epsilon = 0.5):
  """
  Simple function based on linear interpolation to
  estimate gaussian variance from observation of no
  events outside a window of size +epsilon to -epsilon.  
  """
  breaks = [0.00000, 0.68270, 0.95450, .99730, .99994, 1.00000]
  breaks = 1.0 - array(breaks)
  SDbreaks = [0, 1, 2, 3, 4, 5]
  p = 1.0/float(N)
  starts = breaks[breaks >= p]
  start = starts[-1]
  end = breaks[breaks < p][0]
  f = (start - p) / (start - end)
  sd_est = epsilon / ( float(len(starts)) + f )
  return sd_est**2

class xmlRow:
  """"""
  def __init__(self, data):
    """"""
    self.__data = data
    self.__dict = {}
  
  def __findValue(self, key, element):
    """"""
    if element.name == key:
      for child in element:
        if isinstance(child, str):
          return child
      return ""
    if key in element.attr:
      return element.attr[key]
    for child in element:
      if not isinstance(child, str):
        value = self.__findValue(key, child)
        if not value is None:
          return value
    return None
  
  def __getitem__(self, key):
    """"""
    if key in self.__dict:
      return self.__dict[key]
    value = self.__findValue(key, self.__data)
    if value is None:
      raise fieldMissing, key
    self.__dict[key] = value
    return value

class xmlTable:
  """"""
  def __init__(self, filename, magicheader):
    """"""
    #output errors to stderr
    logger = logging.Logger('xmlTable')
    logger.addHandler(logging.StreamHandler())
    #create a stream reader
    self.__reader = anyReader.Reader(self.storeRow, source=filename, magicheader=magicheader)
  
  def __len__(self):
    """"""
    return 0
  
  def storeRow(self, data):
    """"""
    self.__called = True
    self.__row = xmlRow(data)
  
  def next(self):
    """"""
    self.__called = False
    self.__reader.read_once()
    if not self.__called:
      raise StopIteration
    return self.__row
  
  def __iter__(self):
    """"""
    return self

class xmlFileEntry:
  """"""
  def __init__(self, data):
    """"""
    self.__data = data
    self.__dict = {}
  
  def __findValue(self, key, element):
    """"""
    # this might be broken, switch to use .name
    # need an example producing from an XML file
    if element.tag[element.tag.find("}") + 1:] == key:
      if element.text.strip():
        return element.text
      if len(element) > 0:
        child = element[len(element) - 1]
        if child.tail.strip():
          return child.tail
      return ""
    if key in element.keys():
      return element.get(key)
    for child in element:
      value = self.__findValue(key, child)
      if not value is None:
        return value
    return None
  
  def __getitem__(self, key):
    """"""
    if key in self.__dict:
      return self.__dict[key]
    value = self.__findValue(key, self.__data)
    if value is None:
      raise fieldMissing, key
    self.__dict[key] = value
    return value

class xmlFile:
  """"""
  def __init__(self, filename):
    tree = ET.parse(filename)
    self.__root = tree.getroot()
  
  def __len__(self):
    """"""
    return len(self.__root)
  
  class xmlFileIterator:
    """"""
    def __init__(self, parent):
      """"""
      self.__parent = parent
      self.__where = 0
    
    def next(self):
      """"""
      try:
        row = self.__parent[self.__where]
      except:
        raise StopIteration
      self.__where += 1
      return xmlFileEntry(row)
  
  def __iter__(self):
    """"""
    return xmlFile.xmlFileIterator(self.__root)

class uniRecordWrapper:
  """"""
  def __init__(self, row):
    """"""
    self.__row = row
  
  def __getitem__(self, key):
    """"""
    try:
      return self.__row[key]
    except:
      raise fieldMissing, key

class uniTableWrapper:
  """"""
  def __init__(self, table):
    """"""
    self.__iterator = iter(table)
    self.__len = len(table)
  
  def __len__(self):
    """"""
    return self.__len
  
  def next(self):
    """"""
    return uniRecordWrapper(self.__iterator.next())
  
  def __iter__(self):
    """"""
    return self



###
### Producer class starts here.
###


class Producer:
  """"""
  def __init__(self, timing=None, wrapping=True):
    """Config and init the timer."""
    self.__timer = None
    self.logger = logging.Logger('producer')
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(name)s %(asctime)s %(message)s'))
    self.logger.addHandler(handler)
    self.logger.setLevel(logging.DEBUG)
    if not timing is None:
      self.__increment = float(timing) / 100
      if wrapping:
        self.__timer = timer.timer("Producer timer begins", "Producer timer ends", "Total producer time")
      else:
        self.__timer = timer.timer()
    self.metadata = None
  
  def _makeSegments(self, declarations):
    """TODO: Refactor this. (Well, maybe sometime later.)"""
    #produces segments from their declarations
    #make the first segments
    segments = []
    current = declarations[0]
    field = str(current.attr.get("field"))
    if current.name == "explicitSegments":
      if self.metadata is not None:
        self.metadata['Number of Explicitly Segmented Fields'] += 1  
        self.metadata['Number of Requested Explicit Segment Values'].append(len(current))
      #produce a segment for each value declared here
      for value in current:
        segments.append({field:str(value.attr.get("value"))})
    elif current.name == "regularSegments":
      #produce segments for each partition declared here
      if self.metadata is not None:
        self.metadata['Number of Regularly Segmented Fields'] += 1   
        self.metadata['Number of Requested Regular Segment Intervals'] += len(current)     
      for partition in current:
        low = float(partition.attr.get("low"))
        high = float(partition.attr.get("high"))
        step = (high - low) / float(partition.get("divisions"))
        while low + step <= high:
          segments.append({field:(str(low), str(low + step))})
          low += step
        last = segments[len(segments) - 1]
        last[field] = (last[field][0], str(high))
    else:
      raise StandardError, "Element not recognized as a segment declaration:  " + current.name
    #make the rest of the segments
    for current in declarations[1:]:
      field = str(current.attr.get("field"))
      if current.name == "explicitSegments":
        if self.metadata is not None:
          self.metadata['Number of Explicitly Segmented Fields'] += 1  
          self.metadata['Number of Requested Explicit Segment Values'].append(len(current))
        newSegments = []
        #for each of the previous segments
        for rule in segments:
          #produce a new segment for each value declared here
          for value in current:
            newRule = dict(rule)
            try:
              newRule[field] = str(value.attr.get("value"))
            except:
              raise StandardError, "Explicit segment malformed:  a 'value' attribute is required"
            newSegments.append(newRule)
        #save the new set of segments
        segments = newSegments
      elif current.name == "regularSegments":
        newSegments = []
        #for each of the previous segments
        if self.metadata is not None:
          self.metadata['Number of Regularly Segmented Fields'] += 1   
          self.metadata['Number of Requested Regular Segment Intervals'] += len(current)     
        for rule in segments:
          #produce segments for each partition declared here
          for partition in current:
            try:
              low = float(partition.attr.get("low"))
              high = float(partition.attr.get("high"))
              divisions = float(partition.attr.get("divisions"))
            except:
              raise StandardError, "Regular segment malformed:  a required attribute is missing"
            step = (high - low) / divisions
            while low + step <= high:
              newRule = dict(rule)
              newRule[field] = (str(low), str(low + step))
              newSegments.append(newRule)
              low += step
            last = newSegments[len(newSegments) - 1]
            last[field] = (last[field][0], str(high))
        #save the new set of segments
        segments = newSegments
      else:
        raise StandardError, "Element not recognized as a segment declaration: " + current.name
    self._segments.extend(segments)

  @staticmethod
  def testValidatingFunctions(method=None,threshold=None):
    """ Method to validate a test """
    if method is None:
      def validatingFunction(i):
        return True
    else:
      def validatingFunction(i):
        return eval(str(i)+method+str(threshold))
    return validatingFunction

  @staticmethod
  def tupelize(rules):
    """This function turns a segment rule into a tuple."""
    keys = rules.keys()
    keys.sort()
    temp = [(key,rules[key]) for key in keys]
    return tuple(temp)
  
  def inputConfigs(self, file):
    if self.__timer:
      self.__timer.output("Inputting configurations")

    #input generic configurations
    self.__batch = False
    self.__debugFile = None
    self.__skip = None
    self.zeroVarianceOptions = None
    self.testValidation = None
    zeroVarianceHandling = None
    if file.name=='model':
      self.__mode = file.attr.get("mode")
      self.__input = file.attr.get("input")
      self.__output = file.attr.get("output")
    for child in file:
      if child.name=="batch":
        self.__batch = True
      if child.name =="metadata":
        metadatalogger = logging.Logger('ProducerMetaData')
        if 'name' in child.attr:
          metadataHandler = logging.FileHandler(str(child.attr.get('name')))
        else:
          metadataHandler = logging.StreamHandler(stdout)
        metadataHandler.setFormatter(logging.Formatter('%(message)s'))
        metadatalogger.addHandler(metadataHandler)
        metadatalogger.setLevel(logging.INFO)
        self.metadata = metadataLogger(metadatalogger)
        self.metadata.enableMetaDataCollection()
        # This switch produces an additional file that
        # summarizes statistics (mean, std dev) for
        # segments.
        if child.attr.get('extended') is not None:
          self.__debugFile = child.attr.get("extended")
      if child.name=="skip":
        self.__skip = long(child.attr.get("number"))
      if child.name=="test":
        test = child
      if child.name=="validation":
        validmethod = child.attr.get("method")
        validthreshold = child.attr.get("threshold")
        self.testValidation = Producer.testValidatingFunctions(validmethod,validthreshold)
      if child.name=="zeroVarianceHandling":
        self.zeroVarianceOptions = {'method':child.attr['method'],'resolution':0.5,'defaultvalue':1.0}
        if 'resolution' in child.attr.keys():
          self.zeroVarianceOptions['resolution']=float(child.attr['resolution'])
        elif 'value' in child.attr.keys():
          self.zeroVarianceOptions['defaultvalue']=float(child.attr['value'])
    if self.testValidation is None:
      self.testValidation = Producer.testValidatingFunctions(None)
    # pick up baseline and alternate distributions (if any)
    # allow for either order, baseline-alternate or alternate-baseline
    self._baseline = test[0]
    start = 1
    self._alternate = None
    if len(test) > 1:
      next = test[1]
      start = 2
      if next.name != "alternate":
        start = 1
      else:
        self._alternate = next
    #produce segmentation
    self._segments = []
    for segmentDeclarations in test[start:]:
      self._makeSegments(segmentDeclarations)
    
    #prepare for statistics gathering
    self._baseDict = {():None}
    if self._segments:
      for segment in self._segments:
        self._baseDict[Producer.tupelize(segment)] = None
    
    #remember the attributes of the test distribution
    self._attrs = {}
    for key in test.attr.keys():
      self._attrs[str(key)] = str(test.attr.get(key))
 

  def inputPMML(self):
    """This function reads in a pmml file and saves the things we are going to be reusing when we collect stats."""
    if self.__timer:
      self.__timer.output("Inputting model")
    #input the portion of the model that is completed
    reader = pmmlReader()
    reader.parse(str(self.__input))
    self._pmml = reader.root
    self._model = self._pmml.getChildrenOfType(pmmlBaselineModel)[0]
    self._existingTests = self._model.getChildrenOfType(pmmlTestDistributions)
    self._localTransformations = self._model.getChildrenOfType(pmmlLocalTransformations)
    if len(self._localTransformations) > 0:
      self._localTransformations = self._localTransformations[0]
    else:
      self._localTransformations = None

    #It looks like this will only work for explicit segments
    seg={}
    if self.__mode=='Update':
      for t in self._existingTests:
        for e in t.getChildrenOfType(pmmlSegments)[0].getChildrenOfType(pmmlExplicitSegment):
          seg[e.getAttribute('field')]=e.getAttribute('value')
        segment=Producer.tupelize(seg)
        if segment in self._baseDict.keys():
          testdistributions=t.getChildrenOfType(pmmlBaseline)[0].getChildrenOfType(pmmlDiscreteDistribution)
          try:
            test=testdistributions[0]
            test.makeAttribute("sample","0")
            vctable={}
            for vc in test.getChildrenOfType(pmmlValueCount):
              vctable[vc.getAttribute('value')]=float(vc.getAttribute('count'))
            self._baseDict[segment]=[0,vctable,False]
          except:
            pass


  def get(self, field):
    """"""
    return self.row[field]

  @staticmethod  
  def _addResults(teststat, results, stats, impliedZero = None):
    """"""
    if teststat == "dDist":
      for result in results:
        rules = Producer.tupelize(result[2])
        value = result[0]
        try:
          cndval=result[3]["conditionVal"]
        except:
          self.logger.critical("Producer._addResults: conditionVal not set!" + os.linesep)
          cndval=None
        try:
          wght = result[3]["weightVal"]
        except:
          self.logger.critical("Producer._addResults: weightVal not set!" + os.linesep)
          wght=1
        #get/create stats for these rules
        if stats[rules]:
          counts=stats[rules][1]
        else:
          #create stats for these rules and set count to 1 for this key
          counts = {}
          stats[rules] = [0, counts, cndval!=None]
        if cndval is None:
          cvec=counts
        else:
          #print "_addResults: using cndval=",cndval
          if cndval not in counts:
            counts[cndval]={}
          cvec=counts[cndval]
        if value not in cvec:
          cvec[value]=0
        cvec[value]=cvec[value]+wght
    else:
      for result in results:
        rules = Producer.tupelize(result[2])
        value = result[0]
        if stats[rules]:
          #update information
          temp = stats[rules]
          if value < temp[0]:
            temp[0] = value
          elif value > temp[1]:
            temp[1] = value
          temp[2] += value
          temp[3] += value**2
          temp[4] += 1
        else:
          #initialize information
          stats[rules] = [value, value, value, value**2, 1]
  
  def _score(self, tables):
    """This function runs all of the data through a dummy model so that 
        we can collect stats for each segment."""
    
    #Build a brand new model based off of the input pmml
    if self.metadata is not None:
      _scoreStart = datetime.datetime.now()
   
    pmml_children = []
    header = self._pmml.getChildrenOfType(pmmlHeader)
    if header:
      pmml_children.append(header[0])
    data_dict = self._pmml.getChildrenOfType(pmmlDataDictionary)
    pmml_children.append(data_dict[0])
    trans_dict = self._pmml.getChildrenOfType(pmmlTransformationDictionary)
    if trans_dict:
      pmml_children.append(trans_dict[0])
    mining_schema = self._model.getChildrenOfType(pmmlMiningSchema)[0]
    
    #set up the pmml
    attrs = dict(self._attrs)
    teststat = attrs["testStatistic"]
    if teststat == "dDist":
      baseline = pmmlBaseline(children=[pmmlDiscreteDistribution(attributes={"sample":"0"})])
      self.impliedZero = None
    else:
      attrs["testStatistic"] = "zValue"
      attrs["testType"] = "threshold"
      attrs["threshold"] = "0"
      if "impliedZero" in attrs:
        self.impliedZero = int(attrs["impliedZero"])
        attrs.pop("impliedZero") # TEMPORARY -attrs used in testdistributions later.
        self._attrs.pop("impliedZero") # TEMPORARY op. cit.
      else:
        self.impliedZero = None
      baseline = pmmlBaseline(children=[pmmlGaussianDistribution(attributes={"mean":"0","variance":"1"})])
    extensions = []
    if self.__skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":self.__skip})]))
    
    if self._segments:
      #Build each segment
      segments = []
      for rule in self._segments:
        #Build up predicates for this segment
        seg_predicates = []
        for field, value in rule.iteritems():
          if isinstance(value, str):
            seg_predicates.append(pmmlSimplePredicate(attributes={"field":field,"operator":"equal","value":value}))
            #segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
          else:
            #TODO: Make sure that I got the boundaries right.
            seg_predicates.append(pmmlSimplePredicate(attributes={"field":field,"operator":"greaterThan","value":str(value[0])}))
            seg_predicates.append(pmmlSimplePredicate(attributes={"field":field,"operator":"lessOrEqual","value":str(value[1])}))
            #segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))
        if len(seg_predicates) == 1:
          #We can just use a simple predicate
          seg_predicate = seg_predicates[0]
        else:
          #We need to wrap things in a compound predicate
          seg_predicate = pmmlCompoundPredicate(children = seg_predicates, attributes={"booleanOperator":"and"})
        
        #Create the stub model for this segment
        children = list(extensions)
        children.append(baseline)
        test = pmmlTestDistributions(children=children, attributes=attrs)
        model_children = [copy.deepcopy(mining_schema)]
        if self._localTransformations:
          model_children.append(copy.deepcopy(self._localTransformations))
        model_children.append(test)
        test = pmmlBaselineModel(children=model_children, attributes={"functionName":"baseline"})
        
        #Create the segment element
        segments.append(pmmlSegment(children=[seg_predicate, test]))
      
      #Build a Mining model with the segments that we have
      segmentation = pmmlSegmentation(children=segments, attributes={"multipleModelMethod":"selectAll"})
      model = pmmlMiningModel(children=[copy.deepcopy(mining_schema), segmentation], attributes={"functionName":"baseline"})
    else:
      children = list(extensions)
      children.append(baseline)
      test = pmmlTestDistributions(children=children, attributes=attrs)
      model_children = [mining_schema]
      if self._localTransformations:
        model_children.append(self._localTransformations)
      model_children.append(test)
      model = pmmlBaselineModel(children=model_children, attributes={"functionName":"baseline"})
    if self.metadata is not None:
      self.metadata['Time Running _score'] += datetime.datetime.now() - _scoreStart
    pmml_children.append(model)
    self._pmml = pmmlPMML(children=pmml_children, attributes={"version":AUGUSTUS_CONSTS._PMML_VER})
    self._pmml.initialize(self.get, [])
    
    #actually score each event
    #the stats are indexed by segment and keep the following
    #  list of information about values in that segment:
    #  [low value, high value, sum, sum of squares, count]
    stats = dict(self._baseDict)
    for table in tables:
      if self.__timer and len(table) > 0:
        total = len(table)
        increment = self.__increment * total
        increment = int(math.ceil(increment))
        threshold = increment
        #acts as the shift for the % calculation later
        perc = float(100) / total
        cnt = 0
        last = 0
        if self.__batch:
          for row in table:
            self.row = row
            model.batchEvent()
            cnt += 1
            if cnt == threshold:
              num = "%.3f" % (cnt * perc)
              last = float(num)
              self.__timer.output("Events " + num + "% processed")
              threshold += increment
          results = model.batchScore()
          Producer._addResults(teststat, results, stats)
        else:
          for row in table:
            self.row = row
            results = model.score()
            cnt += 1
            if cnt == threshold:
              num = "%.3f" % (cnt * perc)
              last = float(num)
              self.__timer.output("Events " + num + "% processed")
              threshold += increment
            Producer._addResults(teststat, results, stats)
        if last < 100.000:
          self.__timer.output("Events 100.000% processed")
      else:
        if self.__batch:
          for row in table:
            self.row = row
            model.batchEvent()
          results = model.batchScore()
          Producer._addResults(teststat, results, stats)
        else:
          for row in table:
            self.row = row
            results = model.score()
            Producer._addResults(teststat, results, stats)
      #self._model.removeChildren(tests)
    self._pmml.removeChildren(self._pmml.getChildrenOfType(pmmlModels))
    return stats
  
  def _makeTable(self, element):
    """This function takes the data element and figures out what data to read and how to read it."""
    fileName = element.attr.get("file")
    fileType = element.attr.get("type")
    try:
      directory=element.attr.get("dir")
    except:
      directory=''
    try:
      header = element.attr.get("header")
    except:
      header = None
    try:
      sep = element.attr.get("sep")
    except:
      sep = None
    try:
      types = element.attr.get("types")
    except:
      types = None
    if fileType == "UniTable":
      return [uniTableWrapper(uni.UniTable().from_any_file(fileName))]
    elif fileType == "CSV":
      _args={} #Holder for optional keword arguments
      if header:
        #Add header option
        _args['header'] = header
      if sep:
        #Add insep option
        _args['insep'] = sep
      if types:
        #Add types option
        _args['types'] = types
      if directory is not None and os.path.exists(directory):
        filelist=[os.path.join(directory,f) for f in os.listdir(directory)]
        for f in filelist:
          if os.path.isdir(f):
            filelist.remove(f)
        return [uniTableWrapper(uni.UniTable().from_csv_file(files,**_args)) for files in filelist]
      else:
        return [uniTableWrapper(uni.UniTable().from_csv_file(fileName,**_args))]
    elif fileType == "XML":
      return xmlFile(fileName)
    elif fileType == "XMLEvents":
      magicheader = False
      for child in element:
        if child.name == "MagicHeader":
          magicheader = True
          break
      return xmlTable(fileName, magicheader)
    else:
      raise StandardError, "File type not recognized"
  
  def getStats(self):
    """This function collects all the stats that we need to build a model."""
    if self.__timer:
      self.__timer.output("Collecting stats for baseline distribution")
    #produce statistics for the baseline distribution
    startReadTime = datetime.datetime.now()
    baselinedata = self._makeTable(self._baseline)
    if self.metadata is not None:
      self.metadata['Time Reading Data'] += datetime.datetime.now() - startReadTime
    self._stats = self._score(baselinedata)
    #produce statistics for the alternate distribution
    self.__cur = -time.time()
    if self._alternate:
      if self.__timer:
        temp = time.time()
        sec = temp + self.__cur
        self.__cur = -time.time()
        self.logger.info("(%.3f sec) Collecting stats for alternate distribution%s" % (sec,os.linesep))
      child = self._alternate[0]
      if child.name== "shift":
        self._altstats = {}
        mult = float(child.get("mult"))
        for entry in self._stats:
          temp = self._stats[entry]
          if temp:
            stddev = math.sqrt(float(temp[3])/temp[4]-(float(temp[2])/temp[4])**2)
            shift = mult * stddev
            self._altstats[entry] = [temp[0], temp[1], temp[2]+temp[4]*shift, temp[3]+2*shift*temp[2]+temp[4]*shift*shift, temp[4]]
      elif child.name == "input":
        startReadTime = datetime.datetime.now()
        altdata = self._makeTable(child)
        if self.metadata is not None:
          self.metadata['Time Reading Data'] += datetime.datetime.now() - startReadTime
        self._altstats = self._score(altdata)
      else:
        raise StandardError, "Alternate distribution definition not recognized"

  @staticmethod
  def makeDistribution(distribution, stats, fixedLength, testvalidation = None, zeroVarianceOptions = None):
    """"""
    if distribution=='gaussian':
      # These checks are valid for statistics relevant to gaussiand dists.
      if not testvalidation(stats[4]):
        return None
      if fixedLength is not None:
        stats[4] = fixedLength        
    if distribution == "gaussian":
      variance = float(stats[3])/stats[4]-(float(stats[2])/stats[4])**2
      if variance < 0:
        self.logger.critical('A negative variance has been calculated. ' + \
            os.linesep + \
            ' A common cause is if using a fixed time length of baseline ' + \
            ' period which is shorter than the true baseline time period.' + \
            ' You may want to check your config file.' + os.linesep)
        raise(Exception('Negative Variance Calculated!'))
      if zeroVarianceOptions is not None:
        zeroVarianceOption = zeroVarianceOptions['method']
      else:
        zeroVarianceOption = "Exception"
      if variance == 0:
        if zeroVarianceOption == "InterpolateZeroVarianceEstimate":
          # The minimum resolution is interpreted as a region
          # outside of which no values were observed. This fixes a
          # pvalue and hence a variance. The 'resolution' parameter
          # here is set assuming an 'integer'.
          variance = sigmaInterpolate(stats[4], zeroVarianceOptions['resolution'])          
        elif zeroVarianceOption == "VarianceDefault":
          variance = zeroVarianceOptions['defaultvalue']
        elif zeroVarianceOption == "Exception":
          raise(Exception('Variance for this model or segment is calculated to be exactly 0. See documentation for alternate responses to this situation.'))
        elif zeroVarianceOption == "Quiet":
          return None
        else:
          self.logger.critical('Cannot understand zero variance option %s%s'%(zeroVarianceOption,os.linesep))
          raise(Exception('Cannot understand zero variance option %s'%zeroVarianceOption))
      return pmmlGaussianDistribution(attributes={"mean":str(float(stats[2])/stats[4]),"variance":str(variance)})
    elif distribution == "poisson":
      return pmmlPoissonDistribution(attributes={"mean":str(float(stats[2])/stats[4])})
    elif distribution == "exponential":
      return pmmlExponentialDistribution(attributes={"mean":str(float(stats[2])/stats[4])})
    elif distribution == "uniform":
      return pmmlUniformDistribution(attributes={"lower":str(stats[0]),"upper":str(stats[1])})
    elif distribution == "discrete":
      # if distributions are conditional, normalize and average
      if stats[2]:
        modeldist={}
        ncf=0
        self.logger.debug("conditional distribution keys=%s%s"%(stats[1].keys(),os.linesep))
        for cndval in stats[1].keys():
          ncf+=1
          sample=0
          for (val,cnt) in stats[1][cndval].items():
             sample += cnt
          self.logger.debug("condition val=%s sample=%s%s"%(cndval,sample,os.linesep))
          for (val,cnt) in stats[1][cndval].items():
             if modeldist.has_key(val):
               modeldist[val] += cnt/sample
             else:
               modeldist[val] = cnt/sample
        for (val,cnt) in modeldist.items(): 
           modeldist[val]/=ncf
      else:
        modeldist=stats[1]
      sample=0
      ddcounts=[]
      for (val,cnt) in modeldist.items(): 
        sample += cnt
        ddcounts.append(pmmlValueCount(attributes={"value":str(val),"count":str(cnt)}))
      if not testvalidation(sample):
        return None
      else:
        return pmmlDiscreteDistribution(attributes={"sample":str(sample)},children=ddcounts)
    else:
      raise StandardError, "Given distribution type was not recognized:  " + distribution
  
  @staticmethod
  def makeSegments(aTuple):
    """Static method that creates a list of the appropriate pmml objects given a tupelized segment."""
    segments = []
    for field, value in aTuple:
      if isinstance(value, str):
        segments.append(pmmlSimplePredicate(attributes={"field":field,"operator":"equal","value":value}))
        #segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
      else:
        segments.append(pmmlSimplePredicate(attributes={"field":field,"operator":"greaterThan","value":str(value[0])}))
        segments.append(pmmlSimplePredicate(attributes={"field":field,"operator":"lessOrEqual","value":str(value[1])}))
        #segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))
    if len(segments) == 1:
      #We can just use a simple predicate
      segment = segments[0]
    else:
      #We need to wrap things in a compound predicate
      segment = pmmlCompoundPredicate(children = segments, attributes={"booleanOperator":"and"})
    return segment
  
  def makeTests(self):
    """"""
    if self.__timer:
      self.__timer.output("Making test distributions from statistics")
    outFields = []
    outValues = []
    outMeans = []
    outStdDevs = []
    
    #extensions
    extensions = []
    if self.__skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":str(self.__skip)})]))
    
    #create a test for each segment
    segments = []
    keys = self._stats.keys()
    keys.sort()
    if self._alternate:
      #include alternate distributions
      baseDist = self._baseline.attr.get("dist")
      altDist = self._alternate.attr.get("dist")
      for entry in keys:
        if self._stats[entry] and self._altstats[entry]:
          child = Producer.makeDistribution(baseDist, self._stats[entry], self.impliedZero, self.testValidation, self.zeroVarianceOptions)
          if child:
            baseline = pmmlBaseline(children=[child])
            temp = Producer.makeDistribution(altDist, self._altstats[entry], self.impliedZero, self.testValidation, self.zeroVarianceOptions)
            if temp:
              alt = pmmlAlternate(children=[temp])
              
              children = list(extensions)
              children.extend([baseline,alt])
              test = pmmlTestDistributions(children=children,attributes=self._attrs)
              mining_schema = self._model.getChildrenOfType(pmmlMiningSchema)[0]
              model_children = [mining_schema]
              if self._localTransformations:
                model_children.append(self._localTransformations)
              model_children.append(test)
              
              model = pmmlBaselineModel(children = model_children, attributes = {"functionName":"baseline"})
              
              if entry:
                predicate = Producer.makeSegments(entry)
                segments.append(pmmlSegment(children=[predicate, model]))
          else:
            self.logger.info('Segment failed validation or has Zero Variance' + os.linesep)
              
    else:
      #do not include alternate distributions      
      baseDist = self._baseline.attr.get("dist")
      for entry in keys:
        if self._stats[entry]:  
          child = Producer.makeDistribution(baseDist, self._stats[entry], self.impliedZero, self.testValidation, self.zeroVarianceOptions)
          if child:
            baseline = pmmlBaseline(children=[child])
            children = list(extensions)
            children.extend([baseline])
            test = pmmlTestDistributions(children=children,attributes=self._attrs)
            mining_schema = self._model.getChildrenOfType(pmmlMiningSchema)[0]
            model_children = [mining_schema]
            if self._localTransformations:
              model_children.append(self._localTransformations)
            model_children.append(test)
            
            model = pmmlBaselineModel(children = model_children, attributes = {"functionName":"baseline"})
            
            if entry:
              predicate = Producer.makeSegments(entry)
              segments.append(pmmlSegment(children=[predicate, model]))
            
            if (self.metadata is not None) and self.__debugFile:
              if entry:
                outFields.append(entry[0][0])
                outValues.append(entry[0][1])
              stats = self._stats[entry]
              outMeans.append(float(stats[2])/stats[4])
              outStdDevs.append(math.sqrt(max((float(stats[3])/stats[4]-(float(stats[2])/stats[4])**2),0)))
          else:
            self.logger.info('Segment failed validation or has Zero Variance' + os.linesep)
        else:
          if len(entry)>0:
            self.logger.debug('No statistics for this segment %s'%','.join([':'.join(e) for e in entry]))
            if self.metadata is not None:
              self.metadata['Unpopulated Segments'].append(','.join([':'.join(e) for e in entry]))
          else:
            # 'Empty segment', used in unsegmented models.
            self.logger.debug('The Empty Segment' + os.linesep)
    if segments:
      if self.metadata is not None:
        self.metadata['Number of Segments Filled'] = len(segments)
      #Make a mining model
      segmentation = pmmlSegmentation(children=segments, attributes={"multipleModelMethod":"selectAll"})
      model = pmmlMiningModel(children=[mining_schema, segmentation], attributes={"functionName":"baseline"})
    self._pmml.addChild(model)
    
    if (self.metadata is not None) and self.__debugFile:
      out = uni.UniTable(["field", "value", "mean", "stddev"])
      out["field"] = outFields
      out["value"] = outValues
      out["mean"] = outMeans
      out["stddev"] = outStdDevs
      out.to_nab_file(str(self.__debugFile))
  
  def outputPMML(self):
    """Output the produced the pmml."""
    if self.__timer:
      self.__timer.output("Outputting PMML")
    #output the model with the segments that have been produced
    out = open(self.__output,"w")
    out.write(str(self._pmml))
    out.close()
  
  def stop(self):
    """Stops timing if it is going."""
    if self.__timer:
      del self.__timer

def main(config=None, timing=None, wrapping=True):
  """"""
  # Configure *root* logger.
  logging.basicConfig(level=logging.DEBUG)

  #gc.disable()
    
  #define the options
  AUGUSTUS_CONSTS.check_python_version()
  usage = "usage: %prog [options]"
  version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
  options = [
    make_option("-c","--config",default="config.xml",help="The configuration file name"),
    make_option("-t","--timing",default=None,help="Output timing (information in % increments for scoring)")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  if not config:
    (options, arguments) = parser.parse_args()
    config = options.config
    timing = options.timing
  
  #make a producer
  mine = Producer(timing, wrapping)

  if not gc.isenabled():
    mine.logger.info("Python Garbage Collection of circular references is disabled.")

  producerStart = datetime.datetime.now()
  #input the configurations
  config_reader = anyReader.Reader(mine.inputConfigs, source = config, magicheader = False, autoattr = False)
  mine.logger.debug("Read Config File")
  config_reader.read_once()

  #mine.inputConfigs(config)
  #input the PMML
  mine.inputPMML()
  #create the statistics
  producerStartDataProcessing = datetime.datetime.now()
  mine.getStats()
  if mine.metadata is not None:
    mine.metadata['Time Processing Baseline Data'] += datetime.datetime.now() - producerStartDataProcessing
  #make the TestDistributions elements
  producerStartmakingPMMLTests = datetime.datetime.now()
  mine.makeTests()
  if mine.metadata is not None:
    mine.metadata['Time Creating PMML Tests'] += datetime.datetime.now() - producerStartmakingPMMLTests
  #output the PMML
  producerStartModelOutput = datetime.datetime.now()
  mine.outputPMML()
  if mine.metadata is not None:
    mine.metadata['Time Outputting Model'] += datetime.datetime.now() - producerStartModelOutput
  #stop timing if it is going
  mine.stop()
  if mine.metadata:
    mine.metadata['Total Producer Time'] += datetime.datetime.now() - producerStart
    #mine.metadata.collected['DataInput'] = data_input.getMetaData()
    mine.metadata.collected['Producing'] = mine.metadata.getMetaData()
    #mine.metadata.collected[''] = mine.model.getMetaData()
    mine.metadata.report()

if __name__ == "__main__":
  main()
