#!/usr/bin/env python

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
#Needed to deep copy mining schemas and local transformations
import copy
import augustus.const as AUGUSTUS_CONSTS


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
    logger = logging.getLogger()
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
    if not timing is None:
      self.__increment = float(timing) / 100
      if wrapping:
        self.__timer = timer.timer("Producer timer begins", "Producer timer ends", "Total producer time")
      else:
        self.__timer = timer.timer()
  
  def _makeSegments(self, declarations):
    """TODO: Refactor this. (Well, maybe sometime later.)"""
    #produces segments from their declarations
    #make the first segments
    segments = []
    current = declarations[0]
    field = str(current.get("field"))
    if current.tag == "explicitSegments":
      #produce a segment for each value declared here
      for value in current:
        segments.append({field:str(value.get("value"))})
    elif current.tag == "regularSegments":
      #produce segments for each partition declared here
      for partition in current:
        low = float(partition.get("low"))
        high = float(partition.get("high"))
        step = (high - low) / float(partition.get("divisions"))
        while low + step <= high:
          segments.append({field:(str(low), str(low + step))})
          low += step
        last = segments[len(segments) - 1]
        last[field] = (last[field][0], str(high))
    else:
      raise StandardError, "Element not recognized as a segment declaration:  " + current.tag
    #make the rest of the segments
    for current in declarations[1:]:
      field = str(current.get("field"))
      if current.tag == "explicitSegments":
        newSegments = []
        #for each of the previous segments
        for rule in segments:
          #produce a new segment for each value declared here
          for value in current:
            newRule = dict(rule)
            try:
              newRule[field] = str(value.get("value"))
            except:
              raise StandardError, "Explicit segment malformed:  a 'value' attribute is required"
            newSegments.append(newRule)
        #save the new set of segments
        segments = newSegments
      elif current.tag == "regularSegments":
        newSegments = []
        #for each of the previous segments
        for rule in segments:
          #produce segments for each partition declared here
          for partition in current:
            try:
              low = float(partition.get("low"))
              high = float(partition.get("high"))
              divisions = float(partition.get("divisions"))
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
        raise StandardError, "Element not recognized as a segment declaration:  " + current.tag
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
    """TODO: Very much refactor this."""
    if self.__timer:
      self.__timer.output("Inputting configurations")
    #input generic configurations
    tree = ET.parse(file)
    root = tree.getroot()
    self.__mode = root.get("mode")
    self.__input = root.get("input")
    self.__output = root.get("output")
    self.__batch = root.find("batch")
    if self.__batch is not None:
      self.__batch = True
    else:
      self.__batch = False
    self.__debugFile = root.find("debug")
    if self.__debugFile is not None:
      self.__debugFile = self.__debugFile.get("file")
    self.__skip = root.find("skip")
    if self.__skip is not None:
      self.__skip = long(self.__skip.get("number"))
    test = root.find("test")
    
    #input baseline and alternate distributions
    self._build = test[0]
    validation = root.find("validation")
    if validation is not None:
      #Decide on method of validating tests.
      validmethod = validation.get('method')
      validthreshold = validation.get('threshold')
      self.testValidation = Producer.testValidatingFunctions(validmethod,validthreshold)
    else:
      self.testValidation = Producer.testValidatingFunctions(None)
    start = 1
    if len(test) > 1:
      next = test[1]
      start = 2
      #TODO: Naive Bayes shouldn't have alternate.
      if next.tag != "alternate":
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
    for key in test.keys():
      self._attrs[str(key)] = str(test.get(key))

  def inputPMML(self):
    """This function reads in a pmml file and saves the things we are going to be reusing when we collect stats."""
    if self.__timer:
      self.__timer.output("Inputting model")
    #input the portion of the model that is completed
    reader = pmmlReader()
    reader.parse(str(self.__input))
    self._pmml = reader.root
    self._model = self._pmml.getChildrenOfType(pmmlNaiveBayesModel)[0]
    #self._existingTests = self._model.getChildrenOfType(pmmlNaiveBayes)
    self._inputs = self._model.getChildrenOfType(pmmlBayesInputs)[0]
    self._output = self._model.getChildrenOfType(pmmlBayesOutput)[0]
    self._localTransformations = self._model.getChildrenOfType(pmmlLocalTransformations)
    if len(self._localTransformations) > 0:
      self._localTransformations = self._localTransformations[0]
    else:
      self._localTransformations = None

    #It looks like this will only work for explicit segments
    """
    seg={}
    if (self.__mode=='Update'):
      for t in self._existingTests:
        for e in t.getChildrenOfType(pmmlSegments)[0].getChildrenOfType(pmmlExplicitSegment):
          seg[e.getAttribute('field')]=e.getAttribute('value')
        segment=Producer.tupelize(seg)
        if segment in self._baseDict.keys():
          testdistributions=t.getChildrenOfType(pmmlNaiveBayes)[0].getChildrenOfType(pmmlNaiveBayesInputs)
          try:
            test=testdistributions[0]
            test.makeAttribute("sample","0")
            vctable={}
            for vc in test.getChildrenOfType(pmmlValueCount):
              vctable[vc.getAttribute('value')]=float(vc.getAttribute('count'))
            self._baseDict[segment]=[0,vctable,False]
          except:
            pass
    """


  def get(self, field):
    """"""
    return self.row[field]

  @staticmethod
  def _addResults(teststat, results, stats):
    """"""
    for result in results:
      keys = result[-1]
      rules = Producer.tupelize(result[2])
      if stats[rules]:
        for k in keys:
          try:
            stats[rules][k] += 1
          except:
            stats[rules][k]  = 1
      else:
        #initialize information
        stats[rules] = {}
        for k in keys:
          try:
            stats[rules][k]  = 1
          except:
            print teststat
            print results
            print stats
            print '---------'
            print rules
            print stats[rules]
            print k
            #x=input()
  
  def _score(self, tables):
    """This function runs all of the data through a dummy model so that we can collect stats for each segment."""
    
    #Build a brand new model based off of the input pmml
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
    teststat=None 
    
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
        model_children = [copy.deepcopy(mining_schema)]
        if self._localTransformations:
          children.append(copy.deepcopy(self._localTransformations))
        model_children.extend([self._inputs, self._output])
        model = pmmlNaiveBayesModel(children=model_children, attributes={"functionName":"classification", "threshold":self._model.getAttribute("threshold") })
        
        #Create the segment element
        segments.append(pmmlSegment(children=[seg_predicate, model]))
      
      #Build a Mining model with the segments that we have
      segmentation = pmmlSegmentation(children=segments, attributes={"multipleModelMethod":"selectAll"})
      model = pmmlMiningModel(children=[copy.deepcopy(mining_schema), segmentation], attributes={"functionName":"classification"})
    else:
      model_children = [mining_schema]
      if self._localTransformations:
        model_children.append(self._localTransformations)
      model_children.extend([self._inputs, self._output])
      model = pmmlNaiveBayesModel(children=model_children, attributes={"functionName":"classification", "threshold":self._model.getAttribute("threshold")})
    
    pmml_children.append(model)
    self._pmml = pmmlPMML(children=pmml_children, attributes={"version":"4.0"})
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
            try:
              result.append(result[3]["scoredKeys"])
            except IndexError:
              #Something went wrong, this will match augustus v0.3 results
              results.append({})
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
            for result in results:
              try:
                result.append(result[3]["scoredKeys"])
              except IndexError:
                #Something went wrong, this will match augustus v0.3 results
                results.append({})
            Producer._addResults(teststat, results, stats)
    return stats
  
  def _makeTable(self, element):
    """This function takes the data element and figures out what data to read and how to read it."""
    fileName = element.get("file")
    fileType = element.get("type")
    try:
      directory=element.get("dir")
    except:
      directory=''
    try:
      header = element.get("header")
    except:
      header = None
    try:
      sep = element.get("sep")
    except:
      sep = None
    try:
      types = element.get("types")
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
      if (directory is not None and os.path.exists(directory)):
        filelist=[os.path.join(directory,f) for f in os.listdir(directory)]
        for f in filelist:
          if (os.path.isdir(f)):
            filelist.remove(f)
        return [uniTableWrapper(uni.UniTable().from_csv_file(files,**_args)) for files in filelist]
      else:
        return [uniTableWrapper(uni.UniTable().from_csv_file(fileName,**_args))]
    elif fileType == "XML":
      return xmlFile(fileName)
    elif fileType == "XMLEvents":
      magicheader = False
      for child in element:
        if child.tag == "MagicHeader":
          magicheader = True
          break
      return xmlTable(fileName, magicheader)
    else:
      raise StandardError, "File type not recognized"
  
  def getStats(self):
    """This function collects all the stats that we need to build a model."""
    if self.__timer:
      self.__timer.output("Collecting statistics for model")
    #produce statistics for the baseline distribution
    self._stats = self._score(self._makeTable(self._build))
    #produce statistics for the alternate distribution
    self.__cur = -time.time()

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
  
  @staticmethod
  def updateCounts(model, counts):
    ins = model.getChildrenOfType(pmmlBayesInputs)[0]
    new_ins = []
    for bI in ins.getChildrenOfType(pmmlBayesInput):
      f = bI.getAttribute('fieldName')
      new_pc = []
      for pair in bI.getChildrenOfType(pmmlPairCounts):
        v = pair.getAttribute('value')
        new_tv = []
        for tv in (pair.getChildrenOfType(pmmlTargetValueCounts)[0]).getChildrenOfType(pmmlTargetValueCount):
          count = counts.get((f, v, tv.getAttribute('value')), 0)
          #tv.makeAttribute('count', counts.get((f, v, tv.getAttribute('value')), 0))
          new_tv.append(pmmlTargetValueCount(attributes = {'value':tv.getAttribute('value'),'count':str(count)}))
        new_tvc = pmmlTargetValueCounts(children = new_tv)
        new_pc.append(pmmlPairCounts(attributes = pair.getAttributes(), children = [new_tvc]))
      new_ins.append(pmmlBayesInput(attributes={'fieldName':f}, children = new_pc))
    newIn = pmmlBayesInputs(children = new_ins)
    model.replaceChild(ins, newIn)
    
    out = model.getChildrenOfType(pmmlBayesOutput)[0]
    newOut = pmmlBayesOutput(attributes={'fieldName':out.getAttribute('fieldName')}, children = out.getChildren())
    for o in newOut.getChildrenOfType(pmmlTargetValueCounts):
      newcounts=[]
      for t in o.getChildrenOfType(pmmlTargetValueCount):
        newcount = str(counts[(None, None, t.getAttribute('value'))])
        newcounts.append(pmmlTargetValueCount(attributes={'count':newcount,'value':t.getAttribute('value')}))
      newOut.replaceChild(o,pmmlTargetValueCounts(children=newcounts))
    model.replaceChild(out, newOut)
  
  def makeTests(self):
    """"""
    if self.__timer:
      self.__timer.output("Making test distributions from statistics")
    
    toRemove = [] # template(s)
    # not used yet, but anticipating future update procedure for models.
    tests = []
    originals = []
    #create a test for each segment
    
    #Note that unlike other models where we don't have a good way of
    # indicating that no data reached a segment, a naive bayes model
    # with all counts of zero will do this and will still prodouce
    # valid scores when the model is consumed.
    
    model = self._pmml.getChildrenOfType(pmmlMiningModel)
    if not model:
      #Not a segmented model
      model = self._pmml.getChildrenOfType(pmmlNaiveBayesModel)[0]
      key = ()
      counts = self._stats[key]
      Producer.updateCounts(model, counts)
    else:
      #Segmented model
      model = model[0]
      segmentation = model.getChildrenOfType(pmmlSegmentation)[0]
      segments = segmentation.getChildrenOfType(pmmlSegment)
      
      for segment in segments:
        key = tuple(segment.getSegment())
        if key in self._stats:
          counts = self._stats[key]
          seg = segment.getChildrenOfType(pmmlNaiveBayesModel)[0]
          Producer.updateCounts(seg, counts)
    
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
  #input the configurations
  mine.inputConfigs(config)
  #input the PMML
  mine.inputPMML()
  #create the statistics
  mine.getStats()
  #make the TestDistributions elements
  mine.makeTests()
  #output the PMML
  mine.outputPMML()
  #stop timing if it is going
  mine.stop()

if __name__ == "__main__":
  main()
