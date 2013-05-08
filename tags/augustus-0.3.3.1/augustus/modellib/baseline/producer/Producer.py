#!/usr/bin/env python2.5

""""""


__copyright__ = """
Copyright (C) 2006-2007  Open Data ("Open Data" refers to
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

class Producer:
  """"""
  def __init__(self, timing=None, wrapping=True):
    """"""
    self.__timer = None
    if not timing is None:
      self.__increment = float(timing) / 100
      if wrapping:
        self.__timer = timer.timer("Producer timer begins", "Producer timer ends", "Total producer time")
      else:
        self.__timer = timer.timer()
  def _makeSegments(self, declarations):
    """"""
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
          segments.append({field:(low, low + step)})
          low += step
        last = segments[len(segments) - 1]
        last[field] = (last[field][0], high)
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
              newRule[field] = (low, low + step)
              newSegments.append(newRule)
              low += step
            last = newSegments[len(newSegments) - 1]
            last[field] = (last[field][0], high)
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
    """"""
    keys = rules.keys()
    keys.sort()
    temp = [(key,rules[key]) for key in keys]
    return tuple(temp)
  
  def inputConfigs(self, file):
    """"""
    if self.__timer:
      self.__timer.output("Inputting configurations")
    #input basic configurations
    tree = ET.parse(file)
    root = tree.getroot()
    self.__mode = root.get("mode")
    self.__input = root.get("input")
    self.__output = root.get("output")
    self.__batch = root.find("batch")
    if not self.__batch is None:
      self.__batch = True
    else:
      self.__batch = False
    self.__debugFile = root.find("debug")
    if not self.__debugFile is None:
      self.__debugFile = self.__debugFile.get("file")
    self.__skip = root.find("skip")
    if not self.__skip is None:
      self.__skip = long(self.__skip.get("number"))
    test = root.find("test")
    #input baseline and alternate distributions
    self._baseline = test[0]
    validation = root.find("validation")
    if (validation is not None):
      #Decide on method of validating tests.
      validmethod = validation.get('method')
      validthreshold = validation.get('threshold')
      self.testValidation = Producer.testValidatingFunctions(validmethod,validthreshold)
    else:
      self.testValidation = Producer.testValidatingFunctions(None)
    start = 1
    self._alternate = None
    if len(test) > 1:
      next = test[1]
      start = 2
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
    """"""
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

    seg={}
    if (self.__mode=='Update'):
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
  def _addResults(teststat, results, stats):
    """"""
    if teststat == "dDist":
      for result in results:
        rules = Producer.tupelize(result[2])
        value = result[0]
        try:
          cndval=result[3]["conditionVal"]
        except:
          print "Producer._addResults: conditionVal not set!"
          cndval=None
        try:
          wght = result[3]["weightVal"]
        except:
          print "Producer._addResults: weightVal not set!"
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
    """"""
    #set up the pmml
    attrs = dict(self._attrs)
    teststat = attrs["testStatistic"]
    if teststat == "dDist":
      baseline = pmmlBaseline(children=[pmmlDiscreteDistribution(attributes={"sample":"0"})])
    else:
      attrs["testStatistic"] = "zValue"
      attrs["testType"] = "threshold"
      attrs["threshold"] = "0"
      baseline = pmmlBaseline(children=[pmmlGaussianDistribution(attributes={"mean":"0","variance":"1"})])
    extensions = []
    if self.__skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":self.__skip})]))
    if self._segments:
      tests = []
      testSegment={}
      for rule in self._segments:
        segments = []
        for field in rule:
          value = rule[field]
          testSegment[field]=value
          if isinstance(value, str):
            segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
          else:
            segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))
        if (self.__mode!='Update'):
          children = list(extensions)
          children.append(baseline)
          children.append(pmmlSegments(children=segments))
          tests.append(pmmlTestDistributions(children=children,attributes=attrs))
    else:
      children = list(extensions)
      children.append(baseline)
      tests = [pmmlTestDistributions(children=children, attributes=attrs)]
    self._model.addChildren(tests)
    self._pmml.initialize(self.get, [], self.__batch)
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
            self._model.batchEvent()
            cnt += 1
            if cnt == threshold:
              num = "%.3f" % (cnt * perc)
              last = float(num)
              self.__timer.output("Events " + num + "% processed")
              threshold += increment
          results = self._model.batchScore()
          Producer._addResults(teststat, results, stats)
        else:
          for row in table:
            self.row = row
            results = self._model.score()
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
            self._model.batchEvent()
          results = self._model.batchScore()
          Producer._addResults(teststat, results, stats)
        else:
          for row in table:
            self.row = row
            results = self._model.score()
            Producer._addResults(teststat, results, stats)
      self._model.removeChildren(tests)
    return stats
    
  def _makeTable(self, element):
    """"""
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
    """"""
    if self.__timer:
      self.__timer.output("Collecting stats for baseline distribution")
    #produce statistics for the baseline distribution
    self._stats = self._score(self._makeTable(self._baseline))
    #produce statistics for the alternate distribution
    self.__cur = -time.time()
    if self._alternate:
      if self.__timer:
        temp = time.time()
        sec = temp + self.__cur
        self.__cur = -time.time()
        print "(%.3f sec) Collecting stats for alternate distribution" % sec
      child = self._alternate[0]
      if child.tag == "shift":
        self._altstats = {}
        mult = float(child.get("mult"))
        for entry in self._stats:
          temp = self._stats[entry]
          if temp:
            stddev = math.sqrt(float(temp[3])/temp[4]-(float(temp[2])/temp[4])**2)
            shift = mult * stddev
            self._altstats[entry] = (temp[0], temp[1], temp[2]+temp[4]*shift, temp[3]+2*shift*temp[2]+temp[4]*shift*shift, temp[4])
      elif child.tag == "input":
        self._altstats = self._score(self._makeTable(child))
      else:
        raise StandardError, "Alternate distribution definition not recognized"

  @staticmethod
  def makeDistribution(distribution, stats, testvalidation = None):
    """"""
    if distribution == "gaussian":
      variance = float(stats[3])/stats[4]-(float(stats[2])/stats[4])**2
      if variance == 0:
        return None
      return pmmlGaussianDistribution(attributes={"mean":str(float(stats[2])/stats[4]),"variance":str(variance)})
    elif distribution == "poisson":
      return pmmlPoissonDistribution(attributes={"mean":str(float(stats[2])/stats[4])})
    elif distribution == "uniform":
      return pmmlUniformDistribution(attributes={"lower":str(stats[0]),"upper":str(stats[1])})
    elif distribution == "discrete":
      # if distributions are conditional, normalize and average
      if stats[2]:
        modeldist={}
        ncf=0
        #print "conditional distribution keys=",stats[1].keys()
        for cndval in stats[1].keys():
          ncf+=1
          sample=0
          for (val,cnt) in stats[1][cndval].items():
             sample += cnt
          #print "condition val=",cndval," sample=",sample
          for (val,cnt) in stats[1][cndval].items():
             if modeldist.has_key(val):
               modeldist[val] += cnt/sample
             else:
               modeldist[val] = cnt/sample
        for (val,cnt) in modeldist.items(): 
           modeldist[val]/=ncf
        #print "average model=",modeldist
      else:
        modeldist=stats[1]
      sample=0
      ddcounts=[]
      for (val,cnt) in modeldist.items(): 
        sample+=cnt
        ddcounts.append(pmmlValueCount(attributes={"value":str(val),"count":str(cnt)}))
      if not testvalidation(sample):
        return None
      else:
        return pmmlDiscreteDistribution(attributes={"sample":str(sample)},children=ddcounts)
    else:
      raise StandardError, "Given distribution type was not recognized:  " + distribution
  
  @staticmethod
  def makeSegments(aTuple):
    """"""
    segments = []
    for pair in aTuple:
      field = pair[0]
      value = pair[1]
      if isinstance(value, str):
        segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
      else:
        segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))
    return segments
  
  def makeTests(self):
    """"""
    if self.__timer:
      self.__timer.output("Making test distributions from statistics")
    #TEMPORARY
    outFields = []
    outValues = []
    outMeans = []
    outStdDevs = []
    #extensions
    extensions = []
    if self.__skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":str(self.__skip)})]))
    #create a test for each segment
    tests = []
    keys = self._stats.keys()
    keys.sort()
    if self._alternate:
      #include alternate distributions
      baseDist = self._baseline.get("dist")
      altDist = self._alternate.get("dist")
      for entry in keys:
        if self._stats[entry] and self._altstats[entry]:
          child = Producer.makeDistribution(baseDist, self._stats[entry])
          if child:
            baseline = pmmlBaseline(children=[child])
            temp = Producer.makeDistribution(altDist, self._altstats[entry])
            if temp:
              alt = pmmlAlternate(children=[temp])
              segments = Producer.makeSegments(entry)
              segments = pmmlSegments(children=segments)
              children = list(extensions)
              children.extend([baseline,alt,segments])
              tests.append(pmmlTestDistributions(children=children,attributes=self._attrs))
    else:
      #do not include alternate distributions
      baseDist = self._baseline.get("dist")
      for entry in keys:
        if self._stats[entry]:
          child = Producer.makeDistribution(baseDist, self._stats[entry], self.testValidation)
          if child:
            baseline = pmmlBaseline(children=[child])
            segments = Producer.makeSegments(entry)
            segments = pmmlSegments(children=segments)
            children = list(extensions)
            children.extend([baseline,segments])
            tests.append(pmmlTestDistributions(children=children,attributes=self._attrs))
            #TEMPORARY
            if self.__debugFile:
              if entry:
                outFields.append(entry[0][0])
                outValues.append(entry[0][1])
              stats = self._stats[entry]
              outMeans.append(float(stats[2])/stats[4])
              outStdDevs.append(math.sqrt(max((float(stats[3])/stats[4]-(float(stats[2])/stats[4])**2),0)))
    #put the tests in the current model
    originals=self._model.getChildrenOfType(pmmlTestDistributions)
    if (self.__mode=='Update'):
      self._model.removeChildren(originals)
      self._model.addChildren(tests)
    else:
      self._model.addChildren(tests)
    #TEMPORARY
    if self.__debugFile:
      out = uni.UniTable(["field", "value", "mean", "stddev"])
      out["field"] = outFields
      out["value"] = outValues
      out["mean"] = outMeans
      out["stddev"] = outStdDevs
      out.to_nab_file(str(self.__debugFile))
  
  def outputPMML(self):
    """"""
    if self.__timer:
      self.__timer.output("Outputting PMML")
    #output the model with the segments that have been produced
    out = open(self.__output,"w")
    out.write(str(self._pmml))
    out.close()
  
  def stop(self):
    """"""
    if self.__timer:
      del self.__timer

def main(config=None, timing=None, wrapping=True):
  """"""
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog 0.2.6"
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
