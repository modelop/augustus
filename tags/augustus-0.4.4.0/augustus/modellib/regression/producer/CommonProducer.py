

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
#Use unitable to manage growth of matrix data
from augustus.kernel.unitable import *

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
from numpy import *

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

class CommonProducer:
  def __init__(self, timing=None, wrapping=True):
    """"""
    self._timer = None
    if not timing is None:
      self.__increment = float(timing) / 100
      if wrapping:
        self._timer = timer.timer("Producer timer begins", "Producer timer ends", "Total producer time")
      else:
        self._timer = timer.timer()

  def _makeSegments(self, declarations):
    """
      Produces a private list of segments from sub-elements 
      of a <segmentation> element, including information
      needed to resolve them. Note that *all possible* pairings
      of segments are included.
    """
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
    """"""
    keys = rules.keys()
    keys.sort()
    temp = [(key,rules[key]) for key in keys]
    return tuple(temp)


  def inputConfigs(self, file):
    if self._timer:
      self._timer.output("Inputting configurations")
    #input basic configurations
    tree = ET.parse(file)
    root = tree.getroot()    
    self.__mode = root.get("mode")
    self.__input = root.get("input")
    self._output = root.get("output")
    self._batch = True
    self.__debugFile = root.find("debug")
    if not self.__debugFile is None:
      self.__debugFile = self.__debugFile.get("file")
    self._skip = root.find("skip")
    if not self._skip is None:
      self._skip = long(self._skip.get("number"))
    test = root.find("test")

    #First sub-element is the 'build' element, specifying data.
    self._build = test[0]

    #produce segmentation
    start = 1
    self._segments = []
    for segmentDeclarations in test[start:]:
      self._makeSegments(segmentDeclarations)
    self._baseDict = {}
    if self._segments:
      for segment in self._segments:
        self._baseDict[CommonProducer.tupelize(segment)] = None
    else:
      self._baseDict[()] = None
    #remember the attributes of the test distribution
    self._attrs = {}
    for key in test.keys():
      self._attrs[str(key)] = str(test.get(key))

    #Validation methodology.
    validation = root.find("validation")
    if (validation is not None):
      validmethod = validation.get('method')
      validthreshold = validation.get('threshold')
      self.testValidation = CommonProducer.testValidatingFunctions(validmethod,validthreshold)
    else:
      self.testValidation = CommonProducer.testValidatingFunctions(None)

  def inputPMML(self, pmmlModelClass):
    if self._timer:
      self._timer.output("Inputting model")
    #input the portion of the model that is completed
    reader = pmmlReader()
    try:
      reader.parse(str(self.__input))
    except:
      print str(self.__input)
    self._pmml = reader.root

    # Specific elements of regression models:
    self._model = self._pmml.getChildrenOfType(pmmlModelClass)[0]

    # Container of local transformations (if any)
    self._localTransformations = self._model.getChildrenOfType(pmmlLocalTransformations)
    if len(self._localTransformations) > 0:
      self._localTransformations = self._localTransformations[0]
    else:
      self._localTransformations = None

  def get(self, field):
    """"""
    return self.row[field]

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


  def specifySegments(self, prototype, modelPMMLsegment):
    """ Given a prototype and the class for the pmml element,
        this method will attach the PMML segment specifiers
    """
    attrs = dict(self._attrs)
    extensions = []
    if self._skip:
      extensions.append(pmmlExtension(children=[extensionSkip(attributes={"number":self._skip})]))
    if self._segments:
      tests = []
      for rule in self._segments:
        segments = []
        for field in rule:
          value = rule[field]
          if isinstance(value, str):
            segments.append(pmmlExplicitSegment(attributes={"field":field,"value":value}))
          else:
            segments.append(pmmlRegularSegment(attributes={"field":field,"low":str(value[0]),"high":str(value[1])}))       
        children = list(extensions)
        children.append(prototype)
        children.append(pmmlSegments(children=segments))
        tests.append(modelPMMLsegment(children=children, attributes=attrs))
    else:
      children = list(extensions)
      # NOTE THIS CURRENTLY WILL FAIL SINCE 'INPUTS' AND 'OUTPUTS' ARE UNDEFINED
      children.append(inputs)
      children.append(outputs)
      tests = [modelPMMLsegment(children=children, attributes=attrs)]
    self._model.addChildren(tests)

    # Remove model segments with multiple segment specifiers as
    # these are left from initial skeleton PMML.
    prototypes=[]
    for r in self._model.getChildrenOfType(modelPMMLsegment):      
      if len(r.getChildrenOfType(pmmlSegments)) != 1:        
        prototypes.append(r)
    self._model.removeChildren(prototypes)
