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
# Version information
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
  def tupelize(rules):
    """This function turns a segment rule into a tuple."""
    keys = rules.keys()
    keys.sort()
    temp = [(key,rules[key]) for key in keys]
    return tuple(temp)
  
  def inputConfigs(self, file):
    """TODO: Very much refactor this. (Mostly done.)
    Only handle the data and pmml input here and handle the model
    specific stuff later in makeTests or whatever I rename that to."""
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
    
    #Model specific stuff
    model = root.getchildren()[0]
    if model.tag == "rules":
      #Get ruleset model config values from the xml
      self._data = model.find('data')
      segmentations = model.findall('segmentation')
      self.__nboxes = int(model.get('nboxes'))
      
      try:
        self.__peel_alpha = float(model.get('peel_alpha'))
      except TypeError:
        self.__peel_alpha = 0.02
      
      try:
        self.__paste_alpha = float(model.get('paste_alpha'))
      except TypeError:
        self.__paste_alpha = 0.02
      
      self.__weight_field = model.get('weight_field')
      self._modelType = pmmlRuleSetModel
    else:
      raise ValueError("Unable to determine what type of model you want produced. Element %s is not recognized." % (model.tag))
    
    #produce segmentation
    self._segments = []
    for segmentDeclarations in segmentations:
      self._makeSegments(segmentDeclarations)
    
    #prepare for statistics gathering
    self._baseDict = {():None}
    if self._segments:
      for segment in self._segments:
        self._baseDict[Producer.tupelize(segment)] = None
    
    #remember the attributes of the model, they will be included in the PMML
    self._attrs = {}
    for key in model.keys():
      self._attrs[str(key)] = str(model.get(key))
 

  def inputPMML(self):
    """This function reads in a pmml file and saves the things we are going to be reusing when we collect stats."""
    if self.__timer:
      self.__timer.output("Inputting model")
    #input the portion of the model that is completed
    reader = pmmlReader()
    reader.parse(str(self.__input))
    self._pmml = reader.root
    self._model = self._pmml.getChildrenOfType(self._modelType)[0]
    self._localTransformations = self._model.getChildrenOfType(pmmlLocalTransformations)
    if len(self._localTransformations) > 0:
      self._localTransformations = self._localTransformations[0]
    else:
      self._localTransformations = None

  def get(self, field):
    """"""
    return self.row[field]
  
  def _collectStats(self, results, stats):
    """Given a list of scores and the current count tables, update the appropriate count tables."""
    #The last four values of each result are score, alert, segment and extras.
    #Anything before the last four were asked for as the field we are predicting or the fields used to make our prediction.
    for result in results:
      #print result
      rules = Producer.tupelize(result[-2])
      #tbl = stats[rules]
      if stats[rules]:
        tbl = stats[rules][-1]
        #update information
        if (len(tbl)<20000):
          tbl.append(result[:-4])
        else:
          stats[rules].append(uni.UniTable(self.__needed_fields, _prealloc=100000))
          tbl = stats[rules][-1]
          tbl.append(result[:-4])
      else:
        #initialize information, preallocate 100 rows
        tmp = uni.UniTable(self.__needed_fields, _prealloc=100000)
        tmp.append(result[:-4])
        stats[rules] = [tmp]
  
  def _Score(self, tables):
    """This function runs all of the data through a dummy model so that we can collect a count table for each segment."""
    
    #Build a brand new model based off of the input pmml
    #For our purposes, a tree model works just as well as a ruleset would
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
    
    node_attrs = {"score":"0"}
    predicate = pmmlTrue()
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
        node = pmmlNode(children=[predicate], attributes=node_attrs)
        model_attrs = {"functionName":"classification"}
        children = [copy.deepcopy(mining_schema)]
        if self._localTransformations:
          children.append(copy.deepcopy(self._localTransformations))
        children.append(node)
        tree = pmmlTreeModel(children=children, attributes=model_attrs)
        
        #Create the segment element
        segments.append(pmmlSegment(children=[seg_predicate, tree]))
      
      #Build a Mining model with the segments that we have
      segmentation = pmmlSegmentation(children=segments, attributes={"multipleModelMethod":"selectAll"})
      model = pmmlMiningModel(children=[copy.deepcopy(mining_schema), segmentation], attributes=model_attrs)
      
    else:
      #No segments
      node = pmmlNode(children=[predicate], attributes=node_attrs)
      model_attrs = {"functionName":"classification"}
      children = [mining_schema]
      if self._localTransformations:
        children.append(self._localTransformations)
      children.append(node)
      model = pmmlTreeModel(children=children, attributes=model_attrs)
    
    pmml_children.append(model)
    self._pmml = pmmlPMML(children=pmml_children, attributes={"version":"4.0"})
    
    self.__needed_fields = [self.__field]
    if self.__weight_field is not None:
      self.__needed_fields.append(self.__weight_field)
    self.__needed_fields += self.__attributes
    self._pmml.initialize(self.get, self.__needed_fields)
    
    #actually score each event
    #the stats are indexed by segment and we keep a count table for each
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
          self._collectStats(results, stats)
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
            self._collectStats(results, stats)
        if last < 100.000:
          self.__timer.output("Events 100.000% processed")
      else:
        if self.__batch:
          for row in table:
            self.row = row
            model.batchEvent()
          results = model.batchScore()
          self._collectStats(results, stats)
        else:
          for row in table:
            self.row = row
            results = model.score()
            self._collectStats(results, stats)
    self._pmml.removeChildren(self._pmml.getChildrenOfType(pmmlModels))
    return stats
  
  def _makeTable(self, element):
    """This function takes the data element and figures out what data to read and how to read it."""
    if self.__timer:
      self.__timer.output('Read file')
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
      if self.__timer:
        self.__timer.output('Read file')
        w = uniTableWrapper(uni.UniTable().from_any_file(fileName))
        self.__timer.output('returning')
        return [w]
      else:
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
      self.__timer.output("Collecting stats for model")
    
    #TODO: This is the wrong way to do things because the mining fields can only refer to data fields.  This needs to be moved into the config.
    #First we need to find the predicted field and the active fields in the mining schema.
    mining_schema = self._model.getChildrenOfType(pmmlMiningSchema)[0]
    self.__attributes = []
    for field in mining_schema.getChildrenOfType(pmmlMiningField):
      usage = field.getAttribute('usageType')
      if usage == 'predicted':
        #This currently only supports one predicted field.
        self.__field = field.getAttribute('name')
      elif usage is None or usage == 'active':
        self.__attributes.append(field.getAttribute('name'))
    
    #produce count tables for the rulesets
    self._stats = self._Score(self._makeTable(self._data))
  
  def peel(self):
    """Temp hack to count the number of itterations in profiling."""
    pass
  
  def paste(self):
    """Temp hack to count the number of itterations in profiling."""
    pass
  
  def _doPRIM(self, box_tbl, nboxes, peel_alpha, paste_alpha):
    """Give a UniTable count table with the value we want to predict in the first column, compute splits and build a ruleset."""
    #Right now only this is only intended to work with continious predictions.
    #If we can take the mean of it we'll consider it continious.
    try:
      box_tbl.field(0).mean()
    except:
      #TODO: Real error handling here.
      raise
    
    #Find out if we have a weight field, if we do have a weight field it will be the second column in the table
    use_weights = False
    if self.__weight_field is not None:
      use_weights = True
    
    #List of boxes
    rules=[]
    # box_tbl starts as the entire space
    peeled_tbl = None
    
    for box_num in xrange(0, nboxes):
      box_def = {}
      #Init the best rho as the current mean
      if use_weights:
        best_rho = box_tbl.field(0).sum() / box_tbl.field(1).sum()
      else:
        best_rho = box_tbl.field(0).mean()
      
      ##Peeling
      
      #Init a dict with all the the possible split values for each field
      split_vals = {}
      for field in self.__attributes:
        split_vals[field] = set(box_tbl[field])
      
      #This is sort of like a do while loop.  The peeling stopping condition is a condition on an if statement below and when we need to stop we break out of the loop.
      while True:
        #TODO: The following line is a temporary hack for reporting the number of peel itterations in profiling
        self.peel()
        #Because we're going to be using them a lot save the table length, the dependent field and the weight field
        dependent_field = box_tbl.field(0)
        if use_weights:
          weight_field = box_tbl.field(1)
          tbl_len = weight_field.sum()
        else:
          tbl_len = len(box_tbl)
        
        last_rho = best_rho
        
        #for each field we can split on, we want to compute delta for each possible split and select the split that has the max delta.
        for field in split_vals:
          for val in split_vals[field]:
            #For each possible split value in the data, check to see how much data both sides remove from the box and if it's not too much then check to see if it's a good split.
            
            if use_weights:
              cnt = sum(weight_field * (box_tbl[field] <= val))
            else:
              cnt = sum(box_tbl[field] <= val)
            if cnt / float(tbl_len) < peel_alpha:
              rho = sum(dependent_field * (box_tbl[field] > val)) / float(tbl_len - cnt)
              if rho > best_rho:
                best_rho = rho
                f, op , v, op_str = field, operator.gt, val, "greaterThan"
                #print field, val, cnt, best_rho#, " ",
            
            if use_weights:
              cnt = sum(weight_field * (box_tbl[field] >= val))
            else:
              cnt = sum(box_tbl[field] >= val)
            if cnt / float(tbl_len) < peel_alpha:
              rho = sum(dependent_field * (box_tbl[field] < val)) / float(tbl_len - cnt)
              if rho > best_rho:
                best_rho = rho
                f, op , v, op_str = field, operator.lt, val, "lessThan"
                #print field, val, cnt, best_rho#, " ",
        
        #The following line is the peel stopping condition.
        #TODO: Improve this to include things like minimum support.
        if best_rho != last_rho:
          #Keep track of the records we've peeled off so that we can paste them back or use them for later boxes.
          #Note that we're using bitwise negation (~) because it's opperating on an array of bools
          if peeled_tbl is None:
            peeled_tbl = box_tbl.subtbl(~op(box_tbl[f], v))
          else:
            peeled_tbl.extend(box_tbl.subtbl(~op(box_tbl[f], v)))
          box_tbl = box_tbl.subtbl(op(box_tbl[f], v))
          #Update the possible split values for this field
          new_splits = set(box_tbl[f])
          if len(new_splits) == 1:
            del split_vals[f]
          else:
            split_vals[f] = new_splits
          
          #Save the split information
          if f in box_def:
            if op_str == "lessThan":
              box_def[f][1] = (op, v, op_str)
            else:
              box_def[f][0] = (op, v, op_str)
          else:
            #first split for this field
            if op_str == "lessThan":
              box_def[f] = [None,(op, v, op_str)]
            else:
              box_def[f] = [(op, v, op_str),None]
        else:
          #Here is where we break out of the peeling stage.
          break
      
      ##Pasting
      
      #Again, this is sort of like a do while loop.  The pasting stopping condition is a condition on an if statement below and when we need to stop we break out of the loop.
      while peeled_tbl is not None:#True:
        #TODO: The following line is a temporary hack for reporting the number of peel itterations in profiling
        self.paste()
        last_rho = best_rho
        best_rho = None
        
        #Set up a structure that contains which items in the peeled table would be in the box table if only that condition mattered.
        #Each field has a list with a greater than and a less than.
        box_preds = {}
        for f, vs in box_def.iteritems():
          box_preds[f] = [None, None]
          if vs[0] is not None:
            op, v, op_str = vs[0]
            box_preds[f][0] = op(peeled_tbl[f], v)
          if vs[1] is not None: 
            op, v, op_str = vs[1]
            box_preds[f][1] = op(peeled_tbl[f], v)
        
        #Because we're going to be using them a lot save the table length, the dependent field and the weight field
        box_dependent_total = box_tbl.field(0).sum()
        peeled_dependent_field = peeled_tbl.field(0)
        if use_weights:
          peeled_weight_field = peeled_tbl.field(1)
          tbl_len = box_tbl.field(1).sum()
        else:
          tbl_len = len(box_tbl)
        
        #Loop over the fields that were peeled and see if we can paste on some data and improve rho.
        for field in box_preds:
          for val in set(peeled_tbl[field]):
            if box_def[field][0] is not None and val <= box_def[field][0][1]:
              #Figure out which records would be in the box using this field
              pasted_fields = operator.ge(peeled_tbl[field], val)
              if box_def[field][1] is not None:
                pasted_fields &= box_preds[field][1]
              #Now add on which records are in the box using the rest of the fields
              for f in [f for f in box_preds if f != field]:
                if box_preds[f][0] is not None:
                  pasted_fields &= box_preds[f][0]
                if box_preds[f][1] is not None:
                  pasted_fields &= box_preds[f][1]
              
              if use_weights:
                cnt = sum(peeled_weight_field * pasted_fields)
              else:
                cnt = sum(pasted_fields)
              if cnt / float(tbl_len) < paste_alpha:
                rho = (box_dependent_total + sum(peeled_dependent_field * pasted_fields)) / float(tbl_len + cnt)
                #TODO: Get 0.4 from config
                if (best_rho is not None and rho > best_rho) or (best_rho is None and (rho > 0.4 or rho > last_rho)):
                  best_rho = rho
                  best_fields = pasted_fields
                  best_field, op, v, op_str = field, operator.ge, val, "greaterOrEqual"
              
            elif box_def[field][1] is not None and val >= box_def[field][1][1]:
              #Figure out which records would be in the box using this field
              pasted_fields = operator.le(peeled_tbl[field], val)
              if box_def[field][0] is not None:
                pasted_fields &= box_preds[field][0]
              #Now add on which records are in the box using the rest of the fields
              for f in [f for f in box_preds if f != field]:
                if box_preds[f][0] is not None:
                  pasted_fields &= box_preds[f][0]
                if box_preds[f][1] is not None:
                  pasted_fields &= box_preds[f][1]
              
              if use_weights:
                cnt = sum(peeled_weight_field * pasted_fields)
              else:
                cnt = sum(pasted_fields)
              if cnt / float(tbl_len) < paste_alpha:
                rho = (box_dependent_total + sum(peeled_dependent_field * pasted_fields)) / float(tbl_len + cnt)
                if (best_rho is not None and rho > best_rho) or (best_rho is None and (rho > 0.4 or rho > last_rho)):
                  best_rho = rho
                  best_fields = pasted_fields
                  best_field, op, v, op_str = field, operator.le, val, "lessOrEqual"
        
        #The following line is the paste stopping condition.
        #TODO: See if this needs to include anything else
        if best_rho is not None and best_rho != last_rho:
          #print "Best rho is:", best_rho, best_field, op_str, str(v)
          
          #Update the box definition
          if op_str == "greaterOrEqual":
            box_def[best_field][0] = (op, v, op_str)
          else:
            box_def[best_field][1] = (op, v, op_str)
          
          #Move the rows from the best paste to the box table.
          box_tbl.extend(peeled_tbl.subtbl(best_fields))
          peeled_tbl = peeled_tbl.subtbl(~best_fields)
          if len(peeled_tbl) == 0:
            #We've pasted on the last of the peeled data so we don't need to do any more pasting
            peeled_tbl = None #Don't try to create any more boxes
            break
        else:
          #Here is where we break out of the pasteing stage.
          break
      
      print "Mean for box %d is %f" % (box_num + 1, last_rho)
      
      #Build a rule from the predicates.
      if len(box_def) != 0:
        box_preds = []
        
        for field, vs in box_def.iteritems():
          for x in vs:
            if x is not None:
              #x is (op, value, op_string)
              box_preds.append(pmmlSimplePredicate(attributes={"field":field, "operator":x[2], "value":str(x[1])}))
        
        if len(box_preds) > 1:
          box_preds = [pmmlCompoundPredicate(children=box_preds, attributes={"booleanOperator":"and"})]
        rules.append(pmmlSimpleRule(children=box_preds, attributes={"score":"True_" + str(box_num + 1)}))
      
      #Swap out the data for the next box.
      if peeled_tbl is not None:
        box_tbl = peeled_tbl
        peeled_tbl = None
      else:
        #We've used all the data for the boxes and so don't loop again no matter how many boxes we were supposed to produce
        break
    
    #Collect all our rules into a single ruleset
    children = [pmmlRuleSelectionMethod(attributes={"criterion":"firstHit"})]
    children.extend(rules)
    return pmmlRuleSet(children=children, attributes={"defaultScore":"False"})
  
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
    """TODO: Rename this function.
    This function builds the pmml model from the stats we have collected."""
    if self.__timer:
      self.__timer.output("Making rulesets from statistics")
    #create a ruleset for each segment
    segments = []
    keys = self._stats.keys()
    keys.sort()
    for entry in keys:
      if self._stats[entry]:
        counttables = self._stats[entry]
        if len(counttables)>1:
          counttable = counttables[0].concatenate(*counttables[1:])
        else:
          counttable = counttables[0]
        mining_schema = self._model.getChildrenOfType(pmmlMiningSchema)[0]
        profile = False #True
        if not profile:
          ruleset = self._doPRIM(counttable, self.__nboxes, self.__peel_alpha, self.__paste_alpha)
        else:
          #TODO: pass self.__peel_alpha and self.__paste_alpha to _doPRIM
          import cProfile
          cProfile.runctx("self._doPRIM(counttable, %d)" % self.__nboxes, globals(), locals())
        
        #Repeat the list of attributes from the input pmml schema
        model_attrs = self._model.getAttributes()
        unused_attrs = [x for x, y in model_attrs.iteritems() if y is None]
        for x in unused_attrs:
          del model_attrs[x]
        
        children = [mining_schema]
        if self._localTransformations:
          children.append(self._localTransformations)
        children.append(ruleset)
        
        model = pmmlRuleSetModel(children=children, attributes=model_attrs)
        if entry:
          predicate = Producer.makeSegments(entry)
          segments.append(pmmlSegment(children=[predicate, model]))
    
    if segments:
      #Make a mining model
      segmentation = pmmlSegmentation(children=segments, attributes={"multipleModelMethod":"selectAll"})
      model = pmmlMiningModel(children=[mining_schema, segmentation], attributes={"functionName":model_attrs["functionName"]})
    self._pmml.addChild(model)
  
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
