#!/usr/bin/env python

#Augustus PMML consumer

__copyright__ = """
Copyright (C) 2006-2011  Open Data ("Open Data" refers to
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

from pmmlReader import *
from augustus.kernel.unitable.unitable import UniRecord
from augustus.runlib.any_reader import Reader, NativeElement
from augustus.runlib.http_server import *
from StringIO import StringIO
from sys import stdout
import logging
import httplib
import datetime
import augustus.const as AUGUSTUS_CONSTS
import cPickle
import os
import signal
import gc
import platform
import augustus.tools.proctools as ptools

def daemonRestartHandler(signalnumber, stackframe):
  # Re-read configuration
  if not gc.isenabled():
    gc.enable()
    gc.collect()
    gc.disable()
  currentConfig = stackframe.f_locals['config_reader']
  currentConsumer = stackframe.f_locals['consumer']
  currentConfig.callback = currentConsumer.reconfigure
  currentConfig.read_once()
  #currentDataInput = stackframe.f_locals['data_input']
  # Note that the reader instance cannot be changed but
  # its attributes *can*
  for item in currentConsumer.data_input_info:
    if item.name in ["readOnce","daemon"]:
      pass # data_input elements not handled in demon reconfigures.
    elif item.name == "fromFile" or item.name == "fromFifo":
      #No special treatment needed other than UniTable vs XML
      isUni = False
      filetype = None
      if 'type' in item.attr:
        filetype = item.attr['type']
      if filetype == "UniTable":
        isUni = True
      #data_input = Reader(currentConsumer.score, source = item.attr['name'], logger = currentConsumer.logger, magicheader = False, unitable = isUni)
      stackframe.f_locals['data_input'].source = item.attr['name']
    elif item.name == "fromFixedRecordFile":
      isUni = True
      types = None
      ffnames = []
      ffstarts = []
      ffends = []
      fftypes = []
      start = 0
      for field in item:
        ffnames.append(field.attr['name'])
        ffstarts.append(start)
        ffends.append(start + int(field.attr['length']))
        start += int(field.attr['length'])
      if 'cr' in item.attr:
        ffCR = item.attr['cr']
      else:
        ffCR = None
      #data_input = Reader(currentConsumer.score, source = item.attr['name'],
      #  types = None,
      #  logger = currentConsumer.logger, magicheader = False, unitable = isUni, ffConvert = ffConfig(ffnames, ffstarts, ffends, ffCR))
    elif item.name == "fromCSVFile":
      #We have a CSV file that needs special treatment to read in correctly
      isUni = True
      header = None
      sep = None
      types = None
      if 'header' in item.attr:
        header = item.attr['header']
      if 'sep' in item.attr:
        sep = item.attr['sep']
      if 'types' in item.attr:
        types = item.attr['types']
      #data_input = Reader(currentConsumer.score, source = item.attr['name'], logger = currentConsumer.logger, magicheader = False, unitable = isUni, header = header, sep = sep, types = types)
    elif item.name == "fromStandardInput":
      isUni = False
      isUni = True
      filetype = None
      if 'sep' in item.attr:
        sep = item.attr['sep']
      if 'types' in item.attr:
        types = item.attr['types']
      if 'type' in item.attr:
        filetype = item.attr['type']
      if filetype == "UniTable":
        isUni = True
      #data_input = Reader(currentConsumer.score, source = "-", logger = currentConsumer.logger, magicheader = False, unitable = isUni, sep = sep, types = types)
    elif item.name == "fromHTTP":
      #get the stuff we need to setup the server
      input_url = item.attr['url']
      if port:
        input_port = int(port)
      else:
        input_port = int(item.attr['port'])
      datatype = None
      if 'type' in item.attr:
        datatype = item.attr['type']
      if datatype == "UniTable":
        callback = currentConsumer.score_http_uni
      else:
        callback = currentConsumer.score_http_xml
      #Create the server
      #data_input = HTTPInterfaceServer(('',input_port), logger = currentConsumer.logger)
      #Add the callback
      #data_input.register_callback(input_url, callback)
    else:
      #Not recognized
      currentConsumer.logger.debug("Element %s is not a recognized child element of inputData, ignoring." % (item.name))
  for item in currentConsumer.data_output_info:
    if item.name in ["report"]:
      for thing in item:
        if thing.name == "toFile":
          newname = thing.attr['name']
          if not currentConsumer.out.closed:
            currentConsumer.out.close()
          currentConsumer.output_filename = newname
          currentConsumer.out = open(currentConsumer.output_filename,'a')

  if (signalnumber==signal.SIGUSR1):
    currentConsumer.initialize_model()
  #stackframe.f_locals['data_input'].source = data_input.source

class metadataLogger(dict):
  """ 
   Container for options guiding metadata production
  """
  def __init__(self, logger):
    self.log = logger
    # Dictionary of metadata collected by different objects.
    self.collected = {} 

  def enableMetaDataCollection(self):
    self['Time Scoring'] = datetime.timedelta(0,0,0)
    self['Time Parsing PMML'] = datetime.timedelta(0,0,0)
    self['Consumer Initialization'] = datetime.timedelta(0,0,0)
    self['Calls to Score']=0
    self['Memory after Parsing PMML'] = 0
    self['Resident Memory after Parsing PMML'] = 0
    self['Stacksize after Parsing PMML'] = 0
    self['Memory after Scoring'] = 0
    self['Resident Memory after Scoring'] = 0
    self['Stacksize after Scoring'] = 0
    self['Memory after Consumer Initialization'] = 0
    self['Resident Memory after Consumer Initialization'] = 0
    self['Stacksize after Consumer Initialization'] = 0
    self['User Time after Parsing PMML'] = 0

  def getMetaData(self):
    for k in self.keys():
      self[k] = str(self[k])
    return [' : '.join((k,v)) for (k,v) in self.items()]

  def report(self):
    for reporter in self.collected.keys():
      self.log.info(10*' '+'%s\n'%reporter)
      for msg in self.collected[reporter]:
        self.log.info('%s'%msg)

class ffConfig:
  """ This is simply a container for elements needed to
      specify how to read from fixed format records
  """
  def __init__(self, fieldnames, fieldstarts, fieldends, fieldtypes = None, cr = None):
    self.fieldspecs = {}
    if fieldtypes is None:
      fieldtypes = len(fieldnames)*['str']
    fields = []
    for ind in range(len(fieldnames)):
      self.fieldspecs[fieldnames[ind]] = (fieldstarts[ind],fieldends[ind],fieldtypes[ind])
      fields.append((fieldnames[ind],fieldends[ind]-fieldstarts[ind]))
    self.fields = tuple(fields)
    self.cr = cr

class ConfigurationError(Exception):
  """This class is used to indicate problems with parsing the configuration file."""
  def __init__(self, message):
    self.message = message
  
  def __str__(self):
    return self.message

class pmmlConsumer:
  """This is a class that has all the needed parts for a pmml consumer."""
  def __init__(self, logger = None):
    self.data_holder = None
    self.report_header = ""
    self.report_format = None
    self.report_arguments = []
    self.report_footer = ""
    self.report_ancillary_keys = {}
    self.model_input_info = None
    self.data_input_info = None
    self.output_filename = None
    self.event_based = False
    self.transformation_extensions_hook = None
    self.batch_scoring = False
    self.__score_posting = None
    self.metadata = None

    if logger:
      self.logger = logger
    else:
      self.logger = logging.Logger('consumer')
      self.logger.setLevel(logging.DEBUG)
  
  def get_data(self, field_name):
    """This function supplies data from an arbitrary field name, used by the scoring function."""
    data = self.data_holder
    try:
      if isinstance(data, UniRecord):
        return data[field_name]
      elif isinstance(data, NativeElement):
        result = self.__find_value(field_name, data)
        
        if not result is None:
          return result
        else:
          #We haven't found the field we were looking for.
          self.logger.error("Data not found for field: %s" % (field_name))
          return None
      elif isinstance(data, dict):
        return data[field_name]
    except:
      self.logger.error("An error occured while trying to access field: %s" % (field_name))
      return None
  
  def __find_value(self, field_name, data):
    """This function is intended to do a depth first search to find data in the field name requested."""
    #The following code was shamelessly <s>stolen</s> borrowed from the producer.
    #TODO: Find a way to properly reuse this code.
    if data.name == field_name:
      for child in data:
        if isinstance(child, str):
          return child
      return ""
    if field_name in data.attr:
      return data.attr[field_name]
    for child in data:
      if not isinstance(child, str):
        value = self.__find_value(field_name, child)
        if not value is None:
          return value
    return None
    
  def score(self, data):
    """Callback used by any_reader to score the data"""
    if self.metadata is not None:
      scoreStart = datetime.datetime.now()
      self.metadata['Calls to Score'] += 1
    #If there has been a function provided for unsupported transformations to calculated, call that function.
    if self.transformation_extensions_hook:
      self.transformation_extensions_hook(data)
    
    #allow the data to be accessed by the scoring function
    self.data_holder = data
    
    #debugging logging
    #self.logger.debug(str(data))
    
    #Just update the model if we are in batch scoring mode
    if self.batch_scoring:
      self.model.batchEvent()
      return
    else:
      #score and format the data
      #o = self.model.score()
      #report_string = self.format_results(o)
      report_string = self.format_results(self.model.score())

    #debugging logging
    #self.logger.debug(str(report_string))
    
    #And output the result, if needed
    if self.__score_posting:
      try:
        self.__POST_result(report_string)
      except:
        self.logger.error("An error occured while sending results to a downstream connection.")
    
    if self.output_filename:
      self.out.write(report_string)
    if self.metadata is not None:
      self.metadata['Time Scoring'] += datetime.datetime.now()-scoreStart
    return report_string


  def format_results(self, results):
    """This function takes a list of result lists and fills the values into the report format."""
    report_string = ""
    for result in results:
      anc_index = 0
      report_args = []
      for y in self.report_argument_order:
        if y == -2:
          #we want to output the segment info
          segments = ""
          for field, value in result[-2].iteritems():
            if isinstance(value, tuple):
              segments += '<Regular field="%s" low="%s" high="%s" />' % (field, str(value[0]), str(value[1]))
            else:
              segments += '<Explicit field="%s" value="%s" />' % (field, str(value))
          report_args.append(segments)
        elif y == -1:
          ancillarystr = ""
          anc_key=self.report_ancillary_keys[anc_index]
          anc_value=result[-1].get(anc_key)
          anc_index +=1
          report_args.append(str(anc_value))
        else:
          report_args.append(str(result[y]))
      if self.event_based:
        report_string += self.report_header + self.report_format % tuple(report_args) + self.report_footer + '\n'
      else:
        report_string += self.report_format % tuple(report_args) + '\n'

    return report_string
  
  def __POST_result(self, score):
    """This function will send the result to a given url via an HTTP POST."""
    conn = httplib.HTTPConnection(self.__score_posting[0])
    headers = {"Content-type": "text/xml"}
    
    #Send the request and then read the response.
    conn.request("POST", self.__score_posting[1], score, headers)
    response = conn.getresponse()
    message = response.read()
    
    #If we didn't get an OK reponse, then log it.
    if response.status != 200:
      self.logger.error("Recieved a %d response code from downstream connection." % response.status)
    
    conn.close()
  
  def score_http_xml(self, data):
    """This function should be used as the callback for the HTTP server when the data is expected to be XML."""
    wrapper = StringIO(data) #wrap the data in a StringIO object
    rdr = Reader(self.score, source = wrapper, logger = self.logger, magicheader = False, unitable = False)
    pipe = rdr.new_pipe()
    #try:
    return rdr.feed_pipe(None, pipe)
    #except:
      #self.logger.error("something broke.")
    #pass
  
  def score_http_uni(self,data):
    """This function should be used as the callback for the HTTP server when the data is expected to be a UniTable or csv file."""
    wrapper = StringIO(data) #wrap the data in a StringIO object
    rdr = Reader(self.score, source = wrapper, logger = self.logger, magicheader = False, unitable = True)
    pipe = rdr.new_pipe()
    try:
      return rdr.feed_pipe(None, pipe)
    except:
      self.logger.error("something broke.")
    pass
  
  def output_report_header(self, file_handle = None):
    """This function should be called before you do any scoring on a batch scoring"""
    if self.output_filename:
      self.out.write(self.report_header + '\n')
    elif file_handle is not None:
      file_handle.write(self.report_header + '\n')
    else:
      consumer.critical("Output file is unspecified.\n")
      sys.exit(1)
  
  def output_report_footer(self, file_handle = None):
    """This function should be called after scoring is completed """
    if self.output_filename:
      file_handle = self.out
    file_handle.write(self.report_footer + '\n')
    file_handle.flush()

  def reconfigure(self, data):
    if data.name == "pmmlDeployment":
      for child in data:
        if child.name == "inputData":
          self.data_input_info = child
        if child.name == "output":
          self.data_output_info = child

  def configure(self, data):
    """This function parses the configuration XML"""
    if data.name == "pmmlDeployment":
      for child in data:
        if child.name == "metadata":
          #We aren't interested in this, we'll ignore it.
          pass
        elif child.name == "output":
          for x in child:
            if x.name == "report":
              self.__initialize_report(x)
            elif x.name == "eventBased":
              self.event_based = True
            else:
              self.logger.debug("Element %s is not a recognized child element of output, ignoring." % (x.name))
        elif child.name == "inputModel":
          #Save the info and make use of it elsewhere
          self.model_input_info = child
        elif child.name == "inputData":
          #Save the info and make use of it elsewhere
          self.data_input_info = child
        elif child.name == "logging":
          for x in child:
            if x.name == "toFile":
              f = logging.FileHandler(str(x.attr['name']))
              self.logger.addHandler(f)
              self.logger.propagate = False # call to all handlers stops here.
            elif x.name == "toStandardOut":
              f = logging.StreamHandler(stdout)
              self.logger.addHandler(f)
              self.logger.propagate = False
            elif x.name == "toStandardError":
              f = logging.StreamHandler()
              self.logger.addHandler(f)
              self.logger.propagate = False
            elif x.name == "metadata":
              metadatalogger = logging.Logger('ConsumerMetaData')
              if 'name' in x.attr :
                metadataHandler = logging.FileHandler(str(x.attr['name']))
              else:
                metadataHandler = logging.StreamHandler(stdout)
              metadataHandler.setFormatter(logging.Formatter('%(message)s'))
              metadatalogger.addHandler(metadataHandler)
              metadatalogger.setLevel(logging.INFO)
              self.metadata = metadataLogger(metadatalogger)
              self.metadata.enableMetaDataCollection()
              self.getMetaData = self.metadata.getMetaData
            else:
              self.logger.debug("Element %s is not a recognized child element of logging, ignoring." % (x.name))
          if self.logger.handlers == []:
            f = logging.StreamHandler(stdout)
            self.logger.addHandler(f)
            self.logger.propagate = False
        else:
          self.logger.debug("Element %s is not a recognized child element of pmmlDeployment, ignoring." % (child.name))
    else:
      #Raise a big fuss.
      self.logger.error("Got root element of: %s.  Expected: pmmlDeployment" % (data.name))
      raise ConfigurationError("Got root element of: %s.  Expected: pmmlDeployment" % (data.name))

  def __initialize_report(self, data):
    """This function parses the report portion of the configuration XML"""
    self.needed_cols = []
    self.report_argument_order = []
    format = ""
    self.report_header = '<' + data.attr['name'] + '>'
    for x in data:
      if x.name == "toFile":
        self.output_filename = str(x.attr['name'])
        self.out = open(self.output_filename,'a')
      elif x.name == "toHTTP":
        try:
          host = x.attr['host']
        except:
          self.logger.error("Element toHTTP must have host attribute.  Skipping offending element.")
          continue
        try:
          path = x.attr['url']
        except:
          self.logger.error("Element toHTTP must have url attribute.  Skipping offending element.")
          continue
        self.__score_posting = (host, path)
      elif x.name == "outputRow":
        format += '<' + x.attr['name'] + '>'
        next_index = 0
        anc_index = 0
        for y in x:
          if y.name == "outputColumn":
            try:
              col_name = y.attr['name']
            except:
              self.logger.error("Element outputColumn must have name attribute.  Skipping offending element.")
              continue
            if y.attr.has_key('fieldName'):
              format += '<' + col_name + '>%s</' + col_name + '>'
              self.needed_cols.append(y.attr['fieldName'])
              self.report_argument_order.append(next_index)
              next_index += 1
            elif y.attr.has_key('value'):
              format += '<' + col_name + '>' + y.attr['value'] + '</' + col_name + '>'
            else:
              self.logger.error("Element outputColumn must either have a value attribute or a fieldName attribute.  Skipping offending element.")
          elif y.name == "score":
            try:
              col_name = y.attr['name']
              format += '<' + col_name + '>%s</' + col_name + '>'
              self.report_argument_order.append(-4)
            except:
              self.logger.error("Element score must have name attribute.  Skipping offending element.")
          elif y.name == "alert":
            try:
              col_name = y.attr['name']
              format += '<' + col_name + '>%s</' + col_name + '>'
              self.report_argument_order.append(-3)
            except:
              self.logger.error("Element alert must have name attribute.  Skipping offending element.")
          elif y.name == "segments":
            format += '<Segments>%s</Segments>'
            self.report_argument_order.append(-2)
          elif y.name == "ancillary":
            try:
              anc_name=y.attr['name']
              self.report_ancillary_keys[anc_index]=anc_name
              format += '<' + anc_name + '>%s</' + anc_name + '>'
              self.report_argument_order.append(-1)
              anc_index += 1
            except:
              self.logger.error("Element Ancillary must have name attribute.  Skipping offending element.")
          else:
            self.logger.debug("Element %s is not a recognized child element of outputRow, ignoring." % (y.name))
        format += '</' + x.attr['name'] + '>'
      else:
        self.logger.debug("Element %s is not a recognized child element of report, ignoring." % (x.name))
    self.report_footer = '</' + data.attr['name'] + '>'
    self.report_format = format
  
  def initialize_model(self):
    """This function actually inputs the file containing the PMML model"""
    self.logger.debug("Initializing")
    if self.model_input_info:
      input_type = self.model_input_info[0]
      if input_type.name == "fromFile" or input_type.name == "fromFifo":
        model_source = input_type.attr['name']
      elif input_type == "fromHTTP":
        pass #do stuff
      else:
        raise ConfigurationError("Unable to determine model input source.")
    else:
      raise ConfigurationError("inputModel tag missing from configuration XML.")
    self.logger.debug("Create reader for PMML")
    #Create pmmlReader to read in pmml file
    model_reader = pmmlReader()
    self.logger.debug("Parse PMML")
    #--------------------------
    if self.metadata is not None :
      parseStart = datetime.datetime.now()
    model_reader.parse(model_source, self.logger)
    if self.metadata is not None :
      self.logger.debug("Save PMML Parsing Time")
      self.metadata['Time Parsing PMML'] += datetime.datetime.now() - parseStart
      self.logger.debug("Calculate Resources")
      self.logger.debug("Save stacksize")
      self.metadata['Stacksize after Parsing PMML'] = ptools.stacksize()
      self.logger.debug("Save Resident Memory")
      self.metadata['Resident Memory after Parsing PMML'] = ptools.resident()/1e+9
      self.logger.debug("Save Memory after parsing PMML")
      self.metadata['Memory after Parsing PMML'] = ptools.memory()/1e+9
      self.logger.debug("Save User Time after Parsing PMML")
      if not (platform.system() in ('Windows', 'Win', 'Microsoft')):
          import resource
          resources = resource.getrusage(resource.RUSAGE_SELF)
          self.metadata['User Time after Parsing PMML'] = resources.ru_utime
    otherStart = datetime.datetime.now()
    self.myPMML = model_reader.root
    #----------------------
    self.logger.debug("Initialize Model")
    self.myPMML.initialize(self.get_data, self.needed_cols)
    self.logger.debug("find model")
    #Get the model
    if os.path.exists('pickledModel'):
      # The following section is used for testing idea of pickled models.
      self.model = cPickle.load(file('pickledModel'))
    else:
      self.model = self.myPMML.getChildrenOfType(pmmlModels)[0]
      # The following section is used for testing idea of pickled models.
      if False:
        savedModel = model_reader.root
        self.logger.debug(dir(savedModel))
        #savedModel.initialize(self.get_data, self.needed_cols)
        cPickle.dump(savedModel, file('pickledModel','w'))
    if self.metadata is not None:
      self.metadata['Consumer Initialization'] += datetime.datetime.now() - otherStart
      self.metadata['Stacksize after Consumer Initialization'] = ptools.stacksize()
      self.metadata['Resident Memory after Consumer Initialization'] = ptools.resident()/1e+9
      self.metadata['Memory after Consumer Initialization'] = ptools.memory()/1e+9

def main(config, outfile=None, port=None):
  """Main function for controling scoring.  Config, if used should be a string containing a filename where a configuration file can be found."""
  #Read in a config file with a bunch of options describing where everything is
  consumer = pmmlConsumer()
  #The following two logging statements are worse than useless because 
  # they will cause 'No handlers could be found for logger "consumer"'
  # to be printed because we set up the logging handler while we're reading
  # the config file which happens at the end of this section.
  #consumer.logger.debug("Create Reader to get Configuration")
  config_reader = Reader(consumer.configure, source = str(config), magicheader = False, autoattr = False)
  #consumer.logger.debug("Read Config File")
  config_reader.read_once()

  #Overwrite the out file from the config file with the command line option if it was present.
  if outfile:
    consumer.output_filename = outfile
  #Create any reader or http server to read in data
  data_input = None
  run_forever = True
  run_daemon = False
  script_input = False
  
  #Check to make sure that we don't try to iterate over None
  if consumer.data_input_info is None:
    raise ConfigurationError("Data input source missing from configuration.")
  
  for item in consumer.data_input_info:
    if item.name == "readOnce":
      run_forever = False
    elif item.name == "batchScoring":
      consumer.batch_scoring = True
    elif item.name == "daemon":
      run_daemon = True
    elif data_input is not None:
      continue #Only process the first way that we are told to get the data.
    elif item.name == "fromFile" or item.name == "fromFifo":
      #No special treatment needed other than UniTable vs XML
      isUni = False
      filetype = None
      if 'type' in item.attr:
        filetype = item.attr['type']
      if filetype == "UniTable":
        isUni = True
      data_input = Reader(consumer.score, source = item.attr['name'], logger = consumer.logger, magicheader = False, unitable = isUni, framing='EOF')
    elif item.name == "fromFixedRecordFile":
      isUni = True
      types = None
      ffnames = []
      ffstarts = []
      ffends = []
      fftypes = []
      start = 0
      for field in item:
        ffnames.append(field.attr['name'])
        ffstarts.append(start)
        ffends.append(start + int(field.attr['length']))
        start += int(field.attr['length'])
      if 'cr' in item.attr:
        ffCR = item.attr['cr']
      else:
        ffCR = None
      data_input = Reader(consumer.score, source = item.attr['name'],
        types = None,
        logger = consumer.logger, magicheader = False, unitable = isUni, ffConvert = ffConfig(ffnames, ffstarts, ffends, ffCR))
    elif item.name == "fromCSVFile":
      #We have a CSV file that needs special treatment to read in correctly
      isUni = True
      header = None
      sep = None
      types = None
      if 'header' in item.attr:
        header = item.attr['header']
      if 'sep' in item.attr:
        sep = item.attr['sep']
      if 'types' in item.attr:
        types = item.attr['types']
      data_input = Reader(consumer.score, source = item.attr['name'], logger = consumer.logger, magicheader = False, unitable = isUni, header = header, sep = sep, types = types, framing = 'EOF')
    elif item.name == "fromStandardInput":
      isUni = False
      filetype = None
      sep = None
      types = None
      framing = 'EOF'
      if 'sep' in item.attr:
        sep = item.attr['sep']
      if 'types' in item.attr:
        types = item.attr['types']
      if 'type' in item.attr:
        filetype = item.attr['type']
      if filetype == "UniTable":
        isUni = True
      if 'framing' in item.attr:
        framing = item.attr['framing']
      consumer.logger.debug('...Test')
      data_input = Reader(consumer.score, source = "-", logger = consumer.logger, magicheader = False, unitable = isUni, sep = sep, types = types, framing = framing)
    elif item.name == "fromHTTP":
      #get the stuff we need to setup the server
      input_url = item.attr['url']
      if port:
        input_port = int(port)
      else:
        input_port = int(item.attr['port'])
      datatype = None
      if 'type' in item.attr:
        datatype = item.attr['type']
      if datatype == "UniTable":
        callback = consumer.score_http_uni
      else:
        callback = consumer.score_http_xml
      
      #Create the server
      data_input = HTTPInterfaceServer(('',input_port), logger = consumer.logger)
      #Add the callback
      data_input.register_callback(input_url, callback)
    elif item.name == "eventBased":
      script_input = True
      data_input = False #Dummy value to get past a check for None later.
    else:
      #Not recognized
      consumer.logger.debug("Element %s is not a recognized child element of inputData, ignoring." % (item.name))
  
  #TODO: ??? What does the following comment refer to?
  #If summary data is being requested, set it up
  
  if data_input is None:
    #We made it through the config information without finding a data input source.
    raise ConfigurationError("Unable to determine data input source.")
  
  consumer.logger.debug("Initialize model")
  #Initialize the model
  #TODO: ??? What does the following comment refer to?
  #this is after the data information is input so that batch scoring may be faster
  consumer.initialize_model()
  
  if script_input:
    #Another script has called main, return the consumer so it can handle how score is called.
    return consumer
  
  consumer.logger.warning("Ready to score")
  #Start scoring data
  if consumer.metadata:
    # By default, for now, enable collection of
    # metadata by data reader and model (consumer general metadata
    # is enabled earlier).
    data_input.enableMetaDataCollection()
    consumer.model.enableMetaDataCollection()
  if consumer.batch_scoring:
    if consumer.metadata:
      consumer.metadata.log.info('Batch Scoring -One Score Per Segment\n')
    consumer.logger.debug("Batch Scoring")
    if isinstance(data_input, Reader):
      data_input.read_once()
      report = consumer.format_results(consumer.model.batchScore())
      if consumer.output_filename:
        consumer.output_report_header(file_handle = consumer.out)
        consumer.out.write(report)
        consumer.output_report_footer(file_handle = consumer.out)
        consumer.out.close()
  elif run_forever:
    if consumer.metadata:
      consumer.metadata.log.info('Run Forever - One Score Per Event')
    consumer.logger.debug("Run Forever")
    if isinstance(data_input, Reader):
      consumer.output_report_header()
      data_input.read_forever()
      consumer.output_report_footer(consumer.out)
    elif isinstance(data_input, HTTPServer):
      data_input.serve_forever()
    else:
      consumer.logger.critical("Reading data failed.")
  else: #just read once
    finished = False
    while not finished:
      if consumer.metadata is not None:
        consumer.metadata.log.info('Run Once - One Score Per Event')
        consumer.metadata.log.info('Start at %s'%datetime.datetime.now().isoformat())
      consumer.logger.debug("Run Once")
      if isinstance(data_input, Reader):
        consumer.output_report_header()
        data_input.read_once()
        consumer.output_report_footer()
      elif isinstance(data_input, HTTPServer):
        data_input.handle_request()
      else:
        consumer.logger.critical("Reading data failed.")
      if consumer.metadata:
        consumer.metadata.log.info('End at %s'%datetime.datetime.now().isoformat())
      if run_daemon:
        signal.signal(signal.SIGALRM, daemonRestartHandler)
        signal.signal(signal.SIGUSR1, daemonRestartHandler)
        signal.pause() # unix only
        finished = False
      else:
        finished = True
  if consumer.metadata:
    consumer.metadata['Stacksize after Scoring'] = ptools.stacksize()
    consumer.metadata['Resident Memory after Scoring'] = ptools.resident()/1e+9 #Gb
    consumer.metadata['Memory after Scoring'] = ptools.memory()/1e+9 #Gb
    consumer.metadata.collected['DataInput'] = data_input.getMetaData()
    #consumer.metadata.collected['Scoring'] = consumer.metadata.getMetaData()
    consumer.metadata.collected['Scoring'] = consumer.getMetaData()
    consumer.metadata.collected[''] = consumer.model.getMetaData()
    consumer.metadata.report()

if __name__ == '__main__':
  # configure the *root* logger.
  logging.basicConfig(level=logging.DEBUG)
  from optparse import OptionParser, make_option
  #define the options
  AUGUSTUS_CONSTS.check_python_version()
  usage = "usage: %prog [options]"
  version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
  options = [
    make_option("-c","--config",metavar="config",default='config.xml',help="The configuration file name"),
    make_option("-o","--outfile",metavar="outfile",help="The output file name"),
    make_option("-p","--port",metavar="num",help="The port number for the HTTP server")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  (options, arguments) = parser.parse_args()
  config = options.config
  main(options.config, options.outfile, options.port)
