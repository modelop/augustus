#Augustus PMML consumer

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


from pmmlReader import *
from augustus.kernel.unitable.unitable import UniRecord
from augustus.runlib.any_reader import Reader, NativeElement
from augustus.runlib.http_server import *
from StringIO import StringIO
from sys import stdout
import logging
import httplib


class ffConfig:
  """ This is simply a container for elements needed to
      specify how to read from fixed format records
  """
  def __init__(self, fieldnames, fieldstarts, fieldends, fieldtypes = None, cr = None):
    self.fieldspecs = {}
    if (fieldtypes is None):
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
    
    if logger:
      self.logger = logger
    else:
      self.logger = logging.getLogger('consumer')
      self.logger.setLevel(logging.WARNING)
    self.logger.debug("Created")

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
      #self.format_results(o)      
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
      out = open(self.output_filename, 'a')
      out.write(report_string)
      out.close()
    return report_string
  
  def format_results(self, results):
    """This function takes a list of result lists and fills the values into the report format."""
    report_string = ""
    for result in results:
      anc_index=0
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
        #if ('True' in report_string):
        #print report_string
        #print report_args
        #print self.report_format
        #x=input('Report String')      
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
      if not file_handle:
        out = open(self.output_filename, 'a')
        out.write(self.report_header + '\n')
        out.close()
      else:
        file_handle.write(self.report_header + '\n')
  
  def output_report_footer(self, file_handle = None):
    """This function should be called after scoring is completed for a batch scoring"""
    if self.output_filename:
      if not file_handle:
        out = open(self.output_filename, 'a')
        out.write(self.report_footer + '\n')
        out.close()
      else:
        file_handle.write(self.report_footer + '\n')
  
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
              self.__initalize_report(x)
            elif x.name == "eventBased":
              self.event_based = True
            else:
              self.logger.warning("Element %s is not a recognized child element of output, ignoring." % (x.name))
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
              self.logger.propagate = False
            elif x.name == "toStandardOut":
              f = logging.StreamHandler(stdout)
              self.logger.addHandler(f)
              self.logger.propagate = False
            elif x.name == "toStandardError":
              f = logging.StreamHandler()
              self.logger.addHandler(f)
              self.logger.propagate = False
            else:
              self.logger.warning("Element %s is not a recognized child element of logging, ignoring." % (x.name))
        else:
          self.logger.warning("Element %s is not a recognized child element of pmmlDeployment, ignoring." % (child.name))
    else:
      #Raise a big fuss.
      self.logger.error("Got root element of: %s.  Expected: pmmlDeployment" % (data.name))
      raise ConfigurationError("Got root element of: %s.  Expected: pmmlDeployment" % (data.name))

  def __initalize_report(self, data):
    """This function parses the report portion of the configuration XML"""
    self.needed_cols = []
    self.report_argument_order = []
    format = ""
    self.report_header = '<' + data.attr['name'] + '>'
    for x in data:
      if x.name == "toFile":
        self.output_filename = str(x.attr['name'])
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
            self.logger.warning("Element %s is not a recognized child element of outputRow, ignoring." % (y.name))
        format += '</' + x.attr['name'] + '>'
      else:
        self.logger.warning("Element %s is not a recognized child element of report, ignoring." % (x.name))
    self.report_footer = '</' + data.attr['name'] + '>'
    self.report_format = format
  
  def initalize_model(self):
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
    model_reader.parse(model_source)
    self.myPMML = model_reader.root
    self.logger.debug("Initialize")
    self.myPMML.initialize(self.get_data, self.needed_cols, self.batch_scoring)
    self.logger.debug("find model")
    #Get the model
    self.model = self.myPMML.getChildrenOfType(pmmlModels)[0]

def main(config=None):
  """Main function for controling scoring.  Config, if used should be a string containing a filename where a configuration file can be found."""
  logging.basicConfig(level=logging.DEBUG)
  
  from optparse import OptionParser, make_option
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog 0.3.3"
  options = [
    make_option("-c","--config",metavar="config",default="config.xml",help="The configuration file name")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  if not config:
    (options, arguments) = parser.parse_args()
    config = options.config
  
  #Take in a bunch of options describing where everything is
  consumer = pmmlConsumer()
  consumer.logger.debug("Create Reader to get Configuration")
  config_reader = Reader(consumer.configure, source = str(config), magicheader = False, autoattr = False)
  consumer.logger.debug("Read Config File")
  config_reader.read_once()
  
  #Create any reader or http server to read in data
  data_input = None
  run_forever = True
  
  #Check to make sure that we don't try to iterate over None
  if consumer.data_input_info is None:
    raise ConfigurationError("Data input source missing from configuration.")
  
  for item in consumer.data_input_info:
    if item.name == "readOnce":
      run_forever = False
    elif item.name == "batchScoring":
      consumer.batch_scoring = True
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
      data_input = Reader(consumer.score, source = item.attr['name'], logger = consumer.logger, magicheader = False, unitable = isUni)
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
      data_input = Reader(consumer.score, source = item.attr['name'], logger = consumer.logger, magicheader = False, unitable = isUni, header = header, sep = sep, types = types)
    elif item.name == "fromStandardInput":
      isUni = False
      filetype = None
      if 'type' in item.attr:
        filetype = item.attr['type']
      if filetype == "UniTable":
        isUni = True
      data_input = Reader(consumer.score, source = "-", logger = consumer.logger, magicheader = False, unitable = isUni)
    elif item.name == "fromHTTP":
      #get the stuff we need to setup the server
      input_url = item.attr['url']
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
    else:
      #Not recognized
      consumer.logger.warning("Element %s is not a recognized child element of inputData, ignoring." % (item.name))
    
  if data_input is None:
    raise ConfigurationError("Unable to determine data input source.")
  consumer.logger.debug("Initialize model")
  #Initalize the model
  #this is after the data information is input so that batch scoring may be faster
  consumer.initalize_model()  
  consumer.logger.warning("Ready to score")
  #Start scoring data
  if consumer.batch_scoring:
    consumer.logger.debug("Batch Scoring")
    if isinstance(data_input, Reader):
      data_input.read_once()
      report = consumer.format_results(consumer.model.batchScore())
      if consumer.output_filename:
        out = open(consumer.output_filename, 'w')
        consumer.output_report_header(file_handle = out)
        out.write(report)
        consumer.output_report_footer(file_handle = out)
        out.close()
  elif run_forever:
    consumer.logger.debug("Run Forever")
    if isinstance(data_input, Reader):
      consumer.output_report_header()
      data_input.read_forever()
      consumer.output_report_footer()
    elif isinstance(data_input, HTTPServer):
      data_input.serve_forever()
    else:
      print "Reading data failed."
  else: #just read once
    consumer.logger.debug("Run Once")
    if isinstance(data_input, Reader):
      consumer.output_report_header()
      data_input.read_once()
      consumer.output_report_footer()
    elif isinstance(data_input, HTTPServer):
      data_input.handle_request()
    else:
      print "Reading data failed."

if __name__ == '__main__':
  main()
