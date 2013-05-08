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

from augustus.kernel.unitable import *
from augustus.external.etree import ElementTree as ET
import sys
import os
def parse(configFile=None):
  consumer = os.path.dirname(os.path.abspath(configFile))
  config = ET.parse(configFile)
  root = config.getroot()
  structure = root.find('DirectoryStructure')
  producerstructure = structure.find('Producer')
  producer = os.path.join(consumer,producerstructure.find('Home').text)
  try:
    producerConfig = producerstructure.find('Config').text
  except:
    producerConfig = None
  if producerConfig is not None:
    if not(os.path.isfile(producerConfig)):
      producerConfig = None
  if producerConfig is None:
    try:
      modelData = producerstructure.find('ModelData').text
    except:
      return 'FAIL : Neither the producer config nor the model data are specified!'
    # assume model data is relative to producer home
    #print modelData
    modelData = os.path.join(producer,modelData) 
    #if not(os.path.isfile(modelData)):
    #  #'Model Data needs to be specified as relative to producer home!'
    #  return 1

  postprocessing = os.path.join(consumer,structure.find('Postprocessing').text)
  reports = os.path.join(consumer,structure.find('Reports').text)

  # In the particular case of temp, if it doesn't work as
  # relative directory, try absolute case.
  temp = structure.find('Temp').text
  if not os.path.exists(os.path.join(consumer,temp)):
    temparea = temp
  else:
    temparea = os.path.join(consumer,temp)

  inputModel = root.find('inputModel')
  pmmlfile = os.path.join(consumer,inputModel.find('fromFile').attrib['name'])
  outputfile=None
  output = root.find('output')
  report = output.find('report')
  try:
    outputfile = report.find('toFile').attrib['name']
  except:
    print 'No output file in use!'
    sys.exit(1)

  scoresdir = os.path.dirname(os.path.join(consumer,outputfile))
  return consumer, producer, modelData, postprocessing, reports, temparea, scoresdir, pmmlfile

def getInstructions(configFile):
  instructions = {'preprocessing':[],'postprocessing':[]}
  config = ET.parse(configFile)
  root = config.getroot()
  structure = root.find('Processing')
  if structure is not None :
    preprocessing = structure.find('Preprocess')
    consumer = structure.find('Consumer')
    if consumer is not None:
      instructions['consumer']=True  
    producer = structure.find('Producer')
    postprocessing = structure.find('Postprocess')
    if postprocessing is not None:
      postSteps = postprocessing.findall('Instruction')
      if len(postSteps)>0:
        instructions['postprocessing']=[p.text for p in postSteps]    
  else:
    preprocessing = None
    instructions['consumer'] = True
  if (preprocessing is not None):
    preSteps = preprocessing.findall('Instruction')
    if len(preSteps)>0:
      instructions['preprocessing']=[p.text for p in preSteps]    

  return instructions

if __name__=="__main__":
  print('%s %s %s %s %s %s %s %s\n'%parse(sys.argv[1]))
