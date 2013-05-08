#!/usr/bin/env python2.5

#path information for this project
import augustus.runlib.fileTree as fileTree
#timer class
import augustus.runlib.timer as timer
#easy way to make and use program options
from optparse import OptionParser, make_option
#vector based table class
import augustus.kernel.unitable.unitable as uni
#xml tools
from augustus.external.etree import ElementTree as ET
#basic math functions
import math
#the initial producer
import augustus.modellib.baseline.producer.Producer as Producer

def makeSegment(inf, segmentation, field):
  segments = ET.SubElement(segmentation, "explicitSegments")
  segments.set("field", field)
  for value in set(inf[field]):
    segment = ET.SubElement(segments, "segment")
    segment.set("value", str(value))

def makeConfigs(inFile, outFile, inPMML, outPMML):
  #open data file
  inf = uni.UniTable().fromfile(inFile)
  #start the configuration file
  root = ET.Element("model")
  root.set("input", str(inPMML))
  root.set("output", str(outPMML))
  test = ET.SubElement(root, "test")
  test.set("field", "volume")
  test.set("testStatistic", "zValue")
  test.set("testType", "threshold")
  test.set("threshold", "1.5")
  baseline = ET.SubElement(test, "baseline")
  baseline.set("dist", "gaussian")
  baseline.set("file", str(inFile))
  baseline.set("type", "UniTable")
  #create the segmentation declarations for the two fields
  segmentation = ET.SubElement(test, "segmentation")
  makeSegment(inf, segmentation, "fielddeviceid")
  makeSegment(inf, segmentation, "locationtimestamp")
  #output the configurations
  tree = ET.ElementTree(root)
  tree.write(outFile)
  
def makePMML(outFile):
  #create the pmml
  root = ET.Element("PMML")
  root.set("version", "3.1")
  header = ET.SubElement(root, "Header")
  header.set("copyright", " ")
  dataDict = ET.SubElement(root, "DataDictionary")
  dataField = ET.SubElement(dataDict, "DataField")
  dataField.set("name", "volume")
  dataField.set("optype", "continuous")
  dataField.set("dataType", "float")
  dataField = ET.SubElement(dataDict, "DataField")
  dataField.set("name", "fielddeviceid")
  dataField.set("optype", "categorical")
  dataField.set("dataType", "string")
  dataField = ET.SubElement(dataDict, "DataField")
  dataField.set("name", "locationtimestamp")
  dataField.set("optype", "categorical")
  dataField.set("dataType", "string")
  baselineModel = ET.SubElement(root, "BaselineModel")
  baselineModel.set("functionName", "baseline")
  miningSchema = ET.SubElement(baselineModel, "MiningSchema")
  miningField = ET.SubElement(miningSchema, "MiningField")
  miningField.set("name", "volume")
  miningField = ET.SubElement(miningSchema, "MiningField")
  miningField.set("name", "fielddeviceid")
  miningField = ET.SubElement(miningSchema, "MiningField")
  miningField.set("name", "locationtimestamp")
  #output to the file
  tree = ET.ElementTree(root)
  tree.write(outFile)

def main(project, options):
  #get the options
  file = options.file
  timing = options.timing
  #set the file names
  if timing:
    myTimer = timer.timer(start="Beginning timing", total="Lifetime of timer")
  base = file + "."
  analysis = project.volumeExample
  config = analysis.producer + (base + "xml")
  inPmml = analysis.producer + (base + "pmml")
  outPmml = analysis.consumer + (base + "pmml")
  table = project.data + file
  #make the configurations according to the data
  if timing:
    myTimer.output("Creating configuration file")
  makeConfigs(table, config, inPmml, outPmml)
  #make the input pmml
  if timing:
    myTimer.output("Creating input PMML file")
  makePMML(inPmml)
  #call the producer
  if timing:
    myTimer.output("Starting producer")
  Producer.main(config, timing, False)
  if timing:
    del myTimer

if __name__ == "__main__":
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog 0.2.6"
  options = [
    make_option("-f","--file",help="Name of data file"),
    make_option("-t","--timing",default=None,help="Output timing (information in % increments for scoring)")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  (options, arguments) = parser.parse_args()
  
  #call the program
  main(fileTree.project, options)