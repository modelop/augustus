#path information for this project
import augustus.runlib.fileTree as fileTree
#easy way to make and use program options
from optparse import OptionParser, make_option
#vector based table class
import augustus.kernel.unitable.unitable as uni
#xml tools
from augustus.external.etree import ElementTree as ET
#the initial consumer
import augustus.pmmllib.pmmlConsumer as Consumer
import augustus.const as AUGUSTUS_CONSTS


def makeConfigs(config, dataFile, pmml, scores):
  #create the configurations
  root = ET.Element("pmmlDeployment")
  data = ET.SubElement(root, "inputData")
  ET.SubElement(data, "readOnce")
  ET.SubElement(data, "batchScoring")
  temp = ET.SubElement(data, "fromFile")
  temp.set("name", str(dataFile))
  temp.set("type", "UniTable")
  model = ET.SubElement(root, "inputModel")
  temp = ET.SubElement(model, "fromFile")
  temp.set("name", str(pmml))
  output = ET.SubElement(root, "output")
  report = ET.SubElement(output, "report")
  report.set("name", "report")
  temp = ET.SubElement(report, "toFile")
  temp.set("name", str(scores))
  row = ET.SubElement(report, "outputRow")
  row.set("name", "event")
  column = ET.SubElement(row, "score")
  column.set("name", "score")
  column = ET.SubElement(row, "alert")
  column.set("name", "alert")
  column = ET.SubElement(row, "segments")
  column.set("name", "segments")
  logging = ET.SubElement(root, "logging")
  ET.SubElement(logging, "toStandardError")
  #output the configs
  tree = ET.ElementTree(root)
  tree.write(config)

def main(project, options):
  #get the options
  baseline = options.baseline
  file = options.file
  #construct the file names
  bBase = baseline + "."
  base = file + "."
  analysis = project.WeightedBatch
  pmml = analysis.consumer + (bBase + "pmml")
  config = analysis.consumer + (base + bBase + "xml")
  scores = analysis.postprocess + (base + bBase + "xml")
  table = project.data + file
  #make the configuration file
  makeConfigs(config, table, pmml, scores)
  #call the consumer
  Consumer.main(config)

if __name__ == "__main__":
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
  options = [
    make_option("-b","--baseline",help="Name of baseline data file"),
    make_option("-f","--file",help="Name of data file")]
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  (options, arguments) = parser.parse_args()
  
  #call the program
  main(fileTree.project, options)
