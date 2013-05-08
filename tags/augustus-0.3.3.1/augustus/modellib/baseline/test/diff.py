"""checks to see if two xml files contain fundamentally different trees"""

#easy way to make and use program options
from optparse import OptionParser, make_option
#xml tools
import elementtree.ElementTree as ET

def main():
  #define the options
  usage = "usage: %prog [options]"
  version = "%prog 0.2.6"
  options = []
  parser = OptionParser(usage=usage, version=version, option_list=options)
  
  #parse the options
  (options, arguments) = parser.parse_args()
  
  first = ET.parse(arguments[0]).getroot()
  firstChildren = []
  for child in first:
    children = {}
    for x in child:
      children[x.tag] = x.text
    firstChildren.append(children)
  second = ET.parse(arguments[1]).getroot()
  secondChildren = []
  for child in second:
    children = {}
    for x in child:
      children[x.tag] = x.text
    secondChildren.append(children)
  
  differ = False
  for child in firstChildren:
    if not child in secondChildren:
      differ = True
      break
  
  if differ:
    print "Score files differ"

if __name__ == "__main__":
  main()
