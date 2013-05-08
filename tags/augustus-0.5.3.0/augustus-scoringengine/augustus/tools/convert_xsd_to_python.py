import re
import xml.etree.cElementTree as ElementTree

all = ElementTree.ElementTree(file=file("pmml-4-1.xsd"))

elementbase = """class %(PythonName)s(PMML):
    xsd = load_xsdElement(PMML, \"\"\"
    %(XSDText)s\"\"\")

PMML.classMap[\"%(XMLName)s\"] = %(PythonName)s
"""

typebase = """PMML.xsdType[\"%(XMLName)s\"] = load_xsdType(\"\"\"
    %(XSDText)s\"\"\")
"""

groupbase = """PMML.xsdGroup[\"%(XMLName)s\"] = load_xsdGroup(\"\"\"
    %(XSDText)s\"\"\")
"""

print "############################################################################### PMML types\n"

for element in all.getroot().getchildren():
    XMLName = element.attrib["name"]
    XSDText = ElementTree.tostring(element).replace("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" ", "")
    PythonName = XMLName.replace("-", "_")
    if XMLName == "PMML": PythonName = "root"

    if element.tag in ("{http://www.w3.org/2001/XMLSchema}simpleType", "{http://www.w3.org/2001/XMLSchema}complexType"):
        print typebase % vars()

print "############################################################################### PMML groups\n"

for element in all.getroot().getchildren():
    XMLName = element.attrib["name"]
    XSDText = ElementTree.tostring(element).replace("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" ", "")
    PythonName = XMLName.replace("-", "_")
    if XMLName == "PMML": PythonName = "root"

    if element.tag == "{http://www.w3.org/2001/XMLSchema}group":
        print groupbase % vars()

print "############################################################################### PMML elements\n"

for element in all.getroot().getchildren():
    XMLName = element.attrib["name"]
    XSDText = ElementTree.tostring(element).replace("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" ", "")
    PythonName = XMLName.replace("-", "_")
    if XMLName == "PMML": PythonName = "root"

    if element.tag == "{http://www.w3.org/2001/XMLSchema}element":
        print elementbase % vars()

        if XMLName not in ("Array", "Indices", "INT-Entries", "REAL-Entries") and len(element.getchildren()) != 0:
            if len(element.getchildren()) != 1:
                raise Exception(len(element.getchildren()))

            if element.getchildren()[0].tag != "{http://www.w3.org/2001/XMLSchema}complexType":
                raise Exception(element.getchildren()[0].tag)
