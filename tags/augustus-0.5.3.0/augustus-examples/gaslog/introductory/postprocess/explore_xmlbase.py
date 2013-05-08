import sys
from augustus.core import xmlbase

def checkForQuit():
    line = sys.stdin.readline()
    if line.startswith("q") or line.startswith("Q"):
        raise KeyboardInterrupt

try:
    print ""
    print "**************************************************"
    print "       Exploring xml elements using xmlbase"
    print "***************************************************"
    print "The purpose of this script is to demonstrate ways"
    print "to get information out of XML elements using"
    print "Augustus's xmlbase core utility.\n"
    print "It can be run, to show the results, first, and then"
    print "opened to see the actual commands used to that they"
    print "can be copied for use in ScoresAwk configuration files\n"
    
    print "Please hit the return key every time you want to continue through"
    print "this script, or type 'q' then return to quit."
    checkForQuit()
    
    filename = "../results/example_scores.xml"
    print "First, we want show a couple of lines from the actual file", filename ,"here:\n"
    with open(filename) as input_file:
        number_of_rows = 0
        for line in input_file:
            number_of_rows += 1
            if  number_of_rows <= 3:
                print "\t", line.strip()
    print "\t", line
    checkForQuit()

    print "To load an XML file using Augustus' xmlbase library, first include the library"
    print "from augustus.core import xmlbase  # type this at the top of a script"
    print "\nthen load the file:"
    print """
filename = "../results/example_scores.xml"
root_element = xmlbase.loadfile(filename)
    """
    checkForQuit()
    root_element = xmlbase.loadfile(filename)
    
    
    print "To access a tag of an xml element, use 'element.tag' for example,"
    print "if we type:"
    print ">>> root_element.tag"
    checkForQuit()
    print "we get:"
    print root_element.tag
    checkForQuit()
    
    print "The element's attributes are stored as a Python dictionary."
    print "The dictionary is named 'attrib', so for example,"
    print ">>> root_element.attrib"
    checkForQuit()
    print "gets:"
    print root_element.attrib
    checkForQuit()
    
    print "True or False, 'octavius' is in the root element's attributes:"
    print ">>> 'octavius' in root_element.attrib"
    checkForQuit()
    print "gets:"
    print 'octavius' in root_element.attrib
    checkForQuit()
    
    print "True or False, 'model' is in the root element's attributes:"
    print ">>> 'model' in root_element.attrib"
    checkForQuit()
    print "gets:"
    print 'model' in root_element.attrib
    checkForQuit()
    
    print "It will break the program to try to access an element that doesn't exist."
    print "Try to access 'octavius':"
    print "\n>>> try:"
    print "...     root_element.attrib['octavius']"
    print "... except KeyError:"
    print "...     print \"fail!\""
    checkForQuit()
    print "gets:"
    try:
        root_element.attrib['octavius']
    except KeyError:
        print "fail!"

    print "\n(This would have broken the program and made it exit if there was no try/except clause...)"
    checkForQuit()
    
    print "\nAccess an XML attribute's value like this:"
    print ">>> root_element[\"model\"]"
    checkForQuit()
    print "gets:"
    print root_element["model"]
    checkForQuit()
    
    print "\nAll of the child elements of an XML element (the nested items)"
    print "are contained in a list named 'children' within the parent"
    print "element.  It is accessed as: 'root_element.children', for example"
    print "\nprint \"The root element has\", len(root_element.children), \"children.\""
    checkForQuit()
    print "The root element has", len(root_element.children), "children."
    checkForQuit()
    
    print "List its first 5 children:",
    print """
>>> for child in root_element.children[0:5]:
...    print child
"""
    checkForQuit()
    print "gets:"
    for child in root_element.children[0:5]:
        print child
    checkForQuit()
    
    print "List its first child's children:",
    print """
>>> for child in root_element.children[0]:
...     print child
"""
    checkForQuit()
    for child in root_element.children[0]:
        print child
    checkForQuit()
    
    print "and its children have children too:",
    print """
>>> for child in root_element.children[0][0]:
...     print child
"""
    for child in root_element.children[0][0]:
        print child
    checkForQuit()
    
    print "Access the content between XML tags using 'element.content()'."
    print "for example,"
    print ">>> root_element.child(\"Event\").child(\"Segment\").child(\"date\").content()"
    checkForQuit()
    print root_element.child("Event").child("Segment").child("date").content()
    checkForQuit()
    
    print "\nThe 'child' function of an XML element will return the first occurrence"
    print "of an item in the list of children."
    print "\nNote that if you try to access an element that does not exist"
    print "using 'parent.child', you will get an error."
    checkForQuit()
    
    print "\nThe ScoresAwk program will use a 'condition' to test whether"
    print "an element with certain properties exist, and will only perform"
    print "the 'action' when the stated properties match the element we are"
    print "comparing to. Below is an example test.  First the functions to"
    print "check whether a condition is true:"
    print """
>>> def isPricePerGalSegment(element):
...     return element.tag == "Segment" and element["id"].startswith("pricePerGal")
... 
>>> def hasValidScore(element):
...     for child in element:
...         if child.tag == "score":
...             if child.content() != "Invalid":
...                 return True
...     return False
"""
    checkForQuit()
    def isPricePerGalSegment(element):
        return element.tag == "Segment" and element["id"].startswith("pricePerGal")
    
    def hasValidScore(element):
        for child in element:
            if child.tag == "score":
                if child.content() != "Invalid":
                    return True
        return False

    print "And next, the scripts to go through a few rows in the data and extract the score:"
    print """
>>> for event in root_element.children[48:51]:
...     print "\nChecking whether there is a valid score in event ", event["number"], ":"
...     for segment in event:
...         print "  Segment id:", segment["id"], "...",
...         if isPricePerGalSegment(segment):
...             if hasValidScore(segment):
...                 print "Yes! and the score is:", segment.child("score").content()
...             else:
...                 print "No...the score is invalid right now"
...         else:
                print "This segment doesn't have the price per gallon. Not looking at the score."
"""
    checkForQuit()
    
    for event in root_element.children[48:51]:
        print "\nChecking whether there is a valid score in event ", event["number"], ":"
        for segment in event:
            print "  Segment id", segment["id"], "...",
            if isPricePerGalSegment(segment):
                if hasValidScore(segment):
                    print "Yes! and the score is:", segment.child("score").content()
                else:
                    print "No...the score is invalid right now"
            else:
                print "This segment doesn't have the price per gallon. Not looking at the score."
    
    print "\n\nWe hope this helps you get started with Augustus's ScoresAwk tool."
except KeyboardInterrupt:
    print "\n\nSorry to see you go."

print "Let us know how we can make this introduction better.\n\nBye!"
