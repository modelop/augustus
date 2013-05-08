#all the xml handling in this file should be replaced with a real xml parser...
#didn't want to mess with it since it's just an example
#I know...I'm quite possibly the worst example creator ever! ;)

from pmmlReader import *
import augustus.kernel.unitable.unitable as uni

def get(field):
  #Returns an array of values that coresponds to the field name
  if inputValues.has_key(field):
    return inputValues[field]
  return []

def output(row):
  """only prints one returned score"""
  #Format output
  out = "  <" + record + ">"
  cnt = 0
  length = len(row)
  for name in names:
    if cnt < length:
      out += "<" + name + ">" + str(row[cnt]) + "</" + name + ">"
    else:
      break
    cnt += 1
  out += "</" + record + ">\n"
  return out

#Read in the report format
#WARNING: major hack
inf = open("distribution.xml")
names = []
for line in inf:
  start = line.find('"')
  if start > -1:
    name = line[start+1:]
    name = name[:name.find('"')]
    names.append(name)
inf.close()

#Read in the pmml file and set the "get value function"
myReader = pmmlReader()
myReader.parse("sample_events.pmml")
myPMML = myReader.root
myPMML.updateInputFunctions(get)

#open the output file
out = open("sample_events.out", "w")

#read in evente
myTable = uni.UniTable()
myTable.from_any_file("sample_events.nab")
rows = len(myTable)

#will hold fifty (or whatever step is set to) values at a time
inputValues = {}

#Gets the model for convience and speed's sake
model = myPMML.getChildrenOfType(pmmlModels)[0]

#Tell the model which fields we'll want back out for reporting
model.initialize(["Auth_Dt"])

#cache 50 rows at a time
cnt = 0
step = 50
top = names[0]
record = names[1]
names = names[2:]
out.write("<" + top + ">\n")
while cnt < rows:
  #input the values
  inputValues["F25_Cond_cd"] = [entry for entry in myTable["F25_Cond_cd"][cnt:cnt+step]]
  inputValues["F60_MOTO_ECI"] = [entry for entry in myTable["F60_MOTO_ECI"][cnt:cnt+step]]
  inputValues["Auth_Dt"] = [entry for entry in myTable["Auth_Dt"][cnt:cnt+step]]
  inputValues["count"] = [count for count in myTable["count"][cnt:cnt+step]]
  inputValues["F39_resp_cd"] = [entry for entry in myTable["F39_resp_cd"][cnt:cnt+step]]
  #score the values
  mapped = model.score()
  #print the output
  for row in mapped:
    out.write(output(row))
  cnt += step
#don't forget to call the function to output the last tag
out.write("</" + top + ">")
out.close()
