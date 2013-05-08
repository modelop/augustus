import os
import sys

def print3(*args, **kwds):
    invalidArguments = set(kwds.keys()).difference(set(["sep", "end", "file"]))
    if len(invalidArguments) > 0:
        raise TypeError("'%s' is an invalid keyword argument for this function" % invalidArguments[0])
    if "sep" not in kwds or kwds["sep"] is None: kwds["sep"] = " "
    if "end" not in kwds or kwds["end"] is None: kwds["end"] = os.linesep
    if "file" not in kwds or kwds["file"] is None: kwds["file"] = sys.stdout
    kwds["file"].write(kwds["sep"].join(map(str, args)))
    if kwds["end"] != "": kwds["file"].write(kwds["end"])

asciistr = str

if sys.version_info < (3,):
    str = unicode

else:
    basestring = str
    xrange = range
    long = int

# when we stop supporting Python 2, replace all
#     "basestring" -> "str"
#     "xrange" -> "range"
#     "long" -> "int"
#     "print3" -> "print"
# and remove
#     "asciistr"
