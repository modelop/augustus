"""This module provides a generic path class that hides os issues and
provides a tree class which will automatically search for and add all
child directories as attributes.

Ignores child directories which begin with the character '.'
assume that there is a directory structure as follows::

  /temporary
  /temporary/first
  /temporary/second
  /temporary/second/secondFirst
  /temporary/third
  /temporary/third/1
  /temporary/third/.rar
  /temporary/third/2
  /temporary/third/.5
  /temporary/third/me

  >>> #path examples:
  >>> #on unix/linux
  >>> x = path("/temporary/")
  >>> x = x + "x" + str(15) + ".y"
  >>> print x
  /temporary/x/15/.y
  >>> x = path("/temporary/")
  >>> x = x + ("x" + str(15) + ".y")
  >>> print x
  /temporary/x15.y
  >>> x = path("/etc/")
  >>> print x
  /etc
  >>> x = path("/etc")
  >>> print x
  /etc
  >>> x = path("second/secondFirst")
  >>> print x
  second/secondFirst
  >>> x = x + "output.txt"
  >>> print x
  second/secondFirst/output.txt
  >>> y = "/temporary/" + x
  >>> print y
  /temporary/second/secondFirst/outut.txt
  >>> y = "/temporary" + x
  >>> print y
  /temporary/second/secondFirst/outut.txt
  >>> #on windows
  >>> x = path("c:\\windows\\desktop\\")
  >>> print x
  c:\windows\desktop
  >>> x = path("c:\\windows\\desktop")
  >>> print x
  c:\windows\desktop
  >>> #file tree examples:
  >>> x = fileTree("/temporary")
  >>> print x.third.me
  /temporary/third/me
  >>> print x.second.rar.out
  Traceback (most recent call last):
    File "<stdin>", line 1, in ?
  AttributeError: 'fileTree' object has no attribute 'rar'
  >>> print x.first
  /temporary/first
  >>> print x.second
  /temporary/second"""

import os

class path(str):
  """represents a path
  always terminates without a separator"""
  def __new__(cls, value):
    """creates a new path object removing any terminating os.sep and
    any intermediate copies of the separator"""
    value = value.replace(os.sep + os.sep, os.sep)
    if value[-len(os.sep):] != os.sep:
      return str.__new__(cls, value)
    else:
      return str.__new__(cls, value[:-len(os.sep)])
  
  def __add__(self, other):
    """appends os.sep and the new file name to the current path
      and returns it as a new path object"""
    return path(str.__add__(str.__add__(self, os.sep), other))
  
  def __radd__(self, other):
    """appends os.sep and the current path to the new file name
      and returns it as a new path object"""
    return str.__add__(str.__add__(path(other), os.sep), self)

class fileTree(path):
  """represents a directory tree"""
  def __init__(self, directory):
    """dynamically searches for and adds sub-directories as attributes
    ignores all directories beginning with a period"""
    path.__init__(directory)
    subs = os.listdir(directory)
    for entry in subs:
      name = entry
      entry = self + entry
      if os.path.isdir(entry) and (not os.path.islink(entry)) and (not entry[0] == '.'):
        self.__dict__[name] = fileTree(entry)

"""automatically publishes any project information available"""
try:
  project = fileTree(os.path.expandvars("$PROJECT"))
except:
  project = None
