#!/usr/bin/env python

# Copyright (C) 2006-2011  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# This file is part of Augustus.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""a set of timer classes"""

import time

class timer:
  """your basic timer"""
  def _format(duration):
    """formats a given time in seconds to days, minutes, and seconds
    returns a string
    like:
    >>> timer._format(15)
    '(15.000 secs)  '
    >>> timer._format(60)
    '(1 mins )  '
    >>> timer._format(90061.001)
    '(1 days 1 hrs 1 mins 1.001 secs)  '"""
    secs = duration
    format = "("
    units = []
    if secs >= 60:  #number of seconds in a min
      if secs >= 3600:  #number of seconds in an hour
        if secs >= 86400:  #number of seconds in a day
          #calculate the number of days
          days = secs // 86400
          secs -= days * 86400
          format += "%d days "
          units.append(days)
        #calculate the number of hours
        hours = secs // 3600
        if hours:
          secs -= hours * 3600
          format += "%d hrs "
          units.append(hours)
      #calculate the number of minutes
      mins = secs // 60
      if mins:
        secs -= mins * 60
        format += "%d mins "
        units.append(mins)
    #calculate the number if seconds
    format += "%.3f secs)  "
    units.append(secs)
    return format % tuple(units)
  _format = staticmethod(_format)
  
  def __init__(self, start=None, end=None, total=None):
    """starts the timer
    records the output strings
    prints the start message, if any
    like:
    >>> timer = timer("Rar!", "Blah.", "Arg?")
    (0.000 secs)  Rar!
    >>> timer2 = timer(end="Mmph.", total="Ack!")
    >>> timer = None
    (0.001 secs)  Blah.
    (0.001 secs)  Arg?
    >>> del timer2
    (0.001 secs)  Mmph.
    (0.001 secs)  Ack!"""
    self.__base = -time.time()
    self.__start = self.__base
    self.__end = end
    self.__total = total
    if start:
      self.output(start)
  
  def __del__(self):
    """outputs the last two output strings, if any
    like:
    >>> timer = timer(end="Blah.", total="Arg?")
    >>> timer = None
    (0.001 secs)  Blah.
    (0.001 secs)  Arg?
    >>> timer = timer(end="Blah.", total="Arg?")
    >>> timer.output("I waited exactly 5 seconds!")
    (5.000 secs)  I waited exactly 5 seconds!
    >>> timer = None
    (0.001 secs)  Blah.
    (5.001 secs)  Arg?"""
    if self.__end:
      self.output(self.__end)
    if self.__total:
      print timer._format(self._total()) + str(self.__total)
  
  def _total(self):
    """returns the total life of the timer
    when a total string is given, this is the time output by del"""
    cur = time.time()
    return cur + self.__base
  
  def output(self, out):
    """outputs the given string and how long it has been since the last output"""
    cur = time.time()
    duration = self.__start + cur
    self.__start = -cur
    print timer._format(duration) + str(out)

class accumulatingTimer(timer):
  """this class accumulates all the time between the
    creation of its child objects and their deletion"""
  class __accumulator(timer):
    """this class calls a callback function when it is
      deleted passing the lifetime of the object into the
      callback function"""
    def __init__(self, callback):
      """record the callback function"""
      timer.__init__(self)
      self.__callback = callback
    
    def __del__(self):
      """call the callback function passing the lifetime of the timer"""
      self.__callback(self._total())
  def __init__(self, total=""):
    """record the message to be output, if any, and initialize
      the total time to 0"""
    self.__total = total
    self.__time = 0
  
  def __add(self, time):
    """callback function for child objects"""
    self.__time += time
  
  def new(self):
    """create and return a new child object"""
    return accumulatingTimer.__accumulator(self.__add)
  
  def __del__(self):
    """print the total accumulated time and the message to be output, if any"""
    self.output(self.__total)
  
  def output(self, out):
    """outputs the given string and the total time accumulated from all deleted child objects"""
    print timer._format(self.__time) + str(out)

class debugTimers:
  """this class keeps a list of named accumulating timers
  each timer's name is what it will output when it is deleted"""
  def __init__(self, names=[]):
    """initialize the accumulating timers"""
    self.__timers = {}
    for name in names:
      name = str(name)
      self.__timers[name] = accumulatingTimer(name)
  
  def __del__(self):
    """ouput the timing information for the time accumulated for each timer"""
    for name in self.__timers:
      del self.__timers[name]
  
  def new(self, name):
    """return a new child object of the given timer"""
    return self.__timers[str(name)].new()
