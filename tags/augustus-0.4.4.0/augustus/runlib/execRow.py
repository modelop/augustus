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

class execRule:
  def __init__(self, string, error, globalDict={}, localDict={}):
    self.__rule = compile(string, error, "exec")
    self.globalDict = globalDict
    self.localDict = localDict
  
  def __call__(self):
    exec self.__rule in self.globalDict, self.localDict

class execColumn(execRule):
  def __init__(self, name, string, error, globalDict={}, localDict={}):
    self.__name = name
    self.__localDict = dict(localDict)
    if not self.__name in self.__localDict:
      self.__localDict[self.__name] = None
    execRule.__init__(self, string, error, globalDict, self.__localDict)
  
  def value(self):
    return self.localDict[self.__name]
  
  def name(self):
    return self.__name
  
  def revert(self):
    self.localDict[self.__name] = self.__last
  
  def __call__(self):
    self.__last = self.value()
    execRule.__call__(self)

class execRow(dict):
  def __init__(self, columns=[]):
    dict.__init__(self)
    self.__base = []
    self.__columns = []
    self.__updateForced = []
    self.extend(columns)
  
  def append(self, column):
    if not isinstance(column, execColumn):
      raise TypeError, "execRow expects execColumn instances"
    self.__base.append(column)
    self.__columns.append(column)
    self[column.name()] = column.value()
  
  def extend(self, columns):
    for column in columns:
      self.append(column)
  
  def reset(self):
    self.__columns = list(self.__base)
    self.clear()
    self.update()
  
  def update(self):
    for column in self.__columns:
      column()
      self[column.name()] = column.value()
  
  def lastUpdate(self):
    updates = []
    for column in self.__base:
      name = column.name()
      if not name in self:
        updates.append(column)
        self[name] = None
      elif name in self.__updateForced:
        updates.append(column)
    for column in updates:
      column()
      self[column.name()] = column.value()
  
  def limit(self, needed):
    remove = []
    cnt = 0
    for column in self.__columns:
      if not column.name() in needed:
        remove.append(cnt)
      cnt += 1
    remove.reverse()
    for entry in remove:
      del self[self.__columns[entry].name()]
      self.__columns.pop(entry)
  
  def force(self, needed):
    """Force these columns to update when lastUpdate is called."""
    for col in needed:
      if col not in self.__updateForced:
        self.__updateForced.append(col)
  
  def revert(self):
    for column in self.__columns:
      column.revert()
      self[column.name()] = column.value()


  def __getitem__(self, key):
    return dict.__getitem__(self, key)

