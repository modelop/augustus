
"""Generic database access wrapper

Provides a database independent wrapper which supports commonly
used calls of the python DB-API.  This can be used to hide the
specific database in use from application code, or to stub-out
database calls in applications that may not need an actual
database for running (eg. when database is used for logging
purposes, etc)

"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
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


import logging

################################################################

class ConnectFactory(object):
  """Multi-purpose connect() call factory
  
  By default, acts like a transparent wrapper to MySQLdb.
  If execlog is specified, then all SQL execute statements
  are logged to file execlog.
  If dbtype is set to None, then all db access is stubbed
  (but the execlog, if specified, is still used).
  If dbtype is set to another db name, then that module is
  imported instead of MySQLdb, and used instead.

  In typical use, instead of:

    import MySQLdb
    db = MySQLdb.connect(db=dbname,user=username,passwd=password)

  use wrapper as follows:

    from any_db import ConnectFactory
    DB = ConnectFactory(dbtype='MySQLdb',execlog='/tmp/sqlexec.log')
    db = DB.connect(db=dbname,user=username,passwd=password)

  """

  ##############################################################
  # supporting classes

  class wrapped_connection:
    def __init__(self,factory,dbconnect,*args,**kwargs):
      self._factory = factory
      self._dbconnect = dbconnect
      self._args = args
      self._kwargs = kwargs
      self.cnx = self._run_connect()

    def _run_connect(self):
      '''init or re-run db connection'''
      if self._dbconnect is None:
        return None
      cnx = self._dbconnect(*self._args,**self._kwargs)
      return cnx

    def reset_connection(self):
      self.close()
      self.cnx = self._run_connect()
      return self.cnx
    
    def __del__(self):
      self.close()
    def close(self):
      if self.cnx is not None:
        self.cnx.close()
    def ping(self):
      if self.cnx is not None:
        self.cnx.ping()

    def cursor(self):
      cur = self._cursor()
      out = self._factory.wrapped_cursor(self._factory,self,cur)
      return out

    def _cursor(self):
      if self.cnx is not None:
        cur = self.cnx.cursor()
      else:
        cur = None
      return cur


  class wrapped_cursor:
    def __init__(self,factory,cnx,cur):
      self._factory = factory
      self._cnx = cnx
      self.cur = cur
    def __del__(self):
      self.close()
    def close(self):
      if self.cur is not None:
        self.cur.close()
    def __iter__(self):
      return iter(self.fetchone,None)

    def fetchone(self):
      if self.cur is None:
        return None
      row = self.cur.fetchone()
      if row is not None:
        row = self._factory.wrapped_row(self._factory,row)
      return row

    def execute(self,sql,args=None):
      self._factory._report_execute(sql,args)
      if self.cur is None:
        return 1
      # This is intended to catch the case where the MySQLdb is
      # dropped at the far end due to inactivity (after ~2 days)
      # It should also handle the case where the database
      # crashes or is restarted, etc.
      # On an db related execute error, try to reconnect and get
      # a new cursor, then retry the execute once.
      try:
        return self._execute(sql,args)
      except self._cnx.cnx.DatabaseError, msg:
        self._factory._report_error('DatabaseError: %s (trying reconnect...)' % msg)
      try:
        self._cnx.reset_connection()
      except self._cnx.cnx.DatabaseError, msg:
        self._factory._report_error('DatabaseError: %s (reconnect failed...)' % msg)
        return None
      try:
        self.cur = self._cnx._cursor()
      except self._cnx.cnx.DatabaseError, msg:
        self._factory._report_error('DatabaseError: %s (reconnect cursor failed...)' % msg)
        return None
      try:
        return self._execute(sql,args)
      except self._cnx.cnx.DatabaseError, msg:
        self._factory._report_error('DatabaseError: %s (reconnect execute failed for %r)' % (msg,sql))
        return None

    def _execute(self,sql,args=None):
      return self.cur.execute(sql,args)


  class wrapped_row(list):
    def __init__(self,factory,row):
      self._factory = factory
      list.__init__(self,row)

  ##############################################################

  def __init__(self,dbtype='MySQLdb',execlog=None,logger=None):
    self.logger = logger or logging.getLogger()
    self._dbtype = dbtype
    if dbtype is not None:
      dbmodule = __import__(dbtype)
      self._dbconnect = dbmodule.connect
      # this should always be present for Python DB-API v2
      #self._dbexcept = dbmodule.DatabaseError
    else:
      self._dbconnect = None
      #self._dbexcept = StandardError
    self._execlog = execlog
    if execlog is not None:
      if hasattr(execlog,'close'):
        loghandle = execlog
        logwrite = loghandle.write
      elif callable(execlog):
        loghandle = None
        logwrite = execlog
      else:
        loghandle = file(execlog,'wb')
        logwrite = loghandle.write
    else:
      loghandle = None
      logwrite = None
    self._loghandle = loghandle
    self._logwrite = logwrite

  def _report_execute(self,sql,args={}):
    if self._logwrite is None: return
    # do not let logging error take down the application
    try:
      out = sql % args
    except:
      out = 'sql=%r args=%r' % (sql,args)
    try:
      self._logwrite(out+'\n')
    except:
      pass

  def _report_error(self,msg,*args):
    try:
      self.logger.error(msg,*args)
    except:
      self.logger.error('internal error in any_db module')

  def __call__(self,*args,**kwargs):
    return self.connect(*args,**kwargs)

  def connect(self,*args,**kwargs):
    out = self.wrapped_connection(self,self._dbconnect,*args,**kwargs)
    return out



################################################################
################################################################
################################################################
# simple connect wrapper, defaults to MySQLdb when not stubbed

def connect(_use_wrappers=False,_use_stubs=False,**kwargs):
  if _use_stubs:
    db = stub_database()
  else:
    import MySQLdb
    db = MySQLdb
  cnx = db.connect(**kwargs)
  if _use_wrappers:
    cnx = any_connection(cnx)
  return cnx

################################################################
# stub classes, just let the system run for specific tests

class stub_database:
  def connect(self,**kwargs):
    return stub_connection()

class stub_connection:
  def close(self): pass
  def cursor(self): return stub_cursor()

class stub_cursor:
  def close(self): pass
  def fetchone(self):
    return None
  def __iter__(self):
    return iter(self.fetchone,None)
  def execute(self,sql,args=None):
    print sql
    return 1


################################################################
# wrapper classes, pass-thru calls but allow for
# future interception for test suite and/or logging

import pprint

class any_connection:
  def __init__(self,cnx):
    self.cnx = cnx
  def __del__(self):
    self.close()
  def close(self):
    self.cnx.close()
  def cursor(self):
    cur = self.cnx.cursor()
    out = any_cursor(cur)
    return out

class any_cursor:
  def __init__(self,cur):
    self.cur = cur
  def __del__(self):
    self.close()
  def close(self):
    self.cur.close()
  def fetchone(self):
    row = self.cur.fetchone()
    if row is not None:
      row = any_row(row)
    return row
  def __iter__(self):
    return iter(self.fetchone,None)
  def execute(self,sql,args=None):
    #pprint.pprint(sql % args)
    return self.cur.execute(sql,args)

class any_row(list):
  def __init__(self,row):
    list.__init__(self,row)
    #pprint.pprint(row)

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
