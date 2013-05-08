"""
User/Application Specific code for handling various scoring occurences.
"""

__copyright__ = """
Copyright (C) 2005-2007  Open Data ("Open Data" refers to
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
class UserHandlers:
  def __init__(self,opt,dbname,uname,pwd):
    # User deals with whatever db or files they choose-divorced 
    # from scoring engine proper.
    # For this particular application, a db instance was used.
    from runlib.any_db import ConnectFactory
    self.log=logging.getLogger('UserCode')
    DB=ConnectFactory(dbtype=opt.sql_db,execlog=opt.sql_log,logger=self.log)
    self.db=DB.connect(user=str(uname),passwd=str(pwd),db=dbname)
    self.cv=self.db.cursor()

  def userHandler(self,signal,timeIn,modelName,udat=None,i1=101,f1=100.0):
    from Alerts import RFAlert
    from userMySQLInterface import insertRFAlert,insertEv
    if (signal=='alert'):
      _score=udat
      _alert_=RFAlert(i1,f1,str(timeIn),modelName,score)
    elif (signal=='event'):
      _data=udat
      evnum=i1
      insertEv(self.cv,evnum,_data,'events')
    elif (signal=='exit'):
      self.db.close()
