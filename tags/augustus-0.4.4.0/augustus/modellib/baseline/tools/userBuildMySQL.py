#!/usr/bin/env python

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

from userMySQLInterface import make_rfFVtable,make_mtiFVtable,makeAlertTable,makeEventTable,makeMonitoringTable
from config import Config
import MySQLdb
import augustus.const as AUGUSTUS_CONSTS

#----------------------------------------------------------------------

def main():
    """handle user command when run as top level program"""
    from optparse import OptionParser, make_option

    usage = 'usage: %prog '
    version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
    option_list = [
      make_option('-v','--verbose',default=False,help='verbose output (default "False")'),
    ]

    parser = OptionParser(usage=usage,version=version,option_list=option_list)
    (opt,args) = parser.parse_args()
    if not args:
        args=[]
    userBuildMySQL(opt.verbose)

#----------------------------------------------------------------------
def userBuildMySQL(verbose):
    """
    Script to zero out and build tables from scratch.
    Tables produced:
    rfFV
    mtiFV
    alerts
    events
    """
    user_config=Config('bootstrapConfig')
    dbname=user_config.get('database_name')
    uname=user_config.get('database_user')
    pwd=user_config.get('database_upwd')
    rname=user_config.get('database_root')
    rpwd=user_config.get('database_rpwd')

    # try to connect as the user. If it doesn't work, try to use
    # the root account to grant the user permissions.
    try:
        sql = "USE " + str (dbname)
        db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
        cv = db.cursor ()
        
        cv.execute (sql)

        cv.close ()
        db.close ()
    except:
        db=MySQLdb.connect(user=rname,passwd=rpwd)
        cv = db.cursor ()

        sql = "CREATE DATABASE IF NOT EXISTS " + dbname
        cv.execute (sql)
    
        sql = "GRANT ALL PRIVILEGES ON " + dbname + ".* TO %s"\
      "@'localhost'"

        if (pwd != None and pwd != ''):
            sql += " IDENTIFIED BY %s"
            cv.execute (sql, (uname, pwd))
        else:
            cv.execute (sql, uname)

        cv.close ()
        db.close ()

    # try to set up the tables
    #db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    #cv=db.cursor()
    #sql="USE "+str(dbname)
    #cv.execute(sql)

    #cv.close ()
    #db.close ()
    if (verbose):
        print 'Creating Tables. First Signal Feature Vectors'
    make_rfFVtable(dbname,uname,pwd)
    if (verbose):
        print '                 MTI Feature Vectors'
    make_mtiFVtable(dbname,uname,pwd)
    if (verbose):
        print '                 Alerts'
    makeAlertTable(dbname,uname,pwd)
    if (verbose):
        print '                 Events'                      
    makeEventTable(dbname,uname,pwd)
    if (verbose):
        print '                 Monitoring'
    makeMonitoringTable(dbname,uname,pwd)

