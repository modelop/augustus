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

from modellib.baseline.consumer.Alerts import Alert,TaggedAlert
from time import gmtime,time,strftime,asctime,localtime
from modellib.baseline.consumer.benchmark import Benchmarking
import os
#from _mysql_exceptions import *
import stat
import MySQLdb
import sys
import datetime
import re
import StringIO

def makeEventTable(dbname,uname,pwd):
    # Make table "events".
    # should be used from root account (*sigh*, why not the mysql acct?)
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS events"
    cv.execute(sql)
    sql="CREATE TABLE events (record INT, type VARCHAR(10), identifier VARCHAR(80), \
        freq DOUBLE, bandwidth DOUBLE, strength DOUBLE, lat DOUBLE, longitude DOUBLE, range DOUBLE, lobAz DOUBLE, lobEl DOUBLE, birth INT, zulutime INT)"
    cv.execute(sql)
    cv.close()
    db.close ()

def makeMonitoringTable(dbname,uname,pwd):
    # Make "monitoring" table for benchmarking performance.
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS monitoring"
    cv.execute(sql)
    sql="CREATE TABLE monitoring (record INT,\
         nFV INT, \
         nHash INT, \
         nMatch INT, \
         nAlert INT, \
         eventIO DOUBLE, \
         matching DOUBLE, \
         scoring DOUBLE, \
         workflow DOUBLE, \
         updates DOUBLE, \
         throughput DOUBLE, \
         sum DOUBLE)"
    cv.execute(sql)
    cv.close()
    db.close ()

def makeAlertTable(dbname,uname,pwd):
# Make "alert" table.
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS alerts"
    cv.execute(sql)
    sql="CREATE TABLE alerts (ind INT, model varchar(80), featureVector INT, tag varchar(80), \
    scoredVariable varchar(80), birth DATE, alerttime DATETIME,score DOUBLE)"
    cv.execute(sql)
    cv.close()
    db.close ()

def make_rfFVtable(dbname,uname,pwd):
# Make rfFV table in osc database.
# should be used from root account (*sigh*, why not the mysql acct?)
# This table describes feature vectors ID'd by a frequency, corresponding
# to interesting communication channels.  An entry is made at the time
# at which the fv is constructed.
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS rfFV"
    cv.execute(sql)
    sql="CREATE TABLE rfFV (ind INT, id INT, freq DOUBLE, freqWindow DOUBLE, \
        lobcosphi DOUBLE, lobsinphi DOUBLE, type VARCHAR(10),entrydate DATE, birth DATE, nalerts INT)"
    cv.execute(sql)
    # sql="DESCRIBE rfFV"
    # cv.execute(sql)
    # shout(cv)
    cv.close()
    db.close ()

def make_mtiFVtable(dbname,uname,pwd):
# Make rfFV table in osc database.
# should be used from root account (*sigh*, why not the mysql acct?)
# This table describes feature vectors ID'd by a frequency, corresponding
# to interesting communication channels.  An entry is made at the time
# at which the fv is constructed.
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS mtiFV"
    cv.execute(sql)
    sql="CREATE TABLE mtiFV (ind INT, id INT, latitude DOUBLE, longitude DOUBLE,vradial DOUBLE,\
    direction DOUBLE, entrydate DATE, birth DATE, nalerts INT)"
    cv.execute(sql)
    # sql="DESCRIBE mtiFV"
    # cv.execute(sql)
    # shout(cv)
    cv.close()
    db.close ()

def makeModelTable(dbname,uname,pwd):
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS models"
    cv.execute(sql)
    sql="create table models (\
        name varchar(80) NOT NULL PRIMARY KEY, \
        pmmlfile longtext NOT NULL,\
        comment varchar(80),\
        userid integer,    \
        login varchar(20), \
        groupid integer,   \
        atime varchar(30),\
        mtime varchar(30),\
        ctime varchar(30), \
        mode integer      \
    );"
    cv.execute(sql)
    sql="create index files_pathnum_index on models ( name );"
    cv.execute(sql)
    sql="DESCRIBE models"
    cv.execute(sql)
    #shout(cv)
    cv.close()
    db.close ()
    return

def makeConfigTable(dbname,uname,pwd):
    db=MySQLdb.connect(user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="USE " + dbname
    cv.execute(sql)
    sql="DROP TABLE IF EXISTS configs"
    cv.execute(sql)
    sql="create table configs (\
        name varchar(80) NOT NULL PRIMARY KEY, \
        configfile longtext NOT NULL,\
        comment varchar(80),\
        userid integer,    \
        login varchar(20), \
        groupid integer,   \
        atime varchar(30),\
        mtime varchar(30),\
        ctime varchar(30), \
        mode integer      \
    );"
    cv.execute(sql)
    sql="create index files_pathnum_index on configs ( name );"
    cv.execute(sql)
    sql="DESCRIBE configs"
    cv.execute(sql)
    #shout(cv)
    cv.close()
    db.close()
    return

def insertEv(cv,irec,e,evtable,log=sys.stdout):
    freq=bandwidth=strength=lobAz=lobEl=lat=long=range=vrad=direction=0.0
    identifier=None
    try:
            identifier=e.get['missionID']
            type='SOI'
            try: freq=e.get['frequencyOfInterestHz']
            except: pass
            try: bandwidth=e.get['acquisitionBandWidthHz']
            except: pass
            try: lobAz=e.get['trueAzimuthDeg']
            except: pass
            try: lobel=e.get['elevationDeg']
            except: pass
    except:
            pass
    try:
            identifier=e.aoi_id
            type='MTI'
    except:
            pass
    if (identifier is None): 
        msg=os.linesep+"\tCannot insert event of unknown origin. "
        msg+=os.linesep+"\tinto MySQLdb with  insertEv function."
        log.warning(msg)
    else:
        try:
            sql='SELECT CURDATE()'
            cv.execute(sql)
            date=cv.fetchone()[0]
            sql="INSERT INTO " + evtable + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cv.execute (sql,
            ( str(irec) \
            , str(type) \
            , str(identifier) \
            , str(freq) \
            , str(bandwidth) \
            , str(strength) \
            , str(lat) \
            , str(long) \
            , str(range) \
            , str(lobAz) \
            , str(lobEl) \
            , int(time ()) \
            , int(time()) \
            ))
        except:
            msg=os.linesep+"\tUnable to insert event into MySqldb "
            msg+=os.linesep+"\t with insertEv function."
            log.warning(msg)
    return

def insertrfFV(cv,rfFV,rfFVtable):
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    sql='SELECT CURTIME()'
    cv.execute(sql)
    time=cv.fetchone()[0]
    sql='SELECT COUNT(*) from '+str(rfFVtable)
    cv.execute(sql)
    ind=cv.fetchone()[0]+1
    sql="INSERT INTO "+str(rfFVtable)+\
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    cv.execute(sql, \
      ( ind \
      , rfFV.id \
      , rfFV.frequency \
      , (rfFV.max_freq-rfFV.min_freq) \
      , rfFV.direction[0] \
      , rfFV.direction[1] \
      , str(rfFV.type) \
      , date \
      , date \
      , rfFV.numAlerts \
      ))

    return

def insertmtiFV(cv,mtiFV,mtiFVtable):
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    sql='SELECT CURTIME()'
    cv.execute(sql)
    time=cv.fetchone()[0]
    sql='SELECT COUNT(*) from '+str(mtiFVtable)
    cv.execute(sql)
    ind=cv.fetchone()[0]+1
    sql="INSERT INTO "+str(mtiFVtable)+\
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    cv.execute(sql,
      ( ind \
      , mtiFV.id \
      , mti.latitude \
      , mti.longitude \
      , mti.radvelocity \
      , mti.direction \
      , mtiFV.type \
      , date \
      , date \
      , mtiFV.numAlerts \
      ))

    return

def insertAlert(cv,alert,alerttable):
    # insert generic alert
    sql='SELECT COUNT(*) from '+str(alerttable)
    cv.execute(sql)
    ind=cv.fetchone()[0]+1
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    ind=cv.fetchone()[0]+1
    sql="INSERT INTO "+str(alerttable)+\
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    cv.execute(sql,
      ( ind \
      , alert.getModel() \
      , alert.getFvID() \
      , "NULL" \
      , "NULL" \
      , date \
      , alert.getTime() \
      , alert.getScoreSQLstring() \
      ))

    return

def insertTaggedAlert(cv,alert,alerttable):
    # Insert Alert for general signal with some additional external 
    # data (e.g. tipping and cuing).
    sql='SELECT COUNT(*) from '+str(alerttable)
    cv.execute(sql)
    ind=cv.fetchone()[0]+1
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    ind=cv.fetchone()[0]+1
    sql="INSERT INTO "+str(alerttable)+\
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    cv.execute(sql,
      ( ind \
      , alert.getModel() \
      , alert.getFvID() \
      , alert.getTag() \
      , "NULL" \
      , date \
      , alert.getTime() \
      , alert.getScoreSQLstring() \
      ))

    return


def insertRFAlert(cv,alert,alerttable):
    # Insert Alert for generic RF signal
    sql='SELECT COUNT(*) from '+str(alerttable)
    cv.execute(sql)
    ind=cv.fetchone()[0]+1
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    sql="INSERT INTO "+str(alerttable)+\
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    cv.execute(sql,
      ( ind \
      , alert.getModel() \
      , alert.getFvID() \
      , "NULL" \
      , alert.getFreq() \
      , date \
      , alert.getTime() \
      , alert.getScore() \
      ))

    sql='SELECT nalerts FROM rfFV where id=%s'
    cv.execute(sql, alert.getFvID ())
    n=cv.fetchone()
    if (n != None and len(n)>0):
        nAlertNow=n[0]+1
        sql='UPDATE rfFV SET nalerts=%s where id =%s'
        cv.execute(sql, (nAlertNow, alert.getFvID()))
    else:
        nAlertNow=0
    return

def updateFV(cv,fv,FVtable,score):
    pass

def fillModels(cv,mname,fname,comment=None):
    import os
    import time
    import stat
    if (comment==None):
     comment=" "
    pmmlfile=file(fname)
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    atime=os.stat(fname)[stat.ST_ATIME]
    atime=time.asctime(time.localtime(atime))
    ctime=os.stat(fname)[stat.ST_CTIME]
    ctime=time.asctime(time.localtime(ctime))
    mtime=os.stat(fname)[stat.ST_MTIME]
    mtime=time.asctime(time.localtime(mtime))
    mode=os.stat(fname)[stat.ST_MODE]
    mode=oct(mode & 0777)
   
    # we were using the mysql specific LOAD_FILE, but it
    # wasn't working in Korea, so we're doing the file load
    # the hard way
    load_file = file (fname, "rb")
    file_content = load_file.read ()
    load_file.close ()
   
    sql="INSERT INTO models VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
   
    try:
        login=getlogin()
    except:
         login='NULL';
    
    cv.execute (sql,
      ( mname \
      , file_content \
      , comment \
      , str(os.getuid()) \
      , login \
      , str(os.getgid()) \
      , atime \
      , mtime \
      , ctime \
      , str(mode) \
      ))
    
    return


def fillConfig(cv,fname,comment=None):
    import os
    import time
    import stat
    if (comment is None): comment=" "
    configfile=file(fname)
    sql='SELECT CURDATE()'
    cv.execute(sql)
    date=cv.fetchone()[0]
    atime=os.stat(fname)[stat.ST_ATIME]
    atime=asctime(localtime(atime))
    ctime=os.stat(fname)[stat.ST_CTIME]
    ctime=asctime(localtime(ctime))
    mtime=os.stat(fname)[stat.ST_MTIME]
    mtime=asctime(localtime(mtime))
    mode=os.stat(fname)[stat.ST_MODE]
    mode=oct(mode & 0777)

    # we were using the mysql specific LOAD_FILE, but it
    # wasn't working in Korea, so we're doing the file load
    # the hard way
    load_file = file (fname, "rb")
    file_content = load_file.read ()
    load_file.close ()

    sql="INSERT INTO configs VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
   
    try:
        login=os.getlogin()
    except:
        login='NULL'

    cv.execute (sql,
      ( os.path.basename(fname) \
      , file_content \
      , comment \
      , os.getuid () \
      , login \
      , os.getgid () \
      , atime \
      , mtime \
      , ctime \
      , mode \
      ))
    
    return

def configsToFiles(dbname,username,pwd,subdir):
    """
    Write configuration files from the database to local temporary files.
    """
    
    try:
        os.makedirs (subdir)
    except:
        # if there's an error, just try to write the files anyhow.
        # the directory probably already exists.
        pass
    db = MySQLdb.connect (db=dbname,user=username,passwd=pwd)
    cv = db.cursor ()

    sql="SELECT name, configfile from configs"

    cv.execute (sql)

    file_list = []

    for row in cv:
        file_name = os.path.join (subdir, row[0])
        file_list.append (file_name)
        outfile = file(file_name, "w")

        m = re.match(".*# FROM configs TABLE IN DATABASE.*", row[1], re.DOTALL)
        if not m:        
            outfile.write ("# FROM configs TABLE IN DATABASE %s - DO NOT EDIT%s" % (dbname, os.linesep))

        outfile.write (row[1])

    cv.close ()
    db.close ()

    return file_list

def openModelFile(dbname,username,pwd,filename):
    """
    Return the specified model as something that looks like a file
    """

    db = MySQLdb.connect (db=dbname,user=username,passwd=pwd)
    cv = db.cursor ()

    sql="SELECT pmmlfile from models where name=%s"
    cv.execute (sql, filename)

    row = cv.fetchone ()

    retval = StringIO.StringIO (row[0])

    cv.close ()
    db.close ()

    return retval


class MySQLConnectError(Exception):
    def __init__(self,value):
        self.value=value
    def __str__(self):
        return repr(self.value)

def shout(c):
 keep_fetching=True
 while keep_fetching:
	try:
	  i=c.fetchone()
        except:
          print "Cant grab the output."
	  keep_fetching=False
        if i==None:
          keep_fetching=False
          continue
        else:
          output=list(i)
          print output

