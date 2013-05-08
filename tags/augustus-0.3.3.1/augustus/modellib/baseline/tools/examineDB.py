
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

from augustus.runlib.any_db import ConnectFactory
from config import Config

#----------------------------------------------------------------------

def main(configdir='.',datadir='.',pmmldir='.'):
    """handle user command when run as top level program"""
    from optparse import OptionParser, make_option
    usage = 'usage: %prog [options] '
    version = "%prog 0.0 alpha"
    option_list = [
      make_option('-b','--bootstrap',metavar='bootstrapConfig',default='bootstrapConfig',help='Configuration file used to connect to and load db holding other scoring and application config info(default "bootstrapConfig")'),
    ]

    parser = OptionParser(usage=usage,version=version,option_list=option_list)
    (opt,args) = parser.parse_args()
    appListDB(opt.bootstrap,args[0])

#----------------------------------------------------------------------
def appListDB(bootstrapConfig,table):
    """ This routine fills the database with configuration
        files for PMC scoring operations.
    """
    user_config=Config(bootstrapConfig)
    dbname=user_config.get('database_name')
    uname=user_config.get('database_user')
    pwd=user_config.get('database_upwd')

    DB=ConnectFactory(dbtype='MySQLdb',execlog='/tmp/sqllog')
    db=DB.connect(db=dbname,user=str(uname),passwd=str(pwd))
    cv=db.cursor()
    sql="select name from osc."+str(table)
    cv.execute(sql)
    for _e in cv.fetchone():
      print _e.split('/')[-1]
