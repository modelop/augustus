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

from runlib.any_db import ConnectFactory
from userMySQLInterface import fillConfig,makeConfigTable
from config import Config
import augustus.const as AUGUSTUS_CONSTS
try:
    import __setprojpath
except:
    pass

#----------------------------------------------------------------------

def main(paths=None):
    """handle user command when run as top level program"""
    try:
       configdir=__setprojpath.CONFIGDIR
    except:
       configdir='config'       
    try:
        bootdef=__setprojpath.CONFIGDIR+'/bootstrapConfig'
    except:
        bootdef='config/bootstrapConfig'

    from optparse import OptionParser, make_option

    usage = 'usage: %prog [options] '
    version = "%prog " + AUGUSTUS_CONSTS._AUGUSTUS_VER
    option_list = [
      make_option('-b','--bootstrap',metavar='bootstrapConfig',default=bootdef,help='Configuration file used to connect to and load db holding other scoring and application config info(default "bootstrapConfig")'),
      make_option('-z','--zero',metavar='zero',default=None,action="store_true",help='If set, creates empty table. Default is not set.'),
    ]

    parser = OptionParser(usage=usage,version=version,option_list=option_list)
    (opt,args) = parser.parse_args()
    userInitializeConfigs(opt.bootstrap,opt.zero,configdir)

#----------------------------------------------------------------------
def userInitializeConfigs(bootstrapConfig,zero,configdir):
    """ This routine fills the database with configuration
        files for scoring operations.
    """
    user_config=Config(bootstrapConfig)
    dbname=user_config.get('database_name')
    uname=user_config.get('database_user')
    pwd=user_config.get('database_upwd')

    makeConfigTable(str(dbname),str(uname),str(pwd))

    DB=ConnectFactory(dbtype='MySQLdb',execlog='/tmp/sqllog')
    db=DB.connect(db=dbname,user=str(uname),passwd=str(pwd))
    cv=db.cursor()

    if (not zero):
        fillConfig(cv,configdir+'/'+'userConfig','Starting User Data Scoring Config File')
        fillConfig(cv,configdir+'/'+'scoringConfig','Starting config file use for internal maintenance of scoring')

