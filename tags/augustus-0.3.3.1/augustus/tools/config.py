"""User Configuration Accessor Module

Revision 1.2 -- 13May04

1. Introduction

This module provides a simple, but extremely flexible and powerful method
of controlling script execution by use of configuration settings stored in
configuration files.  This module is a more powerful alternative to the 
ConfigParser module already available in the standard Python distribution.

The standard ConfigParser module parses text based user configuration files and
provides access to that information.  This module reads python based
configuration files and provides access to the python objects contained within. 
In addition, this module supports hierarchical organization of the settings
within the configuration files so that settings may be accessed using keys
specified by the user from the command line or by other means.  By use of python
based configuration files, this module provides a simple solution to most
configuration problems but also has the flexibility to solve the most complex
configuration issues.

In addition to providing command line support for experimentation and 
configuration file maintenance, this module exports the following classes 
to which scripts may use to access user configuration settings and catch
errors:

Config -- used to store and retrieve user configuration settings.
ConfigSingleton -- same as Config except the same instance is always returned.
Error -- exception raised when error retrieving setting from configuration.

1.1 Table Of Contents

    1. Introduction
        1.1 Table of Contents
        1.2 Python Requirements
        1.3 SourceForge Project Goal and Information
    2. Quick Start
        2.1 Getting Simple String Settings
        2.2 Other Setting Types
    3. Using Keys and Settings Dictionaries
        3.1 Differentiator Keys
        3.2 Command Line Keys
        3.3 Shell Keys
        3.4 Configuration Keys
        3.5 Default Key
        3.6 Key Priorities
    4. Configuration Files
        4.1 Configuration File Selection
        4.2 Settings
        4.3 Key First Setting Specification
        4.4 Including Additional Configuration Files
        4.5 Default Settings
        4.6 Setting Requirements
        4.8 Conflicts
    5. Command Line Use of config.py
    6. History/Author
    7. License (MIT)

1.2 Python Requirements

This module was developed and tested using Python 2.3.  Because of the use of
new style classes, Python 2.2 or later is required.


1.3 SourceForge Project Goal and Information

http://config-py.sourceforge.net/

The config-py SourceForge project is being used with the goal of:

    1) distribution of module to those that may find it useful
    2) continued refinement of the module
    3) gather support for inclusion of the module in the Python core

The project contains three mailing lists:

    announce -- Low frequency list used to announce newly available releases.
                
    develop  -- Used to discuss module improvements.  It is recommented that
                feature requests and bugs be submitted in the tracker
                facilities provided by SourceForge rather than this mailing
                
    general  -- Used to solicit help or provide feedback on the usefulness of
                this module.  You can help this module become part of the
                Python core by using this mailing list by sharing how you are
                applying this module and what features you are using.  Your
                support in this area would be greatly appreciated!!!

Subscribing to any of the mailing lists is as simple as providing a name,
email address and selecting a password.


2. Quick Start

2.1 Getting Simple String Settings

In this example, consider a script 'unitTest.py' which is used to unit test
"c" code.  Instead of hard coding the compiler executable name and settings
into the script, an instance of the Config class is utilized to obtain a
setting for the compiler from the 'myConfig.py' user configuration script:

    from config import Config
    user_config = Config()
    print user_config.compiler.get()
    print user_config.linker.get()

With no configuration file specified in the instantiation of the Config class,
the file 'myConfig.py' will be searched for in the python module search path
(sys.path) and may look like:

    compiler = 'cl.exe -c -DUNIT_TEST=1'
    linker = 'link.exe'

During instantiation of the Config class "myConfig.py" is executed and the
settings contained saved in the Config instance.  The "get" method then
is used to retrieve the compiler setting.  'unitTest.py' would display the
following when run from the command line:

    $unitTest.py
    cl.exe -c -DUNIT_TEST=1
    link.exe


2.2 Other Setting Types

Configuration file settings may be any valid python object, some simple
(but not all inclusive) examples:

    flag = True

    logFile = 'c:/temp/log.txt'

    inputFiles = ['struct.h','define.h']

    pair = (1,2)

    def filter(x):
        x *= 2

    exception = RuntimeError
    
Note, it may be necessary to create temporary variables to hold intermediate
results to be used when creating settings.  Temporary variables should be
prefixed with an underscore.  Dictionaries may be a valid setting but must be
wrapped because of the use of dictionaries for organizing settings.  See
section 4.2 for more details on hiding intermediate objects and wrapping
dictionaries properly.


3. Using Keys and Settings Dictionaries

3.1 Differentiator Keys

If multiple scripts utilize the same setting from the user's configuration
file, differentiator keys may be used to allow the user to tailor the
compiler for the particular use.  Continuing the example, consider another
script 'integTest.py' used to integration test groups of 'c' files.  Each
script should request the compiler setting using a key:

    compiler = user_config.compiler.get('unitTest')  # unitTest.py
    compiler = user_config.compiler.get('integTest') # integTest.py

This allows the user to configure myConfig.py to tailor the compiler to the
use by the storing the settings under keys using a dictionary:

    compiler = { 'unitTest' : 'cl.exe -c -DUNIT_TEST=1',
                 'integTest': 'cl.exe -c -DINTEG_TEST=1' }

The get method will walk through the settings dictionary utilizing the key
list until a non-dictionary is found and return that value.  Keys are never
required to be present in the user configuration.  The user can specify one
compiler that would be used by both 'unitTest.py' and 'integTest.py':

    compiler = 'cl.exe -c -DTEST=1'


3.2 Command Line Keys

The ability to utilize keys specified in the invocation of the script from
the command line can provided increased flexibility.  For example, again
consider a 'unitTest.py' script for unit testing "c" files:

    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-k", type="string", dest="keys", default=None,
                      help="comma seperated list of keys")
    options, files = parser.parse_args()

    from config import Config
    user_config = Config(keys=options.keys)
    compiler = user_config.compiler.get()
    print ' '.join([compiler]+files)

This allows the user to have multiple compiler settings, for example one
for unit testing with the GNU compiler and one for the Microsoft compiler:

    compiler = { 'microsoft' : 'cl.exe -c -DUNITTEST',
                 'gnu'       : 'gcc.exe -c -DUNITTEST' }

The user can then control from the command line which compiler to use:

    $ unitTest.py -k microsoft code.c
    cl.exe -c -DUNITTEST code.c
    
    $ unitTest.py -k gnu code.c
    gcc.exe -c -DUNITTEST code.c


3.3 Shell Keys

Rather than passing in keys at every script invocation, keys may specified in
the shell's environment variable using 'set' for Windows or 'export' for Linix.
The example code in 3.2 would result in the following behavior when using
a shell environment variable:

    $ set KEYS=microsoft
    $ unitTest.py code.c
    cl.exe -c -DUNITTEST code.c

    $ set KEYS=gnu
    $ unitTest.py code.c
    gcc.exe -c -DUNITTEST code.c

This feature is only available when KEYS_VARIABLE is set in the configuration
file to the name of the environment which holds the comma separated keys list:

    KEYS_VARIABLE = "KEYS"

Note, the KEYS_VARIABLE setting in configuration files included under keys (see
section 4.4) are ignored.


3.4 Configuration Keys

Keys may be specified in the configuration file so that no keys are necessary
to be passed in from the command line or set in the shell environment.  For
example, using the the 'unitTest.py' script in 3.2 combined with the following
placed in 'myConfig.py':

    KEYS='microsoft'
    compiler = { 'microsoft' : 'cl.exe -c -DUNITTEST',
                 'gnu'       : 'gcc.exe -c -DUNITTEST' }

Will result in the following behavior:

    $ unitTest.py code.c
    cl.exe -c -DUNITTEST code.c

Note, KEYS may contain multiple keys using any of these alternative forms:

KEYS = 'microsoft,c'
KEYS = ['microsoft','c']
KEYS = ('microsoft','c')


3.5 Default Key

A key named 'default' is used as a last resort for finding settings.  Using
'unitTest.py' from 2.2 and 'myDefault.py' is:

    # Note, no KEYS!

    compiler = { 'microsoft' : 'cl.exe -c -DUNITTEST',
                 'gnu'       : 'gcc.exe -c -DUNITTEST',
                 'default'   : 'gcc.exe -c -DUNITTEST -DUSING_DEFAULT' }

The following behavior will be observed:

    $ set KEYS=bogusValue
    $ unitTest.py code.c
    gcc.exe -c -DUNITTEST -DUSING_DEFAULT code.c


3.6 Key Priorities

The above methods for specifying keys are in the order of their priority.
Differentiator keys have highest priority while the 'default' key has the
lowest.  In other words, if keys are specified by more than one method, the
highest priority wins.  For example, using the 'unitTest.py' specified in 3.2
and 'myConfig.py' in 3.4, the following behavior would be observed:

    $ set KEYS=gnu                   <-- overrides: 1) KEYS in myDefault.py
    $ unitTest.py code.c                            2) 'default' key
    gcc.exe -c -DUNITTEST code.c

    $ set KEYS=microsoft
    $ unitTest.py -k gnu code.c      <-- overrides: 1) set KEYS
    gcc.exe -c -DUNITTEST code.c                    2) KEYS in myDefault.py
                                                    3) 'default' key

Note, differentiator keys have the highest priority and will override all
others.


4 Configuration Files

4.1 Configuration File Selection

Configuration files to be utilized may be specified in a comma separated string
when instantiating 'Config' otherwise 'myDefault.py' will be used.  If multiple
configuration files are specified, the settings from each will be merged
together as if in a single file.  Configuration files may be specified
with a full path, relative path or no path.  Files with no path may be located
in the current working directory (at the time of Config instantiation) or
anywhere in the python module search path (see sys.path).  Files with a
relative path should be located relative to the current working directory.

This feature of specifying configuration files when instantiating 'Config' is
intended for supporting command line specification of user configuration.  For
example consider again 'unitTest.py':

    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-k", type="string", dest="keys", default=None,
                      help="comma seperated list of keys")
    parser.add_option("-c", type="string", dest="cfg_files", default=None,
                      help="comma separated list of configuration files")
    options, files = parser.parse_args()

    from config import Config
    user_config = Config(cfg_files=options.cfg_files,keys=options.keys)
    compiler = user_config.compiler.get()
    print ' '.join([compiler]+files)

Allowing the configuration file and keys to be specified from the command line
gives great flexibility to solve many problems, but in general, utilizing keys
and a single configuration file is prefered over utilizing multiple
configuration files to control setting selection.  It is intended that the
default 'myConfig.py' configuration file be utilized as the central repository
for all settings.  Some exceptions to this advice include:

    1) Trying (possibly someone else's) different configuration settings
       without disturbing your current settings.
    2) Insuring repeatability, such as when archiving scripts, the associated
       configuration files should be archived as well so that when restoring
       the scripts, they can be run with the exact settings at the time they
       were developed.


4.2 Settings

Configuration file settings may be any valid python object, some simple
examples:

    flag = True
    logFile = 'c:/temp/log.txt'
    inputFiles = ['struct.h','define.h']

Anything can be a setting but preface non-settings with an _underscore so that
they remain hidden (see config.py command line invocation for why):

    import re as _re
    regExpOptions = _re.DOTALL | _re.MULTILINE

    def _filter(a,b)
        return (a+b)*0.5
    filter = _filter

Extra care must be taken when a setting is a dictionary.  A dictionary subclass
must be utilized so that it can be differentiated from dictionaries which are
utilized to hold keyed setting values.  In the following example, 'joe' and
'me' are keys to navigate through to obtain settings, while 'name',
'telephone', and 'email' are keys of the setting values.

    class _dict(dict): pass

    contact = { 'joe' : _dict(name='Joe D. Smith',
                              telephone='(123) 456-7890',
                              email='jdsmith@some.url'),
                'me'  : _dict(name='John Q. Public',
                              telephone='(123) 456-0987',
                              email='jqpublic@some.url') }


4.3 Key First Setting Specification

It may be useful to organize the settings by listing the keys first and the
collection (dictionary) of settings associated with those keys last.  A
'KEYS_FIRST_CONFIG' dictionary construct is supported for this use.  The
following example configuration file demonstrates this feature:

    KEYS_FIRST_CONFIG = {
        'projectA' : { 'unitTest'  : { 'compiler' : 'cl.exe -c -DUNITTEST',
                                       'linker' : 'link.exe' },
                       'integTest' : { 'compiler' : 'cl.exe -c -DINTEGTEST',
                                       'linker' : 'link.exe' } }
        'projectB' : { 'unitTest'  : { 'compiler' : 'gcc -c -DUNITTEST',
                                       'linker' : 'link' },
                       'integTest' : { 'compiler' : 'gcc -c -DINTEGTEST',
                                       'linker' : 'link' } } }

and produces the same result as:

    compiler = {'projectA' : {'unitTest'  : 'cl.exe -c -DUNITTEST',
                              'integTest' : 'cl.exe -c -DINTEGTEST' },
                'projectB' : {'unitTest'  : 'gcc -c -DUNITTEST',
                              'integTest' : 'gcc -c -DINTEGTEST' } }

    linker = {'projectA' : {'unitTest'  : 'link.exe',
                            'integTest' : 'link.exe'},
              'projectB' : {'unitTest'  : 'link',
                            'integTest' : 'link' } }

Note, there is no practical limit to the number of levels of dictionaries.


4.4 Including Additional Configuration Files

A 'CONFIG_FILES' dictionary or list may be defined to specify additional
configuration files to read.  Using a list results in all configuration
files in the list being read and merged together as if their contents
were in a single configuration file.  For example:

    myConfig.py:        CONFIG_FILES = ['cfgFileA.py','cfgFileB.py']
    cfgFileA.py:        flagA = {'integTest':True, 'unitTest':False}
    cfgFileB.py:        flagB = {'integTest':False, 'unitTest':True}

is equivalent to 'myConfig.py' containing:

    flagA = {'integTest':True, 'unitTest':False}
    flagB = {'integTest':False, 'unitTest':True}

Alternative forms of CONFIG_FILES lists:

    CONFIG_FILES = ('cfgFileA.py','cfgFileB.py')
    CONFIG_FILES = 'cfgFileA.py,cfgFileB.py'

Using a dictionary results in the settings for the configuration file
specified under each key only available if the key is present when
a setting is requested by "Config.get".  For example:

    myConfig.py         CONFIG_FILES = {'projectA':'projectA.py',
                                        'projectB':'projectB.py'}
    projectA.py         flag = {'integTest':True, 'unitTest':False}
    projectB.py         flag = {'integTest':False, 'unitTest':True}
            
is equivalent to 'myConfig.py' containing:

    flag = {'projectA' : {'integTest':True, 'unitTest':False},
            'projectB' : {'integTest':False, 'unitTest':True}}

Configuration files that are included have the same options and capabilities
as the top most level configuration file(s) with one exception: the "KEYS"
setting in configuration files included by use of a dictionary in the
'CONFIG_FILES' construct (and all configuration files below) are ignored.

Note, the configuration file path is optional and if present it may be a
relative or absolute path.  Execution of each configuration file
takes place in the directory in which it is located so all relative paths
of any included configuration files are relative to it.

Configuration files are read using the 'execfile' python keyword.  For
efficiency, the configuration files specified in a 'CONFIG_FILES'
dictionary are only read when the key it is under is one of the keys
used to obtain a setting.  This allows a very large number of configuration
files to be present but only the ones that are actually needed will be read.
Use of a comma separated list of configuration files in CONFIG_FILES results
in those configuration files being read immediately.


4.5 Default Settings

As an optional parameter of 'Config.get', a default may be specified so that if
the setting is not found in the user configuration file a default value is
returned:

    user_config = Config()
    flag = user_config.flag.get(default=True)
    logFile = user_config.logFile.get(default='log.txt')
    

4.6 Setting Requirements

As an optional parameter of 'Config.get', a class may be specified to insure
the setting value obtained from the user configuration file is an instance of
that class or a subclass.  Use of this checking provides a meaningful and
consistent manner of reporting problems with settings.

    user_config = Config()
    flag = user_config.flag.get(instance_of=bool)
    logFile = user_config.logFile.get(instance_of=str)


4.7 Conflicts

As discussed in previous sections there are multiple ways of specifying
configuration settings:

    1) simple assignment
    2) "KEYS_FIRST_CONFIG" assignment
    3) "CONFIG_FILE" inclusion

It is possible to use all three methods in combination with one another as they
will be merged automatically.  For example:

    myConfig.py:    logFile = 'log.txt'
                    KEYS_FIRST_CONFIG = {'project1' : {'logFile' : 'log1.txt'}}
                    CONFIG_FILES = {'project2' : 'project2.py'}
    project2.py     logFile = 'log2.txt'

is equivalent to:

    logFile = {'default' : 'log.txt',
               'project1': 'log1.txt',
               'project2': 'log2.txt'}

Note, the insertion of the 'default' key happens automatically and is necessary
when the setting value at a particular level is not a dicationary and other
keyed entries are to be inserted at that level.

Also note, when settings are being merged and a conflict exist, the last one
wins (per the enumerated list above).


5. Command Line Use of config.py

The config.py module may be executed directly from the command line and
supports command line specification of the configuration file to use with the
'-c' option (defaults to 'myConfig.py' if none specified.)  When invoked
without any other options, all configuration files are read and a complete
listing of all the settings is obtained.  Use of the -s option allows a comma
separated list to be specified to narrow the list to those settings of
interest.

For experimentation purposes, additional command line support is present
to utilize and report the results of the "Config.get" method (the -g option).
When using '-g', consider using '-k' and '-i' for specifying keys and
class (instance) requirements.

Finally a '-e' option exists to allow edits to the configuration script before
listing the settings.  To utilize this option, an environment variable
'CONFIG_PY_EDITOR' must be set with the name of the editor to use.  (If not
present the 'EDITOR' environment variable will be used.)


6. History/Author

My name is Dan Gass and I have worked as an embedded software engineer in
Milwaukee Wisconsin for the last 15 years.  I have been writing Python
professionally (part time) for activities associated with developing and
testing real time embedded systems.

I have personally been working on a testing framework that I intend to
release as open source software.  The framework provides a uniform method
for automated unit, integration, and functional tests of Python, C, and
C++ systems.  The framework has the following features:

    assisted automatic test generation
    assisted automatic stub generation (C/C++)
    Python user interface (may someday make parallel XML interface)
    detailed html test reports
    batch capability
    easily extended to other languages

This config.py module was developed in response to the needs I had in
developing the test framework.  I needed a way for the user to tailor the test
framework to their project so that:

    1) User customizations are encapsulated and in isolation from the framework
    2) The framework can remain generic with clearly defined interfaces to the
       necessary customizations
    3) Control and responsibility is pushed into the user customization.  For
       example rather than having the user pass in the compiler executable
       and flags as a string, the user is required to pass in an object that
       when called with a list of files, the object will compile them.  To make
       this not too burdensome, a base class is provided that can be used
       directly for many projects but can be subclassed and tailored as
       necessary.
    4) User has flexibility and easy control of the customizations.  For
       example it should be easy to support multiple compilers and execution
       environments without effecting the framework design or implementation.

From this criteria one can see how the features of this module evolved:

    1) Configuration files encapsulate customizations
    2) Get method includes arguments to specify setting types and valid values.
    3) Configuration files are python scripts so that they can provide python
       objects to hide project specific implementations from the framework.
       (plus I didn't have to invent a syntax and a parser, and besides what's
       more elegant and simple than python?)
    4) Settings can be keyed dictionaries of values and user has easy control
       of keys used.

You may contact the author at dmgass@hotmail.com


7. License (MIT)

Copyright (c) 2004 by Daniel M. Gass

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import pprint
import sys

__all__ = ['Config','ConfigSingleton','command_line','Error']

class Error(Exception):
    """Raised when error retrieving setting from configuration"""
    pass

class _Keys(object):
    """Prioritizes and stores default configuration keys.
    
    This class is used to store default configuration keys for an instance
    of the Config class.  The different sources of keys are supported (stored)
    by the following methods of this class:

    __init__ -- command line keys
    add_env_keys -- invoking shell's environment variables keys
    add_cfg_keys -- configuration file's KEYS specification
    
    The 'get' method of this class supports differentiator keys and returns
    a complete prioritized list of those keys and the stored keys.

    For more information regarding the key sources and prioritization and use,
    see the module documentation in which this class is defined in.
    """
    
    def __init__(self):
        """Initialize Keys Instance"""
        self._cmd_keys = []
        self._env_keys = []
        self._cfg_keys = []
        self._env_vars = []

    def __repr__(self):
        """Return string representation of object"""
        return ','.join(self.get([]))

    def _add_cmd_keys(self,keys):
        """Store keys from command line interface

        keys -- list of keys (typically from the command line) to store.  May
            be a list of keys or a string with keys separated by commas.
            Use any value which evaluates False when no keys.
        """
        if keys:
            try:
                keys = keys.split(',')
            except AttributeError:
                pass
            self._cmd_keys.extend(keys)

    def add_env_keys(self,variable):
        """Store keys from invoking shell's environment variable

        variable -- (string) environment variable name from which to obtain
            keys to store
        """
        # only add keys from shell environment variable if we haven't already
        if variable not in self._env_vars:
            # save key variable name so we can't add same keys twice
            self._env_vars.append(variable)
            # if shell environment variable has a setting save it
            keys = os.getenv(variable,None)
            if keys is not None:
                self._env_keys.extend(keys.split(','))

    def add_cfg_keys(self,keys):
        """Store keys from user's configuration file.

        keys -- list of keys (from user's configuration file) to store.  May
            be a list of keys or a string with keys separated by comma.
            Use any value which evaluates False when no keys.
        """
        if keys:
            try:
                keys = keys.split(',')
            except AttributeError:
                pass
            self._cfg_keys.extend(keys)

    def get(self,keys=None):
        """Return prioritized list of stored configuration keys

        keys -- list of differentiator keys.  May be a list of keys or a string
            with keys separated by commas.  Use any valid which evaluates
            False when no keys.
        """
        if keys:
            try:
                keys = keys.split(',')
            except AttributeError:
                pass
        else:
            keys = []
        return (keys + self._cmd_keys + self._env_keys + self._cfg_keys +
                ['default'])

class _Setting(object):
    """yyy"""
    def __init__(self,config,name):
        self.config = config
        self.name = name
    def get(self,*args,**kwargs):
        return self.config.get(self.name,*args,**kwargs)

class Config(object):
    """Reads and Stores User Configuration Settings

    Instances of this class are used to store and retrieve settings obtained
    from reading user configuration files.  For information regarding the use
    of this class, see module documentation in which this class is defined in.

    Instances of this class have the following methods available:
        __init__ -- constructor
        get -- obtains setting

    Instances of this class have the following properties:
        keys -- list of keys in user configuration
        files -- list of top level user configuration files 
    """

    def __init__(self,cfg_files=None,keys=None,read_all=False):
        """Reads and stores user configuration file settings

        Arguments:
        cfg_files -- list of user configuration files.  May be a list of files
            or a string with files separated by commas.  It is intended (but
            not required) that this argument be used to support command line
            support of configuration  file specification.  Use None (default) 
            when the default 'myConfig.py' should be used.  Use any other value
            that evaluates false when no configuration files are to be read.
            Configuration files may have no path, a relative path or an
            absolute path.  If an absolute path is not specified, the
            configuration file must be located in or relative to the current
            working directory (if a relative path specified) or in the current
            working directory or any directory in the python module search path
            (see sys.path) when no path specified.
        keys -- list of keys (typically from the command line).  May be a list 
            of keys or a string with keys separated by commas.  Use any value
            which evaluates False when no keys.
        read_all -- Flag indicating all configuration files should be read
            (for use when dumping all settings in this module's command line
            interface).

        Note, argument defaulting to None is used so that calling modules may
        more easily support a command line option parser but yet still keep
        knowledge and control of the real default within this class.

        This constructor is designed to be called multiple times in order to
        support a singleton subclass.
        """

        # only initialize internals (and read the default myConfig.py) when
        # constructing a new instance.
        if '_keys' not in self.__dict__:
            self._keys = _Keys()
            self._settings = {}
            self._pending = []
            self._cfg_files = []
            if cfg_files is None:
                cfg_files = 'myConfig.py'

        # update keys and read configuration files in all cases
        self._keys._add_cmd_keys(keys)

        if cfg_files:
            parent = os.path.join(os.getcwd(),'')
            cfg_files = self._read_config_files(cfg_files,[],parent,read_all)
            self._cfg_files.extend(cfg_files)
        
    def __getattr__(self,name):
        """Provide access to settings through instance attributes"""
        setting = _Setting(self,name)
        setattr(self,name,setting)
        return setting

    def _get_keys(self):
        """Return user configuration key list (used by keys property)"""
        return self._keys.get()

    keys = property(_get_keys)

    def _get_files(self):
        """Return top level user config file list (used by files property)"""
        return ','.join(self._cfg_files)

    files = property(_get_files)    

    def _add_config_file(self,value,under_keys,parent,hold,read_all):
        """Add configuration file to pending list or read it (recursive)
        
        arguments:
        value -- list or dictionary of configuration files (list may be a
            string with multiple files separated by commas)
        under_keys -- key list that all settings in the configuration file
            will be placed under when the file is read
        parent -- fully pathed name of the configuration file that was
            responsible for bringing in the configuration file(s) to be
            added to the pending list.  If no parent, this should be just the
            path of the current working directory.
        hold -- flag indicating whether to add (hold) the configuration file(s)
            in the pending list or whether to read them right away.  This flag
            is utilized so that lists of configuration files are read right
            away while configuration files specifed in a dictionary are held
            until they are required to be read.
        read_all -- flag indicating whether to add the configuration file(s)
            to the pending list or read them right away.  This flag exists to
            support this module's command line interface.
        """
        if isinstance(value,dict):
            for next_key,next_value in value.items():
                self._add_config_file(next_value,under_keys+[next_key],
                                              parent,True,read_all)
        else:
            if read_all or hold is False:
                self._read_config_files(value,under_keys,parent,read_all)
            else:
                self._pending.append((under_keys,parent,value))

    def _read_config_files(self,cfg_files,under_keys,parent,read_all):
        """Read user configuration file and store settings contained

        under_keys -- list of keys that have previously been walked
        """
        # Save current working directory so we can restore it when done and
        # then change the current working directory to the path of my parent
        # so that any relative pathing in the configuration file specification
        # works out relative to the parent configuration file.
        path_to_restore = os.getcwd()
        parent_path = os.path.split(parent)[0]
        try:
            cfg_files = cfg_files.split(',')
        except AttributeError:
            pass
        files = []
        for cfg_file in cfg_files:
            os.chdir(parent_path)
            # Locate configuration file using current working directory and
            # paths in the python module search path.
            if read_all:
                try:
                    _find_cfg_file(cfg_file,parent)
                except Error:
                    print "WARNING: '%s' not found (starting in %s)" % (
                        cfg_file,parent_path)
                    continue
            cfg_file_path, cfg_file_name = _find_cfg_file(cfg_file,parent)
            os.chdir(cfg_file_path)
            cfg_file = os.path.join(os.getcwd(),cfg_file_name)
            files.append(cfg_file)

            # Read in configuration file
            settings = {}
            execfile(cfg_file,settings,settings) # TBD may only need in one

            # Update the keys.  "KEYS_VARIABLE" setting used to specify the
            # environment variable that holds additional default keys, if
            # present get keys from the environment using it.  If reading
            # configuration file that is being included under a key, don't
            # bother with it's keys because it would get too confusing.
            if not under_keys:
                keysVariable = settings.pop('KEYS_VARIABLE',None)
                if keysVariable:
                    self._keys.add_env_keys(keysVariable)
                self._keys.add_cfg_keys(settings.pop("KEYS",None))

            KEYS_FIRST_CONFIG = settings.pop('KEYS_FIRST_CONFIG',None)
            CONFIG_FILES = settings.pop('CONFIG_FILES',None)
                
            # Merge all settings that don't start with "_" into the settings
            for name,value in settings.items():
                if not name.startswith('_'):
                    self._merge_setting(name,value,under_keys)

            # Merge all "KEYS_FIRST_CONFIG" settings into the settings
            if KEYS_FIRST_CONFIG is not None:
                if KEYS_FIRST_CONFIG.__class__ is dict:
                    self._walk_keys_first_config(KEYS_FIRST_CONFIG,under_keys)
                else:
                    bad_keys_first_config = '\n'.join(
                        ['KEYS_FIRST_CONFIG is of wrong type',''] +
                        ['KEYS_FIRST_CONFIG must be a dictionary',''] +
                        [_formatSetting('file',cfg_file),''] +
                        [_formatSetting('KEYS_FIRST_CONFIG',KEYS_FIRST_CONFIG),''])
                    raise Error(bad_keys_first_config)

            # Process the "CONFIG_FILES" setting used to merge in configuration
            # from other files. Users specify a comma separated (string)
            # listing of configuration file names or a dictionary of (and
            # optionally - of dictionaries of) file name strings.
            if CONFIG_FILES:
                self._add_config_file(CONFIG_FILES,under_keys,cfg_file,
                                              False,read_all)

        # Change to directory we were in when we started
        os.chdir(path_to_restore)
        return files
    
    def _merge_setting(self,name,value,under_keys):
        """ Merge value (under keys) into settings dictionary.

        name -- setting name
        value -- value to assign setting (may be a dictionary or a setting)
        under_keys -- list of keys to place value in settings dict under
        """
        
        # Add the setting name to the key list so we can start walking at
        # the top level of the settings dictionary.
        keys = [name] + under_keys
        settings_dict = self._settings
        # Move through the settings dictionary using the keys we are
        # supposed to place the value under creating dictionaries and keys 
        # that aren't already present.
        for key in keys[:-1]:
            if key in settings_dict:
                if settings_dict[key].__class__ is not dict:
                    settings_dict[key] = dict(default=settings_dict[key])
            else:
                settings_dict[key]={}
            settings_dict = settings_dict[key]
        # Since the value may be a dict and the settings_dict may still be a
        # dict they must be merged together recursively until one or
        # both can no longer be walked.
        self._merge_setting_walk(settings_dict,{keys[-1] : value})

    def _merge_setting_walk(self,settings_dict,value_dict):
        """Merge value dictionary into settings dictionary (recursive).

        settings_dict -- current position in the self._settings dictionary
            of dictionaries.  
        value_dict -- current position in the value dictionary of dictionaries.
        """
        # If ran out of dictionaries in values dictionary, put the
        # value in under the default key because settings_dict is still a dict.
        if value_dict.__class__ is not dict:
            settings_dict['default'] = value_dict
        else:
            # Walk through value_dict
            for key in value_dict.keys():
                if key in settings_dict and settings_dict[key].__class__ is dict:
                    # If key in both dictionaries, walk down into both
                    self._merge_setting_walk(settings_dict[key],
                                             value_dict[key])
                else:
                    # No need to walk rest of value_dict since it can be
                    # installed in whole in the current settings dict location.
                    settings_dict[key] = value_dict[key]

    def _walk_keys_first_config(self,keys_first_config,under_keys):
        """Merge KEYS_FIRST_CONFIG into settings (recursive)

        keys_first_config -- current position in the KEYS_FIRST_CONFIG
            dictionary.  Each recursive execution of this method will walk
            one level down into this dictionary.
        under_keys -- list of keys walked through (also is the list of
            keys that setting value must be placed under when merged into
            settings dictionary).
        """
        # Keep walking (recursively) down into KEYS_FIRST_CONFIG until out of
        # dicts at which point the normal settings merge method can be used.
        if keys_first_config.__class__ is dict:
            for key,value in keys_first_config.items():
                self._walk_keys_first_config(value,under_keys+[key])
        else:
            self._merge_setting(under_keys[-1],keys_first_config,
                                under_keys[:-1])

    def get(self,name,keys=[],instance_of=None,help=None,checker=None,
            **kwArgs):
        """Return user configuration settings value.

        name -- Name of setting to retrieve value of.
        keys -- List of differentiator configuration keys to use to walk
            through the user's configuration settings.  May be a list of keys
            or a string with keys separated by commas.
        instance_of -- class that value must of or a subclass of.
        help -- Help text to print if error.
        checker -- function to check value (return True if pass, False if fail)
        default -- Value to return if no value found in configuration settings.
                   Must be a keyword argument and absence of argument will
                   cause error if setting is not present.

        For more information regarding the use of this class method, see module
        documentation in which this class is defined in.
        """

        # Make help list so it's ready to go if we need to report an error
        if help is None:
            help = []
        else:
            help = [help,'']
        
        # Look for default value in kwArgs (use of a normal keyword arg would
        # restrict what can be passed as a default)
        haveDefault = 'default' in kwArgs
        if haveDefault:
            default = kwArgs.pop('default')
        if kwArgs:
            fmt = "get() got an unexpected keyword argument '%s'" 
            unexpected_argument = fmt % kwArgs.popitem()[0]
            raise TypeError(unexpected_argument)

        # Get prioritized key list (keys from this call, the command line, the
        # configuration file specification, the shell environment and the
        # 'default' key.
        keys = self._keys.get(keys)

        # Read any pending configuration files that could possibly effect the
        # setting about to be retrieved.  Note, it was decided that if the
        # pending configuration file's under keys are all in the key list
        # computed above it will be installed right away.  The other
        # alternative is to try retrieving the setting with the highest
        # priority key alone (first reading any pending configuration files
        # that are under that key), then if that fails try retrieving the
        # setting with the top two highest priority keys (again first reading
        # any pending configuration files that are under either or both of the
        # keys), and repeating this process for each key in the list until a
        # setting is found.  This would have the benefit of only reading
        # pending configuration files when it is absolutely necessary but at
        # cost of performance and more difficult to explain how it works.
        d = []
        while self._pending:
            under_keys,parent,cfg_files = self._pending.pop(0)
            for key in under_keys:
                if key not in keys:
                    d.append((under_keys,parent,cfg_files))
                    break
            else:
                self._read_config_files(cfg_files,under_keys,parent,False)
        self._pending = d

        # Walk into setting and walk by all dictionary keys present in the
        # default key list and stop when dictionary levels run out.  If value
        # retrieved is still a dictionary, it is not valid as the user will
        # wrap all dictionary values with a subclass of dict!!
        self._settings.setdefault(name,{})
        value = self._walk_setting(self._settings[name],[],keys)
        if value.__class__ is dict:
            if haveDefault:
                return default
            no_setting_available = '\n'.join(
                ["No Valid Setting Available", ""] +
                self._wrap("Could not find a valid '%s' setting." % name) +
                help +
                [_formatSetting('CONFIG_FILES',self.files),''] +
                [_formatSetting(name,self._settings[name]),''] +
                [_formatSetting('KEYS',','.join(keys)),''])
            raise Error(no_setting_available)
            
        # If there are instance requirements check them.
        if instance_of is not None and not isinstance(value,instance_of):
            wrong_instance = '\n'.join(
                ["Wrong Configuration Setting Type", ""] +
                self._wrap("Setting '%s' is required to be an instance "
                    "of %s." % (name,instance_of)) +
                help +
                [_formatSetting('CONFIG_FILES',self.files),''] +
                [_formatSetting(name,value),''] +
                [_formatSetting('KEYS',','.join(keys)),''])
            raise Error(wrong_instance)

        # If there is a checker function check the value.
        if checker is not None and not checker(value):
            wrong_instance = '\n'.join(
                ["Configuration Setting Value Not Valid", ""] +
                help +
                [_formatSetting('CONFIG_FILES',self.files),''] +
                [_formatSetting(name,value),''] +
                [_formatSetting('KEYS',','.join(keys)),''])
            raise Error(wrong_instance)
        return value

    def _wrap(self,msg):
        """Split message and return a list of strings that fit a screen

        msg -- message to be split up into a list of strings that has a maximum
            length of 79 characters
        """
        
        lines = []
        while len(msg) > 79:
            i = msg.rfind(' ',0,79)
            lines.append(msg[:i])
            msg = msg[i+1:]
        lines.extend([msg,''])
        return lines
    
    def _walk_setting(self,setting,under_keys,keys):
        """Returns setting value once default keys are exhausted

        Keyword Arguments:
        setting -- value or dictionary to walk through
        keys -- default keys (list of strings) used to walk through setting.

        Walks into setting and walks by all dictionary keys present in the
        default key list and stops when dictionary levels run out.
        """
        if setting.__class__ is dict:
            for key in keys:
                if key in setting:
                    v = self._walk_setting(setting[key],under_keys+[key],keys)
                    if v.__class__ is not dict:
                        return v
        return setting

    # The following code was utilized for tracing method calls and state
    # logging when developing this code and is left in incase further debugging
    # is needed.  Contact the author to obtain the tracer.py module necessary
    # to use it.
    
    #import tracer
    #__metaclass__ = tracer.Tracer
    #_traceInclude = []
    #_traceExclude = []

    def _traceState(self,log):
        try:
            log('<state _pending="%s"/>' % self._pending)
        except AttributeError:
            log('<state status="not initialized"/>')


class ConfigSingleton(Config):
    """Return previously created ConfigSingleton instance or return new one."""

    _singleton = None

    def __new__(cls,*args,**kwargs):
        """Return previously created instance or return a new one.

        In either case the instance constructor is called with the passed
        arguments to it (ConfigSingleton constructor is designed to be called
        multiple times).
        """

        if cls._singleton is None:
            cls._singleton = Config.__new__(cls,*args,**kwargs)

        return cls._singleton
    

def _find_cfg_file(cfg_file,parent):
    """Returns path and configuration file name found.

    cfg_file -- name of configuration file, may have no path, relative
        path, or absolute path.  If an absolute path is included, the file's
        existance will be checked for in that path.  If a relative path is
        specified, the file's existance will be checked relative to the current
        working directory.  If no path is specified, the file will be searched
        for in the current working directory and all paths in the python module
        search path.
    parent -- Config instance of parent (None if no parent) used for error
        reporting.
    """
    path, cfg_file = os.path.split(cfg_file)

    if path:
        paths = [path]
    else:
        paths = ['.'] + sys.path

    for path in paths:
        if os.path.exists(os.path.join(path,cfg_file)):
            return path,cfg_file
        
    lines = ["File not found: '%s'" % cfg_file]
    parentPath, parentFile = os.path.split(parent)
    if parentFile:
        lines.append('    Parent: %s' % parent)
    lines.append('    Search path: %s' % ','.join(paths))
    file_not_found = '\n'.join(lines)
    raise Error(file_not_found)

def _formatSetting(name,value):
    """Returns setting as a formatted string (uses pretty print)"""
    value = pprint.pformat(value).replace('\n','\n'+' '*(len(name)+3)).strip()
    return '%s = %s' % (name,value)
    
_editorMissingMessage = """
The 'CONFIG_PY_EDITOR' environment variable must specify the name of the editor
to use to edit the config file (or use the EDITOR environment variable).  The
path must be specified unless the editor is located in the system path.
"""

_editorProblemMessage = """
There was a problem utilizing the 'CONFIG_PY_EDITOR' environment variable to
invoke an editor to edit the configuration file.  Either the value of the
environment variable is not valid or the configuration file does not exist.
"""

def command_line(args=None):
    """Perform module's command line associated functionality

    Optional argument:
        args -- list of args (list of strings), defaults to sys.argv[1:]

    This function provides the command line interface functionality for the
    module that contains this function.  The command line interface is used
    for the purpose of experimentation and configuration file maintenance.
    """

    import optparse

    parser = optparse.OptionParser()
    parser.add_option("-c", type="string", dest="config_files",
                      help="comma separated list of configuration files",
                      default=None)
    parser.add_option("-k", type="string", dest="keys",
                      help="comma separated list of keys",
                      default=None)
    parser.add_option("-e", action="store_true", dest="edit",
                  help="edit configuration file", default=False)
    parser.add_option("-g", action="store_true", dest="get",
                  help="get settings (vs. dumping)", default=False)
    parser.add_option("-s", type="string", dest="settings",
                      help="comma seperated list of settings to report",
                      default=None)
    parser.add_option("-i", type="string", dest="instance_of",
                      help="required instance type (used with -k)",
                      default=None)
    (_options, _args) = parser.parse_args(args)

    if len(_args):
        parser.error("takes no arguments")

    if _options.edit:
        # find user's configuration file
        cfg_file = _options.config_files
        if cfg_file is None:
            cfg_file = 'myConfig.py'
        if ',' in cfg_file:
            print "ERROR: file list with -c option incompatible with -e option"
            sys.exit()
        try:
            path, cfg_file = _find_cfg_file(cfg_file,None)
            cfg_file = os.path.join(path,cfg_file)
        except IOError:
            print "ERROR: cannot find configuration file %s" % cfg_file
            sys.exit()

        # get editor name from system environment variable
        editor = os.getenv('CONFIG_PY_EDITOR')
        if editor is None:
            editor = os.getenv('EDITOR')
        if editor is None:
            print 'ERROR: Environment Variable Not Set'
            print _editorMissingMessage
            sys.exit()
        cmd = ' '.join([editor,cfg_file])
        # invoke editor
        print cmd
        if os.system(cmd):
            print 'ERROR: Environment Variable Setting Not Valid'
            print _editorProblemMessage
            print _editorMissingMessage
            sys.exit()

    config = Config(_options.config_files,_options.keys,read_all=True)

    if _options.settings is None:
        keyList = config._settings.keys()
    else:
        keyList = _options.settings.split(',')
    keyList.sort()

    instance_of = _options.instance_of
    if instance_of is not None:
        instance_of = eval(instance_of)

    print _formatSetting('CONFIG_FILES',config.files)

    for name in keyList:
        if _options.get:
            value = config.get(name,instance_of=instance_of)
        elif name not in config._settings:
            value = 'Not in Cfg File(s)'
        else:
            value = config._settings[name]
        print _formatSetting(name,value)

    print _formatSetting('KEYS',str(config._keys))

if __name__ == "__main__":

    command_line()

