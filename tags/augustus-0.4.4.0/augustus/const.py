# To prevent modifying the declared values
class _const:
    def __init__(self):
        # These are the defined constants that we are using.
        self.__dict__['_AUGUSTUS_VER']='0.4.4.0'
        self.__dict__['_PMML_VER']='4.0'
    class ConstError(TypeError): pass
    def __setattr__(self,name,value):
        if self.__dict__.has_key(name):
            print("Can't rebind const(%s)"% (name))
            raise self.ConstError
        self.__dict__[name]=value
    def __delattr__(self,name,value):
        if self.__dict__.has_key(name):
            print("Can't remove const(%s)"% (name))
            raise self.ConstError

    def check_python_version(self):
        import platform
        try:
            version_tup=platform.python_version_tuple()
            if int(version_tup[0]) != 2:
                raise Exception(True)
            else:
                if int(version_tup[1]) < 5:
                    raise Exception(True)
                else:
                    if int(version_tup[1]) > 6:
                        raise Exception(False)
        except:
            import sys
            fatal = sys.exc_info()[1]
            print('Augustus requires at least Python2.5 and Python2.6 is recommended')
            print('Either the default version of Python for this user / machine')
            print('or the version used to invoke this is determined to be %s' % (platform.python_version()) )
            if fatal:
                print('ERROR: Ending the Python session')
                sys.exit()
            print('WARNING: Continuing Python session, but there may be issues')
            print()


# Add to scope
import sys
sys.modules[__name__]=_const()

'''
In the code access these like:
  import augustus.const as AUGUSTUS_CONST
  AUGUSTUS_CONST._PMML_VER
'''
