"""Augustus requires external package: pygsl

Pygsl is available for download from
<http://pygsl.sourceforge.net/> and should be built
using configure option "--array-object=numarray".

Pygsl depends on the GNU Scientific Library available at
<http://www.gnu.org/software/gsl/>

"""

__path__ = None
try:
  import pygsl
except ImportError,e:
  e.args = (e.args[0]+'\n\n'+__doc__,)
  raise
__path__ = pygsl.__path__
__all__ = pygsl.__all__
del pygsl

#################################################################
