#!/usr/bin/env python

import unittest
from augustus.kernel.unitable.asarray import *
import numpy.ma as ma

class AsArrayTestCase( unittest.TestCase ):

    def testAsNumArray( self ):
        a = as_num_array( [1, 2, 3] )
        assert a[0] == 1
        assert a[2] == 3
        a = as_num_array( [1.1, 2.2, 3.3] )
        assert a[1] == 2.2
        ma = as_num_array( [[1, 2, 3], [4, 5, 6]] )
        assert ma[0,0] == 1
        assert ma[0,2] == 3
        assert ma[1,2] == 6
        assert ma.shape == (2,3)
        a = as_num_array( ['1', '2', '3'] )
        try:
            a = as_num_array( ['1', '2', '3'] )
        except TypeError:
            pass
      #  else:
      #     fail( "testAsNumArray: expected a TypeError" )

    def testAsCharArray( self ):
        a = as_char_array( ['1', '2', '3'] )
        assert a[0] == '1'
        try:
            a = as_char_array( [1, 2, 3] )
        except TypeError:
            pass
        #else:
        #    fail( "testAsCharArray: expected a TypeError" )

    def testAsMaskedArray( self ):
        a = as_masked_array( [1, 2, 3], mask = [0, 1, 0] )
        assert a[1] == '--'

    def testIsCharArray( self ):
        a = as_char_array( ['1', '2', '3'] )
        assert is_char_array( a )

    def testAsArrayFunc( self ):
        a = asarray([1])
        assert a[0] == 1
        a = asarray([1,2,3])
        assert len( a ) == 3
        assert a[1] == 2
        a = asarray( [1,2.3,4] )
        assert a[0] == 1.0
        a = asarray( [1, 2, '3'] )
        assert a[0] == '1'
        a = asarray( [1, '2', 3.5] )
        assert a[2] == '3.5'
        a = asarray( [1, None, 2, 3], trymasked = True )
        assert a[1] == None
        #a = asarray( [1,'',2.5], trymasked = True )
        #print a
        #a = asarray( [1,None,'2.5'], trymasked = True)
        
    def testGetFormatFunc( self ):
        i = as_num_array([0,1])
        assert get_format( i ) == 'int32'
        s = as_char_array(['abc','defg'])
        assert get_format( s ) == 'a4'
        f = asarray( 1.1, 2, 3 )
        assert get_format( f ) == 'float64'
        m = ma.array([2.3,-1],mask=[0,1])
        assert get_format( m ) == 'float64'
        assert get_format( None ) == None

    def testPackBinaryMask( self ):
        # For now assuming masked arrays aren't used in current apps
        retval = pack_binary_mask( [1, 2, 3] )# == (None, array([1, 2, 3]))
        
    def testImportAsarrayFunc( self ):
        a = import_asarray(['1','2','3'])
        assert a[0] == 1
        a = import_asarray(['1','2.3','4'])
        assert a[0] == 1.
        assert a[1] == 2.3
        a = import_asarray(['1.9','2.3','4.2'])
        assert a[2] == 4.2
        a = import_asarray(['a',2.3,4.2])
        assert a[0] == 'a'
        a = import_asarray(['a','2.3','4'])
        assert a[1] == '2.3'
        a = import_asarray(asarray([1,2,3]))
        assert a[2] == 3
        a = import_asarray(asarray(['a','2.3','4']))
        assert a[2] == '4'
        a = import_asarray(asarray([2.3,'2.3a','']))
        assert a[0] == '2.3'
        # print import_asarray(['1',None,'3'],trymasked=True)
        a = import_asarray( [1.0, 2.0, 3.0] )
        assert a[1] == 2.0
        
    def testExportStringFunc( self ):
        a = export_string( [0, -1, 1, 2] )
        assert a[1] == -1

    def testTrailingWhitespace( self ):
        a = asarray(['abc   ', 'def'])
        assert a[0] == 'abc'
        
    def suite():
        suite = unittest.makeSuite( AsArrayTestCase)
        return( unittest.TestSuite( suite ) )

if __name__ == '__main__':
    # When this module is executed from the command-line, run all its tests
    unittest.main()
