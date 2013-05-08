#!/usr/bin/env python

import unittest
from augustus.kernel.unitable import UniTable

class UniTableTestCase( unittest.TestCase):

    def test1( self ):
        data = {'a':(1,2,3),'ts':(34567,35678,34657),'values':(5.4,2.2,9.9)}
        keyorder = ('a','ts','values')
        t = UniTable(keys=keyorder,**data)
        rec = t[0]
        assert rec[0] == 1
        
    def suite():
        suite = unittest.makeSuite( UniTableTestCase)
        return( unittest.TestSuite( suite ) )

if __name__ == '__main__':
    # When this module is executed from the command-line, run all its tests
    unittest.main()
