#!/usr/bin/env python

from controllers import SourceController

def testDestination(host, file_name, expected_result):
    source_dir = '/merger/cdr'
    new_source = SourceController(host, source_dir + '/' + file_name)

    if new_source.get_destination() !=  expected_result:
        print file_name
        print ("    : {}".format(new_source.get_destination()))
        print ' Should be ' + expected_result
    else:
        print 'Test OK!'

testDestination('na62merger2', 'na62raw_1494236504-02-007058-0007.dat', '/castor/cern.ch/na62/data/2017/raw/prerun/007058/')
testDestination('na62merger2', 'na62raw_0-02-007058-0007.dat', '/castor/cern.ch/na62/data/2017/raw/prerun/007058/')
testDestination('na62merger2', 'na62raw_0-02-000000-3166.dat', '/castor/cern.ch/na62/data/2017/raw/prerun/000000/')
testDestination('na62merger2', 'list.txt', '/castor/cern.ch/na62/data/2017/raw/prerun/')
testDestination('na62merger2', 'calib_scan', '/castor/cern.ch/na62/data/2017/raw/prerun/')
testDestination('na62merger2', 'calib_scan', '/castor/cern.ch/na62/data/2017/raw/prerun/')
testDestination('na62primitive', 'lav12_run7071_burst0160', '/castor/cern.ch/na62/data/2017/raw/prerun/primitives/')
