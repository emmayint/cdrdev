#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import os
import logging
import sys
import shutil
import datetime
import time
from optparse import OptionParser
from models import Transfer, configSectionMap, showDatabase, Session


def fetchLastFile():

    opts = OptionParser()
    opts.add_option('-o', '--output', dest='outputfilename', default='/opt/cdr/output.list')
    opts.add_option('-u', '--userid', dest='userid', type='int', default=-1)

    (options, args) = opts.parse_args()

    # Check for  transfer status
    log.info('Check for completed transfers')
        #.filter_by(status='FINISHED')
    records = (
        session.query(Transfer)
        .filter(Transfer.src_host != 'na62primitive')
        .filter(Transfer.transfer_attemp == 1)
        .order_by(Transfer.created_at.desc())
        .limit(1)
    )

    staticfilename = "/opt/cdr/lastfile.list"
    if records.count() > 0:
        for record in records:
            #print record.src_host + record.src_path + '/' + record.file_name
            path = record.src_host + '/' + record.file_name +"\n"
            #path = record.src_host + '/' + record.file_name + ' ' + str(record.created_at)
            print path
            newrecord = 1
            if os.path.exists(staticfilename):
	        file = open('%s'%staticfilename, 'r')
	        for line in file:
	            if record.file_name in line:
                        newrecord = 0
                        break
                file.close()
            if newrecord==1:
                file = open('%s'%staticfilename, 'w')
                file.write(path)
                file.close()
                shutil.copy(staticfilename, options.outputfilename)
                os.chown('%s'%options.outputfilename, options.userid, 1338) #gid = 1338 (vl)

    # release the connection to the pool
    session.close()

session = Session()
host = configSectionMap("Default")['host']
log = logging.getLogger(__name__)

if __name__ == '__main__':
    while True:
        fetchLastFile()
        time.sleep(15)
    #fetchLastFile()
