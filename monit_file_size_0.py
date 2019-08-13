#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
# check for submited file with size 0
from datetime import datetime, timedelta
from models import Transfer, configSectionMap, showDatabase, Session
from controllers import SourceController

session = Session()
host = configSectionMap("Default")['host']
print "Running on: " +  host
records = (
    session.query(Transfer)
    .filter_by(is_invalid=0)
    .filter_by(status='FAILED')
    .filter_by(resubmit_id=None)
    .filter_by(src_host=host)
    .filter_by(file_size=0)
    .limit(10)
)
#print str(records)

if records.count() > 0:
    for record in records:
        new_source = SourceController(record.src_host, record.getSourcePath())
        print record
        print "Source File size: " + str(record.file_size) + " " + record.getSourcePath()
else:
    print "No records found"
