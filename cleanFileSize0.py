#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
# check for file size file in the db and deletes them
from datetime import datetime, timedelta
from models import Transfer, configSectionMap, showDatabase, Session
from controllers import SourceController

session = Session()
host = configSectionMap("Default")['host']
print "Running on: " +  host
records = (session.query(Transfer)
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
        print record
        print "Deleting Source File size: " + str(record.file_size) + " " + record.getSourcePath()
        new_source = SourceController(record.src_host, record.getSourcePath())
        if new_source.get_size() == record.file_size:
            new_source.delete()
            #new_source.rename()
            try:
                record.is_src_deleted = 1
                record.is_invalid = 1
                record.comment = "Invalid transfer File size is 0"
                record.deleted_at = datetime.now()
                session.merge(record)
                session.commit()
                #log.info("Commit on the DB %s is_source_deleted" % (record.job_id))
            #except:
            except Exception as e:
                print traceback.format_exception(*sys.exc_info())
                session.rollback()
                log.error("Failed Commit on the DB %s is_source_deleted" % (record.job_id))
        else:
            print "Error Local size " + new_source.get_size()  + " don't match with the one stored in the database: " + record.file_size
else:
    print "No records found"
