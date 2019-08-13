#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
    List Transfers that have been cancelled
'''

import logging
import sys as sys
from datetime import datetime, timedelta
from models import Transfer, configSectionMap, Session
from controllers import SourceController, Gfal2Controller

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

session = Session()
minutes = 60 * 6
allow_resubmit_before = datetime.now() + timedelta(minutes=-int(minutes))
host = configSectionMap("Default")['host']
logger.info('Running on: ' +  host)

records = (
    session.query(Transfer)
    .filter_by(is_invalid=0)
    .filter_by(status='SUBMITTED')
    .filter_by(resubmit_id=None)
    .filter_by(src_host=host)
    .filter(Transfer.updated_at < allow_resubmit_before)
    .order_by(Transfer.transfer_attemp.desc())
    .limit(10)
)

resubmit_count = 0
suitable_for_resubmission_count = 0

if records.count() > 0:
    castor = Gfal2Controller()
    for record in records:
        source_path = record.src_path + record.file_name
        source = SourceController(host, source_path)
        local_exist = source.exists()
        remote_exist = castor.file_exist(record.getDestinationPath())
        if not remote_exist and local_exist:
            suitable_for_resubmission_count += 1

        #if remote_exist:
        #    print "Exist"
        ##    if remote_size == 0:
        ##        size_zero_count += 1
        ##        if database_size == local_size:
        ##            suitable_for_overwrite_count += 1
        ##    else:
        ##        size_non_zero_count += 1
        ##        if database_size == local_size and database_size == local_size:
        ##            fail_fetch_status_count += 1
        ##        else:
        ##            file_recreated_count += 1
        #else:
        #    print "Dont Exist"
        #    remote_dont_exist += 1

        logger.info(str(record.id) + " " + str(record.job_id) + " " +\
            str(record.transfer_attemp) + " Local File Exixst: " + str(local_exist) +\
            " Remote File Exist: " + str(remote_exist) + " " + record.getSourcePath() +\
            " " + record.getDestinationPath())

    logger.info('  Found: ' + str(records.count()) + ' Records')
    logger.info("  Cancelled file suitable for resubmission: " + str(suitable_for_resubmission_count))
else:
    logger.info('No records found')
