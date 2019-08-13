#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
    List failed transfers and tells you if they can be resubmitted
'''

import logging
import sys as sys
from datetime import datetime, timedelta
from models import Transfer, configSectionMap, Session
from controllers import SourceController, Gfal2Controller

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info('Starting resubmission monitor')

session = Session()
minutes = 60 * 6
allow_resubmit_before = datetime.now() + timedelta(minutes=-int(minutes))
host = configSectionMap("Default")['host']
logger.info('Running on: ' +  host)

records = (
    session.query(Transfer)
    .filter_by(is_invalid=0)
    .filter_by(status='FAILED')
    .filter_by(resubmit_id=None)
    .filter_by(src_host=host)
    .filter(Transfer.transfer_attemp >= 3)
    #.order_by(Transfer.updated_at.desc())
    #.filter(Transfer.updated_at < allow_resubmit_before)
    .order_by(Transfer.transfer_attemp.desc())
    .limit(1000)
)
resubmit_count = 0

size_zero_count = 0
size_non_zero_count = 0
suitable_for_overwrite_count = 0
fail_checksum_count = 0
file_recreated_count = 0
remote_dont_exist = 0

if records.count() > 0:
    castor = Gfal2Controller()
    for record in records:
        new_source = SourceController(host, record.getSourcePath())
        database_size = record.file_size
        local_size = new_source.get_size()
        if castor.file_exist(record.getDestinationPath()):
            remote_size = castor.get_size(record.getDestinationPath())
            if remote_size == 0:
                size_zero_count += 1
                if database_size == local_size:
                    suitable_for_overwrite_count += 1
            else:
                size_non_zero_count += 1
                if database_size == local_size and database_size == remote_size:
                    fail_checksum_count += 1
                else:
                    file_recreated_count += 1
            logger.info(str(record.id) + ' ' + record.job_id + " " + str(record.transfer_attemp) +\
            " " + record.getSourcePath() + " Remote Size: " + str(remote_size) +\
            " Database Size: " + str(database_size) + " LocalSize: " + str(local_size))
        else:
            remote_dont_exist += 1
            logger.error('Remote file not found ' + record.getDestinationPath())
            logger.error(record)
            continue

    logger.info('SUMMARY Found: ' + str(records.count()) + ' Records')
    logger.info('Remote file doesnt exist: ' + str(remote_dont_exist) + ' Records')
    logger.info('Remote file_size 0: ' + str(size_zero_count) + ' Records')
    logger.info('Size non 0: ' + str(size_non_zero_count) + ' Records')

    logger.info('Suitable for overwrite:')
    logger.info('- Remote file size 0: '  + str(suitable_for_overwrite_count) + ' Records')
    logger.info('- Checksum failed: '  + str(fail_checksum_count) + ' Records')
    logger.info('To investigate:')
    logger.info('- File has been recreated after submission: '  + str(file_recreated_count) +\
        ' Records')

else:
    logger.info('No records found')
