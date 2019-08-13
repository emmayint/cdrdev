#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import gc
import os
import traceback
import sys
import logging
import logging.config
from datetime import datetime
import time
from models import configSectionMap, Session
from controllers import FtsController, CdrController
from controllers import GracefulInterruptController

if __name__ == '__main__':
    logging.config.fileConfig('/etc/na62cdr-logging.ini')
    logger = logging.getLogger(__name__)
    logger.info("Starting cdr")

    session = Session()
    host = configSectionMap("Default")['host']
    fts_instance = FtsController('https://fts3-daq.cern.ch:8446')
    cdr = CdrController(fts_instance, host)
    with GracefulInterruptController() as handler:
        start_date = datetime.now()
        submit_success = 0
        submit_fail = 0
        transfer_finished = 0
        transfer_failed = 0
        files_deleted = 0
        transfer_resubmitted = 0
        #allows to read multiple path (, separation)
        file_dirs = map(str.strip, configSectionMap('Default')['file_dir'].split(','))

        life_in_minutes = int(configSectionMap("Default")['minutes_of_life'])
        cycle_count = 0
        count_since_beginning = 0
        sleep_time = 10 # in seconds

        while True:
            try:
                # the purpose of this query is to check the db status 
                # in case of failure the code below will be skipped until the connection is back
                result = session.execute('SELECT version()').fetchall()

                for file_dir in file_dirs:
                    if not os.path.isdir(file_dir):
                        logger.error(file_dir + " doesn't exist")
                        continue

                    (submit_success_partial, submit_fail_partial) = cdr.submit_files(session, file_dir)
                    submit_success += submit_success_partial
                    submit_fail += submit_fail_partial
                        
                (transfer_finished_partial, transfer_failed_partial) = \
                    cdr.fetch_transfer_status(session)
                transfer_finished += transfer_finished_partial
                transfer_failed += transfer_failed_partial

                cdr.fetch_tape_status(session)
                life_in_minutes = int(configSectionMap("Default")['minutes_of_life'])
                files_deleted += cdr.delete_files(session, life_in_minutes)

                for file_dir in file_dirs:
                    if not os.path.isdir(file_dir):
                        logger.error(file_dir + " doesn't exist")
                        continue
                    cdr.delete_over_threshold(session, file_dir) #occupancy based selection

                transfer_resubmitted += cdr.do_resubmit(session, 60*8)

                logger.info(" Stats since: " + str(start_date))
                logger.info(" + SUBMITTED:   " + str(submit_success))
                logger.info(" + RESUBMITTED: " + str(transfer_resubmitted))
                logger.info(" - FINISHED:    " + str(transfer_finished))
                logger.info(" - FAILED:      " + str(transfer_failed))
                logger.info(" (should be 0): " + str(submit_success  + transfer_resubmitted -\
                    transfer_finished -  transfer_failed))
                logger.info(" Files deleted: " + str(files_deleted))
                logger.info(" Submit Fail:   " + str(submit_fail))

                cycle_count += 1
                count_since_beginning += 1

                if count_since_beginning * sleep_time  > 60 * 60:
                    logger.info("Scheduled exit ")
                    sys.exit(1) #automatic exit after an hour due to memory leak the The service manager will restart it

                if cycle_count * sleep_time  > 60 * 10:
                    collected = gc.collect()
                    cycle_count = 0
                    logger.info("Garbage collector: collected %d objects.", collected)

                time.sleep(sleep_time)
            except Exception, e:
                # that was the only way to cathc this exception
                # https://mail.python.org/pipermail/python-list/2008-January/483946.html
                if e.__class__.__name__.find('OperationalError') != -1:
                    session.rollback()
                    wait_before_reconnect = 7
                    logger.error("Database connection lost.. waiting " + str(wait_before_reconnect) + "s before reconnecting..")
                    time.sleep(wait_before_reconnect)
                else:
                    #unknown exception
                    logger.error( e.__class__.__name__)
                    logger.error(traceback.print_exc())

            if handler.interrupted:
                logger.info('Cdr Interrupped!')
                time.sleep(1)
                break
