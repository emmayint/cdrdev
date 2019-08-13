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
from controllers import FakeFtsController as FtsController

if __name__ == '__main__':
    #logging.config.fileConfig('./na62cdr-logging.ini')
    logging.basicConfig(level=logging.DEBUG)
    #logger = logging.getLogger(__name__)
    logging.info("Starting cdr")

    #session = Session()
    #host = configSectionMap("Default")['host']
    fts_instance = FtsController('https://fts3-daq.cern.ch:8446')
    source = 'source/file.txt'
    destination = 'source/file.txt'
    job_id = fts_instance.submit(source, destination)

    status = fts_instance.get_status(job_id)
    logging.info(status)
