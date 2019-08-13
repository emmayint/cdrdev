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
from controllers import CdrController
from controllers import SourceController
from controllers import FakeFtsController as FtsController


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    logger = logging.getLogger(__name__)
    logger.info("Starting new cdr..")

    session = Session()
    host = configSectionMap("Default")['host']
    fts_instance = FtsController('https://fts3-daq.cern.ch:8446')
    cdr = CdrController(fts_instance, host)
    source = SourceController(host, '/merger/cdr/file1.txt')
    cdr.submit(session, source)
        
