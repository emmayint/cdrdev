#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import sys
import logging
from models import configSectionMap, Session
from controllers import FtsController, CdrController


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.info('starting script')

if __name__ == '__main__':
    session = Session()
    host = configSectionMap("Default")['host']
    #host = "na62primitive"
    fts_instance = FtsController('https://fts3-daq.cern.ch:8446')
    #fts_instance = None

    cdr = CdrController(fts_instance, host)

    cdr.do_overwrite(session, amount=10)
