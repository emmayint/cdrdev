#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
#Checks for untracked files in the directory
import sys
import gc
import os
import glob
import logging
from datetime import datetime, timedelta
import time
from models import Transfer, configSectionMap, showDatabase, Session
from sqlalchemy import func
from controllers import FtsController, CdrController
from controllers import GracefulInterruptController
from controllers import SourceController


if __name__ == '__main__':
    session = Session()
    host = configSectionMap("Default")['host']
    source_dir = configSectionMap("Default")['file_dir']
    amount = (
        session.query(Transfer)
        .filter_by(is_invalid=0)
        .filter_by(status='FINISHED')
        .filter_by(is_src_deleted=0)
        .filter_by(src_host=host)
        .order_by(None).count()
    )

    source_dir = source_dir + '/'
    files = filter(os.path.isfile, glob.glob(source_dir + "*"))
    source_amount = len(files)
    if amount < source_amount:

        files.sort(key=lambda x: os.path.getmtime(x))

        #print amount
        #print source_amount
        print "There are: " , (source_amount - amount) , " untracked files"

        counter = 1
        offset = 0
        limit = 70
        amount_of_result = limit
        #I'm breaking the query in order to minimize the database load
        while amount_of_result == limit:
            #print "Iteration number: " , counter
            records = session.\
                 query(Transfer).\
                 filter_by(is_invalid=0).\
                 filter_by(status='FINISHED').\
                 filter_by(is_src_deleted=0).\
                 filter_by(src_host=host).\
                 order_by(Transfer.id.asc()).\
                 slice(offset, limit).\
                 offset(offset).\
                 limit(limit).\
                 all()

            #amount_of_result = records.count()
            amount_of_result = 0
            for record in records:
                amount_of_result += 1
                source_path = record.getSourcePath()
                if source_path in files:
                    try:
                        #removing db entry from the os list
                        files.remove(source_path)
                    except:
                        print "Entry doesn't exist"

            #print "New list len: " , len(files)
            counter += 1
            offset += limit

        print "List len: ", len(files)
        print len(files) , " Untracked files:"
        for file in files:
            new_source = SourceController(host, file)
            if (new_source.get_owner() == 'na62cdr' and new_source.get_group() == 'vl'):
                print file + " Was tracked"
                new_source = SourceController(host, file)
                print 'deleting'
                new_source.delete()
                #new_source.rename()
            else:
                print file + " Was not tracked"
                continue
    else:
        print "Nothing untracked"
