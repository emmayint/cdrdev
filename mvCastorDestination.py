#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
#import os
import logging
import sys
import os
import re
from controllers import Gfal2Controller
from models import Session, Transfer

'''
/castor/cern.ch/na62/data/2018/raw/run/008611/na62raw_1525228436-02-008611-1504.dat
/castor/cern.ch/na62/data/2018/raw/run/008610/na62raw_1525228436-02-008610-1504.dat

/castor/cern.ch/na62/data/2018/raw/run/008670/na62raw_1526145727-02-008670-1501.dat
/castor/cern.ch/na62/data/2018/raw/run/008669/na62raw_1526145727-02-008669-1501.dat

/castor/cern.ch/na62/data/2018/raw/run/008720/na62raw_1527114838-01-008720-0381.dat
/castor/cern.ch/na62/data/2018/raw/run/008719/na62raw_1527114838-01-008719-0381.dat

/castor/cern.ch/na62/data/2018/raw/run/008723/na62raw_1527217706-03-008723-1505.dat
/castor/cern.ch/na62/data/2018/raw/run/008722/na62raw_1527217706-03-008722-1505.dat

/castor/cern.ch/na62/data/2018/raw/run/008726/na62raw_1527312551-02-008726-1528.dat
/castor/cern.ch/na62/data/2018/raw/run/008725/na62raw_1527312551-02-008725-1528.dat

/castor/cern.ch/na62/data/2018/raw/run/008729/na62raw_1527375411-02-008729-1501.dat
/castor/cern.ch/na62/data/2018/raw/run/008728/na62raw_1527375411-02-008728-1501.dat

/castor/cern.ch/na62/data/2018/raw/run/008731/na62raw_1527434559-02-008731-1507.dat
/castor/cern.ch/na62/data/2018/raw/run/008730/na62raw_1527434559-02-008730-1507.dat
'''

def main():

    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    LOGGER = logging.getLogger(__name__)

    file_list = [
        ('/castor/cern.ch/na62/data/2018/raw/run/008611/na62raw_1525228436-02-008611-1504.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008610/na62raw_1525228436-02-008610-1504.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008670/na62raw_1526145727-02-008670-1501.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008669/na62raw_1526145727-02-008669-1501.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008720/na62raw_1527114838-01-008720-0381.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008719/na62raw_1527114838-01-008719-0381.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008723/na62raw_1527217706-03-008723-1505.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008722/na62raw_1527217706-03-008722-1505.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008726/na62raw_1527312551-02-008726-1528.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008725/na62raw_1527312551-02-008725-1528.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008729/na62raw_1527375411-02-008729-1501.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008728/na62raw_1527375411-02-008728-1501.dat'),
        ('/castor/cern.ch/na62/data/2018/raw/run/008731/na62raw_1527434559-02-008731-1507.dat',
        '/castor/cern.ch/na62/data/2018/raw/run/008730/na62raw_1527434559-02-008730-1507.dat')
    ]


    castor = Gfal2Controller()
    session = Session()
    for touple in file_list:
        myfile = touple[0]
        if castor.file_exist(myfile):
            LOGGER.info("File %s Exists!", myfile)
        else:
            LOGGER.error("File %s Not Exists!", myfile)
            continue

        if castor.is_on_tape(myfile):
            LOGGER.info("File %s is on tape", myfile)
        else:
            LOGGER.error("File %s is not tape", myfile)
            continue
          
        filename = os.path.split(myfile)
        #basename = os.path.basename(myfile)
        #dirname = os.path.dirname(myfile)
        regex_match = re.match(
            "na62raw_([0-9]*)-([0-9]*)-([0-9]*)-([0-9]*).*\.dat",
            filename[1]
        )
        if regex_match:
            #print regex_match.group(1)
            #print regex_match.group(2)
            #print "Run " + regex_match.group(3)
            #print "Burst " + regex_match.group(4)
            #TODO is a bug id the run number exceed 9999
            new_run = '00' + str(int(regex_match.group(3)) - 1)
            #new_basename = "na62raw_" + regex_match.group(1) + "-" + regex_match.group(2) + "-" + new_run + "-" + regex_match.group(4) + ".dat"
            #new_dirname = os.path.dirname(filename[0]) + '/' + new_run
            new_filename = (
                os.path.dirname(filename[0]) + '/' + new_run,
                "na62raw_" + regex_match.group(1) + "-" + regex_match.group(2) + "-" + new_run + "-" + regex_match.group(4) + ".dat"
            )
            #print "Run" , new_run
            #print new_basename
            #print new_dirname
            new_filepath = '/'.join(new_filename)
        else:
            #Rexeg has not been recognised pushing in the home dir
            LOGGER.error("The regex to match the file destination failed file %s", basename)
            continue

        if  new_filepath == touple[1]:
            LOGGER.info("Destination is correct")
        else: 
            LOGGER.error("Destination is not correct" + new_filepath + ' ' + touple[1])
            continue

            #.limit(1)
        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(status='FINISHED')
            .filter_by(dst_path=filename[0] + '/')
            .filter_by(file_name=filename[1])
            .filter_by(resubmit_id=None)
        )
        #print str(records)

        if records.count() == 0:
            LOGGER.error('No record found')
            continue

        LOGGER.info('Number of records: %s' , records.count())
        if records.count() > 1:
            LOGGER.error('Too many records found')
            continue
            
        for record in records:
            #stackoverflow.com/questions/8253978/sqlalchemy-get-object-not-bound-to-a-session
            #If you want a bunch of objects produced by querying a session to be usable outside
            # the scope of the session, you need to expunge them for the session.
            session.expunge_all()
            LOGGER.info("Associated id %s", record.id)
            print record
            #continue

            if castor.mv(myfile, new_filepath):
                LOGGER.info("mv %s to %s Successful!", myfile, new_filepath)
            else:
                LOGGER.error("Cannot mv %s to %s !", myfile, new_filepath)
                continue

            if castor.file_exist(new_filepath):
                LOGGER.info("File %s Exists!", new_filepath)
            else:
                LOGGER.error("File %s Not Exists!", new_filepath)
                continue

            try:
                record.dst_path = new_filename[0]
                record.file_name = new_filename[1]
                record.comment = 'File moved from: ' + myfile + ' to ' + new_filepath
                session.merge(record)
                session.commit()
                LOGGER.info("Commit is succesful")
            except Exception as exception:
                session.rollback()
                LOGGER.error(
                    'Failed to commit on the database: ' +
                    'Exception message' + exception.message
                )

        #break #Just one
  
if __name__ == '__main__':
    main()
