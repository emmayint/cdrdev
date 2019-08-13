'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''

import sys
import traceback
from datetime import datetime, timedelta
from Transfer import Transfer
from Host import Host
from File import File

#http://stackoverflow.com/questions/448271/what-is-init-py-for

#Common functions
def configSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                print "skip: %s" % option
        except:
            print "exception on %s!" % option
            dict1[option] = None
    return dict1

def showDatabase(session):
    try:
        records = (
            session.query(Transfer)
            .order_by(Transfer.created_at.asc())
            .limit(40)
        )
        if records.count() > 0:
            for record in records:
                print record
        else:
            print 'No record!'
    except:
        print 'Error retrieving  data from the database'
        raise
    finally:
        # release the connection to the pool
        session.close()


import ConfigParser
Config = ConfigParser.ConfigParser()
Config.read('/etc/na62cdr.ini')

USER = configSectionMap("Database")['user']
PASSWORD = configSectionMap("Database")['password']
HOST = configSectionMap("Database")['host_server']
PORT = configSectionMap("Database")['port']
DATABASE = configSectionMap("Database")['database']

from sqlalchemy import create_engine
engine = create_engine(
    'mysql://' + USER +
    ':' + PASSWORD +
    '@' + HOST +
    ':' + PORT +
    '/' + DATABASE,
    echo=False
)


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)

#from controllers import SourceController, FtsController
#host = ConfigSectionMap("Default")['host']
#fts_instance = FtsController('https://fts3-daq.cern.ch:8446')
