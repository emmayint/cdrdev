#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import logging
import logging.config
from controllers import Gfal2Controller

import logging
import gfal2

if __name__ == '__main__':
#    logging.config.fileConfig('na62cdr-logging.ini')
    LOGGER = logging.getLogger(__name__)

    #exist
    URLS = ['/castor/cern.ch/na62/data/2017/raw/run/008042/na62raw_1503965806-02-008042-1501.dat']
    #not exist
    URLS = ['/castor/cern.ch/na62/data/2017/raw/run/008042/na62raw_1503946483-01-008042-0795.dat']

    URLS = [
        '/castor/cern.ch/na62/data/2017/raw/run/TestTransfer-2015-05-15-15-15',
        '/castor/cern.ch/na62/data/2017/raw/run/008042/na62raw_1503946483-01-008042-0795.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505894648-02-008128-3028.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/chod_run08128_burst03030',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/irc_run08128_burst03030',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/muv3_run08128_burst03030',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/lkr_run08128_burst03030',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505894728-01-008128-3033.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/irc_run08128_burst03040',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895239-03-008128-3065.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895255-01-008128-3066.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895270-02-008128-3067.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895286-03-008128-3068.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/muv3_run08128_burst03070',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/irc_run08128_burst03070',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/rich_run08128_burst03070',
        '/castor/cern.ch/na62/data/2017/raw/run/primitives/lkr_run08128_burst03070',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895302-01-008128-3069.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895318-02-008128-3070.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505866527-02-008128-1627.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895350-01-008128-3072.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895366-02-008128-3073.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505866575-02-008128-1630.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895398-01-008128-3075.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505895414-02-008128-3076.dat',
        '/castor/cern.ch/na62/data/2017/raw/run/008128/na62raw_1505894616-03-008128-3026.dat',
        '/castor/cern.ch/na62/data/2016/raw/run/2016-09-25/na62raw_1474829017-03-006356-0827.dat',
        '/castor/cern.ch/na62/data/2016/raw/run/2016-09-26/na62raw_1474841557-02-006356-1468.dat'
    ]

    castor = Gfal2Controller()
    for url in URLS:
        if castor.file_exist(url):
            LOGGER.info("File %s Exists!", url)
        else:
            LOGGER.info("File %s Not Exists!", url)

        print castor.get_size(url)
        #info = castor.stat(url)
        #print info.st_size
        continue
        if castor.is_on_tape(url):
            LOGGER.info("File %s On tape!", url)
        else:
            LOGGER.info("File %s not On tape!", url)

    exit(1)


    path = 'srm://srm-public.cern.ch:8443/srm/managerv2?SFN=' + '/castor/cern.ch/na62/data/2017/raw/run/007987/na62raw_1502717519-01-007987-0066.dat'
    context = gfal2.creat_context()
    try:
        print context.listxattr(path)

        print context.stat(path)
        a = context.stat(path)
        print a
	print a.st_gid

	print a.st_size
        #['user.replicas', 'user.status', 'srm.type', 'spacetoken']
        #print context.getxattr(path, 'user.status')
        #print context.getxattr(path, 'user.replicas')
        #print context.getxattr(path, 'srm.type')
        #print context.getxattr(path, 'spacetoken')
        #self.logger.debug('Castor file status: '+ castor_status + ' ' + path)
    except gfal2.GError, error:
        #logger.debug('Must be an existing file ' + path + ' ' + str(error.code) + ' ' +  error.message)
        print ('Must be an existing file ' + path + ' ' + str(error.code) + ' ' +  error.message)

#    def file_exist(self, path):
#        """
#        Return true if a file exist
#        """
#        try:
#            self.context.lstat(self.get_complete_url(path))
#            self.logger.debug('File exist: ' + path)
#            return True
#        except gfal2.GError, error:
#            self.logger.debug('File NOT exist: ' + path + ' ' + str(error.code) + ' ' +  error.message)
#            return False
#
#    def is_on_tape(self, path):
#        """
#        Return true if a file is on tape
#        """
#        if not self.file_exist(path):
#            return False
#
#
#        try:
#            castor_status = self.context.getxattr(self.get_complete_url(path), 'user.status')
#            self.logger.debug('Castor file status: '+ castor_status + ' ' + path)
#        except gfal2.GError, error:
#            self.logger.debug('Must be an existing file ' + path + ' ' + str(error.code) + ' ' +  error.message)
#
#        if castor_status.strip() in ["NEARLINE", "ONLINE_AND_NEARLINE"]:
#            self.logger.debug('File on tape: ' + path + ' ')
#            return True
#        else:
#            self.logger.debug('File NOT on tape: ' + path)
#            return False
