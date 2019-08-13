'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''

import logging
import gfal2

class Gfal2Controller(object):
    """
    Allow to check the file status using the gfal2 library
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.context = gfal2.creat_context()

    def get_complete_url(self, path):
        """
        Return the complete srm url: srm://srm-public.cern.ch:8443/srm/managerv2?SFN + path
        """
        return 'srm://srm-public.cern.ch:8443/srm/managerv2?SFN=' + path

    def file_exist(self, path):
        """
        Return true if a file exist
        """
        try:
            self.context.lstat(self.get_complete_url(path))
            self.logger.debug('File exist: ' + path)
            return True
        except gfal2.GError, error:
            self.logger.debug('File NOT exist: ' + path + ' ' + str(error.code) + ' ' +  error.message)
            return False

    def is_on_tape(self, path):
        """
        Return true if a file is on tape
        Others possible value to query
        print context.listxattr(path)
        ['user.replicas', 'user.status', 'srm.type', 'spacetoken']
        """
        if not self.file_exist(path):
            return False


        try:
            castor_status = self.context.getxattr(self.get_complete_url(path), 'user.status')
            self.logger.debug('Castor file status: '+ castor_status + ' ' + path)
        except gfal2.GError, error:
            self.logger.error('Must be an existing file ' + path + ' ' + str(error.code) + ' ' +  error.message)

        if castor_status.strip() in ["NEARLINE", "ONLINE_AND_NEARLINE"]:
            self.logger.debug('File on tape: ' + path + ' ')
            return True
        else:
            self.logger.debug('File NOT on tape: ' + path)
            return False

    def mv(self, path1, path2):
        """
        Move a file from path1 to path2
        """
        if not self.file_exist(path1):
            return False
        if self.file_exist(path2):
            return False

        try:
            self.context.rename(self.get_complete_url(path1), self.get_complete_url(path2))
            return True
        except gfal2.GError, error:
            self.logger.error('Cannot move the file: ' + path1 + ' to ' + path2 + ' ' + str(error.code) + ' ' +  error.message)
            return False

    def get_size(self, path):
        """
        Returns the size information of a file on Castor:

        Other variable that can be acceesed
        uid: 47
        gid: 48
        mode: 100644
        size: 3946634256
        nlink: 1
        ino: 0
        ctime: 1502717573
        atime: 0
        mtime: 1502717573

        Append st_ in front:
        """
        try:
            return self.context.stat(self.get_complete_url(path)).st_size
        except gfal2.GError, error:
             self.logger.error('Error while getting the file info ' + path + ' ' + str(error.code) + ' ' +  error.message)

    def is_size_zero(self, path):
        """
        Returns true if the file size is 0
        """
        return self.get_size(self.get_complete_url(path)) == 0
