'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import pwd
import grp
import os
import re
import sys
import logging
import traceback

class SourceController(object):
    """
    Handle source and destination of a local file
    """
    ## TODO change host - host_id? basedestination?
    def __init__(self, host, source):
        self.logger = logging.getLogger(__name__)
        self.host = host
        self.source = source
        self.base_destination = '/castor/cern.ch/na62/data/2019/raw/run/'

    def get_source(self):
        """
        Return the source full name
        """
        return self.source

    def get_owner(self):
        """
        Return the Source owner
        """
        try:
            return pwd.getpwuid(os.stat(self.source).st_uid).pw_name
        except KeyError, error:
            self.logger.error("Cannot fetch the file owner " + error.message)
            return 'nobody'

    def get_group(self):
        """
        Rerurn the group owner
        """
        try:
            group_id = os.stat(self.source).st_gid
            if group_id == 1338:
                return 'vl'

            return pwd.getpwuid(group_id).pw_name
        except KeyError, error:
            #Needs to be logged
            self.logger.error(
                "Cannot fetch the file group " + error.message +
                ' ' + traceback.format_exception(*sys.exc_info())
            )
            return 'nobody'
    
    ## for new Transfer __init__
    def get_file_id(self):
        """
        Return the source file's id
        """
        return self.id

    def get_dir_name(self):
        """
        Return the Source path with the / in the end
        """
        return os.path.dirname(self.source) + '/'

    def get_base_name(self):
        """
        Return the Source basename
        """
        return os.path.basename(self.source)

    def get_size(self):
        """
        Return the Source size
        """
        return os.path.getsize(self.source)

    def is_size_zero(self):
        """
        Return True if the Source file is 0
        """
        return self.get_size() == 0

    def set_group(self):
        """
        Set the the group to the Source
        """
        uid = pwd.getpwnam("na62cdr").pw_uid
        gid = grp.getgrnam("vl").gr_gid
        return os.chown(self.source, uid, gid)

    def delete(self):
        """
        Delete the Source
        """
        if self.exists():
            os.remove(self.source)
            return True
        self.logger.error("Cannot delete file %s not exist", self.source)
        return False

    def rename(self):
        """
        Rename the source appending .del at the end
        """
        if self.exists():
            os.rename(self.source, self.source + '.del')
            return True
        self.logger.error("Cannot rename file %s not exist", self.source)
        return False

    def exists(self):
        """
        Return True if the file exist locally
        """
        if os.path.exists(self.source):
            return True
        return False

    def get_destination(self):
        """
        Return the Remote Source destination
        """
        if self.host != 'na62primitive':
            return self.get_raw_data_destination()
        return self.get_primitive_destination()

    def get_raw_data_destination(self):
        """
        Return the destination for raw file
        """
        regex_match = re.match(
            "na62raw_([0-9]*)-([0-9]*)-([0-9]*)-([0-9]*).*\.dat",
            self.get_base_name()
        )
        if regex_match:
            destination = self.base_destination + regex_match.group(3) + '/'
        else:
            #Rexeg has not been recognised pushing in the home dir
            self.logger.error("The regex to find the file destination failed file %s", self.get_base_name())
            destination = self.base_destination

        return destination

    def get_primitive_destination(self):
        """
        Return the destination for primitive files
        """
        return self.base_destination + 'primitives/'

    ## TODO the source url is now in "hosts" table. obsolete function.
    def cern_host(self):
        """
        Return the host appendinf .cern.ch
        """
        return self.host + '.cern.ch'

    def get_fts_source(self):
        """
        Return the FTS link
        """
        return 'gsiftp://' + self.cern_host() + self.get_source()

    ## TODO the destination url is now in "hosts" table
    def get_fts_castor_destination(self):
        """
        Return the srm link
        """
        return 'srm://srm-public.cern.ch:8443/srm/managerv2?SFN=' + \
            str(self.get_destination()) + self.get_base_name()
