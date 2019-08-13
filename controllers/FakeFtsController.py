'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''

#import sys
import logging
#from datetime import timedelta
#import fts3.rest.client.easy as fts3

import string
from random import *

class FakeFtsController(object):
    """
    Handle the FTS library
    """
    def __init__(self, endpoint):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Initializing: ' + endpoint)

    def submit(self, full_source, full_destination, metadata=None, overwrite=False):
        """
        Submit a job
        """
        min_char = 32
        max_char = 32
        #allchar = string.ascii_letters + string.punctuation + string.digits
        allchar = string.ascii_letters + string.digits
        job_id = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
        self.logger.debug('Submitting from ' + full_source + ' to ' + full_destination + ' with job_id ' + job_id)
        return job_id

    def get_status(self, job_id):
        """
        Retrieve the status of a submitted job
        """

        random_number = uniform(0, 1)
        if random_number < 0.5:
            self.logger.debug('job_id: ' + job_id + ' status: FINISCHED')
            return 'FINISHED'
        #else random_number >= 0.5:
        else:
            self.logger.debug('job_id: ' + job_id + ' status: FAILED')
            return 'FAILED'
