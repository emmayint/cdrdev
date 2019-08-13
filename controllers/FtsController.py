'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''

import sys
import logging
from datetime import timedelta
import fts3.rest.client.easy as fts3

class FtsController(object):
    """
    Handle the FTS library
    """
    def __init__(self, endpoint):
        self.logger = logging.getLogger(__name__)
        self.endpoint = endpoint
        self.context = fts3.Context(self.endpoint)

    def submit(self, full_source, full_destination, metadata=None, overwrite=False):
        """
        Submit a job
        """
        self.context._set_x509(None, None)
        checksum = None #if you already have a checksum of the file
        if metadata is None:
            metadata = 'Test submission'

        try:
            transfer = fts3.new_transfer(
                full_source, full_destination,
                checksum=checksum, filesize=None,
                metadata=metadata
            )

            #retry 1 will not retry the transfer
            job = fts3.new_job(
                [transfer],
                verify_checksum=True,
                overwrite=overwrite,
                metadata=metadata,
                retry=3
            )
            job_id = fts3.submit(self.context, job, timedelta(hours=7), force_delegation=True)

            self.logger.debug('Submitted: ' + job_id)
            return job_id
        except fts3.exceptions.TryAgain, exception:
            self.logger.error(
                'Error while submitting: ' + full_source +
                ' message ' + exception.message
            )
            raise
        except:
            self.logger.error(
                'Unxpected error while submitting: ' +  full_source +
                ' message ' + exception.message
                + ' ' + sys.exc_info()[0]
            )
            raise

    def get_status(self, job_id):
        """
        Retrieve the status of a submitted job
        """
        try:
            job_status = fts3.get_job_status(self.context, job_id)
        except fts3.exceptions.TryAgain, ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = type(ex).__name__ +  ex.args
            self.logger.error(
                'Error while retrieving status of job_id: ' + job_id +
                ' message ' + ex.message +
                ' ' + message +
                ' ' + sys.exc_info()[0]
            )
            raise
        except fts3.exceptions.NotFound, ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(
                'Error while retrieving status of job_id: ' + job_id +
                ' message ' + ex.message +
                ' ' + message +
                ' ' + sys.exc_info()[0]
            )
            raise
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(
                'Error while retrieving status of job_id: ' + job_id +
                ' message ' + ex.message +
                ' ' + message +
                ' ' + sys.exc_info()[0]
            )
            raise

        self.logger.debug('job_id: ' + job_id + ' status: ' + job_status['job_state'])
        return job_status['job_state']
