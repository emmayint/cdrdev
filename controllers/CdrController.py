'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
import logging
import os
import glob
import sys
import traceback
from datetime import datetime, timedelta
from controllers import SourceController
from controllers import Gfal2Controller
from models import Transfer
import time

class CdrController(object):
    """
    Handle all the cdr actions
    """
    def __init__(self, fts_controller, host):
        self.logger = logging.getLogger(__name__)
        self.logger.info('Creating an instance of CdrController')
        self.fts_instance = fts_controller
        self.host = host
        self.castor = Gfal2Controller.Gfal2Controller()

    def get_new_files(self, source_dir):
        """
        Return a list of new files found in the source dir
        """
        new_sources = []
        amount = 0
        source_dir = source_dir + '/'
        files = filter(os.path.isfile, glob.glob(source_dir + "*"))
        files.sort(key=lambda x: os.path.getmtime(x))

        for new_file in files:
            self.logger.info('Checking file: ' + new_file)
            new_source = SourceController(self.host, new_file)
            if new_source.get_owner() == 'na62cdr' and new_source.get_group() == 'root':
                new_sources.append(new_source)
                # Limiting the amount of concurrent submission
                amount += 1
                if amount > 100:
                    break
        return new_sources

    ## TODO separate file logging from submission
    def submit_files(self, session, file_dir):
        """
        Submit new files
        """
        self.logger.info('Check for new files')
        count_success = 0
        count_fail = 0
        for new_file in self.get_new_files(file_dir):
            if self.do_first_submit(session, new_file):
                count_success += 1
                self.logger.info('Succesfully submitted file: ' + new_file.get_source())
            else:
                count_fail += 1
                self.logger.error(
                    'Error during Submission of: ' + new_file.get_source()
                )
        return (count_success, count_fail)
    
    def do_first_submit(self, session, source):
        """
        Submit a job
        """
        #I will not transfer files with local size equal to 0
        if source.is_size_zero():
            self.logger.error("The local File size is 0 deleting: " + source.get_source())
            source.delete()
            #source.rename()
            return False

        try:
            (job_id, database_id) = self.submit(session, source)
        except Exception as exception:
            self.logger.error(
                'Error submitting the job' + source.get_source() +
                ' exception message:  ' + str(exception.message)
            )
            return False

        try:
            #change group owner to vl
            source.set_group()
        except Exception as exception:
            self.logger.error(
                'Error cannot change the group permission to job_id: ' + job_id  +
                ' exception message ' + str(exception.message)
            )
            return False

        return True

    def submit(self, session, source, record=None, overwrite=False):
        """
        Submit a job and store it in the database
        """
        metadata = 'NA62 raw data file'
        attemp = 1
        # I am receiving further information from an existing record on the database
        if record is not None:
            metadata = 'NA62 raw data file, resubmitting from job_id: ' + str(record.job_id) + \
                ' mariadb_id: ' + str(record.id)
            attemp = record.transfer_attemp + 1

        try:
            job_id = self.fts_instance.submit(
                source.get_fts_source(),
                source.get_fts_castor_destination(),
                metadata,
                overwrite=overwrite
            )
        except:
            self.logger.error(
                'Cannot submit the job: ' + source.get_source()
            )
            raise

        try:
        # def __init__(self, file_id, file_size, dst_host_id, dst_path, job_id, attemp):
            new_transfer = Transfer( 
                # self.host,
                # source.get_dir_name(),
                # source.get_base_name(),
                source.get_file_id(),
                source.get_size(),
                9,
                source.get_destination(),
                job_id,
                attemp
            )
            session.add(new_transfer)
            session.flush()

            database_id = new_transfer.id # should be not None
            session.commit()
        except:
            session.rollback()
            self.logger.error(
                'Error cannot add the  job_id: ' + job_id + ' to the database'
            )
            raise
        finally:
            # release the connection to the pool
            session.close()

        return (job_id, database_id)

    def resubmit(self, session, source, record, overwrite=False):
        """
        Resubmit a failed Job
        """
        old_job_id = record.job_id #Because it will expire on session commit
        try:
            (job_id, database_id) = self.submit(session, source, record, overwrite=overwrite)
        except:
            self.logger.error(
                'Unable to resend the record ' + source.get_source()
            )
            raise

        #Updating entry
        try:
            record.resubmit_id = database_id
            session.merge(record)
            session.commit()
            self.logger.info("Commit on the DB %s resubmit_id", record.resubmit_id)
        except:
            session.rollback()
            self.logger.error(
                "Failed Commit on the DB %s resubmit_id traceback %s"
            )
            raise
        finally:
            self.logger.info('Resubmited old_job_id: ' + old_job_id + ' with new job_id: ' + job_id + '!')

        return (job_id, database_id)


    def do_resubmit(self, session, minutes=60):
        """
        Resubmit a FAILAED job and store a new entry in the database
        """
        allow_resubmit_before = datetime.now() + timedelta(minutes=-int(minutes))
        #allow_delete_before = datetime.now() + timedelta(minutes=-life_in_minutes)
        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(src_host=self.host)
            .filter_by(status='FAILED')
            .filter_by(resubmit_id=None)
            .order_by(Transfer.updated_at.desc())
            .filter(Transfer.updated_at < allow_resubmit_before)
            .limit(1)
        )
        resubmit_count = 0

        if records.count() > 0:
            for record in records:
                session.expunge_all()
                self.logger.info('Resubmitting: ' + str(record.id))

                source = SourceController(self.host, record.getSourcePath())
                #Protecting by uncontrolled deleted sources
                if source.exists():
                    try:
                        self.resubmit(session, source, record)#No overwrite
                        resubmit_count += 1
                    except Exception as ex:
                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                        message = template.format(type(ex).__name__, ex.args)
                        self.logger.error(
                            'Error resubmitting status of file: ' + record.getSourcePath() +
                            ' exception message: ' + ex.message +
                            ' message: ' + message
                        )
                        continue
                else:
                    #Invalidating
                    self.logger.error(
                        'Error File doesnt exist on the source host: ' + record.getSourcePath()
                    )
                    try:
                        record.is_invalid = 1
                        record.comment = 'File doesnt exist on the source host'
                        session.merge(record)
                        session.commit()
                    except Exception as ex:
                        session.rollback()
                        self.logger.error(
                            'Error cannot invalidate the log on the Database: ' +
                            record.getSourcePath() +
                            ' exception message: ' + ex.message
                        )
                        continue

            # release the connection to the pool
            session.close()
        else:
            self.logger.info('No record to resubmit!')
        return resubmit_count

    #TODO deprecate
    def do_overwrite(self, session, amount=1):
        """
        Resubmit with overwrite true in case the remote file size is 0
        This function use a wrapper around Castor to understand if the remote
        file exist and the size is 0
         Check for  transfer status
        """
        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(src_host=self.host)
            .filter_by(status='FAILED')
            .filter_by(resubmit_id=None)
            .filter(Transfer.transfer_attemp >= 3)
            .order_by(Transfer.transfer_attemp.desc())
            #.order_by(Transfer.updated_at.desc())
            #.filter(Transfer.updated_at < allow_resubmit_before)
            .limit(amount)
        )

        overwrite_count = 0
        if records.count() > 0:
            for record in records:
                #stackoverflow.com/questions/8253978/sqlalchemy-get-object-not-bound-to-a-session
                #If you want a bunch of objects produced by querying a session to be usable outside
                # the scope of the session, you need to expunge them for the session.
                session.expunge_all()
                source_path = record.getSourcePath()
                source = SourceController(self.host, source_path)
                #Protecting by uncontrolled deleted sources
                if not source.exists():
                    #Invalidating
                    self.logger.error('Error File doesnt exist on the source host')
                    try:
                        record.is_invalid = 1
                        record.comment = 'File doesnt exist on the source host'
                        session.merge(record)
                        session.commit()
                    except Exception as exception:
                        session.rollback()
                        self.logger.error(
                            'Failed to commit on the database: ' +
                            'Exception message' + exception.message
                        )
                    continue

                dst = record.getDestinationPath()
                if not self.castor.file_exist(dst):
                    self.logger.error(
                        'File doesnt exist on the remote host with why I am overwriting? ' + dst
                    )
                    continue

                remote_size = self.castor.get_size(dst)
                database_size = record.file_size
                local_size = source.get_size()
                if self.castor.is_size_zero(dst):
                    if database_size == local_size:
                        # Cases where the file has 0 size on castor
                        self.logger.info('File suitable for overwrite: ' + dst)
                        try:
                            self.resubmit(session, source, record, overwrite=True)#overwrite
                            overwrite_count += 1
                        except Exception as exception:
                            self.logger.error(
                                'Failed resubmission with overwrite file: ' + dst +
                                'exception message: ' + exception.message
                            )
                            continue

                    else:
                        self.logger.error('File not suitable for overwrite:' + dst)
                        continue
                else:
                    # Cases where the file has the same size on database mergers and castor
                    # it must be a checksum error
                    all_sizes_are_equal = local_size == database_size and \
                        database_size == remote_size
                    if all_sizes_are_equal:
                        self.logger.info('File suitable for overwrite: ' + dst +
                            ' All sizes match size: ' + str(remote_size)
                        )
                        try:
                            self.resubmit(session, source, record, overwrite=True)#overwrite
                            overwrite_count += 1
                        except Exception as exception:
                            self.logger.error(
                                'Failed resubmission with overwrite file: ' + dst +
                                'exception message: ' + exception.message
                            )
                            continue
                    else:
                        ## In those cases file has been recreated with the same name..
                        self.logger.error('Sizes mismatch ' + dst)

        else:
            self.logger.info('No record to overwrite!')

        # release the connection to the pool
        session.close()
        return overwrite_count

    def fetch_transfer_status(self, session):
        """
        Fetch for transfer status of submitted files
        """
        self.logger.info('Fetch transfers status')

        count_finished = 0
        count_failed = 0
        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(src_host=self.host)
            .filter_by(status='SUBMITTED')
            .filter_by(resubmit_id=None)
            .order_by(Transfer.created_at.asc())
            .limit(100)
        )
        if records.count() > 0:
            for record in records:
                self.logger.info(
                    'Fetching transfer status of ' + record.job_id +
                    ' ' + record.getSourcePath()
                )

                try:
                    job_status = self.fts_instance.get_status(record.job_id)
                    self.logger.info('Job ' + record.job_id + ' status ' + job_status)
                except Exception as ex:
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    self.logger.error(
                        'Error retrieving status of job: ' + record.job_id +
                        ' message ' + message
                    )
                    continue

                if job_status in ('CANCELED'):
                    #I store the CANCELED one as FAILED otherwise I need to
                    #rewrite all the select queries
                    job_status = 'FAILED'

                if job_status in ('FINISHED', 'FAILED'):
                    try:
                        record.status = job_status
                        record.transferred_at = datetime.now()
                        session.merge(record)
                        session.commit()
                        self.logger.info("Commit on the DB %s transferred", record.job_id)
                    except Exception as exception:
                        session.rollback()
                        self.logger.error(
                            "Failed to Commit on the DB job_id %s \
                            exception message %s",
                            record.job_id, exception.message
                        )

                    if job_status == 'FINISHED':
                        count_finished += 1
                    elif job_status == 'FAILED':
                        count_failed += 1
                else:
                    # I'm not interested in other status
                    continue
        # release the connection to the pool
        session.close()
        return (count_finished, count_failed)

    def fetch_tape_status(self, session):
        """
        Fetch the tape status Of transferred files
        """
        minutes = 60*4
        allow_fetch_tape_status_before = datetime.now() + timedelta(minutes=-int(minutes))
        self.logger.info('Check if completed transfers are on tape')

        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(status='FINISHED')
            .filter_by(src_host=self.host)
            .filter_by(is_on_tape=0)
            .filter(Transfer.updated_at < allow_fetch_tape_status_before)
            .order_by(Transfer.created_at.asc())
            .limit(100)
        )
        if records.count() > 0:
            for record in records:
                if self.castor.is_on_tape(record.getDestinationPath()):
                    self.logger.info(record.getDestinationPath() + " On tape!")
                    try:
                        record.is_on_tape = 1
                        record.on_tape_at = datetime.now()
                        session.merge(record)
                        session.commit()
                    except Exception as exception:
                        session.rollback()
                        self.logger.error(
                            "Failed to Commit on the DB bool is_on_tape job_id %s \
                            exception message %s",
                            record.job_id, exception.message
                        )
                else:
                    #store the time of the last is on tape check
                    self.logger.info(record.getDestinationPath() + " Not On tape")
                    try:
                        record.on_tape_at = datetime.now()
                        session.merge(record)
                        session.commit()
                    except Exception as exception:
                        session.rollback()
                        self.logger.error(
                            "Failed to Commit on the DB bool is_on_tape job_id %s \
                            exception message %s",
                            record.job_id, exception.message
                        )
        else:
            self.logger.info('No Record found!')

        session.close()
        return

    def delete_files(self, session, life_in_minutes):
        """
        Check for files to delete and deletes them
        """
        allow_delete_before = datetime.now() + timedelta(minutes=-life_in_minutes)
        self.logger.info('Delete Files before: ' + str(allow_delete_before))
        records = (
            session.query(Transfer)
            .filter_by(is_invalid=0)
            .filter_by(src_host=self.host)
            .filter_by(status='FINISHED')
            .filter_by(is_src_deleted=0)
            .filter_by(is_on_tape=1)
            .order_by(Transfer.created_at.asc())
            .filter(Transfer.created_at < allow_delete_before)
            .limit(100)
        )
        count_delete = 0
        if records.count() > 0:
            for record in records:
                self.logger.info("Deleting Source: " + record.getSourcePath())
                new_source = SourceController(self.host, record.getSourcePath())
                new_source.delete()
                try:
                    record.is_src_deleted = 1
                    record.deleted_at = datetime.now()
                    session.merge(record)
                    session.commit()
                    self.logger.info("Commit on the DB %s is_source_deleted", record.job_id)
                    count_delete += 1
                except Exception as exception:
                    session.rollback()
                    self.logger.error(
                        "Failed Commit on the DB %s is_source_deleted exception\
                        message %s",
                        record.job_id, exception.message
                    )
        else:
            self.logger.info("No files to delete")

        # release the connection to the pool
        session.close()
        return count_delete

    def is_disk_full(self, file_dir):
        """
        Check if the disk size goes beyon a cerntain limit
        """
        threshold = 0.90
        statvfs = os.statvfs(file_dir)
        disk_occupancy = 1 - float(statvfs.f_bfree) / float(statvfs.f_blocks)
        if disk_occupancy > threshold:
            self.logger.error("Disk occupancy: " + str(disk_occupancy))
            return True

        return False

    def delete_over_threshold(self, session, file_dir):
        """
        Deletes file transferred but not on tape
        """
        number_of_cicle = 0
        #if self.is_disk_full(file_dir):
        while self.is_disk_full(file_dir) and number_of_cicle < 15 :
            number_of_cicle += 1
            #Check first for files on tape non deleted because of the timeout
            records = (
                session.query(Transfer)
                .filter_by(is_invalid=0)
                .filter_by(src_host=self.host)
                .filter_by(status='FINISHED')
                .filter_by(is_src_deleted=0)
                .filter_by(is_on_tape=1)
                .order_by(Transfer.created_at.asc())
                .limit(100)
            )
            count_deleted_on_tape = 0
            count_deleted_not_on_tape = 0
            if records.count() > 0:
                for record in records:
                    self.logger.info("Deleting Source on tape: " + record.getSourcePath())
                    new_source = SourceController(self.host, record.getSourcePath())
                    new_source.delete()
                    try:
                        record.is_src_deleted = 1
                        record.deleted_at = datetime.now()
                        session.merge(record)
                        session.commit()
                        self.logger.info("Commit on the DB %s is_source_deleted", record.job_id)
                        count_delete_on_tape += 1
                    except Exception as exception:
                        session.rollback()
                        self.logger.error(
                            "Failed Commit on the DB %s is_source_deleted exception\
                            message %s",
                            record.job_id, exception.message
                        )
                    break

            if count_deleted_on_tape == 0:
                # I need to delete data non stored on tape
                records = (
                    session.query(Transfer)
                    .filter_by(is_invalid=0)
                    .filter_by(src_host=self.host)
                    .filter_by(status='FINISHED')
                    .filter_by(is_src_deleted=0)
                    .filter_by(is_on_tape=0)
                    .order_by(Transfer.created_at.asc())
                    .limit(100)
                )
                if records.count() > 0:
                    for record in records:
                        self.logger.info("Deleting Source not on tape: " + record.getSourcePath())
                        new_source = SourceController(self.host, record.getSourcePath())
                        new_source.delete()
                        try:
                            record.is_src_deleted = 1
                            record.deleted_at = datetime.now()
                            record.comment = 'File has been deleted when was not on tape'
                            session.merge(record)
                            session.commit()
                            self.logger.info("Commit on the DB %s is_source_deleted", record.job_id)
                            count_deleted_not_on_tape += 1
                        except Exception as exception:
                            session.rollback()
                            self.logger.error(
                                "Failed Commit on the DB %s is_source_deleted exception\
                                message %s",
                                record.job_id, exception.message
                            )
                        break

                    if count_deleted_not_on_tape == 0:
                        self.logger.error("Cannot delete any file!")
               
            self.logger.info("number of cycle " + str(number_of_cicle)  +  " Sleeping..")
            time.sleep(1)
