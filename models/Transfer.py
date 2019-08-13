'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
'''
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Enum, ForeignKey, TIMESTAMP, Boolean
from sqlalchemy.sql import func

from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import false, true

class Transfer(declarative_base()):
    """
    Model for the table: transfers
    """
    __tablename__ = 'transfers'

    id = Column(BIGINT, primary_key=True)
    file_id = Column(BIGINT, ForeignKey("files.id"), nullable=False)
    job_id = Column(String(255))
    dst_host_id = Column(BIGINT, ForeignKey("hosts.id"), nullable=False)
    dst_path = Column(String(255))
    status = Column(
        Enum(
            'SUBMITTED',
            'ACTIVE',
            'FINISHED',
            'READY',
            'STAGING',
            'STARTED',
            'FAILED'
        ),
        default='SUBMITTED',
        nullable=False
    )
    transferred_at = Column(TIMESTAMP(timezone=True), nullable=True, default=None)
    transfer_attemp = Column(Integer, default=1)
    resubmit_id = Column(BIGINT, ForeignKey('transfers.id'))
    is_invalid = Column(Boolean, default=0) #Integer
    comment = Column(String(255), default=None)
    is_on_tape = Column(Boolean, default=0) #Integer
    on_tape_at = Column(TIMESTAMP(timezone=True), nullable=True, default=None)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())

    # id = Column(Integer, primary_key=True)
    # file_name = Column(String(255))
    # src_host = Column(
    #     Enum(
    #         'na62merger1',
    #         'na62merger2',
    #         'na62merger3',
    #         'na62merger4',
    #         'na62merger5',
    #         'na62primitive'
    #     )
    # )
    # src_path = Column(String(255))
    # file_size = Column(Integer)
    # is_src_deleted = Column(Integer, default=0)
    # deleted_at = Column(TIMESTAMP(timezone=True), nullable=True, default=None)
    # resubmit_id = Column(Integer, ForeignKey('transfers.id'))


    def __init__(self, file_id, file_size, dst_host_id, dst_path):
        self.file_id = file_id
        self.file_size = file_size
        self.dst_host_id = dst_host_id
        self.dst_path = dst_path
        
    # def __init__(self, src_host, src_path, file_name, file_size, dst_path, job_id, attemp):
    def __init__(self, file_id, file_size, dst_host_id, dst_path, job_id, attemp):
        self.file_id = file_id
        self.file_size = file_size
        self.dst_host_id = dst_host_id
        self.dst_path = dst_path
        self.job_id = job_id
        self.transfer_attemp = attemp

    def __repr__(self):
        return "<Transfer(%s %s %s %s %s %s %s)>" % (self.id, self.job_id, self.status, self.dst_path, self.created_at, self.updated_at, self.transfer_attemp)

    ##### moved to File.py
    # def getSourcePath(self):
    #     """
    #     Return the full source path: src_path + file_name
    #     """
    #     return self.src_path + self.file_name

    def getDestinationPath(self):
        """
        Return the full destination path: dst_path + file_name
        """
        return self.dst_path + self.file_name

