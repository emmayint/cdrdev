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

class File(declarative_base()):
    """
    Model for the table: files
    """
    __tablename__ = 'files'

    id = Column(BIGINT, primary_key=True)
    file_name = Column(String(255))
    src_host_id = Column(BIGINT, ForeignKey("hosts.id"), nullable=False)
    src_path = Column(String(255))
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    file_size = Column(BIGINT, nullable=False)
    is_src_deleted = Column(Boolean, nullable=False, default=False) #Integer
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True, default=None)

    #### TO DO
    def __init__(self, file_name, file_size, src_host_id, src_path):
        self.file_name = file_name
        self.file_size = file_size
        self.src_host_id = src_host_id
        self.src_path = src_path


    def __repr__(self):
        return "<File(%s %s %s %s %s %s %s %s)>" % (self.id, self.src_host, self.src_path, self.file_name, self.file_size, self.created_at, self.updated_at, self.deleted_at)

    def getSourcePath(self):
        """
        Return the full source path: src_path + file_name
        """
        return self.src_path + self.file_name

    def getDestinationPath(self):
        """
        Return the full destination path: dst_path + file_name
        """
        return self.dst_path + self.file_name
