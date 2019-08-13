
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Enum, ForeignKey, TIMESTAMP, Boolean

from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import false, true

class Host(declarative_base()):
    """
    Model for the table: hosts
    """
    __tablename__ = 'hosts'

    id = Column(BIGINT, primary_key=True)
    name = Column(String(255))
    host_url = Column(String(255))

    #### TO DO
    def __init__(self, name, host_url):
        self.name = name
        self.host_url = host_url
    
    def __init__(self, name):
        self.name = name
        self.host_url = ''

    def __repr__(self):
        return "<Host(%s %s %s)>" % (self.id, self.name, self.host_url)

