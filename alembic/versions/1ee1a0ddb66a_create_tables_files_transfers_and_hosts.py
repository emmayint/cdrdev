"""create tables files, transfers and hosts

Revision ID: 1ee1a0ddb66a
Revises: 
Create Date: 2019-08-01 13:43:14.096230

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Table, Column, Float, Integer, BigInteger, String, MetaData, DateTime, ForeignKey, Enum
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import false, true

from sqlalchemy.sql import func

# revision identifiers, used by Alembic.
revision = '1ee1a0ddb66a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    hosts = op.create_table(
        'hosts',
        sa.Column('id', BIGINT(unsigned=True), primary_key=True),
        ## make "name" a key (unique?)
        sa.Column('name', String(255), nullable=False, unique=True),
        sa.Column('host_url', String(255), nullable=True)
    )
    ## initialize rows
    op.bulk_insert(hosts,
        [
            {'id':1, 'name':'merger1',
                    'host_url':'na62merger1.cern.ch'},
            {'id':2, 'name':'merger2',
                    'host_url':'na62merger2.cern.ch'},
            {'id':3, 'name':'merger3',
                    'host_url':'na62merger3.cern.ch'},
            {'id':4, 'name':'merger4',
                    'host_url':'na62merger4.cern.ch'},
            {'id':5, 'name':'merger5',
                    'host_url':'na62merger5.cern.ch'},
            {'id':6, 'name':'na62primitive',
                    'host_url':'na62primitive.cern.ch'},
            {'id':7, 'name':'CASTOR',
                    'host_url':'srm-public.cern.ch:8443/srm/managerv2?SFN='},
            ## EOS host ulr?
            {'id':8, 'name':'EOS',
                    'host_url':''},

            {'id':9, 'name':'localsrc',
                    'host_url':'cc7alembicsqlalchemy.cern.ch'}
            
        ]
    )
    files = op.create_table(
        'files',
        sa.Column('id', BIGINT(unsigned=True), primary_key=True),
        sa.Column('file_name',       String(255), nullable=False),
        ## Foreign key action ondelete: do nothing by default
        sa.Column('src_host_id' ,    BIGINT(unsigned=True), ForeignKey("hosts.id"), nullable=False),
        sa.Column('src_path' ,       String(255), nullable=False),
        sa.Column('created_at',      sa.TIMESTAMP, nullable=False, server_default=func.current_timestamp()),
        sa.Column('updated_at',      sa.TIMESTAMP, nullable=False),
        sa.Column('file_size',       BIGINT(unsigned=True), nullable=False, server_default=None),
        sa.Column('is_src_deleted',  sa.types.BOOLEAN, nullable=False, default=False, server_default=false()),
        sa.Column('deleted_at',      sa.TIMESTAMP, nullable=True)
    )
    op.bulk_insert(files, 
        [
            {'id':1, 'file_name':'file1.txt',
                    'src_host_id': '9',
                    'src_path': '/merger/cdr',
                    'file_size': 304},
            {'id':2, 'file_name':'file2.txt',
                    'src_host_id': '9',
                    'src_path': '/merger/cdr',
                    'file_size': 304},
            {'id':3, 'file_name':'file3.txt',
                    'src_host_id': '9',
                    'src_path': '/merger/cdr',
                    'file_size': 304},
            {'id':4, 'file_name':'file4.txt',
                    'src_host_id': '9',
                    'src_path': '/merger/cdr',
                    'file_size': 304}
        ]
    )
    
    transfers = op.create_table(
        'transfers',
        sa.Column('id', BIGINT(unsigned=True), primary_key=True),
        sa.Column('file_id', BIGINT(unsigned=True), ForeignKey("files.id"), nullable=False),
        sa.Column('job_id',          String(255)),
        sa.Column('dst_host_id' ,    BIGINT(unsigned=True), ForeignKey("hosts.id"), nullable=False),
        sa.Column('dst_path' ,       String(255)),
        ## delcair Enum with new_enum_type from revision fde0be5859f7
        sa.Column('status',          sa.Enum('SUBMITTED', 'ACTIVE', 'FINISHED', 'READY', 'STAGING', 'STARTED', 'FAILED', name='status'), nullable=False, default=1, server_default="SUBMITTED"),
        sa.Column('transferred_at',  sa.TIMESTAMP, nullable=True),
        sa.Column('transfer_attemp', sa.types.Integer, nullable=False, default=1, server_default="1"),
        sa.Column('resubmit_id',     BIGINT(unsigned=True), ForeignKey("transfers.id"), default=None),
        sa.Column('is_invalid',      sa.types.BOOLEAN, nullable=False, default=False, server_default=false()),
        sa.Column('comment',         String(255), nullable=True, default=None),
        ## sa.types.BOOLEAN?
        sa.Column('is_on_tape',      sa.Boolean(), nullable=False, server_default=false()),
        sa.Column('on_tape_at',      sa.TIMESTAMP, nullable=True),
        ## same columns as in files table:
        sa.Column('created_at',      sa.TIMESTAMP, nullable=False, server_default=func.current_timestamp()),
        sa.Column('updated_at',      sa.TIMESTAMP, nullable=False)
    )
    
    #op.create_index('ik_is_on_tape', 'transfers', ['is_on_tape'])

    
def downgrade():
    #op.drop_index('ik_is_on_tape', 'transfers')
    
    op.drop_table('transfers')
    op.drop_table('files')
    op.drop_table('hosts')
    