from sqlalchemy import Boolean,Column,Float,ForeignKey,Integer,JSON,String,Text
from ..config.database import Base
from ..config.settings import CONFIG
class Run(Base):
    __tablename__=CONFIG['run_table']
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=False)
    name=Column(String(160),nullable=False)
    # queued=待算 running=执行 recovering=恢复 completed=已完成 failed=业务数据终止
    state=Column(String(20),default='queued',nullable=False)
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # Fencing lease: only the holder of the current owner+token may commit.
    lease_owner=Column(String(120),default='',nullable=False)
    lease_token=Column(String(64),default='',nullable=False)
    # Lease timestamps are epoch seconds and need sub-second precision. Plain
    # Float maps to MySQL FLOAT (single) which cannot represent 1.7e9 precisely.
    lease_expires_at=Column(Float(53),default=0.0,nullable=False)
    heartbeat_at=Column(Float(53),default=0.0,nullable=False)
    attempts=Column(Integer,default=0,nullable=False)
    # Publication bookkeeping: result is stored before the archive files are written.
    result_checksum=Column(String(64),default='',nullable=False)
    artifact_token=Column(String(64),default='',nullable=False)
    artifact_checksum=Column(String(64),default='',nullable=False)
    artifact_size=Column(Integer,default=0,nullable=False)
    artifact_ready=Column(Boolean,default=False,nullable=False)
    completed_at=Column(String(40),default='',nullable=False)
