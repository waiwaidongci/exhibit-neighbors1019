import os,tempfile,pathlib
ROOT=pathlib.Path(tempfile.mkdtemp(prefix='exhibit-tests-'))
os.environ['DATABASE_URL']=f'sqlite:///{ROOT}/test.db'
os.environ['ARTIFACT_DIR']=str(ROOT/'artifacts')
os.environ['REDIS_URL']='redis://127.0.0.1:1/0'
os.environ['RUN_LEASE_SECONDS']='60'
os.environ['RUN_DISPATCH_GRACE_SECONDS']='0'
os.environ['RUN_MAX_ATTEMPTS']='5'
os.environ['SEED_DEMO']='false'
os.environ['JWT_SECRET']='test-secret'
import sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
import pytest,fakeredis,time
from src.config.database import ensure_schema,Session
from src.models import user,source,record,run,note,audit
ensure_schema()
from src.models.run import Run
from src.models.source import Source
from src.models.record import Record
from src.models.user import User
from src.models.note import Note
from src.services import lease,artifacts,runs,export,notes
from src.services.audit import log
from src.utils.serialization import now
import src.worker as worker

@pytest.fixture(autouse=True)
def fake_queue(monkeypatch):
    client=fakeredis.FakeRedis()
    monkeypatch.setattr(runs,'queue',client)
    monkeypatch.setattr(worker,'queue',client)
    for module in (runs,worker):module.queue.flushall()
    yield client

@pytest.fixture()
def clean():
    with Session() as db:
        for model in (Note,Run,Record,Source,User,audit.Audit):
            for row in db.query(model):db.delete(row)
        db.commit()
    directory=pathlib.Path(os.environ['ARTIFACT_DIR'])
    for path in directory.rglob('*'):
        if path.is_file():path.unlink()
    yield

def make_source(db,n=12):
    source=Source(key=f'K-{time.time_ns()}',name='测试来源',filename='x.csv',description='d')
    db.add(source);db.flush()
    for index in range(n):
        visitor=f'V{index//3:02}';exhibit=f'E{index%3:02}'
        db.add(Record(source_id=source.id,reference=f'R{index:03}',values={'visitor':visitor,'exhibit':exhibit,'rating':float(1+index%5)}))
    db.commit()
    return source

def make_run(db,source,records,parameters=None,name='t'):
    r=Run(source_id=source.id,name=name,state='queued',parameters=parameters or {'neighbors':5,'min_common':2},
          snapshot={'source':{'id':source.id,'name':source.name,'filename':source.filename},'records':records},created_at=now())
    db.add(r);db.flush();db.commit();return r

def records_for(db,source_id):
    return [{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values}
            for r in db.query(Record).filter(Record.source_id==source_id).order_by(Record.id)]
