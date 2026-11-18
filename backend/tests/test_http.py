import time
import pytest,fakeredis
from fastapi.testclient import TestClient
from src.config.database import Session
from src.models.user import User
from src.models.run import Run
from src.models import source,record,note,audit,user
from src.utils.security import hash_password
from src.services import runs as runs_service
import src.worker as worker
import src.main as main


@pytest.fixture()
def client(monkeypatch,clean,fake_queue):
    monkeypatch.setattr(main,'queue',fake_queue)
    with Session() as db:
        if not db.query(User).filter(User.email=='analyst@test.demo').first():
            db.add(User(email='analyst@test.demo',password_hash=hash_password('Research2026!'),role='analyst'))
            db.add(User(email='viewer@test.demo',password_hash=hash_password('Research2026!'),role='viewer'))
            db.commit()
    with TestClient(main.app) as c:yield c


def login(client,email):
    response=client.post('/auth/login',json={'email':email,'password':'Research2026!'})
    assert response.status_code==200,response.text
    return {'Authorization':'Bearer '+response.json()['token']}


def build_source(client,headers):
    created=client.post('/sources',headers=headers,json={'name':'测试试展','filename':'t.csv','description':'测试数据来源'})
    assert created.status_code==200,created.text
    source_id=created.json()['id']
    values=[('V1','E1',4),('V1','E2',5),('V1','E3',3),('V2','E1',2),('V2','E2',4),('V2','E3',5),
            ('V3','E1',5),('V3','E2',3),('V3','E3',4),('V4','E1',3),('V4','E2',5),('V4','E3',2)]
    records=[{'reference':f'R{i:02}','values':{'visitor':v,'exhibit':e,'rating':r}} for i,(v,e,r) in enumerate(values)]
    added=client.post(f'/sources/{source_id}/records',headers=headers,json={'records':records})
    assert added.status_code==200,added.text
    return source_id


def wait(client,headers,run_id):
    for _ in range(100):
        detail=client.get(f'/runs/{run_id}',headers=headers).json()
        if detail['state'] in ('completed','failed'):return detail
        time.sleep(.1)
    raise AssertionError('run did not settle')


def test_end_to_end_accept_execute_download(client):
    headers=login(client,'analyst@test.demo')
    source_id=build_source(client,headers)
    created=client.post('/runs',headers=headers,json={'source_id':source_id,'name':'十一月复核研究','parameters':{'neighbors':5,'min_common':2}})
    assert created.status_code==200,created.text
    run=created.json()
    assert run['state']=='queued', 'queue delivery problems must not fail acceptance'
    assert 'lease_token' not in run
    # Queue message is present, worker consumes it.
    item=runs_service.queue.blpop(runs_service.QUEUE_KEY,timeout=1)
    assert item and int(item[1])==run['id']
    assert worker.execute(run['id']) is True
    detail=wait(client,headers,run['id'])
    assert detail['state']=='completed' and detail['artifact_ready']
    assert detail['completed_at']
    export=client.get(f"/runs/{run['id']}/export",headers=headers)
    assert export.status_code==200
    assert export.headers['X-Artifact-SHA256']==detail['artifact_checksum']
    assert export.text.splitlines()[0].lstrip('﻿').startswith('visitor,')
    # Frozen parameters and input count survive.
    assert detail['parameters']=={'neighbors':5,'min_common':2}
    assert len(detail['snapshot']['records'])==12


def test_export_blocked_until_archive_matches(client):
    headers=login(client,'analyst@test.demo')
    source_id=build_source(client,headers)
    created=client.post('/runs',headers=headers,json={'source_id':source_id,'name':'留档校验研究','parameters':{'neighbors':3,'min_common':2}}).json()
    assert client.get(f"/runs/{created['id']}/export",headers=headers).status_code==409
    assert worker.execute(created['id']) is True
    detail=wait(client,headers,created['id'])
    from src.services import artifacts
    with Session() as db:token=db.get(Run,created['id']).artifact_token
    artifacts.artifact_path(created['id'],token).write_bytes(b'bad')
    blocked=client.get(f"/runs/{created['id']}/export",headers=headers)
    assert blocked.status_code==409
    with Session() as db:assert db.get(Run,created['id']).state=='recovering'
    assert worker.execute(created['id']) is True
    assert wait(client,headers,created['id'])['state']=='completed'
    assert client.get(f"/runs/{created['id']}/export",headers=headers).status_code==200


def test_bad_parameters_rejected_at_acceptance(client):
    headers=login(client,'analyst@test.demo')
    source_id=build_source(client,headers)
    response=client.post('/runs',headers=headers,json={'source_id':source_id,'name':'越界参数','parameters':{'neighbors':99,'min_common':2}})
    assert response.status_code==422
    assert 'neighbors' in response.json()['detail']
