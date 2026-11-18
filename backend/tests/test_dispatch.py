import time
import pytest
from sqlalchemy import update
from src.config.database import Session
from src.models.run import Run
from src.services import lease,artifacts,runs
from src.utils.serialization import now
from conftest import make_source,make_run,records_for


def wait_completed(run_id,timeout=20):
    deadline=time.time()+timeout
    while time.time()<deadline:
        with Session() as db:
            run=db.get(Run,run_id)
            if run.state in ('completed','failed'):return run
        time.sleep(.2)
    raise AssertionError('run did not settle')


def process(run_id=None):
    import src.worker as worker
    return worker.execute(run_id)


def test_acceptance_survives_queue_outage(clean):
    with Session() as db:
        source=make_source(db)
        records=records_for(db,source.id)
        run=make_run(db,source,records)
        run_id=run.id
    # Simulate the queue being unavailable for the initial push.
    original=runs.queue.rpush
    runs.queue.rpush=lambda *a,**k:(_ for _ in ()).throw(ConnectionError('down'))
    captured={}
    try:
        # create_run is the acceptance path used by the API.
        class Data:
            source_id=source.id;name='队列中断研究';parameters={'neighbors':5,'min_common':2}
        with Session() as db:
            user=type('U',(),{'email':'analyst@test'})()
            created=runs.create_run(db,user,Data())
            captured['id']=created['id']
    finally:
        runs.queue.rpush=original
    with Session() as db:
        kept=db.get(Run,captured['id'])
        assert kept.state=='queued', 'accepted run must stay pending, not fail'
        assert kept.error==''
    # Recovery: dispatcher redelivers once the queue is back.
    with Session() as db:
        sent=runs.dispatch_pending(db)
    assert captured['id'] in sent
    delivered=[]
    while True:
        item=runs.queue.blpop(runs.QUEUE_KEY,timeout=1)
        if not item:break
        delivered.append(int(item[1]))
    assert captured['id'] in delivered
    # And the worker completes it.
    assert process(captured['id']) is True
    done=wait_completed(captured['id'])
    assert done.state=='completed' and done.artifact_ready
    assert artifacts.archive_status(done)[0]


def test_uses_frozen_snapshot_after_source_changes(clean):
    with Session() as db:
        source=make_source(db)
        records=records_for(db,source.id)
        parameters={'neighbors':2,'min_common':2}
        run=make_run(db,source,records,parameters)
        run_id=run.id
    with Session() as db:
        # Mutate the live source after acceptance; frozen snapshot must win.
        first=db.query(__import__('src.models.record',fromlist=['Record']).Record).first()
        first.values={'visitor':'CHANGED','exhibit':'E99','rating':1.0}
        db.commit()
    assert process(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed'
        assert done.parameters==parameters
        assert all(r['values']['visitor']!='CHANGED' for r in done.snapshot['records'])
        # The result checksum covers the frozen input, not the edited source.
        import hashlib,json
        expected=hashlib.sha256(json.dumps(done.snapshot['records'],sort_keys=True).encode()).hexdigest()
        assert done.result['checksum']==expected


def test_concurrent_claims_have_single_winner(clean):
    with Session() as db:
        source=make_source(db)
        run=make_run(db,source,records_for(db,source.id))
        run_id=run.id
    with Session() as db:
        first=lease.claim(db,'worker-a',run_id)
    with Session() as db:
        second=lease.claim(db,'worker-b',run_id)
    assert first is not None and second is None
    assert first.lease_owner=='worker-a'


def test_lease_takeover_and_old_owner_fenced(clean):
    with Session() as db:
        source=make_source(db)
        run=make_run(db,source,records_for(db,source.id))
        run_id=run.id
    with Session() as db:
        old=lease.claim(db,'worker-old',run_id);old_token=old.lease_token
    # Old owner goes silent; its lease expires.
    with Session() as db:
        db.execute(update(Run).where(Run.id==run_id).values(lease_expires_at=time.time()-1));db.commit()
    with Session() as db:
        lease.reap_expired(db)
        assert db.get(Run,run_id).state=='recovering'
    with Session() as db:
        new=lease.claim(db,'worker-new',run_id)
    assert new is not None and new.lease_token!=old_token
    # Old owner comes back: it may not commit a result, publish or complete.
    with pytest.raises(lease.LeaseLost):
        with Session() as db:lease.commit_result(db,run_id,old_token,{'rows':[],'columns':[]},'deadbeef')
    with pytest.raises(lease.LeaseLost):
        with Session() as db:lease.publish_artifact(db,run_id,old_token,'x',1,'deadbeef')
    with Session() as db:
        assert lease.complete(db,run_id,old_token,'a','b') is False
    with Session() as db:
        current=db.get(Run,run_id)
        assert current.state=='running' and current.lease_owner=='worker-new'
        assert current.result is None and current.artifact_ready is False
    # And it may not overwrite the archive file.
    with pytest.raises(PermissionError):
        artifacts.write_artifact(run_id,'deadbeef',b'forged',guard=lambda:lease.owns(Session(),run_id,old_token))
    with Session() as db:
        assert not artifacts.artifact_path(run_id,'deadbeef').exists()


def test_stale_owner_cannot_write_after_takeover(clean):
    with Session() as db:
        source=make_source(db)
        run=make_run(db,source,records_for(db,source.id))
        run_id=run.id
    with Session() as db:
        old=lease.claim(db,'worker-old',run_id);old_token=old.lease_token
    with Session() as db:
        db.execute(update(Run).where(Run.id==run_id).values(lease_expires_at=time.time()-1));db.commit()
    with Session() as db:
        lease.reap_expired(db);new=lease.claim(db,'worker-new',run_id)
    old_archive=artifacts.artifact_path(run_id,old_token)
    with pytest.raises(PermissionError):
        artifacts.write_artifact(run_id,old_token,b'old',guard=lambda:lease.owns(Session(),run_id,old_token))
    assert not old_archive.exists()
    assert new.state=='running'
