import time
import pytest
from fastapi import HTTPException
from sqlalchemy import update
from src.config.database import Session
from src.models.run import Run
from src.models.note import Note
from src.services import lease,artifacts,runs,export,notes
from conftest import make_source,make_run,records_for
import src.worker as worker


def settle(run_id,timeout=20):
    deadline=time.time()+timeout
    while time.time()<deadline:
        with Session() as db:
            run=db.get(Run,run_id)
            if run.state in ('completed','failed'):return run
        time.sleep(.2)
    raise AssertionError('run did not settle')


def seed(clean,records=None):
    with Session() as db:
        source=make_source(db)
        run=make_run(db,source,records_for(db,source.id) if records is None else records)
        return run.id


class Data:
    def __init__(self,**kw):self.__dict__.update(kw)


def test_result_then_archive_interrupted_is_repaired(clean,monkeypatch):
    run_id=seed(clean)
    calls={'n':0}
    original=worker.publish
    def half(run_db,run,result,checksum,note):
        calls['n']+=1
        if calls['n']==1:
            # Result is committed, archive publishing is interrupted.
            raise RuntimeError('disk interrupted')
        return original(run_db,run,result,checksum,note)
    monkeypatch.setattr(worker,'publish',half)
    assert worker.execute(run_id) is False
    with Session() as db:
        state=db.get(Run,run_id)
        assert state.state=='recovering'
        assert state.result is not None                       # result survived
        assert state.artifact_ready is False                  # archive missing
        first_checksum=state.result_checksum
    monkeypatch.setattr(worker,'publish',original)
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed' and done.artifact_ready
        assert done.result_checksum==first_checksum
        ok,reason=artifacts.archive_status(done)
        assert ok,reason
        assert len(list(artifacts.run_dir(run_id).glob('*.csv')))==1


def test_retry_does_not_republish_result(clean,monkeypatch):
    run_id=seed(clean)
    original_complete=lease.complete
    def crash_before_complete(db,*args,**kwargs):
        raise RuntimeError('crashed after archive write')
    monkeypatch.setattr(lease,'complete',crash_before_complete)
    assert worker.execute(run_id) is False
    with Session() as db:
        state=db.get(Run,run_id)
        assert state.state=='recovering'
        assert state.result is not None and state.artifact_ready
        token_before=state.artifact_token
        archive=list(artifacts.run_dir(run_id).glob('*.csv'))
        assert len(archive)==1
    # Retry: same archive is adopted rather than written twice.
    monkeypatch.setattr(lease,'complete',original_complete)
    repair=worker.publish
    writes=[]
    def tracked(db,run,result,checksum,note):
        existed=artifacts.find_matching(run.id,artifacts.csv_bytes(result))
        assert existed is not None
        writes.append(existed)
        return repair(db,run,result,checksum,note)
    monkeypatch.setattr(worker,'publish',tracked)
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed'
        assert done.artifact_token==token_before
        assert len(list(artifacts.run_dir(run_id).glob('*.csv')))==1
    assert writes and writes[0]==token_before


def test_download_requires_result_and_archive_agreement(clean):
    run_id=seed(clean)
    assert worker.execute(run_id) is True
    with Session() as db:
        run=db.get(Run,run_id)
        # Corrupt the archive: content no longer matches the stored result.
        artifacts.artifact_path(run_id,run.artifact_token).write_bytes(b'tampered')
    with Session() as db:
        with pytest.raises(HTTPException) as error:export.run_csv(db,run_id)
        assert error.value.status_code==409
    with Session() as db:
        state=db.get(Run,run_id)
        assert state.state=='recovering', 'broken archive must leave completed state'
    # Recovery restores a valid archive and download works again.
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed'
        response=export.run_csv(db,run_id)
        assert response.body==artifacts.csv_bytes(done.result)


def test_missing_archive_is_rebuilt_on_next_dispatch(clean):
    run_id=seed(clean)
    assert worker.execute(run_id) is True
    with Session() as db:
        run=db.get(Run,run_id)
        artifacts.artifact_path(run_id,run.artifact_token).unlink()
    with Session() as db:
        ok,reason=artifacts.archive_status(db.get(Run,run_id))
        assert not ok and '缺失' in reason
        assert lease.mark_archive_broken(db,run_id,reason) is True
    with Session() as db:
        # Requeued for recovery, then repaired.
        assert db.get(Run,run_id).state=='recovering'
        runs.dispatch_pending(db)
    with Session() as db:
        runs.queue.blpop(runs.QUEUE_KEY,timeout=1)
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed' and artifacts.archive_status(done)[0]


def test_bad_business_data_fails_with_reason_and_no_archive(clean):
    with Session() as db:
        source=make_source(db)
        bad=[{'id':1,'source_id':source.id,'reference':'R1','values':{'visitor':'V1','exhibit':'E1','rating':9}},
             {'id':2,'source_id':source.id,'reference':'R2','values':{'visitor':'V2','exhibit':'E2','rating':4}},
             {'id':3,'source_id':source.id,'reference':'R3','values':{'visitor':'V3','exhibit':'E3','rating':3}}]
        run=make_run(db,source,bad);run_id=run.id
    assert worker.execute(run_id) is False
    with Session() as db:
        failed=db.get(Run,run_id)
        assert failed.state=='failed'
        assert '1–5' in failed.error or '评分' in failed.error
        assert failed.lease_token==''
        assert not artifacts.run_dir(run_id).exists() or not list(artifacts.run_dir(run_id).glob('*.csv'))
    # A failed business run is terminal: no further claim.
    with Session() as db:assert lease.claim(db,'w',run_id) is None


def test_completed_requires_both_parts(clean):
    run_id=seed(clean)
    with Session() as db:
        run=lease.claim(db,'w1',run_id)
        token=run.lease_token
    result=__import__('json').loads('{"rows":[],"columns":[],"summary":[]}')
    checksum=artifacts.result_checksum(result)
    with Session() as db:lease.commit_result(db,run_id,token,result,checksum)
    with Session() as db:
        # Result alone must not complete.
        assert lease.complete(db,run_id,token,checksum,'anything') is False
        assert db.get(Run,run_id).state=='running'
    with Session() as db:lease.publish_artifact(db,run_id,token,'deadbeef',10,'tok')
    with Session() as db:
        assert lease.complete(db,run_id,token,checksum,'mismatch') is False
        assert db.get(Run,run_id).state=='running'


def test_retry_rejects_tampered_stored_result(clean):
    run_id=seed(clean)
    assert worker.execute(run_id) is True
    with Session() as db:
        run=db.get(Run,run_id)
        tampered=dict(run.result);tampered['summary']=[];tampered['rows']=[]
        db.execute(update(Run).where(Run.id==run_id).values(result=tampered,artifact_ready=False));db.commit()
    with Session() as db:
        # A download/verify failure sends the run back for repair.
        assert lease.mark_archive_broken(db,run_id,'结果内容与入库校验和不符') is True
    # Recovery recomputes from the frozen snapshot instead of trusting the row.
    with Session() as db:
        runs.dispatch_pending(db);runs.queue.blpop(runs.QUEUE_KEY,timeout=1)
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed'
        ok,reason=artifacts.archive_status(done)
        assert ok,reason
        assert len(done.result['evaluation'])>0
        assert artifacts.result_checksum(done.result)==done.result_checksum


def test_legacy_completed_run_without_archive_is_repaired(clean):
    run_id=seed(clean)
    assert worker.execute(run_id) is True
    with Session() as db:
        run=db.get(Run,run_id)
        # Simulate a row completed before archive bookkeeping existed.
        db.execute(update(Run).where(Run.id==run_id).values(
            state='completed',artifact_ready=False,artifact_token='',
            artifact_checksum='',artifact_size=0))
        db.commit()
        artifacts.remove_other_archives(run_id,'none',guard=None)
        assert not list(artifacts.run_dir(run_id).glob('*.csv'))
    with Session() as db:
        assert lease.requeue_unarchived(db)==[run_id]
        assert db.get(Run,run_id).state=='recovering'
        runs.dispatch_pending(db);runs.queue.blpop(runs.QUEUE_KEY,timeout=1)
    assert worker.execute(run_id) is True
    with Session() as db:
        done=db.get(Run,run_id)
        assert done.state=='completed' and artifacts.archive_status(done)[0]
        assert len(list(artifacts.run_dir(run_id).glob('*.csv')))==1


def test_note_creation_blocked_when_archive_broken(clean):
    run_id=seed(clean)
    assert worker.execute(run_id) is True
    with Session() as db:
        run=db.get(Run,run_id)
        artifacts.artifact_path(run_id,run.artifact_token).write_bytes(b'broken')
    with Session() as db:
        run=db.get(Run,run_id)
        with pytest.raises(HTTPException) as error:
            notes.create_note(db,type('U',(),{'email':'a@b'})(),Data(run_id=run_id,title='标题',body='内容够长'))
        assert error.value.status_code==409
        assert db.get(Run,run_id).state=='recovering'
