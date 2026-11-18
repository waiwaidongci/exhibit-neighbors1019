import time,uuid
from sqlalchemy import or_,select,update
from ..config.settings import LEASE_SECONDS,MAX_ATTEMPTS
from ..models.run import Run
from ..utils.serialization import now
from .audit import log

class LeaseLost(Exception):
    """Raised when this worker no longer owns the run's fencing lease."""

def new_token():
    return uuid.uuid4().hex

def reap_expired(db,clock=None):
    """Move running runs whose lease expired to recovering.

    The lease expiry is part of the UPDATE predicate, so a worker that renewed
    its lease between the read and the write is never evicted by mistake.
    """
    clock=time.time() if clock is None else clock
    rows=list(db.scalars(select(Run).where(Run.state=='running',Run.lease_expires_at<clock)))
    recovered=[]
    for run in rows:
        result=db.execute(update(Run).where(
            Run.id==run.id,Run.state=='running',Run.lease_expires_at==run.lease_expires_at
        ).values(state='recovering',lease_owner='',lease_token='',lease_expires_at=0.0,heartbeat_at=clock,
                 error='执行者失联或执行中断，等待自动恢复'))
        if result.rowcount==1:
            recovered.append(run.id);log(db,'system','run.recovering',run.id,'执行者失联，任务等待恢复')
    if recovered:db.commit()
    return recovered

def claim(db,worker_id,run_id=None):
    """Atomically take the next runnable run. Returns the Run or None."""
    reap_expired(db)
    clock=time.time()
    query=select(Run.id).where(Run.state.in_(('queued','recovering')))
    if run_id is not None:query=query.where(Run.id==run_id)
    for candidate in list(db.scalars(query.order_by(Run.id))):
        run=db.get(Run,candidate)
        if run is None:continue
        if run.attempts>=MAX_ATTEMPTS:
            reason='多次自动恢复仍未完成，已停止重试'+(('：'+run.error) if run.error else '')
            changed=db.execute(update(Run).where(
                Run.id==candidate,Run.state.in_(('queued','recovering'))
            ).values(state='failed',error=reason[:2000],lease_owner='',lease_token='',
                     lease_expires_at=0.0,heartbeat_at=clock))
            if changed.rowcount==1:log(db,'system','run.failed',candidate,reason[:500])
            db.commit();continue
        token=new_token()
        changed=db.execute(update(Run).where(
            Run.id==candidate,
            Run.state.in_(('queued','recovering')),
            or_(Run.lease_token=='',Run.lease_expires_at<clock),
        ).values(state='running',lease_owner=worker_id,lease_token=token,lease_expires_at=clock+LEASE_SECONDS,
                 heartbeat_at=clock,attempts=Run.attempts+1,error=''))
        if changed.rowcount==1:
            db.commit()
            return db.get(Run,candidate)
        db.rollback()
    return None

def owns(db,run_id,token,clock=None):
    # Ownership includes liveness: an expired lease is not authority even before
    # the reaper has noticed it.
    clock=time.time() if clock is None else clock
    return db.scalar(select(Run.id).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running',Run.lease_expires_at>=clock
    )) is not None

def heartbeat(db,run_id,token,clock=None):
    clock=time.time() if clock is None else clock
    result=db.execute(update(Run).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running'
    ).values(heartbeat_at=clock,lease_expires_at=clock+LEASE_SECONDS))
    db.commit()
    return result.rowcount==1

def _owned(db,run_id,token,values,clock=None):
    clock=time.time() if clock is None else clock
    values={**values,'heartbeat_at':clock,'lease_expires_at':clock+LEASE_SECONDS}
    return db.execute(update(Run).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running',Run.lease_expires_at>=clock
    ).values(**values))

def commit_result(db,run_id,token,result,checksum):
    changed=_owned(db,run_id,token,{'result':result,'result_checksum':checksum,
        'artifact_ready':False,'artifact_checksum':'','artifact_size':0,'artifact_token':''})
    if changed.rowcount!=1:
        db.rollback();raise LeaseLost('任务持有权已失效')
    db.commit()

def publish_artifact(db,run_id,token,checksum,size,artifact_token):
    changed=_owned(db,run_id,token,{'artifact_checksum':checksum,'artifact_size':size,
        'artifact_ready':True,'artifact_token':artifact_token})
    if changed.rowcount!=1:
        db.rollback();raise LeaseLost('任务持有权已失效')
    db.commit()

def complete(db,run_id,token,result_checksum,artifact_checksum):
    clock=time.time()
    changed=db.execute(update(Run).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running',Run.lease_expires_at>=clock,
        Run.result_checksum==result_checksum,Run.artifact_ready.is_(True),
        Run.artifact_checksum==artifact_checksum,
    ).values(state='completed',completed_at=now(),lease_owner='',lease_token='',lease_expires_at=0.0,
             heartbeat_at=clock,error=''))
    if changed.rowcount!=1:
        db.rollback();return False
    log(db,'worker','run.completed',run_id,result_checksum)
    db.commit()
    return True

def release_for_retry(db,run_id,token,reason):
    # If the lease expired, takeover already handled the run; leave it alone.
    changed=db.execute(update(Run).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running'
    ).values(state='recovering',lease_owner='',lease_token='',lease_expires_at=0.0,heartbeat_at=time.time(),
             error=reason[:2000]))
    if changed.rowcount==1:log(db,'system','run.recovering',run_id,reason[:500])
    db.commit()
    return changed.rowcount==1

def fail(db,run_id,token,reason):
    # A stale holder may not terminate a run it no longer owns. Expiry alone is
    # not enough to fence this: an unexpired takeover always replaces the token.
    changed=db.execute(update(Run).where(
        Run.id==run_id,Run.lease_token==token,Run.state=='running'
    ).values(state='failed',error=reason[:2000],lease_owner='',lease_token='',lease_expires_at=0.0,
             heartbeat_at=time.time()))
    if changed.rowcount==1:log(db,'worker','run.failed',run_id,reason[:500])
    db.commit()
    return changed.rowcount==1

def mark_archive_broken(db,run_id,reason):
    """A completed run whose archive failed verification goes back to recovery."""
    changed=db.execute(update(Run).where(
        Run.id==run_id,Run.state=='completed'
    ).values(state='recovering',artifact_ready=False,error=reason[:2000],lease_owner='',lease_token='',
             lease_expires_at=0.0,heartbeat_at=time.time()))
    if changed.rowcount==1:log(db,'system','run.recovering',run_id,reason[:500])
    db.commit()
    return changed.rowcount==1

def requeue_unarchived(db):
    """Requeue completed rows that have no published archive.

    This is how runs completed before the archive field existed (or a run whose
    publication was rolled back) get their missing archive materialised exactly
    once. The stored result is reused, so nothing is recomputed or republished.
    """
    clock=time.time()
    rows=list(db.scalars(select(Run).where(Run.state=='completed',Run.artifact_ready.is_(False))))
    requeued=[]
    for run in rows:
        changed=db.execute(update(Run).where(
            Run.id==run.id,Run.state=='completed',Run.artifact_ready.is_(False)
        ).values(state='recovering',error='结果已入库但缺少留档，等待补齐',lease_owner='',lease_token='',
                 lease_expires_at=0.0,heartbeat_at=clock))
        if changed.rowcount==1:
            requeued.append(run.id);log(db,'system','run.recovering',run.id,'补齐缺失留档')
    if requeued:db.commit()
    return requeued
