import logging,time
from sqlalchemy import select,update
from fastapi import HTTPException
from redis import Redis
from ..config.settings import QUEUE_KEY,REDIS_URL,DISPATCH_GRACE_SECONDS
from ..models.run import Run
from ..utils.serialization import row,now
from .sources import source_or_404,list_records
from .domain_validation import validate_parameters
from .audit import log
from . import lease
queue=Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2)
def get_run(db,id):
    r=db.get(Run,id)
    if not r:raise HTTPException(404,'未找到分析任务')
    return r
def create_run(db,user,data):
    s=source_or_404(db,data.source_id);records=list_records(db,s.id)
    if len(records)<3:raise HTTPException(422,'至少录入3条样本后才能执行')
    try: params=validate_parameters(data.parameters)
    except ValueError as e:raise HTTPException(422,str(e))
    r=Run(source_id=s.id,name=data.name,parameters=params,snapshot={'source':row(s),'records':records},
          created_at=now(),state='queued',heartbeat_at=time.time())
    db.add(r);db.flush();log(db,user,'run.queued',r.id,data.name);db.commit()
    # Acceptance is durable: even if the queue is down, the run stays queued and
    # the dispatcher re-delivers it once Redis returns.
    enqueue(db,r.id)
    return public(r)
def enqueue(db,run_id):
    """Best-effort queue delivery. The database row remains the source of truth."""
    try:
        queue.rpush(QUEUE_KEY,run_id)
        return True
    except Exception:
        logging.warning('queue unavailable; run %s kept pending for redelivery',run_id)
        return False
def dispatch_pending(db,limit=200):
    """Re-deliver pending runs that no live worker picked up.

    Both queued (never started) and recovering (executor lost) rows are pushed,
    so a queue that was down when the run was accepted is healed automatically.
    A claim re-checks the row state, so duplicate deliveries are harmless.
    """
    clock=time.time()
    cutoff=clock-DISPATCH_GRACE_SECONDS
    candidates=list(db.scalars(select(Run).where(
        Run.state.in_(('queued','recovering')),Run.heartbeat_at<=cutoff
    ).order_by(Run.id).limit(limit)))
    sent=[]
    for run in candidates:
        # Fence the marker update: only the tick that still sees the same
        # pending row claims the redelivery, so parallel dispatchers agree.
        marked=db.execute(update(Run).where(
            Run.id==run.id,Run.state==run.state,Run.heartbeat_at<=cutoff
        ).values(heartbeat_at=clock))
        if marked.rowcount!=1:db.rollback();continue
        db.commit()
        if enqueue(db,run.id):sent.append(run.id)
    if sent:logging.info('redelivered pending runs %s',sent)
    return sent
def public(run,full=False):
    data=row(run)
    hidden=['lease_owner','lease_token','lease_expires_at','heartbeat_at','artifact_token']
    if not full:hidden+=['snapshot','result']
    for key in hidden:data.pop(key,None)
    return data
def list_runs(db):
    return [public(r) for r in db.scalars(select(Run).order_by(Run.id.desc()))]
