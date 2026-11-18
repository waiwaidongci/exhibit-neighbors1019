import json,logging,os,socket,threading,time
from redis import Redis
from .config.database import Session,ensure_schema
from .config.settings import ARTIFACT_DIR,DISPATCH_SECONDS,HEARTBEAT_SECONDS,QUEUE_KEY,REDIS_URL
from .models.run import Run
from .models import source,record,user,note,audit
from .services import lease,artifacts
from .services.algorithm import calculate
from .services.runs import dispatch_pending

class PublicationIncomplete(Exception):
    """Archive is not yet complete; the run should be recovered, not failed."""

WORKER_ID=f'{socket.gethostname()}:{os.getpid()}'
queue=Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2)

class Heartbeat(threading.Thread):
    """Keep the fencing lease alive while a calculation is running."""
    def __init__(self,run_id,token):
        super().__init__(daemon=True);self.run_id=run_id;self.token=token;self.stop=threading.Event();self.lost=False
    def run(self):
        while not self.stop.wait(HEARTBEAT_SECONDS):
            try:
                with Session() as db:
                    if not lease.heartbeat(db,self.run_id,self.token):self.lost=True;return
            except Exception:logging.exception('lease heartbeat failed')
    def halt(self):
        self.stop.set();self.join(timeout=5)

def guard(run_id,token):
    with Session() as db:return lease.owns(db,run_id,token)

def prepare(db,run):
    """Reuse an already stored result, or compute it once under the lease."""
    if run.result is not None and run.result_checksum:
        try:
            intact=artifacts.result_checksum(run.result)==run.result_checksum
        except (TypeError,ValueError):
            intact=False
        if intact:return run.result,run.result_checksum,'复用了已入库结果'
    result=calculate(run.snapshot['records'],run.parameters)
    # Refuse non-finite JSON before it ever reaches the database or archive.
    json.dumps(result,allow_nan=False)
    checksum=artifacts.result_checksum(result)
    lease.commit_result(db,run.id,run.lease_token,result,checksum)
    return result,checksum,'重新计算'

def publish(db,run,result,checksum,note):
    """Publish the archive idempotently, then mark complete only if both agree."""
    token=run.lease_token
    data=artifacts.csv_bytes(result)
    digest=artifacts.sha256(data)
    # Adopt a byte-identical archive left behind by an interrupted attempt, so a
    # retry never publishes the result twice.
    existing=artifacts.find_matching(run.id,data)
    artifact_token=existing or token
    if existing is None:
        artifacts.write_artifact(run.id,token,data,guard=lambda:guard(run.id,token))
        artifact_token=token
    if not lease.owns(db,run.id,token):raise lease.LeaseLost('任务持有权已失效')
    artifacts.remove_other_archives(run.id,artifact_token,guard=lambda:guard(run.id,token))
    lease.publish_artifact(db,run.id,token,digest,len(data),artifact_token)
    # Completion gate: the stored result and the published archive must agree.
    # The publish above used a Core UPDATE, so drop cached ORM state first.
    db.expire_all()
    run=db.get(Run,run.id)
    ok,reason=artifacts.archive_status(run)
    if not ok:raise PublicationIncomplete('结果与留档校验未通过：'+reason)
    if not lease.complete(db,run.id,token,checksum,digest):
        raise lease.LeaseLost('任务持有权已失效')
    logging.info('run %s %s and published archive %s (%s)',run.id,note,artifact_token,digest[:12])

def execute(run_id=None):
    with Session() as db:
        run=lease.claim(db,WORKER_ID,run_id)
        if run is None:return False
        token=run.lease_token;run_id=run.id
    beat=Heartbeat(run_id,token);beat.start()
    try:
        with Session() as db:
            run=db.get(Run,run_id)
            if run is None or run.lease_token!=token:raise lease.LeaseLost('任务持有权已失效')
            result,checksum,note=prepare(db,run)
        with Session() as db:
            if beat.lost:raise lease.LeaseLost('任务持有权已失效')
            run=db.get(Run,run_id)
            if run is None or run.lease_token!=token:raise lease.LeaseLost('任务持有权已失效')
            publish(db,run,result,checksum,note)
        return True
    except lease.LeaseLost as error:
        logging.warning('run %s lease lost: %s',run_id,error)
        return False
    except PublicationIncomplete as error:
        logging.warning('run %s publication incomplete: %s',run_id,error)
        with Session() as db:lease.release_for_retry(db,run_id,token,str(error))
        return False
    except (ValueError,KeyError,TypeError) as error:
        # Bad business data, parameters or result structure: stop, explain,
        # never auto-retry.
        logging.warning('run %s rejected: %s',run_id,error)
        with Session() as db:lease.fail(db,run_id,token,str(error))
        return False
    except Exception as error:
        logging.exception('run %s interrupted',run_id)
        with Session() as db:lease.release_for_retry(db,run_id,token,'执行中断，等待自动恢复：'+str(error))
        return False
    finally:
        beat.halt()

def sweep():
    """Redeliver pending runs and recover interrupted or orphaned runs."""
    with Session() as db:
        lease.reap_expired(db)
        lease.requeue_unarchived(db)
        dispatch_pending(db)

def main():
    ensure_schema()
    logging.basicConfig(level=logging.INFO)
    logging.info('worker %s started',WORKER_ID)
    last_sweep=0.0
    while True:
        # Liveness file consumed by the container healthcheck.
        try:(ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
        except OSError:logging.warning('cannot write heartbeat file')
        # Recovery and redelivery run on their own timer, independent of Redis.
        if time.time()-last_sweep>=DISPATCH_SECONDS:
            try:sweep()
            except Exception:logging.exception('recovery sweep failed')
            last_sweep=time.time()
        try:
            value=queue.blpop(QUEUE_KEY,timeout=2)
        except Exception:
            # Queue is down. Work still moves forward through the database.
            value=None;time.sleep(1)
        if value:
            try:
                if execute(int(value[1])):continue
            except (TypeError,ValueError):logging.warning('ignored malformed queue message %r',value)
        # No usable queue message: poll the database directly so accepted runs
        # still start when Redis is unavailable or a message was lost.
        try:execute()
        except Exception:logging.exception('worker poll failed');time.sleep(1)

if __name__=='__main__':main()
