import csv,hashlib,io,json,os
from pathlib import Path
from ..config.settings import ARTIFACT_DIR

def result_json(result):
    return json.dumps(result,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))

def result_checksum(result):
    return hashlib.sha256(result_json(result).encode('utf-8')).hexdigest()

def csv_bytes(result):
    buf=io.StringIO();fields=result['columns'];writer=csv.DictWriter(buf,fieldnames=fields)
    writer.writeheader()
    for item in result['rows']:writer.writerow({key:item.get(key,'') for key in fields})
    return ('﻿'+buf.getvalue()).encode('utf-8')

def sha256(data):
    return hashlib.sha256(data).hexdigest()

def run_dir(run_id):
    return ARTIFACT_DIR/f'run-{run_id}'

def artifact_path(run_id,token):
    return run_dir(run_id)/f'{token}.csv'

def write_artifact(run_id,token,data,guard=None):
    """Write atomically, refusing to publish if the lease guard fails."""
    directory=run_dir(run_id);directory.mkdir(parents=True,exist_ok=True)
    target=artifact_path(run_id,token);temporary=directory/f'.tmp-{token}-{os.getpid()}'
    with open(temporary,'wb') as handle:
        handle.write(data);handle.flush();os.fsync(handle.fileno())
    # A superseded holder must not modify the published archive.
    if guard is not None and not guard():
        try:temporary.unlink()
        except OSError:pass
        raise PermissionError('任务持有权已失效')
    os.replace(temporary,target)
    # Re-check after the rename so a lease lost mid-write cannot leave a
    # foreign archive behind for later readers.
    if guard is not None and not guard():
        try:target.unlink()
        except OSError:pass
        raise PermissionError('任务持有权已失效')
    return target

def find_matching(run_id,data):
    """Return the token of an already published, byte-identical archive.

    This makes a retry idempotent: an archive written just before a crash is
    adopted instead of being published a second time.
    """
    directory=run_dir(run_id)
    if not directory.is_dir():return None
    digest=sha256(data)
    for path in sorted(directory.glob('*.csv')):
        try:
            if path.stat().st_size!=len(data):continue
            if sha256(path.read_bytes())==digest:return path.stem
        except OSError:continue
    return None

def remove_other_archives(run_id,keep_token,guard=None):
    """Drop stale archives; a fenced-out holder must not delete anything."""
    directory=run_dir(run_id)
    if not directory.is_dir():return
    for path in directory.glob('*.csv'):
        if path.stem==keep_token:continue
        if guard is not None and not guard():return
        try:path.unlink()
        except OSError:pass

def archive_status(run):
    """Verify that the stored result and its archive are complete and agree."""
    if run.result is None:return False,'结果尚未入库'
    try:canonical=result_checksum(run.result)
    except (TypeError,ValueError):return False,'结果内容不是可校验的有限数值'
    if not run.result_checksum or canonical!=run.result_checksum:return False,'结果内容与入库校验和不符'
    if not run.artifact_ready or not run.artifact_token:return False,'留档尚未发布'
    path=artifact_path(run.id,run.artifact_token)
    try:data=path.read_bytes()
    except OSError:return False,'留档文件缺失'
    if len(data)!=run.artifact_size or sha256(data)!=run.artifact_checksum:return False,'留档文件校验和不符'
    try:expected=csv_bytes(run.result)
    except Exception:return False,'结果无法生成留档内容'
    if expected!=data:return False,'留档与结果内容不符'
    return True,''
