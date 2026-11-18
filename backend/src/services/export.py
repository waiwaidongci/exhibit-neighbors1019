import csv,io
from fastapi import HTTPException
from fastapi.responses import Response
from ..models.note import Note
from . import artifacts,lease
from .runs import get_run

def csv_response(result,filename):
    buf=io.StringIO();fields=result['columns'];writer=csv.DictWriter(buf,fieldnames=fields);writer.writeheader()
    for item in result['rows']:writer.writerow({key:item.get(key,'') for key in fields})
    return Response('﻿'+buf.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="'+filename+'"'})

def require_published(db,run):
    """Only a completed run whose result and archive agree may be downloaded."""
    if run.state!='completed':raise HTTPException(409,'结果尚未完成')
    ok,reason=artifacts.archive_status(run)
    if not ok:
        # A completed row with a missing or mismatched archive is not a valid
        # delivery. Send it back to recovery and refuse the download.
        lease.mark_archive_broken(db,run.id,reason)
        raise HTTPException(409,'结果留档校验未通过，已转入自动恢复：'+reason)
    return artifacts.artifact_path(run.id,run.artifact_token)

def run_csv(db,id):
    run=get_run(db,id)
    path=require_published(db,run)
    try:data=path.read_bytes()
    except OSError:
        reason='留档文件读取失败';lease.mark_archive_broken(db,run.id,reason)
        raise HTTPException(409,'结果留档校验未通过，已转入自动恢复：'+reason)
    return Response(data,media_type='text/csv; charset=utf-8',
                    headers={'Content-Disposition':'attachment; filename="run-'+str(id)+'.csv"',
                             'X-Artifact-SHA256':run.artifact_checksum})

def note_csv(db,id):
    note=db.get(Note,id)
    if not note:raise HTTPException(404,'未找到说明')
    if note.state!='approved':raise HTTPException(403,'仅批准后的业务说明可导出')
    return csv_response(note.frozen_result,'approved-'+str(id)+'.csv')
