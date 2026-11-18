from fastapi import HTTPException
from sqlalchemy import select
from ..models.note import Note
from ..utils.serialization import row
from .runs import get_run
from .export import require_published
from .audit import log

def list_notes(db):return [row(n) for n in db.scalars(select(Note).order_by(Note.id.desc()))]

def create_note(db,user,data):
    run=get_run(db,data.run_id)
    # Freezing a note on a run whose archive is broken would publish bad data.
    require_published(db,run)
    note=Note(run_id=run.id,title=data.title,body=data.body,state='draft',frozen_result=run.result,author=user.email)
    db.add(note);db.flush();log(db,user,'note.created',note.id,note.title);db.commit();return row(note)

def approve_note(db,user,id):
    note=db.get(Note,id)
    if not note:raise HTTPException(404,'未找到说明')
    if note.state!='draft':raise HTTPException(409,'该说明已经批准')
    note.state='approved';note.approved_by=user.email;log(db,user,'note.approved',note.id,note.title);db.commit();return row(note)
