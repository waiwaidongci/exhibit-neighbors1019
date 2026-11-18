import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from .config.database import engine,ensure_schema
from .config.settings import CONFIG
from .models import user,source,record,run,note,audit
from .middlewares.errors import error_boundary
from .middlewares.rate_limit import rate_limit
from .routes import auth,sources,runs,notes,audit as audit_routes
from .services.runs import queue
@asynccontextmanager
async def lifespan(app):
    ensure_schema()
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:logging.exception('seed failed; service remains available')
    yield
app=FastAPI(title=CONFIG['title'],lifespan=lifespan)
app.middleware('http')(error_boundary)
app.middleware('http')(rate_limit)
for router in (auth.router,sources.router,runs.router,notes.router,audit_routes.router):app.include_router(router)
@app.get('/health')
def health():
    # Database is required. A queue outage degrades to deferred dispatch: runs
    # stay queued durably and are redelivered when Redis returns, so it is not a
    # reason to pull the API out of service.
    with engine.connect() as c:c.execute(text('SELECT 1'))
    try:
        queue.ping();queue_state='ok'
    except Exception:queue_state='unavailable'
    return {'status':'ok','queue':queue_state}
@app.get('/catalog')
def catalog():return CONFIG
