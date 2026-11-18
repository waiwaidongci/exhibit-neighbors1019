from sqlalchemy import create_engine,inspect,text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL,CONFIG
class Base(DeclarativeBase): pass
engine=create_engine(DATABASE_URL,pool_pre_ping=True)
Session=sessionmaker(bind=engine,expire_on_commit=False)
def db_session():
    with Session() as db: yield db
# Columns added after the first release. Tables are created from metadata, so a
# fresh database already has them; ALTER keeps an existing demo volume usable.
_RUN_COLUMNS={
    'lease_owner':"VARCHAR(120) NOT NULL DEFAULT ''",
    'lease_token':"VARCHAR(64) NOT NULL DEFAULT ''",
    'lease_expires_at':'DOUBLE NOT NULL DEFAULT 0',
    'heartbeat_at':'DOUBLE NOT NULL DEFAULT 0',
    'attempts':'INTEGER NOT NULL DEFAULT 0',
    'result_checksum':"VARCHAR(64) NOT NULL DEFAULT ''",
    'artifact_token':"VARCHAR(64) NOT NULL DEFAULT ''",
    'artifact_checksum':"VARCHAR(64) NOT NULL DEFAULT ''",
    'artifact_size':'INTEGER NOT NULL DEFAULT 0',
    'artifact_ready':'BOOLEAN NOT NULL DEFAULT 0',
    'completed_at':"VARCHAR(40) NOT NULL DEFAULT ''",
}
def ensure_schema():
    """Create missing tables and add post-release columns idempotently.

    Backend and worker start concurrently, so both may race on the same ALTER.
    Each column is attempted separately; a duplicate-column error from the
    other process is harmless while any other failure is logged and skipped.
    """
    Base.metadata.create_all(engine)
    table=CONFIG['run_table']
    try:
        inspector=inspect(engine)
        if not inspector.has_table(table):return
        existing={column['name'] for column in inspector.get_columns(table)}
    except Exception:
        import logging;logging.exception('run schema inspection skipped')
        return
    for name,definition in _RUN_COLUMNS.items():
        if name in existing:continue
        try:
            with engine.begin() as connection:
                connection.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {definition}'))
        except Exception:
            import logging;logging.exception('run schema upgrade skipped for %s',name)
