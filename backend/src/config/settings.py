import json, os
from pathlib import Path
CONFIG=json.loads((Path(__file__).parent / 'domain.json').read_text())
DATABASE_URL=os.getenv('DATABASE_URL','sqlite:///./dev.db')
REDIS_URL=os.getenv('REDIS_URL','redis://localhost:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-only-change-before-deployment')
ARTIFACT_DIR=Path(os.getenv('ARTIFACT_DIR','/artifacts'))
# Dispatch protocol tuning. QUEUE_KEY holds run ids; the database row remains
# the source of truth so a lost queue message can always be re-delivered.
QUEUE_KEY=os.getenv('RUN_QUEUE_KEY','analysis:queue')
LEASE_SECONDS=float(os.getenv('RUN_LEASE_SECONDS','30'))
HEARTBEAT_SECONDS=float(os.getenv('RUN_HEARTBEAT_SECONDS','5'))
DISPATCH_SECONDS=float(os.getenv('RUN_DISPATCH_SECONDS','5'))
# How long a queued run may sit without a live consumer before recovery re-queues it.
DISPATCH_GRACE_SECONDS=float(os.getenv('RUN_DISPATCH_GRACE_SECONDS','10'))
# A run that repeatedly loses its executor stops retrying and is reported failed.
MAX_ATTEMPTS=int(os.getenv('RUN_MAX_ATTEMPTS','8'))
