from enum import Enum
class Role(str,Enum):
    admin='admin'; analyst='analyst'; viewer='viewer'
class RunState(str,Enum):
    queued='queued'; running='running'; recovering='recovering'; completed='completed'; failed='failed'
class ReviewState(str,Enum):
    draft='draft'; approved='approved'
