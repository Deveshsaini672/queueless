from pydantic import BaseModel
from datetime import datetime
from models import Status


class JoinResponse(BaseModel):
    token: str
    position: int
    people_ahead: int
    estimated_wait_minutes: int


class CallNextResponse(BaseModel):
    token: str
    counter_id: str
    message: str


class QueueEntryResponse(BaseModel):
    token: str
    status: Status
    joined_at: datetime
    called_at: datetime | None


class DashboardResponse(BaseModel):
    counter_id: str
    currently_serving: str | None
    waiting_count: int
    entries: list[QueueEntryResponse]
