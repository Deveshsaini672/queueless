from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import engine, get_db
from models import Base, QueueEntry, Status
from schemas import JoinResponse, CallNextResponse, DashboardResponse, QueueEntryResponse
from redis_client import redis_client
from tasks import send_notification

AVG_SERVICE_TIME_MINUTES = 4
NO_SHOW_GRACE_MINUTES = 2

STATUS_ORDER = {
    Status.called: 0,
    Status.waiting: 1,
    Status.served: 2,
    Status.no_show: 3,
    Status.left: 4,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(lifespan=lifespan)


# Redis helpers 
async def redis_get_next_token(counter_id: str) -> str:
    # INCR is atomic, therefore race condition (that can arise in case of only postgres) is fixed 
    next_number = await redis_client.incr(f"token_counter:{counter_id}")
    return f"A{100 + next_number}"


async def redis_add_waiting(counter_id: str, token: str, joined_at: datetime):
    score = joined_at.timestamp()
    await redis_client.zadd(f"queue:{counter_id}", {token: score})


async def redis_remove_waiting(counter_id: str, token: str):
    await redis_client.zrem(f"queue:{counter_id}", token)


async def redis_get_position(counter_id: str, token: str) -> int | None:
    rank = await redis_client.zrank(f"queue:{counter_id}", token)
    return rank  # 0-indexed, None if not present


# JOIN 
@app.post("/queue/{counter_id}/join", response_model=JoinResponse)
async def join_queue(counter_id: str, db: AsyncSession = Depends(get_db)):
    joined_at = datetime.utcnow()
    token = await redis_get_next_token(counter_id)

    # Postgres = permanent record
    entry = QueueEntry(
        token=token,
        counter_id=counter_id,
        joined_at=joined_at,
        status=Status.waiting,
    )
    db.add(entry)
    await db.commit()

    # Redis = live position tracker
    await redis_add_waiting(counter_id, token, joined_at)

    people_ahead = await redis_get_position(counter_id, token)
    people_ahead = people_ahead if people_ahead is not None else 0

    return JoinResponse(
        token=token,
        position=people_ahead + 1,
        people_ahead=people_ahead,
        estimated_wait_minutes=people_ahead * AVG_SERVICE_TIME_MINUTES,
    )


# STATUS 
@app.get("/queue/{counter_id}/status/{token}", response_model=JoinResponse)
async def get_status(counter_id: str, token: str, db: AsyncSession = Depends(get_db)):
    entry = await _get_entry(db, counter_id, token)
    if entry.status != Status.waiting:
        raise HTTPException(400, f"Token status is '{entry.status}', not waiting")

    people_ahead = await redis_get_position(counter_id, token)
    if people_ahead is None:
        # Fallback: Redis lost this key somehow (e.g. if restarted) ,not expected in normal operation
        raise HTTPException(500, "Position tracker missing for this token — try rejoining")

    return JoinResponse(
        token=token,
        position=people_ahead + 1,
        people_ahead=people_ahead,
        estimated_wait_minutes=people_ahead * AVG_SERVICE_TIME_MINUTES,
    )


# CALL NEXT 
@app.post("/queue/{counter_id}/call-next", response_model=CallNextResponse)
async def call_next(counter_id: str, db: AsyncSession = Depends(get_db)):
    now = datetime.utcnow()

    # Auto-expire anyone 'called' past the grace period
    result = await db.execute(
        select(QueueEntry).where(
            QueueEntry.counter_id == counter_id,
            QueueEntry.status == Status.called,
        )
    )
    for e in result.scalars():
        if e.called_at and (now - e.called_at).total_seconds() / 60 > NO_SHOW_GRACE_MINUTES:
            e.status = Status.no_show
    await db.commit()

    # Get the earliest-joined waiting token directly from Redis ie,0th index
    front = await redis_client.zrange(f"queue:{counter_id}", 0, 0)
    if not front:
        raise HTTPException(400, "No one is waiting in this queue")
    token = front[0]

    result = await db.execute(
        select(QueueEntry).where(QueueEntry.counter_id == counter_id, QueueEntry.token == token)
    )
    next_entry = result.scalar_one_or_none()
    if next_entry is None:
        raise HTTPException(500, "Redis/Postgres out of sync — token in Redis but not DB")

    next_entry.status = Status.called
    next_entry.called_at = now
    await db.commit()

    # No longer "waiting" ,so have to remove from the live position tracker
    await redis_remove_waiting(counter_id, token)
    await notify_if_close(counter_id)
    send_notification.delay(token, f"Please proceed to {counter_id}")

    return CallNextResponse(
        token=next_entry.token,
        counter_id=counter_id,
        message=f"Please proceed to {counter_id}",
    )


# SERVE 
@app.post("/queue/{counter_id}/serve/{token}")
async def mark_served(counter_id: str, token: str, db: AsyncSession = Depends(get_db)):
    entry = await _get_entry(db, counter_id, token)
    if entry.status != Status.called:
        raise HTTPException(400, f"Cannot serve, status is '{entry.status}'")
    entry.status = Status.served
    await db.commit()
    return {"token": token, "status": "served"}


# NO SHOW 
@app.post("/queue/{counter_id}/no-show/{token}")
async def mark_no_show(counter_id: str, token: str, db: AsyncSession = Depends(get_db)):
    entry = await _get_entry(db, counter_id, token)
    if entry.status != Status.called:
        raise HTTPException(400, f"Token is '{entry.status}', not called")
    entry.status = Status.no_show
    await db.commit()
   
    return {"token": token, "status": "no_show"}


# LEAVE
@app.post("/queue/{counter_id}/leave/{token}")
async def leave_queue(counter_id: str, token: str, db: AsyncSession = Depends(get_db)):
    entry = await _get_entry(db, counter_id, token)
    if entry.status != Status.waiting:
        raise HTTPException(400, f"Cannot leave, status is '{entry.status}'")
    entry.status = Status.left
    await db.commit()

    # leaving the live queue, not just the DB record
    await redis_remove_waiting(counter_id, token)
    await notify_if_close(counter_id)

    return {"token": token, "status": "left"}


# DASHBOARD 
@app.get("/queue/{counter_id}/dashboard", response_model=DashboardResponse)
async def get_dashboard(counter_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(QueueEntry).where(QueueEntry.counter_id == counter_id)
    )
    entries = list(result.scalars())
    if not entries:
        raise HTTPException(404, "Counter not found or has no entries yet")

    currently_serving_entry = next((e for e in entries if e.status == Status.called), None)
    currently_serving = currently_serving_entry.token if currently_serving_entry else None
    waiting_count = sum(1 for e in entries if e.status == Status.waiting)

    sorted_entries = sorted(entries, key=lambda e: (STATUS_ORDER[e.status], e.joined_at))

    return DashboardResponse(
        counter_id=counter_id,
        currently_serving=currently_serving,
        waiting_count=waiting_count,
        entries=[
            QueueEntryResponse(
                token=e.token, status=e.status, joined_at=e.joined_at, called_at=e.called_at
            )
            for e in sorted_entries
        ],
    )


# helpers 
async def _get_entry(db: AsyncSession, counter_id: str, token: str) -> QueueEntry:
    result = await db.execute(
        select(QueueEntry).where(QueueEntry.counter_id == counter_id, QueueEntry.token == token)
    )
    entry = result.scalar_one_or_none()
    if entry is None:
        raise HTTPException(404, "Token not found")
    return entry



async def notify_if_close(counter_id: str):
    # Check the person now at position 2 (rank 1, 0-indexed) and notify them
    upcoming = await redis_client.zrange(f"queue:{counter_id}", 1, 1)
    if upcoming:
        token = upcoming[0]
        send_notification.delay(token, "You're 2 positions away.")