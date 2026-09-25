from celery_app import celery_app
import asyncio
from datetime import datetime

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def send_notification(self, token: str, message: str):
 
    print(f"[{token}] {message}")
    return {"token": token, "message": message, "sent": True}


NO_SHOW_GRACE_MINUTES = 2

@celery_app.task
def expire_no_shows():
    asyncio.run(_expire_no_shows_async())

async def _expire_no_shows_async():
    from database import AsyncSessionLocal
    from models import QueueEntry, Status
    from sqlalchemy import select

    async with AsyncSessionLocal() as db:
        now = datetime.utcnow()
        result = await db.execute(
            select(QueueEntry).where(QueueEntry.status == Status.called)
        )
        for e in result.scalars():
            if e.called_at and (now - e.called_at).total_seconds() / 60 > NO_SHOW_GRACE_MINUTES:
                e.status = Status.no_show
        await db.commit()
