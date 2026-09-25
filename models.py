from sqlalchemy import String, Integer, DateTime, Enum as SAEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from enum import Enum


class Base(DeclarativeBase):
    pass


class Status(str, Enum):
    waiting = "waiting"
    called = "called"
    served = "served"
    no_show = "no_show"
    left = "left"


class QueueEntry(Base):

    __tablename__ = "queue_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(String, index=True)
    counter_id: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[Status] = mapped_column(SAEnum(Status), default=Status.waiting)
    joined_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    called_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
