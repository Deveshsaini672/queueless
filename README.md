# QueueLess — Smart Queue Management System

## Problem
People waste significant time standing in physical queues — colleges, hospitals,
government offices, banks, labs. QueueLess lets users join a queue digitally,
see their live position and estimated wait time, and get notified as their
turn approaches — instead of standing in line.

## Architecture
FastAPI (API layer)

PostgreSQL — permanent source of truth (queue entries, status history)

Redis — live state: atomic token generation (INCR), position tracking(Sorted Set), Celery message broker

Celery worker + Celery Beat — async notifications, scheduled no-show
expiry


## Tech stack
FastAPI, PostgreSQL (SQLAlchemy async), Redis, Celery + Celery Beat, Docker (for Redis)

## Features
- Join a queue, get token + live position + estimated wait time
- Staff can call next, mark served/no-show, view a full dashboard
- Atomic token generation under concurrent load (load-tested)
- Async notifications: "please proceed to counter" and "2 positions away"
- Scheduled auto-expiry of no-shows via Celery Beat
- DB-level safeguard against Redis/Postgres state desync

## Setup

1. `pip install -r requirements.txt`
2. Start Redis: `docker run -d --name queueless-redis -p 6379:6379 redis`
3. Create Postgres DB `queueless` (via pgAdmin)
4. Set your DB credentials in `database.py` or `.env`
5. Run all four processes, each in its own terminal:

uvicorn main:app --reload
celery -A celery_app worker --loglevel=info --pool=solo
celery -A celery_app beat --loglevel=info

   (Redis is already running from step 2)
6. Open `http://127.0.0.1:8000/docs`

# Note:- --pool=solo is used here because Celery's default prefork multiprocessing pool does not work on Windows in the same way as on Unix-like systems. solo runs tasks in a single worker process.

## Can be added or improved:-
- No Redis-state rebuild from Postgres on restart( currently we need to `flushall` every time)
- No auth on staff endpoints
- Priority queue, Docker Compose

##  real bugs I hit and fixed
race condition- (if multiple joins at same time,they can get same token)- fixed by using redis

Leftover Postgres rows from pre-Redis testing collided with Redis's fresh
token counter, producing duplicate `(counter_id, token)` rows and
`MultipleResultsFound` errors. Fixed with a Postgres unique constraint
(fails loudly at insert time) plus graceful error handling in the API.