from celery import Celery

celery_app = Celery(
    "queueless",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)
celery_app.conf.imports = ["tasks"]

celery_app.conf.beat_schedule = {
    "expire-no-shows-every-30-seconds": {
        "task": "tasks.expire_no_shows",
        "schedule": 30.0,  # seconds
    },
}
celery_app.conf.task_routes = {}