from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'every-minute': {
        'task': 'app.hello',
        'schedule': timedelta(seconds=30)
    },
}
MONGO_DBNAME = 'dapdap'
CELERY_BROKER_URL = 'redis://localhost:6379',
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
