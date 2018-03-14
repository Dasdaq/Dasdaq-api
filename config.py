from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'every-minute': {
        'task': 'app.hello',
        'schedule': timedelta(seconds=30)
    },
}
MONGO_DBNAME = 'dapdap'
MONGO_USERNAME = 'dapdap'
MONGO_PASSWORD = 'dapdapmima123'
CELERY_BROKER_URL = 'redis://localhost:6379',
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
CACHE_REDIS_HOST = '127.0.0.1'
CACHE_REDIS_PORT = 6379
CACHE_REDIS_DB = 1
