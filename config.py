from datetime import timedelta

CELERYBEAT_SCHEDULE = {
    'every-minute': {
        'task': 'app.upcontract',
        'schedule': timedelta(seconds=60 * 5)
    }, 'every-hour': {
        'task': 'app.getmaxblock',
        'schedule': timedelta(seconds=60 * 55)
    }, 'every-hour1': {
        'task': 'app.run_daily',
        'schedule': timedelta(seconds=60 * 60)
    },
}
MONGO_URI = 'mongodb://dapdap:dapdapmima123@172.31.135.89:27017/dapdap'
CELERY_BROKER_URL = 'redis://localhost:6379',
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
CACHE_REDIS_HOST = '127.0.0.1'
CACHE_REDIS_PORT = 6379
CACHE_REDIS_DB = 1
