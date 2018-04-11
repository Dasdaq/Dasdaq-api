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
# MONGO_URI = 'mongodb://dapdap:dapdapmima123@127.0.0.1:27017/dapdap'
CELERY_BROKER_URL = 'redis://localhost:6379',
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
CACHE_REDIS_HOST = '127.0.0.1'
CACHE_REDIS_PORT = 6379
CACHE_REDIS_DB = 1
block_address = ['0x8d12a197cb00d4747a1fe03395095ce2a5cc6819',
                 '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208',
                 '0x06012c8cf97bead5deae237070f9587f8e7a266d',
                 '0xddf0d0b9914d530e0b743808249d9af901f1bd01',
                 '0xb1690c08e213a35ed9bab7b318de14420fb57d8c',
                 '0xc7af99fe5513eb6710e6d5f44f9989da40f27f26',
                 '0xb3775fb83f7d12a36e0475abdd1fca35c091efbe',
                 '0xb6ed7644c69416d67b522e20bc294a9a9b405b31',
                 '0x1ce7ae555139c5ef5a57cc8d814a867ee6ee33d8',
                 '0x09678741bd50c3e74301f38fbd0136307099ae5d']
