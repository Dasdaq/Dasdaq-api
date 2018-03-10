from flask import Flask
from flask_restful import Resource, Api
from flask_cache import Cache
from flask_pymongo import PyMongo
from flask_cors import CORS
from celery import Celery


def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


app = Flask(__name__)
CORS(app)
app.config.from_pyfile('config.py')
api = Api(app)
mongo = PyMongo(app)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
celery = make_celery(flask_app)


class Dapps(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self):
        dapp = mongo.db.dapps.find({}, {'_id': 0, 'address': 0})
        return {'data': list(dapp)}


class Dapp(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        dapp = mongo.db.dapps.find_one(
            {'id': dapp_id}, {'_id': 0, 'address': 0})
        return {'data': dapp}


class DappContract(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        contract = mongo.db.dapps.find_one(
            {'id': dapp_id}, {'_id': 0, 'address': 1})
        return {'data': contract}


class DappTop(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        contract = '0xc7069173721f6cd6322ce61f5912b31315c40fc2'
        contract = mongo.db.top.find_one(
            {'contract': contract}, {'_id': 0})
        return {'data': contract}

    def post(self,):
        # todo: 留言内容入库
        pass


api.add_resource(Dapps, '/api/dapps')
api.add_resource(Dapp, '/api/dapps/<string:dapp_id>')
api.add_resource(DappContract, '/api/dapps/<string:dapp_id>/contract')
api.add_resource(DappTop, '/api/dapps/<string:dapp_id>/top')


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
