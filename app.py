from __future__ import absolute_import, unicode_literals

from flask import Flask
from flask_restful import Resource, Api
from flask_cache import Cache
from flask_pymongo import PyMongo
from flask_cors import CORS
from celery import Celery
from ut import getBalance


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
CORS(app, origins=["http://dapdap.io/*", "http://www.dapdap.io/*",
                   "https://dapdap.io/*", "https://www.dapdap.io/*",
                   "http://localhost:8080"])
app.config.from_pyfile('config.py')
api = Api(app)
mongo = PyMongo(app)
cache = Cache(app, config={'CACHE_TYPE': 'redis'})
celery = make_celery(app)


class Dapps(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self):
        dapp = mongo.db.dapps.find({}, {'_id': 0, 'address': 0, 'h1': 0, 'd1': 0, 'd7': 0})
        return {'data': list(dapp)}


class Dapp(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        dapp = mongo.db.dapps.find_one(
            {'id': dapp_id}, {'_id': 0, 'address': 0})
        if dapp:
            return {'data': dapp}
        else:
            return {'error': {'code': "NOT_EXISIT", 'message': '没有找到该dapp'}}


class DappContract(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        contract = mongo.db.dapps.find_one(
            {'id': dapp_id}, {'_id': 0, 'address': 1})
        contract = [
            {'address': i, 'link': 'https://etherscan.io/address/{}'.format(i)} for i in contract['address']]
        return {'data': contract}


class DappTop(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, dapp_id):
        data = mongo.db.top.find_one(
            {'id': dapp_id}, {'_id': 0, 'id': 0})
        if data:
            return {'data': sortTop(data)}
        else:
            return {'error': {'code': "NOT_EXISIT", 'message': '无数据'}}

    def post(self, ):
        # todo: 留言内容入库
        pass


class User(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self, address):
        a = mongo.db.usercontract.find_one({}, {'_id': 0})
        if address in a:
            data = sorted(a[address], key=lambda x: x['value'], reverse=True)
            total_x = sum(i['value'] for i in a[address])
            return {'data': data, 'balance': getBalance(address), 'total': total_x}
        else:
            return {'error': {'code': -1, 'message': u'该地址没有玩过任何游戏'}}


class UserTop(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self):
        a = mongo.db.topuser.find({}, {'_id': 0})
        data = sorted(a, key=lambda x: x['sum'], reverse=True)
        return {'data': data}


api.add_resource(Dapps, '/dapps')
api.add_resource(Dapp, '/dapps/<string:dapp_id>')
api.add_resource(DappContract, '/dapps/<string:dapp_id>/contract')
api.add_resource(DappTop, '/dapps/<string:dapp_id>/top')
api.add_resource(User, '/user/<string:address>')
api.add_resource(UserTop, '/user')


@celery.task
def hello():
    print('hello')


def sortTop(data):
    win = sorted(data['data'], key=lambda x: x['value'], reverse=True)[:10]
    loss = sorted(data['data'], key=lambda x: x['value'], reverse=False)[:10]
    return {'win': win,
            'loss': loss}


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
