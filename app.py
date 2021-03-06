from __future__ import absolute_import, unicode_literals

from flask import Flask
from flask_restful import Resource, Api, reqparse
from flask_cache import Cache
from flask_pymongo import PyMongo
from flask_cors import CORS
from celery import Celery
from ut import getBalance
from main import updateContractBalance, run, getMaxBlockNumber
import pymongo
from tt import playGame
import urllib.parse
from check import run as checktext


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
                   "http://localhost:8080", "http://test.dapdap.io/*"])
app.config.from_pyfile('config.py')
api = Api(app)
mongo = PyMongo(app)
cache = Cache(app, config={'CACHE_TYPE': 'redis'})
celery = make_celery(app)

parser = reqparse.RequestParser()
parser.add_argument('reverse')


class Dapps(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self):
        dapp = mongo.db.dapps.find(
            {}, {'_id': 0, 'address': 0, 'h1': 0, 'd1': 0, 'd7': 0, 'links': 0})
        dapp = [item for item in dapp if
                item['title'] not in ['0xBitcoin', 'ForkDelta', 'CryptoKitties', 'IDEX', 'Etheroll', 'PoWH 3D']]
        return {'data': sorted(dapp, key=lambda x: int(x['dauLastDay']), reverse=True)}


class Dapps2(Resource):
    @cache.cached(timeout=60 * 5)
    def get(self):
        dapp = mongo.db.dapps.find(
            {}, {'_id': 0, 'address': 0, 'h1': 0, 'd1': 0, 'd7': 0, 'links': 0})
        return {'data': sorted(dapp, key=lambda x: int(x['dauLastDay']), reverse=True)}


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
        a = mongo.db.usercontract.find_one({'address': address}, {'_id': 0})
        if a:
            data = sorted(a['data'], key=lambda x: x['value'], reverse=True)
            total_x = sum(i['value'] for i in a['data'])
            total_rank = mongo.db.topuser.count()
            rank = mongo.db.topuser.find_one(
                {'address': address}, {'_id': 0, 'rank': 1})
            return {'data': data, 'balance': getBalance(address), 'total': total_x,
                    'rank': rank['rank'], 'total_rank': total_rank}
        else:
            return {'error': {'code': -1, 'message': u'该地址没有玩过任何游戏'}}


class UserTop(Resource):
    @cache.memoize(timeout=60 * 5)
    def get(self):
        args = parser.parse_args()
        if args['reverse'] == 'true':
            data = mongo.db.topuser.find({}, {'_id': 0}).sort(
                "rank", pymongo.DESCENDING).limit(100)
        else:
            data = mongo.db.topuser.find({}, {'_id': 0}).sort(
                "rank", pymongo.ASCENDING).limit(100)
        return {'data': list(data)}

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, parser.parse_args())


class Inlian(Resource):
    def get(self, text):
        try:
            if text:
                if checktext(text):
                    text = urllib.parse.unquote(text)
                    text = text.strip()
                    tx = playGame(text)
                    return {'tx': tx, 'ok': 1}
                else:
                    return {'ok': 0, 'msg': '内容待审核！'}

        except:
            pass
        return {'msg': '系统错误,请重试', 'ok': 0}


class AD(Resource):
    '''
    广告，
    第一种是首页的, /1
    第二种是详情页的 /2
    '''

    @cache.cached(timeout=60 * 5)
    def get(self, type):
        if type == 'home':
            _id = mongo.db.ad.find_one({"type": "home"})['id']
            return mongo.db.dapps.find_one({'id': _id},
                                           {'_id': 0, 'address': 0, 'h1': 0, 'd1': 0, 'd7': 0, 'links': 0, "type": 0})
        elif type == 'detail':
            return mongo.db.ad.find_one({"type": "detail"}, {"_id": 0, "type": 0, })


api.add_resource(Dapps, '/dapps')
api.add_resource(Dapps2, '/dapps2')
api.add_resource(Dapp, '/dapps/<string:dapp_id>')
api.add_resource(DappContract, '/dapps/<string:dapp_id>/contract')
api.add_resource(DappTop, '/dapps/<string:dapp_id>/top')
api.add_resource(User, '/user/<string:address>')
api.add_resource(UserTop, '/user')
api.add_resource(AD, '/featured/<string:type>')
api.add_resource(Inlian, '/write/<string:text>')


@celery.task
def upcontract():
    '每5分钟更新一次合约内的余额'
    updateContractBalance()


@celery.task
def getmaxblock():
    '每小时把最新的链接插入redis'
    getMaxBlockNumber()


@celery.task
def run_daily():
    '每小时跑一次数据'
    run()


def sortTop(data):
    win = sorted(data['data'], key=lambda x: x['value'], reverse=True)[:10]
    loss = sorted(data['data'], key=lambda x: x['value'], reverse=False)[:10]
    return {'win': win,
            'loss': loss}


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
