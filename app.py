from flask import Flask
from flask_restful import Resource, Api
from flask_cache import Cache
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config.from_pyfile('config.py')
api = Api(app)
mongo = PyMongo(app)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


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


api.add_resource(Dapps, '/api/dapps')
api.add_resource(Dapp, '/api/dapps/<string:dapp_id>')
api.add_resource(DappContract, '/api/dapps/<string:dapp_id>/contract')


if __name__ == '__main__':
    app.run(debug=True)
