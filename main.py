import pandas as pd
import arrow
from pymongo import MongoClient
from collections import defaultdict
from ut import getBalance, getblockNumber
import redis

db = MongoClient(host='172.31.135.89')['dapdap']
# db = MongoClient()['dapdap']
db.authenticate('dapdap', 'dapdapmima123')


def coverStrToFloat(df, internal=False):
    if not internal:
        df['gasUsed'] = df['gasUsed'].astype('float')
        df['gasPrice'] = df['gasPrice'].astype('float')
    df['value'] = df['value'].astype('float')
    return df


def x(df, dt=None, day=False):
    '返回给定数据中的玩家人数，花费金额...等数据'
    # 玩家人数
    plays = len(df['from'].value_counts().index)
    # 花费总金额
    totalVolume = df.value.sum()
    # 交易笔数
    transactionsCount = len(df)
    # 手续费金额
    totalGasCost = df.sxf.sum()
    # 时间
    if day:
        dt = dt.strftime(r'%Y-%m-%d')
    if dt:
        return {'blockDate': str(dt), 'plays': plays, 'totalVolume': totalVolume,
                'transactionsCount': transactionsCount, 'totalGasCost': totalGasCost
                }
    else:
        return {'plays': plays, 'totalVolume': totalVolume,
                'transactionsCount': transactionsCount, 'totalGasCost': totalGasCost
                }


def todt(a):
    '时间戳格式转换成时间格式'
    return arrow.get(a).datetime


def dateRange(df, d7, day=False):
    '返回指定时间范围内的数据'
    ret = []
    for i in zip(d7, d7[1:]):
        start_date, end_date = i
        mask = (df['t'] > start_date) & (df['t'] <= end_date)
        df1 = df.loc[mask]
        ret.append(x(df1, start_date, day=day))
    return ret


def init_df(df):
    '初始化df'
    df = coverStrToFloat(df)
    df['t'] = df.timeStamp.apply(todt)

    # 把交易失败的金额全部设置为0，主要是手续费还是需要交的
    df.loc[(df.isError == 1) | (df.isError == '1'), 'value'] = 0
    df['sxf'] = df.gasPrice * df.gasUsed
    df['total_price'] = df.value + df.sxf
    return df


def address_Input_Data(df, address):
    '返回某个地址的所有输入值'
    return -df[df['from'] == address].total_price.sum()


def toploss(df1, df2, gameid, top=10):
    '返回某个合约赢的最多与输的最多的人'
    x1 = df1['total_price'].groupby(df1['from']).sum()
    x2 = df2['value'].groupby(df2['to']).sum()
    # 本次查出来的玩家情况
    df3 = x2.sub(x1, fill_value=0)

    # 之前玩家情况
    q = db['top'].find_one({'id': gameid})
    if q:
        df4 = pd.DataFrame(df3, columns=['value'])
        df5 = pd.DataFrame(q['data'])
        df5.index = df5.address
        del df5['address']
        # 两次相加
        df6 = df4.add(df5, fill_value=0)
        return {"data": toploss_tolist(df6)}
    else:
        return {"data": toploss_tolist(df3, value=False)}


def userToContract():
    '返回每个用户玩的每个合约情况'
    user_contract = defaultdict(list)
    contracts = db['top'].find({}, {'_id': 0})
    for i in contracts:
        for x in i['data']:
            user_contract[x['address']].append(
                {'id': i['id'], 'name': i['title'], 'value': x['value']})
    db['usercontract'].drop()
    db['usercontract'].insert_many([{'address': k, 'data': v} for k, v in user_contract.items()])


def playsTop():
    p = db['usercontract'].find({}, {'_id': 0})
    total_x = []
    for i in p:
        sum_ = sum(x['value'] for x in i['data'])
        max_ = max(i['data'], key=lambda x: x['value'])
        total_x.append({'sum': sum_, 'address': i['address'],
                        'id': max_['id'], 'value': max_['value'], 'name': max_['name']})
    total_x = sorted(total_x, key=lambda x: x['sum'], reverse=True)
    for index, i in enumerate(total_x, 1):
        i['rank'] = index
    if total_x:
        db['topuser'].drop()
        db['topuser'].insert_many(total_x)
        db['topuser'].create_index('address', unique=True)


def toploss_tolist(df, value=True):
    '把Series 数据转成list'
    if value:
        d = df['value'].iteritems()
    else:
        d = df.iteritems()
    ret = []
    for k, v in d:
        ret.append({'address': k, 'value': v})
    return ret


def from_Mongo_Find_Address(address):
    '查找某个合约地址的id是多少...'
    f = db['dapps'].find_one({'address': {"$in": [address]}})
    return f['_id']


def savetomongo(data, dbname, key):
    if isinstance(data, list):
        for i in data:
            db[dbname].update_one({key: i[key]}, {"$set": i}, True)
    else:
        db[dbname].update_one({key: data[key]}, {"$set": data}, True)


def lastxxt(t):
    '最近多久的时间'
    end_time = arrow.utcnow()
    if t == '1h':
        start_time = end_time.replace(hours=-3)
    elif t == '7d':
        start_time = end_time.replace(days=-7)
    elif t == '1d':
        start_time = end_time.replace(days=-1)
    start_time = start_time.format(r'YYYY-MM-DD HH:mm:ss')
    end_time = end_time.format(r'YYYY-MM-DD HH:mm:ss')
    return start_time, end_time


def updateContractBalance():
    '更新合约的余额'
    client = db['dapps']
    a = client.find({}, {'_id': 0, 'id': 1, 'address': 1})
    for i in a:
        balance = 0
        for _address in i['address']:
            balance += getBalance(_address)
        savetomongo(data={'id': i['id'], 'balance': balance}, dbname='dapps',
                    key='id')


def getMaxBlockNumber():
    '返回每个合约地址最大的block number'
    url = 'http://api.etherscan.io/api?module=account&action={}&address={}&startblock={}' \
          '&endblock=99999999&page=1&offset=1000&sort=asc&apikey=YourApiKeyToken'
    urls = []
    maxBlockNumber = getblockNumber()
    # 不需要去跑的。走ws通道拿数据
    block_address = ['0x8d12a197cb00d4747a1fe03395095ce2a5cc6819',
                     '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208',
                     '0x06012c8cf97bead5deae237070f9587f8e7a266d',
                     '0xddf0d0b9914d530e0b743808249d9af901f1bd01',
                     '0xb1690c08e213a35ed9bab7b318de14420fb57d8c',
                     '0xc7af99fe5513eb6710e6d5f44f9989da40f27f26',
                     '0xb3775fb83f7d12a36e0475abdd1fca35c091efbe',
                     '0xb6ed7644c69416d67b522e20bc294a9a9b405b31', ]
    _BlockNumber = str(int(maxBlockNumber) - 60000)
    r = redis.Redis(host='39.108.11.148', password='redismima123')
    if not r.scard('contract'):
        client = db['tokens']
        df = pd.DataFrame(list(client.find({"blockNumber": {"$gt": _BlockNumber}}, {'_id': 0, 'blockNumber': 1,
                                                                                    'address': 1, 'action': 1, })))
        df1 = df[df.action == 'txlist']
        df2 = df[df.action == 'txlistinternal']
        for address, blocknumber in df1[['blockNumber']].groupby(df1.address).max().to_dict()['blockNumber'].items():
            if address.lower() not in block_address:
                urls.append(url.format('txlist', address, blocknumber))
        for address, blocknumber in df2[['blockNumber']].groupby(df2.address).max().to_dict()['blockNumber'].items():
            urls.append(url.format('txlistinternal', address, blocknumber))
        # 把数据更新进redis，然后爬虫直接从里面拿数据
        r.sadd('contract', *urls)
        # print(len(urls))


def run():
    '每小时更新一次，更新7天内的数据'
    d7ago = arrow.utcnow().replace(days=-7).timestamp
    client = db['tokens']
    df5 = pd.DataFrame(list(client.find({"timeStamp": {"$gt": str(d7ago)}}, {'_id': 0, 'blockHash': 0, 'blockNumber': 0,
                                                                             'confirmations': 0, 'contractAddress': 0,
                                                                             'cumulativeGasUsed': 0,
                                                                             'hash': 0, 'input': 0,
                                                                             'nonce': 0, 'traceId': 0,
                                                                             "transactionIndex": 0,
                                                                             "txreceipt_status": 0, "type": 0
                                                                             })))
    df5 = init_df(df5)
    # 所有的合约
    a = list(db['dapps'].find(
        {}, {'id': 1, 'address': 1, '_id': 0, "title": 1}))
    for i in a:
        # 筛选出某个游戏的所有交易记录（可能包括多个合约）
        df_ = df5[df5.address.isin(i['address'])]
        if not df_.empty:
            _id = i['id']
            # 普通交易记录
            df1 = df_[df_.action == 'txlist']

            ret = {}
            # 更新数据库的时间
            ret['updatedAt'] = arrow.utcnow().format(r'YYYY-MM-DD HH:mm:ss')
            ret['id'] = _id

            # 1h的数据，按每分钟分割
            start_time, end_time = lastxxt('1h')
            _ = pd.date_range(start_time, end_time, freq='T')
            h1 = dateRange(df1, _, day=False)
            # 1d的数据,按每小时分割
            start_time, end_time = lastxxt('1d')
            _ = pd.date_range(start_time, end_time, freq='H')
            d1 = dateRange(df1, _, day=False)
            # 一天的总体运营情况
            df1d = df1[(df1['t'] > start_time) & (df1['t'] <= end_time)]
            ret['volumeLastDay'] = df1d.value.sum() / 1e+18
            ret['txLastDay'] = len(df1d)
            ret['dauLastDay'] = len(df1d['from'].value_counts().index)
            # 7d的数据，按每天分割
            start_time, end_time = lastxxt('7d')
            _ = pd.date_range(start_time, end_time, freq='D')
            d7 = dateRange(df1, _, day=True)
            # 7天内的总体运营情况
            df7d = df1[(df1['t'] > start_time) & (df1['t'] <= end_time)]
            ret['volumeLastWeek'] = df7d.value.sum() / 1e+18
            ret['txLastWeek'] = len(df7d)

            ret['h1'] = h1
            ret['d1'] = d1
            ret['d7'] = d7
            savetomongo(data=ret, dbname='dapps', key='id')


def runWithGameId(gameid):
    contracts = db['dapps'].find_one({'id': gameid})['address']
    df5 = pd.DataFrame(list(db['tokens'].find({"address": {"$in": contracts}})))
    df5 = init_df(df5)
    df1 = df5[df5.action == 'txlist']
    ret = x(df1)
    ret['updatedAt'] = arrow.utcnow().format(r'YYYY-MM-DD HH:mm:ss')
    ret['id'] = gameid
    # 1h的数据，按每分钟分割
    start_time, end_time = lastxxt('1h')
    _ = pd.date_range(start_time, end_time, freq='T')
    h1 = dateRange(df1, _, day=False)
    # 1d的数据,按每小时分割
    start_time, end_time = lastxxt('1d')
    _ = pd.date_range(start_time, end_time, freq='H')
    d1 = dateRange(df1, _, day=False)
    # 一天的总体运营情况
    df1d = df1[(df1['t'] > start_time) & (df1['t'] <= end_time)]
    ret['volumeLastDay'] = df1d.value.sum() / 1e+18
    ret['txLastDay'] = len(df1d)
    ret['dauLastDay'] = len(df1d['from'].value_counts().index)
    # 7d的数据，按每天分割
    start_time, end_time = lastxxt('7d')
    _ = pd.date_range(start_time, end_time, freq='D')
    d7 = dateRange(df1, _, day=True)
    # 7天内的总体运营情况
    df7d = df1[(df1['t'] > start_time) & (df1['t'] <= end_time)]
    ret['volumeLastWeek'] = df7d.value.sum() / 1e+18
    ret['txLastWeek'] = len(df7d)

    ret['h1'] = h1
    ret['d1'] = d1
    ret['d7'] = d7
    savetomongo(data=ret, dbname='dapps', key='id')


def run_yl():
    d7ago = '1521030065'
    client = db['tokens']
    df5 = pd.DataFrame(list(client.find({"timeStamp": {"$gt": str(d7ago)}}, {'_id': 0, 'blockHash': 0, 'blockNumber': 0,
                                                                             'confirmations': 0, 'contractAddress': 0,
                                                                             'cumulativeGasUsed': 0,
                                                                             'hash': 0, 'input': 0,
                                                                             'nonce': 0, 'traceId': 0,
                                                                             "transactionIndex": 0,
                                                                             "txreceipt_status": 0, "type": 0
                                                                             })))
    df5 = init_df(df5)
    # 所有的合约
    a = list(db['dapps'].find(
        {}, {'id': 1, 'address': 1, '_id': 0, "title": 1}))
    for i in a:
        # 筛选出某个游戏的所有交易记录（可能包括多个合约）
        df_ = df5[df5.address.isin(i['address'])]
        if not df_.empty:
            _id = i['id']
            # 普通交易记录
            df1 = df_[df_.action == 'txlist']
            # 内部交易记录
            df2 = df_[df_.action == 'txlistinternal']
            # 玩家盈利情况
            yl = toploss(df1, df2, _id)
            yl['id'] = _id
            yl['title'] = i['title']
            savetomongo(data=yl, dbname='top', key='id')
    # 用户玩过哪些游戏
    userToContract()
    # 用户收益排行版
    playsTop()


if __name__ == '__main__':
    run()
