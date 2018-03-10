import pandas as pd
import arrow
from pymongo import MongoClient
from collections import defaultdict

db = MongoClient()['dapdap']
global USER_CONTRACT
USER_CONTRACT = defaultdict(list)


def coverStrToFloat(df, internal=False):
    if not internal:
        df['gasUsed'] = df['gasUsed'].astype('float')
        df['gasPrice'] = df['gasPrice'].astype('float')
    df['value'] = df['value'].astype('float')
    return df


def x(df, dt=None, day=False):
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
    df.loc[df.isError == 1, 'value'] = 0
    df['sxf'] = df.gasPrice * df.gasUsed
    df['total_price'] = df.value + df.sxf
    return df


def address_Input_Data(df, address):
    '返回某个地址的所有输入值'
    return df[df['from'] == address].total_price.sum()


def toploss(df1, df2, top=10):
    '返回某个合约赢的最多与输的最多的人'
    x1 = df1['total_price'].groupby(df1['from']).sum()
    x2 = df2['value'].groupby(df2['to']).sum()

    df3 = x2 - x1
    for i in df3[df3.isnull()].index:
        df3[i] = address_Input_Data(df1, i)
    if len(df1['to'].value_counts().index):
        userToContract(df3, contract_address=df1['to'].value_counts().index[0])
    return {'loss': toploss_tolist(df3.sort_values(ascending=True)[:top]),
            'win': toploss_tolist(df3.sort_values(ascending=False)[:top])}


def userToContract(df, contract_address):
    '用户参加的所有合约'
    for i in df.index:
        USER_CONTRACT[i].append({'address': contract_address, 'value': df[i]})


def toploss_tolist(df):
    '把Series 数据转成list'
    ret = []
    for i, (k, v) in enumerate(df.to_dict().items(), 1):
        ret.append({'index': i, 'address': k, 'value': v})
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


def run():
    client = db['tokens']
    df5 = pd.DataFrame(list(client.find({}, {'_id': 0, 'blockHash': 0, 'blockNumber': 0,
                                             'confirmations': 0, 'contractAddress': 0, 'cumulativeGasUsed': 0,
                                             'hash': 0, 'input': 0,
                                             'nonce': 0, 'traceId': 0, "transactionIndex": 0,
                                             "txreceipt_status": 0, "type": 0
                                             })))
    df5 = init_df(df5)
    # 所有的合约
    address = df5.address.value_counts().index
    for contract_address in address:
        df_ = df5[df5.address == contract_address]
        df1 = df_[df_.action == 'txlist']
        df2 = df_[df_.action == 'txlistinternal']

        # 游戏总体情况
        ret = x(df1)
        ret['contract'] = contract_address
        # 更新数据库的时间
        ret['updatedAt'] = arrow.utcnow().format(r'YYYY-MM-DD HH:mm:ss')
        _id = from_Mongo_Find_Address(contract_address)
        ret['_id'] = _id

        # 1h的数据，按每分钟分割
        start_time, end_time = lastxxt('1h')
        m1 = pd.date_range(start_time, end_time, freq='T')
        h1 = dateRange(df1, m1, day=False)
        # 1d的数据，安
        start_time, end_time = lastxxt('1d')
        _ = pd.date_range(start_time, end_time, freq='H')
        d1 = dateRange(df1, _, day=False)
        # 7d
        start_time, end_time = lastxxt('7d')
        _ = pd.date_range(start_time, end_time, freq='D')
        d7 = dateRange(df1, _, day=True)

        ret['h1'] = h1
        ret['d1'] = d1
        ret['d7'] = d7
        savetomongo(data=ret, dbname='dapps', key='_id')

        # 玩家盈利情况
        yl = toploss(df1, df2)
        yl['contract'] = contract_address
        savetomongo(data=yl, dbname='top', key='contract')
    db['usercontract'].drop()
    db['usercontract'].insert_one(USER_CONTRACT)


if __name__ == '__main__':
    run()
