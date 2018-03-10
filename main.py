import pandas as pd
import arrow


def coverStrToFloat(df, internal=False):
    if not internal:
        df['gasUsed'] = df['gasUsed'].astype('float')
        df['gasPrice'] = df['gasPrice'].astype('float')
    df['value'] = df['value'].astype('float')
    return df


def x(df, dt, day=False):
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
    return {'blockDate': str(dt), 'plays': plays, 'totalVolume': totalVolume,
            'transactionsCount': transactionsCount, 'totalGasCost': totalGasCost
            }


def todt(a):
    '时间戳格式转换成时间格式'
    return arrow.get(a).datetime


# # 一小时内，每分钟的数据
# m1 = pd.date_range('2018-02-15','2018-02-16',freq='T')

# # 一天内，每小时的数据
# h24 = pd.date_range('2018-02-15','2018-02-16',freq='H')

# #  7天内，每天的数据
# d7 = pd.date_range('2018-01-1','2018-03-07',freq='D')
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
    df.loc[df.isError == '1', 'value'] = 0
    df['sxf'] = df.gasPrice * df.gasUsed


def address_Input_Data(df, address):
    '返回某个地址的所有输入值'
    return df[df['from'] == address].total_price.sum()


def toploss(df1, df2, top=10):
    '返回某个合约赢的最多与输的最多的人'
    df2 = coverStrToFloat(df2, internal=True)
    df1['total_price'] = df1.value + df1.sxf
    x1 = df1['total_price'].groupby(df1['from']).sum()
    x2 = df2['value'].groupby(df2['to']).sum()

    df3 = x1 - x2
    for i in df3[df3.isnull()].index:
        df3[i] = address_Input_Data(df1, i)
    return {'win': df3.sort_values(ascending=True)[:top],
            'loss': df3.sort_values(ascending=False)[:top]}
