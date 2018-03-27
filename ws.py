#!/usr/bin/env python

import asyncio
import websockets
import json
import time
import datetime
from pymongo import MongoClient
from config import block_address


async def hello():
    async def xt():
        name = {"event": "ping"}
        time.sleep(5)
        await websocket.send(json.dumps(name))

    async def sub(address):
        for i in address:
            await websocket.send(json.dumps({"event": "txlist", "address": i}))

    async with websockets.connect('wss://socket.etherscan.io/wshandler') as websocket:
        # 订阅时间
        await sub(block_address)
        while True:
            await xt()
            greeting = json.loads(await websocket.recv())
            # print("< {}".format(greeting))
            if 'address' in greeting:
                await save2mongo(greeting)

            time.sleep(1)


async def save2mongo(r):
    if r.get('result', False):
        rs = []
        n = datetime.datetime.utcnow()
        for i in r['result']:
            i['address'] = r['address']
            i['action'] = 'txlist'
            i['ts'] = n
            rs.append(i)
        client.insert_many(rs)


if __name__ == '__main__':
    db = MongoClient()['dapdap']
    db.authenticate('dapdap', 'dapdapmima123')
    client = db['tokens']
    loop = asyncio.get_event_loop()
    loop.run_until_complete(hello())
