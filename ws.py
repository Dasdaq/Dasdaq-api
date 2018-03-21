#!/usr/bin/env python

import asyncio
import websockets
import json
import time
import datetime
from pymongo import MongoClient


async def hello():
    async def xt():
        name = {"event": "ping"}
        time.sleep(10)
        await websocket.send(json.dumps(name))

    async def sub(address):
        for i in address:
            await websocket.send(json.dumps({"event": "txlist", "address": i}))

    async with websockets.connect('wss://socket.etherscan.io/wshandler') as websocket:
        # 订阅时间
        await sub(address)
        while True:
            await xt()
            greeting = json.loads(await websocket.recv())
            print("< {}".format(greeting))
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
    address = ['0x8d12a197cb00d4747a1fe03395095ce2a5cc6819',
               '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208',
               '0x06012c8cf97bead5deae237070f9587f8e7a266d',
               '0xddf0d0b9914d530e0b743808249d9af901f1bd01',
               '0xb1690c08e213a35ed9bab7b318de14420fb57d8c',
               '0xc7af99fe5513eb6710e6d5f44f9989da40f27f26',
               '0xb3775fb83f7d12a36e0475abdd1fca35c091efbe',
               '0xb6ed7644c69416d67b522e20bc294a9a9b405b31', ]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(hello())
