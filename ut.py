from web3 import Web3, HTTPProvider
import requests
from parsel import Selector

web3 = Web3(HTTPProvider('https://mainnet.infura.io/D7qVYwcifGfuGYBN92qw'))


def getBalance(address):
    address = address.lower()
    return web3.eth.getBalance(address) / 1e+18


def getblockNumber():
    return web3.eth.blockNumber


def getTotalTransationCount(ad):
    url = 'https://etherscan.io/txs?a={}'.format(ad)
    z = requests.get(url, headers={
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36'})
    sel = Selector(text=z.text)
    return int(sel.css('.hidden-xs').xpath('span[2]').re('\d+')[0])


if __name__ == '__main__':
    print(getblockNumber())
