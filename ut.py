from web3 import Web3, HTTPProvider

web3 = Web3(HTTPProvider('https://mainnet.infura.io/D7qVYwcifGfuGYBN92qw'))


def getBalance(address):
    address = address.lower()
    return web3.eth.getBalance(address) / 1e+18


def getblockNumber():
    return web3.eth.blockNumber


if __name__ == '__main__':
    print(getblockNumber())
