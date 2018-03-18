from web3 import Web3, HTTPProvider

web3 = Web3(HTTPProvider('https://mainnet.infura.io/D7qVYwcifGfuGYBN92qw'))


def getBalance(address):
    address = address.lower()
    return web3.eth.getBalance(address) / 1e+18


if __name__ == '__main__':
    print(getBalance('0xc7069173721f6cd6322ce61f5912b31315c40fc2'))
