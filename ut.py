from web3 import Web3, HTTPProvider

web3 = Web3(HTTPProvider('https://node.web3api.com/'))


def getBalance(address):
    address = address.lower()
    return web3.eth.getBalance(address) / 1e+18


if __name__ == '__main__':
    print(getBalance('0xeC1FAaD0Ae9aD83279f99eEd2CBfF9f1C8dc4550'))
