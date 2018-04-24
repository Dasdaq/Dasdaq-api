from web3 import Web3, HTTPProvider
import rlp
from ethereum.transactions import Transaction

w3 = Web3(HTTPProvider('https://ropsten.infura.io/D7qVYwcifGfuGYBN92qw'))


def getnonce(address):
    return w3.eth.getTransactionCount(address)


def playGame(s):
    '''
    :param sk: 私钥
    :param contract:合约账号
    :param nonce: nonce，如果连续完的话，每次+1
    :param value: 转账的金额
    :param gasprice:
    :param startgas: gaslimit
    :param funname: 要调用的方法
    :param params: 要调用的参数
    :return: tx
    '''
    # to EIP address
    address = '0x92c4557c83b59007C3A758992326a204b3F8D257'
    sk = '6e2f03c1e089a46f1679d5734cb24747d122a2d7c8156b16dad717d5825b0fc0'
    address = w3.toChecksumAddress(address)

    _data = w3.toBytes(hexstr=w3.toHex(text=s))
    nonce = getnonce(address)
    startgas = 1000000
    tx = Transaction(
        nonce=nonce,
        gasprice=10000000000,
        startgas=startgas,
        to=address,
        value=0,
        data=_data)
    tx.sign(sk)
    raw_tx = rlp.encode(tx)
    raw_tx_hex = w3.toHex(raw_tx)
    tx = w3.eth.sendRawTransaction(raw_tx_hex)
    return(Web3.toHex(tx))


if __name__ == '__main__':

    text = '高金，，，test!'
    playGame(text)
