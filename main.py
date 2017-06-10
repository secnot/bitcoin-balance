import signal
import sys

from bitbalance import BitcoinBalanceFacade, Session, bitcoin_to_string
import time

BITCOIND_URL = 'http://secnot:12345@localhost:8332'




bitcoin_balance = BitcoinBalanceFacade(db_session=Session,
                                       bitcoind_url=BITCOIND_URL)


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    bitcoin_balance.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


if __name__ == '__main__':

    while True:
        address = input()
        if isinstance(address, str):
            balance = bitcoin_balance.get_balance(address) 
            print(bitcoin_to_string(balance))
