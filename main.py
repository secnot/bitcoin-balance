from bitbalance import BitcoinBalanceFacade
import time

if __name__ == '__main__':

    bitcoin_balance = BitcoinBalanceFacade('http://secnot:12345@localhost:8332')
    while True:
        time.sleep(10)
