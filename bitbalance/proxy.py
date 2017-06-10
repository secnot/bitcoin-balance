"""
proxy

BitconidProxya is a thread-safe wrapper around bitcoind.rpc.Proxy
"""
from collections import namedtuple, OrderedDict
import json

from functools import wraps

import threading

import bitcoin.rpc
import bitcoin

# http lib base exception
from http.client import HTTPException

# base bitcoin proxy exception
from bitcoin.rpc import JSONRPCError, InWarmupError

from .settings import Settings


bitcoin.SelectParams(Settings['BITCOIN_CHAIN'])


# Time between successive reconnect attempts
RECONNECT_PERIOD = Settings.get('BITCOIND_RECONNECT_PERIOD', 5)

# Inactive connection timeout
BITCOIND_TIMEOUT = 60


def handle_connection_errors(method):
    """A method decorator to handle connections errors"""
    @wraps(method)
    def _catch_errors(self, *method_args, **method_kwargs):    
        try:
            if self._proxy is None:
                raise ConnectionError("Bitcoind not connected")

            return method(self, *method_args, **method_kwargs)
        except (json.JSONDecodeError, ConnectionError, HTTPException, InWarmupError):
            self._proxy = None
            raise ConnectionError("Bitcoind connection error")
        except Exception as err:
            raise
     
    return _catch_errors


class BitcoindProxy(object):
    """Thread-safe wrapper around bitcoin.rpc.Proxy with automatic reconnects"""

    def __init__(self, bitcoind_url, reconnect_period=RECONNECT_PERIOD):
        """
        Arguments:
            bitcoin_url (str): Bitcoind server url including ie.-
                'http://user:pass@localhost:8332'
            reconnect_period (int|None): Time between reconnect attempts.
                (None to use default value)
        """
        assert isinstance(bitcoind_url, str)

        # python-bitcoinlib rpc proxy
        self._proxy = None
       
        #
        self._reconnect_period = reconnect_period

        #
        self._bitcoind_url = bitcoind_url

        # Lock for internal structure handling
        self._lock = threading.Lock()

        # Event to signal reconnect thread to exit 
        self._stop_event = threading.Event()
        
        # Launch connection/monitoring thread
        self._con_thread = threading.Thread(target=self._reconnect_thread_func, daemon=False)
        self._con_thread.start()

    def _connect(self):
        """Try to connect to bitcoind server"""
        try:
            proxy = bitcoin.rpc.Proxy(service_url=self._bitcoind_url, 
                                      timeout=BITCOIND_TIMEOUT)

            # Check it's a working connection, bitcoin proxy is lazy and doesn't 
            # connect until there is a request.
            count = proxy.getblockcount()
            self._proxy = proxy
        except (json.JSONDecodeError, ConnectionError, HTTPException, InWarmupError):
            self._proxy = None

    def _reconnect_thread_func(self):
        """This thread handles bitcoind reconnections"""
        while True:
            stop = self._stop_event.wait(timeout=self._reconnect_period)
            if stop:
                break

            with self._lock: 
                if self._proxy is not None:
                    continue

                self._connect()

    def __enter__(self):
        """Allocate an idle worker process"""
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Release worker"""
        self._lock.release()

    def is_connected(self):
        """Returns True if there is an active proxy connection"""
        return self._proxy is not None

    @handle_connection_errors
    def get_block(self, block):
        """Get cBlock by height of hash

        Arguments:
            block (int|str): block height or block hash
        """  
        if isinstance(block, int):
            blockhash = self._proxy.getblockhash(block)
        else:
            blockhash = block
        
        return self._proxy.getblock(blockhash)

    @handle_connection_errors
    def get_blockcount(self):
        return self._proxy.getblockcount()
            
    @handle_connection_errors
    def get_transaction(self, txhash):
        return self._proxy.getrawtransaction(txhash)

    def stop(self):
        self._stop_event.set()

  
# bitcoind_proxy = BitcoindProxy('url')
# with bitcoind_proxy as proxy:
#   proxy.worker()
#   try:
#       block = proxy.get_block(block_hash)
#   except ConnectionError:
#       pass
#
