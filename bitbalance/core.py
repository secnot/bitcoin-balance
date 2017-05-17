from collections import deque
import threading
import time
import logging
import json

import bitcoin
import bitcoin.rpc
from bitcoin.core import str_money_value, b2lx, b2x, x

from .primitives import TxOut, Block, BlockFactory
from .balance import BalanceProcessor
from .exceptions import ChainError
from .logger import LOGGING_FORMAT




settings = {
    # 
    'MAX_BACKTRACK_BLOCKS': 100,

    # Retry period for bitcoind connection
    'BITCOIND_POLL_PERIOD': 0.01,

    # Chain used by bitcoind 'testnet' or 'mainnet'
    'BITCOIN_CHAIN': 'testnet',
}


logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
logger = logging.getLogger("Bitcoin")

bitcoin.SelectParams(settings['BITCOIN_CHAIN'])









class BalanceTracker(object):
    """ """
   
    def __init__(self, start_height=-1, backtrack_limit=100, proxy=None):
        # Hash and heigh for the last N blocks
        self._block_hash  = deque()
        self._block_height = deque()

        # Longest backtrack blocks allowed
        self._backtrack_limit = backtrack_limit
       
        # Lock the tracker while the balance is being updated
        self._lock = threading.Lock()

        #
        self._block_factory = BlockFactory()

        # TODO: Load initial state from DB
        self._balance = BalanceProcessor(backtrack_limit=backtrack_limit)
        
        #
        self._proxy = None
        self.set_proxy(proxy)

    @property
    def height(self):
        """Return height of top block"""
        if not self._block_height:
            return -1
        else:
            return self._block_height[-1]

    def set_proxy(self, proxy):
        self._proxy = proxy
        self._block_factory.set_proxy(proxy)

    def get_balance(self, address, confirmations):
        with self._lock:
            return self._balance.get_balance(address, confirmations)

    def _get_cblock(self, block_height):
        """Use bitcoind.rpc to get block of given block height
        
        Returns:
            bitcoin.CBlock
        """
        if self._proxy is None:
            raise ConnectionError("Bitcoind proxy not available")

        # Check requested block exists
        newest_block = self._proxy.getblockcount()
        if newest_block < block_height:
            return None

        # Get block
        blockhash = self._proxy.getblockhash(block_height)
        return self._proxy.getblock(blockhash)

    def _add_block(self, block):
        """Add new block to tracked to update balance"""
        if len(self._block_hash) >= self._backtrack_limit:
            self._block_hash.popleft()
            self._block_height.popleft()

        with self._lock:
            self._block_hash.append(block.block_hash)
            self._block_height.append(block.height)

            # Process record into balance
            self._balance.add_block(block)

    def _backtrack(self):
        # TODO: Check there are block remainint
        with self._lock:
            self._block_hash.pop()
            self._block_height.pop()
            #TODO: Use a better system so there is no need to purge
            # the complete cache each backtrack
            self._block_factory.purge_cache()
            self._balance.backtrack()

    def poll_bitcoin(self):
        """
        Poll bitcoind for new blocks.

        Return:
            (bool) True if there was an update or backtrack, False otherwise
        """
        if self._proxy is None:
            raise ConnectionError("Bitcoind proxy not available")

        try:
            cblock = self._get_cblock(self.height+1)
        except (json.JSONDecodeError, ConnectionError) as err:
            self._proxy = None
            raise ConnectionError("Bitcoind proxy error")

        if cblock is None: # No new blocks available
            return False

        if not (self.height+1) % 500:
            logger.info("block {}".format(self.height+1))
            f = self._block_factory
            logger.info("cache {} ({} hits | {} miss)".format(len(f._txout_cache), f._cache_hit, f._cache_miss))

        # Before building a block check the block follows the current 
        # top block if not backtrack
        if self._block_hash and cblock.hashPrevBlock != self._block_hash[-1]:
            self._backtrack()
            return True

        # Fully construct block to asure there won't be a ConnectionError 
        # during update.
        try:
            block = self._block_factory.build_block(cblock, self.height+1)
        except (json.JSONDecodeError, ConnectionError) as err:
            self._proxy = None
            raise ConnectionError("Bitcoind proxy error")

        # Update state with the complete block
        self._add_block(block)
        return True

    def stop(self):
        """Safely stop and record state"""
        # TODO:
        pass

    def __len__(self):
        return len(self._block_hash)









# -------- Threads and all that stuff --------------

class BitcoinBalanceFacade(object):

    def __init__(self, bitcoind_url='http://user:pass@localhost:8332', start_block=-1):
        self._bitcoind_url = bitcoind_url
       
        # Lock access to BalanceTracker
        self._lock = threading.Lock()
        
        # Event to signal polling thread to stop
        self._stop_event = threading.Event()

        # Connection to bitcoind service, it's initialized by automatic
        # reconnect code.
        self._bitcoind_proxy = None

        self._balance_tracker = BalanceTracker()

        # Load 
        self._fast_load=True

        self._balance_thread_func() #TODO: Testing
        # Launch bitcoin polling thread
        #self._poll_thread = threading.Thread(target=self._balance_thread_func, daemon=False)
        #self._poll_thread.start()

    def _connect_bitcoind(self):
        """
        Try to reconnect to bitcoind server

        Returns:
            (bool): True if was reconnected false otherwise
        """
        try:
            proxy = bitcoin.rpc.Proxy(service_url=self._bitcoind_url)

            # Check it's a valid connection, bitcoin proxy is lazy and doesn't 
            # connect until there is a request.
            count = proxy.getblockcount()
            self._bitcoind_proxy = proxy
            self._balance_tracker.set_proxy(proxy)
            logger.info("Bitcoind connected")
            return True
        except (ConnectionError, bitcoin.rpc.InWarmupError) as err:
            logger.debug("Bitcoind reconnect error: {}".format(err.__class__.__name__))
            pass
        except Exception as err:
            logger.error(err, exc_info=True)
            raise err

        return False

    def _poll_bitcoin(self):
        try:
            return self._balance_tracker.poll_bitcoin()
        except ConnectionError:
            self._bitcoind_proxy = None
            logger.info("Bitcoind connection lost")
            return False

    def _balance_thread_func(self):
        last_update = time.perf_counter()
        
        while True:
            stop = self._stop_event.wait(timeout=0.02)
            if stop:
                break
           
            if time.perf_counter()-last_update < settings['BITCOIND_POLL_PERIOD']:
                continue
            else:
                last_update=time.perf_counter()

            # If bitcoind connection was lost first try to connect 
            if self._bitcoind_proxy is None:
                if not self._connect_bitcoind():
                    continue # Unable to connect
            
            # If enabled fast load blockchain
            if self._fast_load:
                while self._poll_bitcoin():
                    continue
            else:
                self._poll_bitcoin()

    def stop(self):
        """Send stop signal to polling thread and wait until it terminated"""
        self._stop_event.set()
        self._poll_thread.join()

    def get_balance(address, confirmations):
        """ """
        return self._balance_tracker.get_balance(address, confirmations)


