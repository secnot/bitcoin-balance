from collections import deque
import multiprocessing
import threading
import time
import logging
import json
import queue

import bitcoin
import bitcoin.rpc
from bitcoin.core import str_money_value, b2lx, b2x, x

from .primitives import TxOut, Block, BlockFactory
from .balance import BalanceProcessor
from .exceptions import ChainError, BacktrackError
from .logger import LOGGING_FORMAT
from .storage import MemoryBalanceStorage, SQLBalanceStorage, BalanceProxyCache
from .database import Session
from .settings import Settings
from .proxy import BitcoindProxy

logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
logger = logging.getLogger("Bitcoin")

bitcoin.SelectParams(Settings['BITCOIN_CHAIN'])



# Cache size for pending blocks
BLOCK_CACHE_SIZE = 10


class BlockPrefetchingCache(object):
    """BlockCache is a prefetching cache for sequential blockchain blocks"""

    def __init__(self, height, proxy, cache_size=BLOCK_CACHE_SIZE):
        """
        Arguments:
            height (int): Cache starting height, the first time 
                get_next_block is successful this is the block it
                will return.

            proxy (proxy.BitcoindProxy|str): initialized BitcoindProxy or 
                bitcoind_url string
        """
        if isinstance(proxy, BitcoindProxy):
            self._proxy = proxy
        else:
            self._proxy = BitcoindProxy(proxy)

        # Height for the next block to request from bitcoind
        self._height = height

        # Cached blocks
        self._cache_size = cache_size
        self._cache = queue.Queue(self._cache_size)

        # lock polling while purging or setting new height
        self._lock = threading.Lock()

        # Event to signal threads to stop
        self._stop_event = threading.Event()

        # Launch polling thread
        self._fetch_thread = threading.Thread(target=self._fetch_thread_func, 
                                             daemon=False)
        self._fetch_thread.start()


    def _fetch_thread_func(self):
        """Thread polling bitcoind looking for the next block"""
        # height for the top blockchain block
        blockchain_height = 1
       
        # Initialize height and cache for first iteration
        with self._lock:
            current_height = self._height - 1
            cache = self._cache

        # Flag the connection was lost
        connection_lost = False
        
        # TODO: Log when connection to bitcoind is lost and recovered
        while True:
            # Check cache wasn't purged
            with self._lock:
                if id(cache) != id(self._cache):
                    cache = self._cache
                    current_height = self._height-1

            # Did an exit signal arrive?
            if self._stop_event.wait(timeout=0):
                break
           
            # If we are not yet at the top of the blockchain, get the next block
            # as fast as possible. If it is the top or the connection to bitcoind
            # was lost, wait default poll period and check for new blocks.
            if connection_lost or current_height >= blockchain_height:
                stop = self._stop_event.wait(timeout=Settings['BITCOIND_POLL_PERIOD'])
                if stop:
                    break

            
            with self._proxy as proxy:

                # Already at the top, are there new blocks????
                if current_height >= blockchain_height:
                    try:
                        blockchain_height = proxy.get_blockcount()
                        connection_lost = False
                        if current_height >= blockchain_height:
                            continue
                    except ConnectionError:
                        connection_lost = True
                        continue


                # Request the next block in the sequence
                try:
                    cblock = proxy.get_block(current_height+1)
                    connection_lost = False
                except ConnectionError:
                    connection_lost = True
                    continue

            # try to add the block at the end of the cache for a reasonable
            # amount of time, if it fails discard it and try again later.
            while True:
                try:
                    cache.put((current_height, cblock), timeout=3)
                    current_height +=1
                    break
                except queue.Full:
                    pass
            
                if self._stop_event.wait(timeout=0):
                    break
                
        # Clean up before exiting
        self._proxy.stop()


    def set_height(self, height):
        """Purge current cache and start caching at a different height"""
        new_cache = queue.Queue(self._cache_size)
        with self._lock: 
            old_cache = self._cache
            self._cache = new_cache
            self._height = height

        # Purge all blocks from the old cache and fill it with None
        # to assure there isn't a get_next_block call stuck
        while True:
            try:
                old_cache.get(block=False)
            except queue.Empty:
                break

        while True:
            try:
                old_cache.put((None, None), block=False)
            except queue.Full:
                break

    def get_next_block(self, block=True, timeout=None):
        """Get the next block in the chain
        
        Arguments:
            block (bool): Block if necessary until an item is available
            timeout (int|None): If timeout is a positive number, it blocks at 
                most timeout seconds and raises the Empty exception if no item 
                was available within that time
        
        Exceptions:
            queue.Empty

        Returns:
            (int, cBlock)-> block height and block tuple
        """
        while True:
            cache = self._cache

            height, cblock = cache.get(block, timeout)
            if cblock is None:
                continue
       
            # the previous get call could have blocked for a long time,
            # check cache wasn't purged meanwhile
            if id(cache) != id(self._cache):
                continue
            else:
                break

        return height, cblock

    def stop(self, block=False):
        """
        Stop BlockCache

        Arguments:
            block (bool): If true block until thread has exited
        """
        self._stop_event.set()
        if block:
            self._fetch_thread.join()




class BitcoinBalanceFacade(object):
    """ """
     
    def __init__(self, db_session=None, bitcoind_url=None, backtrack_limit=None):
        """
        Arguments:
            db_session (SQLAlchemy.Session)
            bitcoin_url (string):
            backtrack_limit (int):
        """
        self._db_session = db_session
        self._bitcoind_url = bitcoind_url or Settings['BITCOIND_URL']
        self._backtrack_limit = backtrack_limit or Settings['MAX_BACKTRACK_BLOCKS']
        
        # Initialize balance 
        if self._db_session:
            self._storage = SQLBalanceStorage(Session)
        else:
            logger.info("No Database available, using memory storage")
            self._storage = MemoryBalanceStorage()

        self._balance_storage = BalanceProxyCache(self._storage, Settings['BALANCE_CACHE_SIZE'])
        
        # Load initial balance state from DB with the current height
        self._balance_processor = BalanceProcessor(backtrack_limit=self._backtrack_limit,
                                                   storage=self._balance_storage)

        # Block cache
        self._block_cache = BlockPrefetchingCache(self._balance_processor.height+1,
                                                  self._bitcoind_url) 

        # Connection to bitcoind rpc, it's initialized by reconnect code.
        self._bitcoind_proxy = BitcoindProxy(self._bitcoind_url)

        # Hash and heigh for the last N blocks added to balance processor
        self._block_hash  = deque()
        self._block_height = deque()

        # thread-safe lock during balance updated
        self._lock = threading.Lock()

        # 
        self._block_factory = BlockFactory(self._bitcoind_proxy)

        # Event to signal threads to stop
        self._stop_flag = threading.Event()

        # Launch polling thread
        self._poll_thread = threading.Thread(target=self._poll_thread_func, 
                                             daemon=False)
        self._poll_thread.start()

    @property
    def height(self):
        """Return height of top block if there isn't any loaded, used
        stored balance height"""
        return self._balance_processor.height

    def _add_block(self, block):
        """Add new block to tracked to update balance"""
        if len(self._block_hash) >= self._backtrack_limit:
            self._block_hash.popleft()
            self._block_height.popleft()

        with self._lock:
            self._block_hash.append(block.block_hash)
            self._block_height.append(block.height)

            # Process record into balance
            self._balance_processor.add_block(block)

    def _backtrack(self):
        # TODO: Check there are block remainint
        with self._lock:
            if not self._block_height:
                logger.error("Backtrack limit reached (height: {})".format(self.height))
                raise BacktrackError("Backtrack limit reached")
            else:
                logger.info("Backtracking one block (height: {})".format(self.height))

            self._block_hash.pop()
            current_height = self._block_height.pop()
            #TODO: Use a better system so there is no need to purge
            # the complete cache each backtrack
            #self._block_factory.purge_cache()
            self._balance_processor.backtrack()
            self._block_cache.set_height(current_height)

    def _poll_thread_func(self):
        """Thread polling bitcoind looking for the next block"""
        last_update = time.perf_counter()
        
        while True:
            if self._stop_flag.is_set():
                self._block_cache.stop()
                break

            # Wait until the next block is available
            height, cblock = self._block_cache.get_next_block()
         
            # Before building a block check the block follows the current 
            # top block if not backtrack
            if self._block_hash and cblock.hashPrevBlock != self._block_hash[-1]:
                self._backtrack()
                continue

            # Construct Block before adding to balance
            while True:
                try:
                    block = self._block_factory.build_block(cblock, height)
                    break
                except ConnectionError:
                    if self._stop_flag.wait(timeout=Settings['BITCOIND_POLL_PERIOD']):
                        return
                except Exception as e:
                    logger.exception("Unexpected exception:")
                    if self._stop_flag.wait(timeout=Settings['BITCOIND_POLL_PERIOD']):
                        return
            
            if height % 10000 == 0:
                logger.info("Block {}".format(height))

            self._add_block(block)

    def stop(self, block=False):
        """Safely stop and record state"""
        self._balance_processor.commit()
        self._stop_flag.set()
        self._bitcoind_proxy.stop()
        if block:
            self._poll_thread.join()
        logger.info("Closing")

    def get_balance(self, address):
        """Get current bitcoin address balance"""
        with self._lock:
            return self._balance_processor.get_balance(address)

    def get_transaction(self, address, confirmations=0):
        #TODO
        pass

    def __len__(self):
        return len(self._block_hash)






