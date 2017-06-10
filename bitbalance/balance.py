from collections import deque, defaultdict, namedtuple
import threading
import time

from .exceptions import BacktrackError
from .primitives import COINBASE_TX, bitcoin_to_string
from .storage import MemoryBalanceStorage, SQLBalanceStorage, BalanceProxyCache
from .settings import Settings


TxoRecord = namedtuple('TxoRecord', ['tx', 'value', 'height'])


from bitcoin.core import str_money_value, b2lx, b2x, x



def block_record_iter(block):
    """Iterate through block inputs and outputs"""
    address_total = 0
    address_discovered = False
    
    # First add new unspent outputs so balance is positive
    for vout in block.vout:
        if not vout.addr:
            continue
        
        yield (vout.addr, TxoRecord(vout.tx, vout.value, block.height))

    # Spend outputs
    for vin in block.vin:
        if not vin.addr or vin.tx==COINBASE_TX:
            continue

        yield (vin.addr, TxoRecord(vin.tx, -vin.value, block.height))


#
# TODO: Add self._update_lock to wrap add_block and backtrack while still
# allowing balance get requests
#########################################################################3
class BalanceProcessor(object):


    def __init__(self, backtrack_limit=100, storage=None):
        """
        Arguments:
            storage (BalanceProxyCache):
        """

        # 
        self._blocks = deque()

        # Last time a block was added
        self._last_block_time = time.perf_counter()

        # Max number of block being tracked
        self._backtrack_limit = backtrack_limit

        # Address balance permanent storage
        self._storage = storage

        # Accumulated address balance for the blocks not yet placed into storage
        self._pending_balance = defaultdict(int)

        # Operation records for the blocks not yet stored, (by address)
        self._pending_records = defaultdict(deque)

        # Main lock
        self._lock = threading.Lock()

 
    def _add_record(self, address, record):
        """Add transaction record to address"""
        self._pending_records[address].append(record)
        self._pending_balance[address] += record.value
      
        # Cleanup
        if not self._pending_balance[address]:
            del self._pending_balance[address]
        
    def _del_record(self, address, last=True):
        """Remove first or last record for given address"""
        if address not in self._pending_records:
            return

        records = self._pending_records[address]

        record = records.pop() if last else records.popleft()

        self._pending_balance[address] -= record.value
 
        # Cleanup
        if not records:
            del self._pending_records[address]
        
        if not self._pending_balance[address]:
            del self._pending_balance[address]

    def add_block(self, block): 
        """Add next block in the chain"""
        # Add newest block 
        with self._lock:
            self._blocks.append(block) 
       
            for address, record in block_record_iter(block):
                self._add_record(address, record)

        # Commit oldest block to storage if the limit has been reached
        if len(self._blocks) > self._backtrack_limit:
            with self._lock:
                block = self._blocks.popleft()
            
                for address, record in block_record_iter(block):
                    self._del_record(address, last=False)
                    self._storage.update(address, record.value)

        # Determine if it's the best time for a storage commit
        # more than 30000 pending updates or more than 30 seconds 
        # since the last block. Doesn't need locking
        now = time.perf_counter()
        if len(self._storage) > 30000 or now-self._last_block_time>30:
            self._storage.commit(self._blocks[0].height-1)

        self._last_block_time = now

    def backtrack(self):
        """Backtrack one block up to a max of backtrack_limit blocks"""
        
        
        if not self._blocks:
            raise BacktrackError("Reached backtrack limit")

        with self._lock:
            block = self._blocks.pop()

            for address, record in block_record_iter(block):
                self._del_record(address, last=True)

    def get_transactions(self, address, confirmations=0): 
        """Return a list of the unconfirmed incoming/outgoing transactions.

        Arguments:
            address (str): Bitcoin address
            confirmations (int): Number of confirmations required for a transaction
                to be considered confirmed (must be smalled than backtrack_limit)

        Returns:
            (transaction_hash, value, block_height)
        """
        if address not in self._pending_records:
            return []

        # transactions from blocks lower than this height are confirmed
        limit_height = self.height-confirmations

        unconfirmed = [] # Unconfirmed records

        with self._lock:
            for record in reversed(self._pending_records[address]):
                if record.height < limit_height:
                    break

                unconfirmed.append(record)

        return unconfirmed


    def get_balance(self, address):
        """Return bitcoin address balance, can be called concurrently with:
        commit, backtrack, and add_blok"""
        with self._lock:
            return self._storage.get(address)+self._pending_balance.get(address, 0)

    def commit(self):
        """Force commit balance to storage"""
        if self._blocks:
            self._storage.commit(self._blocks[0].height-1)

    @property
    def height(self):
        return self._blocks[-1].height if self._blocks else self._storage.height
