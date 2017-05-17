from collections import deque, defaultdict, namedtuple
from .storage import Session, get_or_create, make_session_scope, AddressBalance

OP_RECEIVED = 1
OP_SPENT    = 2
TxoRecord = namedtuple('UtxoRecord', ['op_type', 'txo', 'height'])


COINBASE_TX = '0'*64

class BalanceProcessor(object):

    
    def __init__(self, backtrack_limit=100):
        self._blocks = deque()

        # Accumulated balance up to the oldest tracked block
        self._address_balance = defaultdict(int)
       
        # records per address  in the las N blocks (using list
        # instead of deque to save memory)
        self._address_records = defaultdict(list)

        # Max number of blocks with full records
        self._backtrack_limit = backtrack_limit
        
        self._db_session = Session


    def _add_record(self, address, record):
        """Add transaction record to address"""
        self._address_records[address].append(record)
       
        with make_session_scope(self._db_session) as session:
            balance, created = get_or_create(session, 
                    AddressBalance, defaults= {'balance': 0}, address=address)
             
            if record.op_type == OP_RECEIVED:
                balance.balance = balance.balance + record.txo.value
                #self._address_balance[address] += record.txo.value
            else:
                balance.balance = balance.balance - record.txo.value
                #self._address_balance[address] -= record.txo.value
       
            if balance.balance == 0:
                if created:
                    session.expunge(balance)
                else:
                    session.delete(balance)

        assert balance.balance >= 0

        #balance.balance=12
        #session = self._db_session()
        #ubalance = AddressBalance(address=address, balance=new_balance)
        #session.merge(balance)
        #session.commit()

        # Don't keep balance if it is 0
        #if self._address_balance[address] == 0:
        #    del self._address_balance[address]

    def _del_record(self, address, height):
        """Remove from address all records equal or smaller"""
        if address not in self._address_records:
            return

        records = self._address_records[address]
        while records and records[0].height <= height:
            records.pop(0)

        # If empty remove deque
        if not records:
            del self._address_records[address]

    def _remove_oldest_block(self):
        """ """
        block = self._blocks.popleft()

        # Remove all the records added by this block
        for vout in block.vout:
            if vout.addr:
                self._del_record(vout.addr, block.height)

        for vin in block.vin:
            if vin.addr:
                self._del_record(vin.addr, block.height)

    def add_block(self, block):
        """Update address balance with block utxo"""

        if len(self._blocks) > self._backtrack_limit:
            self._remove_oldest_block()

        self._blocks.append(block) 
    
        # First add new unspent outputs so balance is positive
        for vout in block.vout:
            if not vout.addr or vout.value==0:
                continue

            self._add_record(vout.addr, TxoRecord(OP_RECEIVED, vout, block.height))

        # Spend outputs
        for vin in block.vin:
            if not vin.addr or vin.tx==COINBASE_TX:
                continue
           
            self._add_record(vin.addr, TxoRecord(OP_SPENT, vin, block.height))

    def backtrack(self):
        """Backtrack one block update"""
        pass

    @property
    def top_block(self):
        """Return top BlockRecord"""
        if not self._last_blocks:
            return None
        else:
            return self._last_blocks[-1]

    @property
    def top_block_height(self):
        """Current blockchain height"""
        if not self.top_block:
            return -1
        else:
            return self.top_block.height

    def _update_address_balance(self, block):
        """
        Update address balance with a new block positive value for new unspent
        and negative value for spent.
        """
        assert isintance(utxo.addr, str)

        try:
            self._address_balance[utxo.addr] += utxo.amount
            assert self._address_balance[utxo.addr] > 0
        except KeyError:
            assert utxo.value >= 0
            self._address_balance[utxo.addr] = utxo_value


    def _get_unconfirmed(self, address, confirmations):
        """Return a list of unconfirmed 'movements' for the address
        
        Arguments:
            address (str): Wallet address
            confirmations (int): Number of confirmations required for a 
                movement (must be smallet than the number of tracked blocks

        Returns:
            List of (operation_type, utxo, confirmations)
            
            operation_type: RECEIVED|SPENT
            utxo: TxOut
            confirmations: int
        """
        if address not in self._address_utxo:
            return []

        # transactions from blocks lower than this height are confirmed
        limit_height = self.top_block_height-confirmations

        unconfirmed = []

        for record in reversed(self_address_utxo[addr]):
            if record.height >= limit_height:
                unconfirmed.append((op_type, utxo, self.top_block_height-record.height))
            else:
                # Because they are ordered as soon as there is one confirmed
                # utxo the ones following must also be confirmed
                break

        return unconfirmed

    def get_balance(self, address, confirmations):
        """
        Arguments:
            address (string):
            confirmations (int): Number of confirmations required to accept
                the balance, any utxo
        Returns:
            (balance, [unconfirmed1, unconfirmed2, ...])
        """
        if confirmations < 0:
            confirmations = 0

        balance = self._address_balance.get(address, 0)
        unconfirmed = self._get_unconfirmed(address, confirmations)

        return (balance, unconfirmed)


