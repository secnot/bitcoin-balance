from collections import OrderedDict


from bitcoin.core import str_money_value, b2lx, b2x, x
from bitcoin.wallet import CBitcoinAddress, CBitcoinAddressError
from bitcoin.rpc import unhexlify, hexlify
from bitcoin.core import COutPoint

from .exceptions import ChainError, BacktrackError

COINBASE_TX = b'\x00'*32

def bitcoin_to_string(value):
    """Convert bitcoin value to a string"""
    #TODO: Append zeroes up to standard length
    bitcoin_str = str_money_value(abs(value))
    if value < 0:
        return '- '+bitcoin_str
    else:
        return bitcoin_str



class TxOut(object):
    """Transaction ouput"""
    __slots__ = ('tx', 'nout', 'addr', 'value')

    def __init__(self, tx, nout, addr=None, value=0):
        """
        Arguments:
            tx (string): Transaction hash
            nout (int): Transaction output number
            addr (string):
            value (int): Output value
        """
        self.tx = tx
        self.nout = nout
        self.addr = addr
        self.value = value

    @staticmethod
    def addr_from_script(script):
        """Generate output addres from scriptPubKey"""
        try:
            addr = str(CBitcoinAddress.from_scriptPubKey(script))
        except CBitcoinAddressError:
            addr = None
      
        return addr

    @classmethod
    def from_tx(cls, tx, nout):
        """
        WARNING: This is not efficient to process all the transaction outputs
        because of GetTxid() does not cache the result.

        Arguments:
            tx (bitcoin.CTransaction): Transaction
            nout (int): Output number

        Returns:
            Inialized TxOut

        Exceptions:
            CBitcoinAddressError: Couldn't convert transaction output scriptPubKey 
                to address
            IndexError: The requested output doesn't exist
        """
        # GetTxid instead of GetHash for segwit support (bip-0141)
        txhash = tx.GetTxid()
        cout = tx.vout[nout]
        addr = TxOut.addr_from_script(cout.scriptPubKey)
        return cls(txhash, nout, addr, value=cout.nValue)

    def __hash__(self):
        return hash((self.tx, self.nout))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self.tx == other.tx and self.nout == other.nout
    
    def __repr__(self):
        return "TxOut({}, {}, {}, {})".format(
                    self.tx, 
                    self.nout, 
                    self.addr, 
                    self.value)

    def __str__(self):     
        return "TxOut({}, {}, {}, {})".format(
                    b2x(self.tx), 
                    self.nout, 
                    self.addr, 
                    str_money_value(self.value))



class Block(object):

    __slots__=('block_hash', 'height', 'vin', 'vout')

    def __init__(self, block_hash, height, vin=None, vout=None):
        
        self.block_hash = block_hash
        self.height = height
        if not vin:
            vin = []
        if not vout:
            vout = []

        self.vin = list(vin)
        self.vout = list(vout)

    def __hash__(self):
        return hash(self.block_hash)

    def __eq__(self, other):
        if isintance(other, self.__class__):
            return self.block_hash==other.block_hash
        else:
            return False

    def __repr__(self):
        return "{}({},{},{},{})".format(self.__class__.__name__,
                                        self.block_hash,
                                        self.height,
                                        self.vin,
                                        self.vout)

    def __str__(self):
        return "{}: {} ({})".format(self.__class__.__name,
                                    self.block_hash,
                                    self.height)

    def check_balance(self):
        """Check block input value sum is equeal to output value sum"""
        input_value=0
        output_value=0

        for vin in self.vin:
            input_value += vin.value

        for vout in self.vout:
            output_value += vout.value

        return input_value == output_value


class TxOutCache(object):
    
    def __init__(self, proxy, size=500000):
        """
        Arguments:
            size (int): max cache size
            proxy (proxy.BitcoindProxy)
        """
        self._proxy = proxy
        self._max_size = size

        self._txout_cache = OrderedDict()

        self._cache_miss = 0
        self._cache_hit = 0

    def del_txout(self, txout):
        """Remove txout from cache"""
        self._txout_cache.pop(txout, None)
    
    def add_txout(self, txout):
        """Add TxOut to cache"""
        if len(self._txout_cache)>=self._max_size:
            self._txout_cache.popitem(last=False)
        
        self._txout_cache[txout] = txout

    def purge_cache(self):
        """Purge complete cache"""
        self._txout_cache = OrderedDict()

    def get_txout(self, txhash, nout):
        """
        Get TxOut from cache or if not available query bitcoind_proxy
        
        Arguments:
            txhash (str): Transactions hash
            nout (int): Output number
        """
        try:
            txout = self._txout_cache[TxOut(txhash, nout)]
            self._cache_hit += 1
            return txout
        except KeyError:
            pass

        self._cache_miss += 1

        with self._proxy as proxy: 
            try:
                tx = proxy.get_transaction(txhash)
            except ConnectionError:
                raise
            except Exception:
                raise ChainError("Unknown Txout {} {}".format(txhash, nout))
 
        # Manually initilize TxOut so there is no need to generate the transaction
        # hash a second time. (faster than:txout = TxOut.from_tx(rawtx, nout))
        for out, cout in enumerate(tx.vout):
            addr = TxOut.addr_from_script(cout.scriptPubKey)
            self.add_txout(TxOut(txhash, out, addr, value=cout.nValue))

        # Now txout must be in cache
        self._cache_hit -= 1 # Fix hit/miss counter
        return self.get_txout(txhash, nout)
   

class BlockFactory(object):

    def __init__(self, proxy, size=1000000):
        """
        Arguments:
            size (int): max cache size
            proxy (proxy.BitcoindProxy)
        """
        self._proxy = proxy
        self._max_size = size
       
        self._cache = TxOutCache(proxy, size)

    def purge_cache(self):
        """Completely purge cache"""
        self._cache.purge()

    def _transaction_inputs(self, tx):
        """Generate transaction inputs from source transaction outputs""" 
        inputs = []
        txhash = tx.GetTxid()
       
        for vin in tx.vin:
            txin = vin.prevout
            
            if txin.hash == COINBASE_TX:
                continue

            txout = self._cache.get_txout(txin.hash, txin.n)
            if txout is None:
                logger.error("Unable to find TxOut {} {}".format(
                        txin_hash, txin_n))
            else:
                inputs.append(txout)

        return inputs

    def _transaction_outputs(self, tx):
        """Generate transaction TxOut""" 
        outputs = []

        # GetTxid instead of GetHash for segwit support (bip-0141)
        txhash = tx.GetTxid()

        for n, utxo in enumerate(tx.vout):  
            
            addr = TxOut.addr_from_script(utxo.scriptPubKey)
            out = TxOut(txhash, n, addr, value=utxo.nValue)
            outputs.append(out)

        return outputs

    def _block_outputs(self, block):
        """Generate the TxOut for all the block outputs"""
        block_txouts = []

        for tx in block.vtx:
            block_txouts.extend(self._transaction_outputs(tx))
            
        return block_txouts

    def _block_inputs(self, block):
        """Generate the TxOut for all the block inputs"""
        block_inputs = []

        for tx in block.vtx:
            block_inputs.extend(self._transaction_inputs(tx))

        return block_inputs

    def build_block(self, block, height=None):
        """Build Block from bitcoin.CBlock"""
        blockhash = block.GetHash()
        
        
        outputs = self._block_outputs(block)
        
        # Add outputs to cache, because the outputs from a transaction
        # can be used as inputs for other transactions in the same block
        for txout in outputs:
            if txout.value > 0:
                self._cache.add_txout(txout)

        # Generate inputs 
        inputs = self._block_inputs(block)
        #TODO: Remove outputs added to cache if input generations fails???


        # With the complete block remove used inputs from cache to save space
        #for txout in inputs:
        #    self._cache.del_txout(txout)

        block = Block(blockhash, height, inputs, outputs)
        return block
