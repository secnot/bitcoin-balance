from unittest import TestCase

from bitbalance.storage import MemoryBalanceStorage, BalanceProxyCache
from bitbalance.primitives import TxOut, Block, COINBASE_TX
from bitbalance.balance import BalanceProcessor

from bitbalance.exceptions import BitcoinError, BacktrackError, ChainError


class TestBalanceProcessor(TestCase):

    def setUp(self):
        self.balance_storage = BalanceProxyCache(MemoryBalanceStorage(), 10000)

    def tearDown(self):
        pass

    def test_add_block(self):
        """Basic add block tests"""
        txout = TxOut(tx = "transaction_hash",
                      nout = 1,
                      addr = "bitcoin_address",
                      value = 133)

        block = Block(block_hash="block_hash",
                      height=100,
                      vout=[txout,],)
                      
        balance_processor = BalanceProcessor(storage=self.balance_storage)
        balance_processor.add_block(block)

        self.assertEqual(balance_processor.height, 100)
        self.assertEqual(balance_processor.get_balance("bitcoin_address"), 133)
        
        # Commit only commits the data already flushed into storage
        balance_processor.commit()

        self.assertEqual(balance_processor.get_balance("bitcoin_address"), 133)
        self.assertEqual(self.balance_storage.get("bitcoin_address"), 0)

        # Add empty blocks until the first block is flushed into storage
        for x in range(200):
            block = Block(block_hash="block_hash_{}".format(x),
                            height=x+100)
            balance_processor.add_block(block)

        self.assertEqual(balance_processor.get_balance("bitcoin_address"), 133)
        self.assertEqual(self.balance_storage.get("bitcoin_address"), 133)
        balance_processor.commit()
        self.assertEqual(self.balance_storage.get("bitcoin_address"), 133)
        storage_height = self.balance_storage.height

        # Create a new balance_processor and check balance hasn't changed
        new_processor = BalanceProcessor(storage=self.balance_storage)
        self.assertEqual(self.balance_storage.get("bitcoin_address"), 133)
        self.assertEqual(new_processor.get_balance("bitcoin_address"), 133)
        self.assertEqual(new_processor.height, storage_height) 
 
    def test_backtrack(self):
        """Test balance after block backtracking."""

        # First block
        txout1 = TxOut(tx = "transaction_hash",
                      nout = 1,
                      addr = "bitcoin_address1",
                      value = 133)

        block1 = Block(block_hash="block_hash",
                      height=100,
                      vout=[txout1,],)

        # Second block
        txout3 = TxOut(tx = "transaction_hash2",
                      nout = 1,
                      addr = "bitcoin_address2",
                      value = 100)

        txout4 = TxOut(tx = "transaction_hash2",
                      nout = 2,
                      addr = "bitcoin_address3",
                      value = 3)

        txout5 = TxOut(tx = "transaction_hash2",
                       nout = 3,
                       addr = "bitcoin_address1",
                       value = 30)

        block2 = Block(block_hash="block_hash",
                      height=101,
                      vin=[txout1],
                      vout=[txout3, txout4, txout5],)
     
        # Add first block
        balance_processor = BalanceProcessor(storage=self.balance_storage)
        balance_processor.add_block(block1)
        self.assertEqual(balance_processor.get_balance("bitcoin_address1"), 133)
        self.assertEqual(balance_processor.height, 100)

        # Add second block
        balance_processor.add_block(block2)
        self.assertEqual(balance_processor.get_balance("bitcoin_address1"), 30)
        self.assertEqual(balance_processor.get_balance("bitcoin_address2"), 100)
        self.assertEqual(balance_processor.get_balance("bitcoin_address3"), 3)
        self.assertEqual(balance_processor.height, 101)

        # Backtrack second block
        balance_processor.backtrack()
        self.assertEqual(balance_processor.get_balance("bitcoin_address1"), 133)
        self.assertEqual(balance_processor.get_balance("bitcoin_address2"), 0)
        self.assertEqual(balance_processor.get_balance("bitcion_address3"), 0)
        self.assertEqual(balance_processor.height, 100)

        # backtrack first block
        balance_processor.backtrack()
        self.assertEqual(balance_processor.get_balance("bitcoin_address1"), 0)
        self.assertEqual(balance_processor.get_balance("bitcoin_address2"), 0)
        self.assertEqual(balance_processor.get_balance("bitcion_address3"), 0)
        self.assertEqual(balance_processor.height, -1)

        # Error not more block to backtrack
        with self.assertRaises(BacktrackError):
            balance_processor.backtrack()

    def test_balance_tracking(self):
        """Test balance with more complex blocks"""
        # TODO
        pass

    def test_commit(self):
        """Test address balance is succesfully commited and recovered from DB"""
        # TODO: Test errors while committing and recovery
        pass
