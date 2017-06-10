import threading
import time

from unittest import TestCase
from unittest.mock import MagicMock

import sqlalchemy

from bitbalance.storage import (MemoryBalanceStorage, SQLBalanceStorage, 
        BalanceProxyCache, make_session_scope)
from bitbalance.storage import AddressBalance, BlockHeight
from .database import create_memory_db

class TestSQLBalanceStorage(TestCase):

    def setUp(self):
        """Create test in-memory sqlite DB"""
        self.db_engine, self.db_session = create_memory_db()

    def tearDown(self):
        """Discard sqlite DB"""
        self.db_session.close()
        self.db_engine.dispose()

    def test_init(self):
        """Test initialized from db"""
        storage = SQLBalanceStorage(self.db_session)
        self.assertEqual(storage.height, -1)
    
        with make_session_scope(self.db_session) as session:
            session.add(BlockHeight(height=12))

        storage = SQLBalanceStorage(self.db_session)
        self.assertEqual(storage.height, 12)

    def test_get_balance(self):
        """Test get method with and without default"""
        storage = SQLBalanceStorage(self.db_session)

        with make_session_scope(self.db_session) as session:
            session.add(AddressBalance(address='address_one', balance=44))
            session.add(AddressBalance(address='address_two', balance=55))

        self.assertEqual(storage.get('address_one'), 44)
        self.assertEqual(storage.get('address_two'), 55)
        self.assertEqual(storage.get('address_one', 99), 44)

        with self.assertRaises(KeyError):
            storage.get('address_three')

        self.assertEqual(storage.get('address_three', 3), 3)

    def test_get_bulk_balance(self):
        """ """
        storage = SQLBalanceStorage(self.db_session)
        
        insert = {
            "addr1": 1,
            "addr2": 2,
            "addr3": 3,
        }

        storage.update(insert=insert)
        result = dict(storage.get_bulk(set(["addr1", "addr2", "addr4"])))
        self.assertEqual(len(result), 2)
        self.assertTrue('addr1' in result)
        self.assertTrue('addr2' in result)

        result = dict(storage.get_bulk({"addr4":55}))
        self.assertEqual(len(result), 0)

    def test_insert_balance(self):
        """Test adding balance for new address """
        storage = SQLBalanceStorage(self.db_session)

        with self.assertRaises(KeyError):
            storage.get("address_one")

        storage.update(insert={"address_one": 32})

        self.assertEqual(storage.get("address_one"), 32)

        # 
        with make_session_scope(self.db_session) as session:
            addr_bal = session.query(AddressBalance)\
                              .filter_by(address="address_one")\
                              .first()
        
        self.assertEqual(addr_bal.balance, 32)

        # Error when insertion an existing address
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            storage.update(insert={'address_one': 33})

        # Insert several at a time
        storage.update(insert={"a1": 55, "a2": 66})
        self.assertEqual(storage.get('a1'), 55)
        self.assertEqual(storage.get('a2'), 66)

    def test_udpate_balance(self):
        storage = SQLBalanceStorage(self.db_session)

        storage.update(insert={"address_four": 44})

        # Error when insertion an existing address
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            storage.update(insert={'address_four': 33})

        # Fine when updating an existing balance
        storage.update(update={'address_four': 777})
        self.assertEqual(storage.get('address_four'), 777)

        with make_session_scope(self.db_session) as session:
            addr_bal = session.query(AddressBalance)\
                              .filter_by(address="address_one")\
                              .first()
        
        # Update several address at a time
        storage.update(insert={'b1': 11, 'b2': 22})
        self.assertEqual(storage.get('b1'), 11)
        self.assertEqual(storage.get('b2'), 22)

        storage.update(update={'b1': 44, 'b2': 55})
        self.assertEqual(storage.get('b1'), 44)
        self.assertEqual(storage.get('b2'), 55)
        
        with make_session_scope(self.db_session) as session:
            addr1 = session.query(AddressBalance)\
                              .filter_by(address="b1")\
                              .first()
            
            addr2 = session.query(AddressBalance)\
                              .filter_by(address="b2")\
                              .first()

        self.assertEqual(addr1.balance, 44)
        self.assertEqual(addr2.balance, 55)
       
    def test_delete_balance(self):
        """Test deleting existing address"""
        storage = SQLBalanceStorage(self.db_session)
        
        storage.update(insert={'addr1': 33, 'addr2': 44, 'addr3': 55})
        self.assertEqual(storage.get('addr1'), 33)
        self.assertEqual(storage.get('addr2'), 44)

        storage.update(delete=['addr1', 'addr2'])
        self.assertEqual(storage.get('addr1', 99), 99)
        self.assertEqual(storage.get('addr2', 88), 88)
     
        with make_session_scope(self.db_session) as session:
            addr1 = session.query(AddressBalance)\
                              .filter_by(address="addr1")\
                              .first()
            
            addr2 = session.query(AddressBalance)\
                              .filter_by(address="addr2")\
                              .first()

        self.assertEqual(addr1, None)
        self.assertEqual(addr2, None)

        # Check deleting unknown address
        storage.update(delete=['addr1'])

    def test_mixed_ops(self):
        """Test mixing inserts/updates/deletions in a single operation"""
        storage = SQLBalanceStorage(self.db_session)
        insert = {str(x): x for x in range(100)}
        storage.update(insert=insert)

        for a in range(100):
            self.assertEqual(storage.get(str(a)), a)
       
        # Insert and update
        insert = {str(x): x for x in range(100, 110)}
        update = {str(x): 5 for x in range(100)}
        storage.update(insert=insert, update=update)

        for a in range(100):
            self.assertEqual(storage.get(str(a)), 5)

        for a in range(100, 110):
            self.assertEqual(storage.get(str(a)), a)

        # Insert udpate and delete
        insert = {str(x): x for x in range(200, 300)}
        update = {str(x): 7 for x in range(50)}
        delete = [str(x) for x in range(50, 100)]
        storage.update(insert=insert, update=update, delete=delete, height=10)

        for a in range(50):
            self.assertEqual(storage.get(str(a)), 7)

        for a in range(200, 300):
            self.assertEqual(storage.get(str(a)), a)

        for a in range(50, 100):
            self.assertEqual(storage.get(str(a), 99999), 99999)


    def test_update_height(self):
        """Test heigh provided during update is stored"""
        storage = SQLBalanceStorage(self.db_session)

        self.assertEqual(storage.height, -1)

        storage.update(insert={'addr1': 1}, height=10)

        self.assertEqual(storage.height, 10)
       
        # Check it was recorded to db
        with make_session_scope(self.db_session) as session:
            block_height = session.query(BlockHeight).first()
 
        self.assertEqual(block_height.height, 10)


        # Test BlockHeight table has a single row 
        storage.update(insert={'addr3': 3}, height=11)
        storage.update(insert={'addr4': 4}, height=12)
        
        with make_session_scope(self.db_session) as session:
            block_count = session.query(BlockHeight).count()
 
        self.assertEqual(block_count, 1)




class TestMemoryBalanceStorage(TestCase):
    """MemoryBalanceStorage is very simple in comparison, and it's itself
    used mainly for testing, so the tests are much more simple"""
    
    def test_init(self):
        storage = MemoryBalanceStorage()
        self.assertEqual(storage.height, -1)

    def test_get_balance(self):
        
        storage = MemoryBalanceStorage()
        storage.update(insert={"addr1": 1, "addr2": 2}, height=33)
   
        self.assertEqual(storage.height, 33)
        self.assertEqual(storage.get("addr1", 1), 1)
        self.assertEqual(storage.get("addr2", 2), 2)

        # Missing address raise exception
        with self.assertRaises(KeyError):
            storage.get("addr3")

        # Check default keyword
        self.assertEqual(storage.get("addr3", 77), 77)
        self.assertEqual(storage.get("addr1", 77), 1)
        self.assertEqual(storage.get("addr2", 77), 2)

    def test_get_bulk_balance(self):
        """ """
        storage = MemoryBalanceStorage()
        
        insert = {
            "addr1": 1,
            "addr2": 2,
            "addr3": 3,
        }

        storage.update(insert=insert)
        result = dict(storage.get_bulk(set(["addr1", "addr2", "addr4"])))
        self.assertEqual(len(result), 2)
        self.assertTrue('addr1' in result)
        self.assertTrue('addr2' in result)

        result = dict(storage.get_bulk({"addr4":55}))
        self.assertEqual(len(result), 0)
   
    def test_insert_balance(self):
       
        storage = MemoryBalanceStorage()
        
        insert = {str(a): a for a in range(10000)}
        storage.update(insert=insert, height=66)

        for a in range(10000):
            self.assertEqual(storage.get(str(a)), a)

        self.assertEqual(storage.height, 66)

    def test_update_balance(self):
        storage = MemoryBalanceStorage()
        
        insert = {str(a): a for a in range(10000)}
        storage.update(insert=insert)

        for a in range(10000):
            self.assertEqual(storage.get(str(a)), a)

        update = {str(a): 10 for a in range(5000)}
        storage.update(update=update, height=88)

        for a in range(5000):
            self.assertEqual(storage.get(str(a)), 10)

        self.assertEqual(storage.height, 88)

    def test_delete_balance(self):
        storage = MemoryBalanceStorage()

        insert = {str(a): a for a in range(10000)}
        storage.update(insert=insert)

        for a in range(10000):
            self.assertEqual(storage.get(str(a)), a)

        delete = set([str(a) for a in range(5000)])
        storage.update(delete=delete, height=44)
        for a in range(5000):
            with self.assertRaises(KeyError):
                storage.get(str(a))

        self.assertEqual(storage.height, 44)

    def test_mix_ops(self):
        """Test mixin insert/update/delete operations"""
        storage = MemoryBalanceStorage()

        insert = {str(a): a for a in range(5000)}
        storage.update(insert=insert, height=77)

        for a in range(5000):
            self.assertEqual(storage.get(str(a)), a)


        # Mixed op
        insert = {str(a): a for a in range(10000, 15000)}
        update = {str(a): 66 for a in range(1000)}
        delete = [str(a) for a in range(1000, 3000)]

        storage.update(insert=insert, update=update, delete=delete, height=99)

        for a in range(10000, 15000):
            self.assertEqual(storage.get(str(a)), a)

        for a in range(3000, 5000):
            self.assertEqual(storage.get(str(a)), a)

        for a in range(1000):
            self.assertEqual(storage.get(str(a)), 66)

        for a in range(1000, 3000):
            with self.assertRaises(KeyError):
                storage.get(str(a))

        self.assertEqual(storage.height, 99)
        

        

class TestBalanceProxyCache(TestCase):


    def setUp(self):
        self.storage = MemoryBalanceStorage()

    def test_get(self):
        """Test balance is cached"""
        balance_proxy = BalanceProxyCache(self.storage, 1000)
        for a in range(1000):
            balance_proxy.update(str(a), a)
        
        for a in range(1000):
            self.assertEqual(balance_proxy.get(str(a)), a)

        for a in range(1000, 2000):
            self.assertEqual(balance_proxy.get(str(a)), 0)

        self.assertEqual(len(balance_proxy), 999) # 0 Update Discarded
        balance_proxy.commit(12)
        self.assertEqual(len(balance_proxy), 0)

        # Update all address and try again 
        for a in range(1000):
            balance_proxy.update(str(a), 44)
        
        for a in range(1000):
            self.assertEqual(balance_proxy.get(str(a)), a+44)

        self.assertEqual(len(balance_proxy), 1000)
        balance_proxy.commit(14)
        self.assertEqual(len(balance_proxy), 0)

        for a in range(1000):
            self.assertEqual(balance_proxy.get(str(a)), a+44)
        
        for a in range(1000, 2000):
            self.assertEqual(balance_proxy.get(str(a)), 0)

    def test_storate_insert_update_delete(self):
        """Test how BalanceProxyCache translate updates into
        insert, update and delete operations"""
        self.storage.update = MagicMock()
        balance_proxy = BalanceProxyCache(self.storage, 1000)
        
        # Insert 
        balance_proxy.update('address_one', 1) # Insert
        balance_proxy.update('address_two', 2) # Insert
        self.storage.update.assert_not_called()
        balance_proxy.commit(33)
        
        args = self.storage.update.call_args[1]
        self.assertEqual(len(args['delete']), 0)
        self.assertEqual(len(args['update']), 0)
        self.assertEqual(len(args['insert']), 2)
        self.assertEqual(args['insert']['address_one'], 1)
        self.assertEqual(args['insert']['address_two'], 2)

        # Update + Insert
        self.storage.update.reset_mock()
        balance_proxy.update('address_two', 2) # Update
        balance_proxy.update('address_three', 3) # Insert
        balance_proxy.commit(44)

        args = self.storage.update.call_args[1]
        self.assertEqual(len(args['delete']), 0)
        self.assertEqual(len(args['update']), 1)
        self.assertEqual(len(args['insert']), 1)
        self.assertEqual(args['insert']['address_three'], 3)
        self.assertEqual(args['update']['address_two'], 4)

        # Update + Insert + Delete
        self.storage.update.reset_mock()
        balance_proxy.update('address_four', 4)  # Insert
        balance_proxy.update('address_three', 1) # Update
        balance_proxy.update('address_one', -1)  # Delete
        balance_proxy.commit(55)

        args = self.storage.update.call_args[1]
        self.assertEqual(len(args['insert']), 1)
        self.assertEqual(len(args['update']), 1)
        self.assertEqual(len(args['delete']), 1)

        self.assertEqual(args['insert']['address_four'], 4)
        self.assertEqual(args['update']['address_three'], 4)
        self.assertTrue('address_one' in args['delete'])
        self.assertEqual(args['height'], 55)

    def test_cache_trim(self):
        """Test cache is trimed when it reaches max_size"""
        self.storage.get = MagicMock(return_value=0)
        balance_proxy = BalanceProxyCache(self.storage, 1000)

        # Completely fill cache
        for a in range(1000):
            balance_proxy.update(str(a), a+1)

        balance_proxy.commit(10)
        
        # When the address is cached storage.get() isn't called:
        self.storage.get.reset_mock()
        for a in range(1000):
            self.assertEqual(balance_proxy.get(str(a)), a+1)
        
        self.storage.get.assert_not_called()

        # Adding another address will cause the proxy to discard the oldest one
        self.assertEqual(len(balance_proxy._cache), 1000)
        balance_proxy.update('new_address', 44)
        balance_proxy.commit(444)
        
        # This will call storage.get because atleast one addr isn't cached
        self.storage.get.reset_mock()
        for a in range(1000):
            # WARNING: returned value is mangled because mock get allways 
            # return 0
            balance_proxy.get(str(a))
        self.assertGreater(self.storage.get.call_count, 0)

    def test_commit(self):
        """Test data is stored correctly"""
        balance_proxy = BalanceProxyCache(self.storage, 50000)
        
        for a in range(2000):
            balance_proxy.update(str(a), a)

        balance_proxy.commit(1000)

        for a in range(1, 2000):
            self.assertEqual(self.storage.get(str(a)), a)

        # 0 updates are not stored
        with self.assertRaises(KeyError):
            self.storage.get('0')
        self.assertEqual(balance_proxy.get('0'), 0)
    
    @staticmethod
    def concurrent_get(balance_proxy, error_event):
        """ """
        time.sleep(0.1) # Delay to allow commit to start
        for a in range(50000):
            if balance_proxy.get(str(a)) != a+1:
                error_event.set()
    
    def test_concurrent_commit(self):
        """Test concurrent get() and commit() calls"""
        self.db_engine, self.db_session = create_memory_db()
        db_storage = SQLBalanceStorage(self.db_session)
        balance_proxy = BalanceProxyCache(db_storage, 50000)

        # Test get during a commit
        ###########################
        for a in range(100000):
            balance_proxy.update(str(a), a+1)

        error_event = threading.Event() 
        get_thread = threading.Thread(target=self.concurrent_get, 
                                      args=(balance_proxy, error_event), 
                                      daemon=True)
        get_thread.start()
        balance_proxy.commit(12)

        # Check no error was detected
        get_thread.join() 
        self.assertFalse(error_event.is_set())

        # Cleanup database
        self.db_session.close()
        self.db_engine.dispose()
 
    def test_concurrent_update(self):
        """Test concurrent get() and update() calls"""
        self.db_engine, self.db_session = create_memory_db()
        db_storage = SQLBalanceStorage(self.db_session)
        balance_proxy = BalanceProxyCache(db_storage, 50000)

        # Test get during a commit
        ###########################
        for a in range(100000):
            balance_proxy.update(str(a), a+1)

        balance_proxy.commit(12)
        
        
        error_event = threading.Event() 
        get_thread = threading.Thread(target=self.concurrent_get, 
                                      args=(balance_proxy, error_event), 
                                      daemon=True)

        get_thread.start()
        
        for a in range(100000, 300000):
            balance_proxy.update(str(a), a)

        # Check no error was detected
        get_thread.join() 
        self.assertFalse(error_event.is_set())

        # Verify  updated address
        for a in range(100000, 150000):
            self.assertEqual(balance_proxy.get(str(a)), a)

        # Cleanup database
        self.db_session.close()
        self.db_engine.dispose()












