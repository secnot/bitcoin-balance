import threading
from collections import OrderedDict, defaultdict

from .database import AddressBalance, BlockHeight, Session, make_session_scope


class MemoryBalanceStorage(object):
    """In-Memory balance storage"""

    def __init__(self, initial_height=None):
        """
        Arguments:
            initial_height (int|None): Force initial height used
                mainly for debugging
        """
        self._balance = {}
        if initial_height is not None:
            self._height = initial_height
        else:
            self._height = -1

        self._lock = threading.Lock()

    @property
    def height(self):
        return self._height

    def get(self, address, default=None):
        """Get address balance"""
        with self._lock:
            if default is None:
                return self._balance[address]
            else:
                return self._balance.get(address, default)

    def get_bulk(self, address):     
        """ 
        Obtain the stored balance of a set of address in a single call

        Arguments:
            address (iterable): Set of address to retrieve

        Returns: 
            Address and balance for the address stored in the db, the ones
            not stored are ignored
            [('address', balance), ('address', balance), ....]
        """
        with self._lock:
            return [(a, self._balance[a]) for a in address if a in self._balance]

    def update(self, insert=None, update=None, delete=None, height=-1):
        """Update Balance by inserting/updating/deleting in a single transaction
        
        Insert (dict): {"address1": balance1, "address2": balance2, ...}
        Update (dict): {"address3": balance3, ...}
        Delete (iterable): ['address4', 'address5', ...]
        """
        with self._lock:
            if insert:
                self._balance.update(insert)
            
            if update:
                self._balance.update(update)
           
            if delete:
                for addr in delete:
                    self._balance.pop(addr, None)

            self._height = height


class SQLBalanceStorage(object):
    """SQLAlchemy balance storage"""
    
    def __init__(self, db_session):
        """
        Arguments:
            db_session (SQLAlchemy session):
        """
        # TODO: Add Force initial height
        self._height = -1
        self._db_session = db_session

        # Load initial height from db 
        with make_session_scope(self._db_session) as session:

            # If there is no block height the db is empty
            block_height = session.query(BlockHeight).order_by(
                    BlockHeight.id.desc()).first()
        
        if block_height is not None:
            self._height = block_height.height

    @property
    def height(self):
        return self._height

    def get(self, address, default=None):
        """
        Arguments:
            address (str):
        """
        with make_session_scope(self._db_session) as session:
            addr_bal = session.query(AddressBalance.balance)\
                              .filter_by(address=address)\
                              .first()

        if addr_bal is not None:
            return addr_bal[0]
        elif default is not None:
            return default

        raise KeyError

    def get_bulk(self, address):
        """ 
        Obtain the stored balance of a set of address in a single call

        Arguments:
            address (iterable): Set of address to retrieve

        Returns: 
            Address and balance for the address stored in the db, the ones
            not stored are ignored
            [('address', balance), ('address', balance), ....]
        """
        with make_session_scope(self._db_session) as session:
            query = session.query(AddressBalance.address, AddressBalance.balance)\
                           .filter(AddressBalance.address.in_(address))
            results = query.all()

        return results

    def update(self, insert=None, update=None, delete=None, height=-1):
        """Update Balance by inserting/updating/deleting in a single transaction
        
        Arguments:
            Insert (dict): Insert new address balance
                {"address1": balance1, "address2": balance2, ...}
            Update (dict): Update existing address balance
                {"address3": balance3, ...}
            Delete (iterable): Remove esisting address
                ['address4', 'address5', ...]

            Address for update and delete must exist
        """ 
        # TODO: If the updates are still too slow the only alternative is using 
        # SQLAlchemy core directly, see benchmarks: 
        # http://stackoverflow.com/questions/11769366/why-is-sqlalchemy-insert-with-
        # sqlite-25-times-slower-than-using-sqlite3-directly
        with make_session_scope(self._db_session) as session:
            
            if insert:
                in_map = [{"address": a, "balance":b } for a, b in insert.items()]
                session.bulk_insert_mappings(AddressBalance, in_map, 
                                             return_defaults=False)

            if update:
                up_map = [{"address": a, "balance":b } for a, b in update.items()]
                session.bulk_update_mappings(AddressBalance, up_map)


            if delete:
                session.query(AddressBalance)\
                       .filter(AddressBalance.address.in_(delete))\
                       .delete(synchronize_session=False)

            session.query(BlockHeight).delete()
            session.add(BlockHeight(height=height))

        self._height = height


class BalanceProxyCache(object):
    """
    update and commit can't be calladed concurrently
    """
    def __init__(self, balance_storage, max_cache_size):
        """
        Arguments:
            balance_storage (BalanceStorage)
            max_cache_size (int): Max cached addresses, 
                WARNING: during commits the cache_size can be larger
        """
        self._cache = OrderedDict()
        self._max_cache = max_cache_size
        self._storage = balance_storage
        self._height = self._storage.height

        # Updates received but not yet commited
        self._updates = defaultdict(int)
        
        # When disabled cache can't be trimed, this way is
        # possible to call get() without removing from cache
        # a value placed by commit()
        self._trim_cache = True

        # Read and write locks
        self._lock = threading.Lock() 
        
        # Cache Hit/miss stats
        self._cache_hit_count = 0
        self._cache_miss_count = 0

    @property
    def height(self):
        return self._height

    def __len__(self):
        """Number of updates since last commit"""
        return len(self._updates)

    def _load_to_cache(self, address):
        """Load address from storage into cache
        
        Arguments:
            address (str): Address for the balance to load
        """
        if address in self._cache:
            self._cache.move_to_end(address)
            self._cache_hit_count += 1
            return

        self._cache_miss_count += 1
        self._cache[address] = self._storage.get(address, 0)

        if self._trim_cache and len(self._cache) > self._max_cache:
            self._cache.popitem(last=False)

    def _load_to_cache_bulk(self, address):
        """Load several addresses from storage into cache
        
        Arguments:
            address (set|dict|list): addresses to load
        """
        to_load = []

        for addr in address:
            if addr not in self._cache:
                to_load.append(addr)
            else:
                self._cache.move_to_end(addr)

        # to_load = [addr for addr in address if addres not in self._cache]
        self._cache_hit_count += len(address) - len(to_load)
        self._cache_miss_count += len(to_load)
        
        stored = dict(self._storage.get_bulk(to_load))
    
        for addr in to_load:
            self._cache[addr] = stored.get(addr, 0)

        while self._trim_cache and len(self._cache) > self._max_cache:
            self._cache.popitem(last=False)


    def get(self, address):
        """Get address balanced"""
        with self._lock:
            try:
                return self._cache[address] + self._updates.get(address, 0)
            except KeyError:
                # balance not cached, load from storage   
                self._load_to_cache(address)

        return self.get(address)

    def update(self, address, value):
        """Update address balance by adding or substracting an ammount,
        this changes are not saved until there is a commit"""
        if value == 0:
            return
        
        with self._lock:
            self._updates[address] += value

            # Cleanup empty updates
            if self._updates[address] == 0:
                self._updates.pop(address, None)

    def _commit(self, height):
        """Commit to storage all updates since last commit.

        This code tries to lock the minimum time possible so get 
        request are responsive even during a big commit.

        WARNING: CAN'T CALL UPDATE() WHILE COMMITING... GET() IS OKAY
        
        Arguments:
            height (int): Block height for the
        """
        assert height >= self.height
        if height == self.height:
            return

        to_insert = {}
        to_update = {}
        to_delete = set()
       
        self._height = height

        # Preload balance for all updated address into cache
        with self._lock:
            
            self._load_to_cache_bulk(self._updates)

            # Convert updates into insert/update/delete operations
            for addr, value in self._updates.items():
                stored_value = self._cache[addr]

                if stored_value == 0:
                    to_insert[addr] = value
                elif stored_value + value == 0:
                    to_delete.add(addr)
                else:
                    to_update[addr] = stored_value + value
           
        # Merge updates into cache.
        with self._lock:
            for addr, update in self._updates.items():
                self._cache[addr] += update
 
            self._updates = defaultdict(int)
        
        # Can update without a lock because update isn't called until
        # the commit is finished, and all get requests for any of the
        # updated address are cached.
        self._storage.update(insert=to_insert,
                                 update=to_update,
                                 delete=to_delete,
                                 height=height)

        # Trim cache to correct size
        while len(self._cache) > self._max_cache:
            with self._lock:
                self._cache.popitem(last=False)


    def commit(self, height):
        """Wrapper to disable cache trim before calling commit"""
        try:
            self._trim_cache = False
            self._commit(height)
        except:
            raise
        finally:
            self._trim_cache = True

    def cache_clear(self):
        """Clear balance cache"""
        with self._lock:
            self._cache = OrderedDict()
