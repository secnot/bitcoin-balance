"""
mem_database

Utils for creating and handling in-memory sqlite database
"""


from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.pool import QueuePool, NullPool, StaticPool
from bitbalance.storage import AddressBalance, BlockHeight



def create_memory_db():
    """Configure temp memory database for testing"""
    engine = create_engine('sqlite:///:memory:',
        connect_args={'check_same_thread':False},
        poolclass=StaticPool) 
    # See for explanation on engine options:
    # http://www.sameratiani.com/2013/09/17/flask-unittests-with-in-memory-sqlite.html
    # http://sqlite.org/inmemorydb.html
   
    db_session = scoped_session(sessionmaker(
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
        bind=engine))
 
    # Create model tables
    BlockHeight.metadata.create_all(engine)
    AddressBalance.metadata.create_all(engine)
    return engine, db_session


