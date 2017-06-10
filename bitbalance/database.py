from contextlib import contextmanager
from sqlalchemy import Column, Integer, String, create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from sqlalchemy.engine import Engine

from .settings import Settings

Base = declarative_base()

class AddressBalance(Base):
    __tablename__ = 'address_balance'
    address = Column(String(32), primary_key=True, index=True)
    balance =  Column(Integer)


class BlockHeight(Base):
    __tablename__ = 'blocks'
    id =  Column(Integer, primary_key=True, autoincrement=True)
    height =  Column(Integer)




@event.listens_for(Engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA page_size=4096')
    cursor.execute('PRAGMA cache_size=10000')
    #cursor.execute('PRAGMA locking_mode=EXCLUSIVE')
    cursor.execute('PRAGMA temp_store=MEMORY')
    cursor.execute('PRAGMA journal_mode=MEMORY') #TODO: NOT SAFE but faster
    #cursor.execute('PRAGMA journal_mode=WAL')
    cursor.close()

engine = create_engine('sqlite:///balance.db')


Base.metadata.create_all(engine)


Session = scoped_session(sessionmaker(
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
                bind=engine))


@contextmanager
def make_session_scope(db_session):
    """Provide a transactional scope around a series of operations."""
    session = db_session()
    session.expire_on_commit = False
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


