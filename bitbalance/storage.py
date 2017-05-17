from contextlib import contextmanager
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class AddressBalance(Base):
    __tablename__ = 'address_balance'
    address = Column(String(32), primary_key=True)
    balance =  Column(Integer)


engine = create_engine('sqlite://')
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)



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


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items())
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True
