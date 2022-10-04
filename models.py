from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float


PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/sw-netology'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session_db = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Persons(Base):
    __tablename__ = 'persons'

    id = Column(Integer, primary_key=True)
    name = Column(String(40), nullable=False)
    gender = Column(String(20))
    homeworld = Column(String(40))
    birth_year = Column(String(10))
    height = Column(Float)
    mass = Column(Float)
    hair_color = Column(String(60))
    skin_color = Column(String(60))
    eye_color = Column(String(60))
    species = Column(String)
    vehicles = Column(String)
    starships = Column(String)
    films = Column(String)

    def __init__(self, data):
        for attr in data:
            if data[attr] in ('unknown', ''):
                data[attr] = None
            elif attr in ['height', 'mass']:
                data[attr] = float(data[attr].replace(',', '.'))
            elif attr == 'id':
                data[attr] = int(data[attr])
            setattr(self, attr, data[attr])
