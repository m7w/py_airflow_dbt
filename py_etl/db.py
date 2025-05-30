import os

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Float, Integer, create_engine
from sqlalchemy.orm import DeclarativeMeta, registry

load_dotenv()

DB_URL = (
    "postgresql+psycopg2://{DB_USER}:{DB_USER_PASSWORD}@localhost/{DB_NAME}".format(
        **os.environ
    )
)

engine = create_engine(DB_URL, echo=True, future=True)

mapper_registry = registry()


class Base(metaclass=DeclarativeMeta):
    __abstract__ = True

    registry = mapper_registry
    metadata = mapper_registry.metadata

    __init__ = mapper_registry.constructor


class Crash(Base):
    __tablename__ = "crash"

    crash_id = Column(Integer, primary_key=True)
    crash_date = Column(DateTime)
    latitude = Column(Float)
    longitude = Column(Float)
