import os

from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Integer,
    String,
    create_engine,
)
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


class Vehicle(Base):
    __tablename__ = "vehicle"

    unique_id = Column(Integer, primary_key=True)
    crash_id = Column(Integer, ForeignKey("crash.crash_id"), nullable=False)
    travel_direction = Column(String)
    vehicle_occupants = Column(Integer)
    driver_license_status = Column(String)
    pre_crash = Column(String)
    point_of_impact = Column(String)

class Person(Base):
    __tablename__ = "person"

    unique_id = Column(Integer, primary_key=True)
    crash_id = Column(Integer, ForeignKey("crash.crash_id"), nullable=False)
    vehicle_id = Column(Integer, ForeignKey("vehicle.unique_id"))
    person_type = Column(String)
    person_injury = Column(String)
    person_age = Column(Integer)
    person_sex = Column(String)
    safety_equipment = Column(String)
    position_in_vehicle = Column(String)

class Status(Base):
    __tablename__ = "etl_status"

    id = Column(Integer, Identity(start=1), primary_key=True)
    task_id = Column(String)
    execution_date = Column(DateTime)
    last_crash_date = Column(DateTime)
    status = Column(String)
