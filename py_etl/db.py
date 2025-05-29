import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv()

DB_URL = (
    "postgresql+psycopg2://{DB_USER}:{DB_USER_PASSWORD}@localhost/{DB_NAME}".format(
        **os.environ
    )
)

engine = create_engine(DB_URL, echo=True)


class Base(DeclarativeBase):
    pass


class Crash(Base):
    __tablename__ = "crash"

    crash_id: Mapped[int] = mapped_column(primary_key=True)
    crash_date: Mapped[datetime]
    latitude: Mapped[float]
    longitude: Mapped[float]
