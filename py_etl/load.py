import pandas as pd
from db import Base, engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase


def load_data(df: pd.DataFrame, cls: type[Base]) -> None:
    primary_key = inspect(cls).primary_key[0].name

    stmt = insert(cls).values(df.to_dict("records"))
    stmt = stmt.on_conflict_do_nothing(index_elements=[primary_key])

    with engine.connect() as conn, conn.begin():
        conn.execute(stmt)
