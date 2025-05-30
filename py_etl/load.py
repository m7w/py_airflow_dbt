import pandas as pd
from db import Base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session


def load_data(df: pd.DataFrame, cls: type[Base], session: Session) -> None:
    primary_key = inspect(cls).primary_key[0].name

    stmt = insert(cls).values(df.to_dict("records"))
    stmt = stmt.on_conflict_do_nothing(index_elements=[primary_key])

    with session.begin():
        session.execute(stmt)
