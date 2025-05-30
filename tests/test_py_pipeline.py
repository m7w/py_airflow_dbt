from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from py_etl.db import Base, Crash
from py_etl.load import load_data
from py_etl.transform import transform_crashes_data


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield session
    session.close()

def extract_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["1", "2025-05-28T00:00:00.000", "1:40", "10", "5", "fake"],
            ["2", "2025-05-29T00:00:00.000", "12:00", "-10", "NaN", "fake"],
        ],
        columns=[
            "collision_id",
            "crash_date",
            "crash_time",
            "latitude",
            "longitude",
            "fake",
        ],
    )


def test_py_pipeline(session) -> None:
    data = extract_data()

    data = transform_crashes_data(data)
    assert len(data.columns) == 4

    load_data(data, Crash, session)

    with session.begin():
        count = session.scalars(select(func.count(Crash.crash_id))).first()
        crash = session.scalars(select(Crash).where(Crash.crash_id == 2)).first()

    assert count == 2
    assert crash.crash_date == datetime(2025, 5, 29, 12, 00)
    assert crash.longitude is None

