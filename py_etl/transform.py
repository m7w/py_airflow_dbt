from datetime import datetime

import pandas as pd

SELECT_COLUMNS = ["collision_id", "crash_date", "latitude", "longitude"]
RENAME_COLUMNS = {"collision_id": "crash_id"}
TYPE_COLUMNS = {"crash_id": "int32", "latitude": "float", "longitude": "float"}

def transform_crashes_data(df: pd.DataFrame) -> pd.DataFrame:
    df["crash_date"] = pd.to_datetime(df["crash_date"]).combine(
        pd.to_datetime(df["crash_time"], format="%H:%M").dt.time, func=datetime.combine
    )

    df = df.loc[:, SELECT_COLUMNS]
    df = df.rename(columns=RENAME_COLUMNS)
    df = df.astype(TYPE_COLUMNS)

    return df
