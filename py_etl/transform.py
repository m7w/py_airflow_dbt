from datetime import datetime

import pandas as pd

SELECT_COLUMNS = ["collision_id", "crash_date", "latitude", "longitude"]
RENAME_COLUMNS = {"collision_id": "crash_id"}

def transform_crashes_data(df: pd.DataFrame) -> pd.DataFrame:
    df["crash_date"] = pd.to_datetime(df["crash_date"]).combine(
        pd.to_datetime(df["crash_time"], format="%H:%M").dt.time, func=datetime.combine
    )

    df = df.loc[:, SELECT_COLUMNS]
    df = df.rename(columns=RENAME_COLUMNS)

    return df
