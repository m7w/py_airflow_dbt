from datetime import datetime

import numpy as np
import pandas as pd

SELECT_CRASHES_COLUMNS = ["crash_id", "crash_date", "borough", "zip_code", "latitude", "longitude"]

RENAME_COLUMNS = {"collision_id": "crash_id"}
TYPE_CRASHES_COLUMNS = {"crash_id": "Int32", "zip_code": "Int32", "latitude": "float", "longitude": "float"}

SELECT_VEHICLES_COLUMNS = [
    "unique_id",
    "crash_id",
    "travel_direction",
    "vehicle_occupants",
    "driver_license_status",
    "pre_crash",
    "point_of_impact",
]
TYPE_VEHICLES_COLUMNS = {
    "unique_id": "Int32",
    "crash_id": "Int32",
    "vehicle_occupants": "Int32",
}

SELECT_PERSONS_COLUMNS = [
    "unique_id",
    "crash_id",
    "vehicle_id",
    "person_type",
    "person_injury",
    "person_age",
    "person_sex",
    "safety_equipment",
    "position_in_vehicle",
]
TYPE_PERSONS_COLUMNS = {
    "unique_id": "Int32",
    "crash_id": "Int32",
    "vehicle_id": "Int32",
    "person_age": "Int32",
}


def transform_data(
    df: pd.DataFrame, select_columns, type_columns, is_crashes=None
) -> pd.DataFrame:
    if is_crashes:
        df["crash_date"] = pd.to_datetime(df["crash_date"]).combine(
                pd.to_datetime(df["crash_time"], format="%H:%M").dt.time, func=datetime.combine
                )

    df = df.rename(columns=RENAME_COLUMNS)
    df = df.loc[:, select_columns]
    df = df.drop_duplicates()
    df = df.replace({np.nan: None})
    df = df.astype(type_columns)

    return df

def transform_crashes_data(df: pd.DataFrame) -> pd.DataFrame:
    return transform_data(df, SELECT_CRASHES_COLUMNS, TYPE_CRASHES_COLUMNS, True)

def transform_vehicles_data(df: pd.DataFrame) -> pd.DataFrame:
    return transform_data(df, SELECT_VEHICLES_COLUMNS, TYPE_VEHICLES_COLUMNS)

def transform_persons_data(df: pd.DataFrame) -> pd.DataFrame:
    return transform_data(df, SELECT_PERSONS_COLUMNS, TYPE_PERSONS_COLUMNS)
