from datetime import datetime

import pandas as pd
import requests


def extract_data(url: str, from_date: datetime, limit: int = 100000) -> pd.DataFrame:
    date, time = from_date.strftime("%Y-%m-%dT%H:%M:%S").split("T")
    query = f"$where=crash_date>='{date}' AND crash_time>='{time}'&$limit={limit}&$order=crash_date,crash_time"
    response = requests.get(f"{url}?{query}")
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
    else:
        raise Exception(f"Error extracting data. Response status code: {response.status_code}")
    return df
