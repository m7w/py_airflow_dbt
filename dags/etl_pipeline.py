from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from py_etl.db import Crash, Person, Status, Vehicle, engine
from py_etl.extract import extract_data
from py_etl.load import load_data
from py_etl.transform import (
    transform_crashes_data,
    transform_persons_data,
    transform_vehicles_data,
)

CRASH_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
VEHICLE_URL = "https://data.cityofnewyork.us/resource/bm4k-52h4.json"
PERSON_URL = "https://data.cityofnewyork.us/resource/f55k-p6yu.json"

DATE_START_FROM = datetime(2025, 1, 1)


def log(context, status):
    task_id = context["task_instance"].task_id
    execution_date = context["logical_date"]
    last_crash_date = context["ti"].xcom_pull(
        task_ids="person_task", key="last_crash_date"
    )
    status = Status(
        task_id=task_id,
        execution_date=execution_date,
        last_crash_date=last_crash_date,
        status=status,
    )
    session = Session(engine)
    session.add(status)
    session.commit()


def log_success(context):
    log(context, "success")


def log_failure(context):
    log(context, "failed")


def get_last_crash_date(**context):
    session = Session(engine)
    last_crash_date = session.scalars(
        select(func.max(Status.last_crash_date)).where(
            and_(Status.task_id == "person_task", Status.status == "success")
        )
    ).first()
    if last_crash_date is not None:
        context["ti"].xcom_push(key="last_crash_date", value=last_crash_date)
    else:
        context["ti"].xcom_push(key="last_crash_date", value=DATE_START_FROM)


def etl_crash_pipeline(**context):
    last_crash_date = context["ti"].xcom_pull(
        task_ids="get_last_crash_date_task", key="last_crash_date"
    )
    df: pd.DataFrame = extract_data(CRASH_URL, last_crash_date)
    df = transform_crashes_data(df)
    load_data(df, Crash, Session(engine))


def etl_vehicle_pipeline(**context):
    last_crash_date = context["ti"].xcom_pull(
        task_ids="get_last_crash_date_task", key="last_crash_date"
    )
    df: pd.DataFrame = extract_data(VEHICLE_URL, last_crash_date)
    df = transform_vehicles_data(df)
    load_data(df, Vehicle, Session(engine))


def etl_person_pipeline(**context):
    last_crash_date = context["ti"].xcom_pull(
        task_ids="get_last_crash_date_task", key="last_crash_date"
    )
    df: pd.DataFrame = extract_data(PERSON_URL, last_crash_date)
    if df is not None:
        last_crash_date = df["crash_date"].max()
        context["ti"].xcom_push(key="last_crash_date", value=last_crash_date)
    df = transform_persons_data(df)
    load_data(df, Person, Session(engine))


with DAG(
    "etl_pipeline",
    default_args={
        "retry": 1,
        "retry_delay": timedelta(minutes=5),
        "on_success_callback": log_success,
        "on_failure_callback": log_failure,
    },
    description="NY Crashes ETL pipleline",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 7),
) as dag:
    date_task = PythonOperator(
        task_id="get_last_crash_date_task", python_callable=get_last_crash_date
    )
    crash_task = PythonOperator(
        task_id="crash_task", python_callable=etl_crash_pipeline
    )
    vehicle_task = PythonOperator(
        task_id="vehicle_task", python_callable=etl_vehicle_pipeline
    )
    person_task = PythonOperator(
        task_id="person_task", python_callable=etl_person_pipeline
    )

    date_task >> crash_task >> vehicle_task >> person_task
