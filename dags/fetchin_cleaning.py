from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import subprocess
import os

# Path to the folder where cleaned CSVs are stored
GROWING_CSV_DIR = "/opt/airflow/data"

def fetch_weather():
    subprocess.run(["python", "dags/fetch_weather.py"], check=True)

def clean_data():
    subprocess.run(["python", "dags/cleaning_data.py"], check=True)

with DAG(
    dag_id="weather_to_snowflake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    # Stage CSVs to Snowflake
    stage_location_dim = SQLExecuteQueryOperator(
        task_id="stage_location_dim",
        conn_id="weather_pollution",
        sql=f"PUT file://{os.path.join(GROWING_CSV_DIR, 'location_dim.csv')} @%LOCATION_DIM OVERWRITE = TRUE;"
    )

    stage_date_dim = SQLExecuteQueryOperator(
        task_id="stage_date_dim",
        conn_id="weather_pollution",
        sql=f"PUT file://{os.path.join(GROWING_CSV_DIR, 'date_dim.csv')} @%DATE_DIM OVERWRITE = TRUE;"
    )

    stage_weather_fact = SQLExecuteQueryOperator(
        task_id="stage_weather_fact",
        conn_id="weather_pollution",
        sql=f"PUT file://{os.path.join(GROWING_CSV_DIR, 'weather_fact.csv')} @%WEATHER_FACT OVERWRITE = TRUE;"
    )

    stage_pollutants_fact = SQLExecuteQueryOperator(
        task_id="stage_pollutants_fact",
        conn_id="weather_pollution",
        sql=f"PUT file://{os.path.join(GROWING_CSV_DIR, 'pollutants_fact.csv')} @%POLLUTANTS_FACT OVERWRITE = TRUE;"
    )

    stage_weather_dim = SQLExecuteQueryOperator(
        task_id="stage_weather_dim",
        conn_id="weather_pollution",
        sql=f"PUT file://{os.path.join(GROWING_CSV_DIR, 'weather_dim.csv')} @%WEATHER_DIM OVERWRITE = TRUE;"
    )

    # Truncate tables in Snowflake
    truncate_location_dim = SQLExecuteQueryOperator(
        task_id="truncate_location_dim",
        conn_id="weather_pollution",
        sql="TRUNCATE TABLE POLLUTION_OVERTIME.LOCATION_DIM;"
    )

    truncate_date_dim = SQLExecuteQueryOperator(
        task_id="truncate_date_dim",
        conn_id="weather_pollution",
        sql="TRUNCATE TABLE POLLUTION_OVERTIME.DATE_DIM;"
    )

    truncate_weather_fact = SQLExecuteQueryOperator(
        task_id="truncate_weather_fact",
        conn_id="weather_pollution",
        sql="TRUNCATE TABLE POLLUTION_OVERTIME.WEATHER_FACT;"
    )

    truncate_pollutants_fact = SQLExecuteQueryOperator(
        task_id="truncate_pollutants_fact",
        conn_id="weather_pollution",
        sql="TRUNCATE TABLE POLLUTION_OVERTIME.POLLUTANTS_FACT;"
    )

    truncate_weather_dim = SQLExecuteQueryOperator(
        task_id="truncate_weather_dim",
        conn_id="weather_pollution",
        sql="TRUNCATE TABLE POLLUTION_OVERTIME.WEATHER_DIM;"
    )

    # Load CSVs into Snowflake
    load_location_dim = SQLExecuteQueryOperator(
        task_id="load_location_dim",
        conn_id="weather_pollution",
        sql=f"""
        COPY INTO POLLUTION_OVERTIME.LOCATION_DIM
        FROM '@%LOCATION_DIM'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE = TRUE;
        """
    )

    load_date_dim = SQLExecuteQueryOperator(
        task_id="load_date_dim",
        conn_id="weather_pollution",
        sql=f"""
        COPY INTO POLLUTION_OVERTIME.DATE_DIM
        FROM '@%DATE_DIM'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE = TRUE;
        """
    )

    load_weather_fact = SQLExecuteQueryOperator(
        task_id="load_weather_fact",
        conn_id="weather_pollution",
        sql=f"""
        COPY INTO POLLUTION_OVERTIME.WEATHER_FACT
        FROM '@%WEATHER_FACT'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE = TRUE;
        """
    )

    load_pollutants_fact = SQLExecuteQueryOperator(
        task_id="load_pollutants_fact",
        conn_id="weather_pollution",
        sql=f"""
        COPY INTO POLLUTION_OVERTIME.POLLUTANTS_FACT
        FROM '@%POLLUTANTS_FACT'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE = TRUE;
        """
    )

    load_weather_dim = SQLExecuteQueryOperator(
        task_id="load_weather_dim",
        conn_id="weather_pollution",
        sql=f"""
        COPY INTO POLLUTION_OVERTIME.WEATHER_DIM
        FROM '@%WEATHER_DIM'
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE = TRUE;
        """
    )

    # Define DAG chain
    chain(
        fetch,
        clean,
        [stage_location_dim, stage_date_dim, stage_weather_fact, stage_pollutants_fact, stage_weather_dim],
        [truncate_location_dim, truncate_date_dim, truncate_weather_fact, truncate_pollutants_fact, truncate_weather_dim],
        [load_location_dim, load_date_dim, load_weather_fact, load_pollutants_fact, load_weather_dim],
    )
