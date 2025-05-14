####
## Airflow v3 DAG to run sample weather data generator script
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging
import json
import random
import subprocess

# Variables
job_name        = "weather_data_generator"
minio_creds     = Variable.get("minio_creds")

# Default arguments for this DAG
default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 13),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

# Define DAGs
dag = DAG(
    dag_id            = f"99_dag_{job_name}_daily",
    default_args      = default_args,
    schedule          = "0 10 * * *",
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["data_generator", "weather_data_engineering", "daily_ingestion"]
)

# Env. Variables
def set_env_vars(**kwargs):
    """
    Generate random empty_rate, error_rate, and prepare JSON credentials string.
    """
    # Draw rates from a triangular distribution with mode=25
    empty_rate = int(random.triangular(0, 100, 25))
    error_rate = int(random.triangular(0, 100, 25))

    # Prepare credentials JSON string
    creds_str = minio_creds

    # Assemble environment dict
    env = {
        "EMPTY_RATE"      : str(empty_rate),
        "ERROR_RATE"      : str(error_rate),
        "CREDENTIALS_JSON": creds_str
    }

    # Push to XCom for downstream tasks
    ti = kwargs['ti']
    ti.xcom_push(key="env", value=env)
    logging.info(f"Generated env vars: {env}")
    return env

# Python functions
def run_generator(**kwargs):
    """
    Python function to run the sample_data_generator.py script with rates and credentials from XCom.
    """
    ti  = kwargs['ti']
    env = ti.xcom_pull(task_ids=set_env_task.task_id, key="env")

    # Extract values
    empty_rate = env["EMPTY_RATE"]
    error_rate = env["ERROR_RATE"]
    creds_str  = env["CREDENTIALS_JSON"]

    # Build command
    cmd = [
        "python", "sample_data_generator.py",
        "--empty_rate", empty_rate,
        "--error_rate", error_rate,
        "--credentials", creds_str
    ]

    # Execute script
    subprocess.run(cmd, check=True)

# Dummy End
task_start = EmptyOperator(
    task_id='task_start',
    dag=dag
)

# Task to set environment variables
set_env_task = PythonOperator(
    task_id         = f"set_env_vars_{job_name}",
    python_callable = set_env_vars,
    dag             = dag,
)

# Task to run data generator
run_generator_task = PythonOperator(
    task_id         = "run_sample_data_generator",
    python_callable = run_generator,
    dag             = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id='task_end',
    dag=dag
)

# Define task dependencies
task_start >> set_env_task >> run_generator_task >> task_end