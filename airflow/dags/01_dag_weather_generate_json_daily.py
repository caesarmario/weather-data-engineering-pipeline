####
## Airflow v3 DAG for daily weather JSON generation
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from utils.alerting.alert_utils import send_alert

import logging
import subprocess

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_generate_json"
duration        = "daily"
minio_creds     = Variable.get("minio_creds")

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"01_dag_{job_name}_{duration}",
    default_args      = default_args,
    schedule          = "0 10 * * *",
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["data_generator", "weather_data_engineering", f"{duration}"]
)

# -- Function: prepare env vars and push to XCom
def set_env_vars(**kwargs):
    """
    Push EMPTY_RATE and ERROR_RATE into XCom for downstream tasks.
    """
    # Draw rates from a triangular distribution with mode=5
    empty_rate = 0 ### [WIP] int(random.triangular(0, 100, 5))
    error_rate = 0 ### [WIP] int(random.triangular(0, 100, 5))

    # Assemble environment dict
    env = {
        "EMPTY_RATE"      : str(empty_rate),
        "ERROR_RATE"      : str(error_rate)
    }

    # Push to XCom for downstream tasks
    kwargs['ti'].xcom_push(key="env", value=env)

    logging.info(f"Generated env vars with EMPTY_RATE={empty_rate} and ERROR_RATE={error_rate}.")
    return env


# -- Function: run the external data generator script
def run_generator(**kwargs):
    """
    Execute sample_data_generator.py with XCom rates and pass exec_date.
    """
    ti  = kwargs['ti']
    env = ti.xcom_pull(task_ids=set_env_task.task_id, key="env")

    # Extract values & get creds.
    empty_rate = env["EMPTY_RATE"]
    error_rate = env["ERROR_RATE"]
    dag_run = kwargs.get("dag_run")
    
    # Determine exec_date: prefer dag_run.conf, else use next day's ds
    if dag_run and dag_run.conf and "exec_date" in dag_run.conf:
        exec_date = dag_run.conf["exec_date"]
    else:
        exec_date = kwargs["ds"]
        exec_date  = (datetime.strptime(exec_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Execution date being passed to script: {exec_date}")

    # Build command
    cmd = [
        "python", "scripts/sample_data_generator.py",
        "--empty_rate", empty_rate,
        "--error_rate", error_rate,
        "--credentials", minio_creds,
        "--exec_date", exec_date
    ]

    # Execute script
    subprocess.run(cmd, check=True)


# -- Function: to sent alert to messaging apps
def alert_failure(context):
    """
    Sends a formatted alert message to the messaging platform.
    """
    creds         = Variable.get("messaging_creds", deserialize_json=True)

    send_alert(creds=creds, alert_type="ERROR", context=context)


# -- Tasks: start, set env, run generator, trigger next DAG, end
# Dummy Start
task_start = EmptyOperator(
    task_id         = "task_start",
    dag             = dag
)

# Task to set environment variables
set_env_task = PythonOperator(
    task_id             = "set_env_vars",
    python_callable     = set_env_vars,
    on_failure_callback = alert_failure,
    dag                 = dag,
)

# Task to run data generator
run_generator_task = PythonOperator(
    task_id             = f"run_{job_name}",
    python_callable     = run_generator,
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Trigger next DAG
trigger_process = TriggerDagRunOperator(
    task_id         = "trigger_parquet_staging_dag",
    trigger_dag_id  = "02_dag_weather_json_to_parquet_daily",

    conf            = {
                        "exec_date": "{{ macros.ds_add(ds, 1) }}"
                    },
    dag             = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id         = "task_end",
    dag             = dag
)

# -- Define execution order
task_start >> set_env_task >> run_generator_task >> trigger_process >> task_end