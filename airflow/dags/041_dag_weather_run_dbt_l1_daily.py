####
## Airflow v3 DAG to run dbt models for l1
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

import subprocess
import json

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_run_dbt_l1"
duration        = "daily"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 0,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

dag = DAG(
    dag_id            = f"041_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["dbt", "l1", "weather_data_engineering", f"{duration}"]
)


# -- Function: to retrieve dbt creds
def get_dbt_env_vars():
    """
    Retrieve DBT credentials stored as Airflow Variable and return them as environment variables
    """
    dbt_pg_creds = json.loads(Variable.get("dbt_pg_creds"))
    return dbt_pg_creds


# -- Function: to sent alert to messaging apps
def alert_failure(context):
    """
    Sends a formatted alert message to the messaging platform.
    """
    creds         = json.loads(Variable.get("messaging_creds"))

    send_alert(creds=creds, alert_type="ERROR", context=context)


# -- Function: run dbt with error handling
def run_dbt_command(command: str):
    """
    Function to run dbt command using subprocess.
    """
    env = get_dbt_env_vars()
    cmd = [
        "bash", "-c",
        f"export PATH=$PATH:/home/airflow/.local/bin && cd /dbt && {command}"
    ]
    subprocess.run(cmd, check=True, env=env)


# -- Tasks: start, run dbt l1, dbt test, trigger next dag, end
# Dummy Start
task_start = EmptyOperator(
    task_id = "task_start",
    dag     = dag
)

# Task to run dbt l1
run_dbt_l1 = PythonOperator(
    task_id             = "run_dbt_l1",
    python_callable     = run_dbt_command,
    op_kwargs           = {"command": "dbt run --select l1_weather"},
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Run dbt test for l1 layer
test_dbt_l1 = PythonOperator(
    task_id             = "test_dbt_l1",
    python_callable     = run_dbt_command,
    op_kwargs           = {"command": "dbt test --select l1_weather.*"},
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id = "task_end",
    dag     = dag
)

# -- Define execution order
task_start >> run_dbt_l1 >> test_dbt_l1 >> task_end
