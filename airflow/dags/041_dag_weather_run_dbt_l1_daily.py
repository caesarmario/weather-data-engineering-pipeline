####
## Airflow v3 DAG to run dbt models for l1
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from utils.alerting.alert_utils import send_alert

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_run_dbt_l1"
duration        = "daily"

default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
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
    dbt_pg_creds = Variable.get("dbt_pg_creds", deserialize_json=True)
    return dbt_pg_creds


# -- Function: to sent alert to messaging apps
def alert_failure(context):
    """
    Sends a formatted alert message to the messaging platform.
    """
    creds         = Variable.get("messaging_creds", deserialize_json=True)

    send_alert(creds=creds, alert_type="ERROR", context=context)


# -- Tasks: start, run dbt l1, dbt test, trigger next dag, end
# Dummy Start
task_start = EmptyOperator(
    task_id = "task_start",
    dag     = dag
)

# Task to run dbt l1
run_dbt_l1 = BashOperator(
    task_id             = "run_dbt_l1",
    bash_command        = """
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /dbt && \
        dbt run --profiles-dir . --project-dir . --select l1_weather
    """,
    env                 = get_dbt_env_vars(),
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Run dbt test for l1 layer
test_dbt_l1 = BashOperator(
    task_id             = "test_dbt_l1",
    bash_command        = """
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /dbt && \
        dbt test --select l1_weather.*
    """,
    on_failure_callback = alert_failure,
    env                 = get_dbt_env_vars(),
    dag                 = dag
)

# Trigger next DAG
trigger_process = TriggerDagRunOperator(
    task_id        = "trigger_weather_run_dbt_dwh",
    trigger_dag_id = "042_dag_weather_run_dbt_dwh_daily",

    conf           = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    dag            = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id = "task_end",
    dag     = dag
)

# -- Define execution order
task_start >> run_dbt_l1 >> test_dbt_l1 >> trigger_process >> task_end
