####
## Airflow v3 DAG to run dbt models for dwh
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_run_dbt_dwh"
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
    dag_id            = f"042_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["etl", "weather_data_engineering", f"{duration}"]
)

# -- Function: to retrieve dbt creds
def get_dbt_env_vars():
    """
    Retrieve DBT credentials stored as Airflow Variable and return them as environment variables
    """
    dbt_pg_creds = Variable.get("dbt_pg_creds", deserialize_json=True)
    return dbt_pg_creds

# -- Tasks: start, run loader, end
# Dummy Start
task_start = EmptyOperator(
    task_id         = "task_start",
    dag             = dag
)

# Task to run dbt l1
run_dbt_dwh = BashOperator(
    task_id="run_dbt_dwh",
    bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /dbt && \
        dbt run --profiles-dir . --project-dir . --select dwh
    """,
    env=get_dbt_env_vars(),
    dag=dag
)

# Run dbt test for l1 layer
test_dbt_dwh = BashOperator(
    task_id="test_dbt_dwh",
    bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /dbt && \
        dbt test --select dwh.*
    """,
    env=get_dbt_env_vars(),
    dag=dag
)

# Trigger next DAG
trigger_process = TriggerDagRunOperator(
    task_id         = "xxx",
    trigger_dag_id  = "xxx",

    conf            = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    dag             = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id         = "task_end",
    dag             = dag
)

# -- Define execution order
task_start >> run_dbt_dwh >> test_dbt_dwh >> trigger_process >> task_end