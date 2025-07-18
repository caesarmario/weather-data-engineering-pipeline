####
## Airflow v3 DAG to load parquet into staging db
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

import subprocess

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_load_parquet"
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
    dag_id            = f"03_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["etl", "weather_data_engineering", f"{duration}"]
)

# -- Function: run the load parquet script
def run_loader(table, exec_date, **kwargs):
    """
    Execute sample_data_generator.py with XCom rates and pass exec_date.
    """
    # Credentials
    minio_creds     = Variable.get("minio_creds")
    postgres_creds  = Variable.get("postgresql_creds")

    # Build command
    cmd = [
        "python", "scripts/loader_parquet_to_db.py",
        "--table", table,
        "--minio_creds", minio_creds,
        "--postgres_creds", postgres_creds,
        "--exec_date", exec_date
    ]

    # Execute script
    subprocess.run(cmd, check=True)

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

# Task to run data generator
with TaskGroup("load_data", tooltip="Parquet→Staging db", dag=dag) as load_group:
    load_current = PythonOperator(
        task_id="load_current",
        python_callable=run_loader,

        op_kwargs={
            "table": "current",
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )

    load_location = PythonOperator(
        task_id="load_location",
        python_callable=run_loader,

        op_kwargs={
            "table": "location",
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )

    load_forecast = PythonOperator(
        task_id="load_forecast",
        python_callable=run_loader,

        op_kwargs={
            "table": "forecast",
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )

# Run dbt test for l0 layer
test_dbt_l0 = BashOperator(
    task_id="test_dbt_l0",
    bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /dbt && \
        dbt test --select source:l0_weather.*
    """,
    env=get_dbt_env_vars(),
    dag=dag
)

# Trigger next DAG
trigger_process = TriggerDagRunOperator(
    task_id         = "trigger_weather_run_dbt_staging",
    trigger_dag_id  = "04_dag_weather_run_dbt_staging_daily",

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
task_start >> load_group >> test_dbt_l0 >> trigger_process >> task_end