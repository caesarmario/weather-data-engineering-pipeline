####
## Airflow v3 DAG: process raw json -> parquet
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

# Import process scripts
from scripts.process.process_current import run as run_current
from scripts.process.process_location import run as run_location
from scripts.process.process_forecast import run as run_forecast

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_json_to_parquet"
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
    dag_id            = f"02_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["etl", "weather_data_engineering", f"{duration}"]
)

# -- Tasks: start, extract transform, parquet staging, end
# Dummy Start
task_start = EmptyOperator(
    task_id="task_start",
    dag=dag
)

# Extract & transform JSON to Parquet files
with TaskGroup("extract_transform", tooltip="JSON→Parquet", dag=dag) as extract_group:
    process_current = PythonOperator(
        task_id="process_current",
        python_callable=run_current,

        op_kwargs={
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )

    process_location = PythonOperator(
        task_id="process_location",
        python_callable=run_location,

        op_kwargs={
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )

    process_forecast = PythonOperator(
        task_id="process_forecast",
        python_callable=run_forecast,

        op_kwargs={
            "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
        },
        dag=dag,
    )


# Trigger next DAG
trigger_process = TriggerDagRunOperator(
    task_id         = "trigger_parquet_staging_dag",
    trigger_dag_id  = "03_dag_weather_load_parquet",

    conf            = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    dag             = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# -- Define execution order
task_start >> extract_group >> trigger_process >> task_end