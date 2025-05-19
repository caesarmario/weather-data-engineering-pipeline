####
## Airflow v3 DAG to process extracted parquet to staging db
## Mario Caesar // caesarmario87@gmail.com
####

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

# Import process scripts
from scripts.raw.process_current import run as run_current
from scripts.raw.process_location import run as run_location
from scripts.raw.process_forecast import run as run_forecast

# Variables
job_name        = "weather_parquet_staging"
duration        = "daily"
minio_creds     = Variable.get("minio_creds")

# Default arguments for this DAG
default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

# Define DAGs
dag = DAG(
    dag_id            = f"02_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["etl", "weather_data_engineering", f"{duration}"]
)


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

# # Load into staging tables [WIP]
# with TaskGroup("load_staging", tooltip="Parquet→Postgres via dbt", dag=dag) as load_group:
#     dbt_run = PythonOperator(
#         task_id="dbt_run_staging",
#         python_callable=lambda: subprocess.run([
#             "dbt", "run", "--models", "staging",
#             "--profiles-dir", ".", "--target", "prod"
#         ], cwd="/opt/project/dbt", check=True),
#         dag=dag,
#     )

# Dummy End
task_end = EmptyOperator(
    task_id="task_end",
    dag=dag
)

# Define task dependencies
# task_start >> extract_group >> load_group >> task_end
task_start >> extract_group >> task_end