####
## Airflow v3 DAG to run weather dbt daily (master)
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_master_run_dbt"
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
    dag_id            = f"04_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["master", "dbt", "weather_data_engineering", f"{duration}"]
)


# -- Tasks: start, run dbt dwh, dbt test, trigger next dag, end
# Dummy Start
task_start = EmptyOperator(
    task_id         = "task_start",
    dag             = dag
)

# Trigger next DAG
trigger_dbt_l1 = TriggerDagRunOperator(
    task_id             = "trigger_weather_run_dbt_l1",
    trigger_dag_id      = "041_dag_weather_run_dbt_l1_daily",

    conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    wait_for_completion = True,
    dag                 = dag
)

# Trigger next DAG
trigger_dbt_dwh = TriggerDagRunOperator(
    task_id             = "trigger_weather_run_dbt_dwh",
    trigger_dag_id      = "042_dag_weather_run_dbt_dwh_daily",

    conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    wait_for_completion = True,
    dag                 = dag
)

# Trigger next DAG
trigger_master_reporting = TriggerDagRunOperator(
    task_id        = "trigger_weather_master_reporting",
    trigger_dag_id = "05_dag_weather_master_reporting_daily.py",

    conf           = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
    dag            = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id         = "task_end",
    dag             = dag
)

# -- Define execution order
task_start >> trigger_dbt_l1 >> trigger_dbt_dwh >> trigger_master_reporting >> task_end