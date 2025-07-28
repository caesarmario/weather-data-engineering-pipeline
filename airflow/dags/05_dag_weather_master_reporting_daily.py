####
## Airflow v3 DAG weather reporting master DAG
## Mario Caesar // caesarmario87@gmail.com
####

# -- Imports: Airflow core, operators, and Python stdlib
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_master_reporting"
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
    dag_id            = f"05_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["master", "reporting", "weather_data_engineering", f"{duration}"]
)


# -- Tasks: start, run dbt dwh, dbt test, trigger next dag, end
# Dummy Start
task_start = EmptyOperator(
    task_id         = "task_start",
    dag             = dag
)

# Trigger DAG - High Priority (P0)
with TaskGroup("reporting_p0_tasks", dag=dag) as p0_tasks:
    trigger_dag_055 = TriggerDagRunOperator(
        task_id             = "trigger_dag_055_weather_alert_notification_daily",
        trigger_dag_id      = "055_weather_alert_notification_daily",
        conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
        wait_for_completion = False,
        dag                 = dag
    )


# Trigger DAG - Medium Priority (P1)
with TaskGroup("reporting_p1_tasks", dag=dag) as p1_tasks:

    trigger_dag_051 = TriggerDagRunOperator(
        task_id             = "trigger_dag_051_xxx",
        trigger_dag_id      = "051_xxx_daily",
        conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
        wait_for_completion = False,
        dag                 = dag
    )

    trigger_dag_052 = TriggerDagRunOperator(
        task_id             = "trigger_dag_052_xxx",
        trigger_dag_id      = "052_xxx",
        conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
        wait_for_completion = False,
        dag                 = dag
    )

    trigger_dag_053 = TriggerDagRunOperator(
        task_id             = "trigger_dag_053_xxx",
        trigger_dag_id      = "053_xxx",
        conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
        wait_for_completion = False,
        dag                 = dag
    )


# Trigger DAG - Low Priority (P2)
with TaskGroup("reporting_p2_tasks", dag=dag) as p2_tasks:
    trigger_dag_054 = TriggerDagRunOperator(
        task_id             = "trigger_dag_054_xxx",
        trigger_dag_id      = "054_xxx",
        conf                = {
                        "exec_date": "{{ dag_run.conf.get('exec_date', macros.ds_add(ds, 1)) }}"
                    },
        wait_for_completion = False,
        dag                 = dag
    )


# Dummy End
task_end = EmptyOperator(
    task_id         = "task_end",
    dag             = dag
)

# -- Define execution order
task_start >> p0_tasks >> p1_tasks >> p2_tasks >> task_end