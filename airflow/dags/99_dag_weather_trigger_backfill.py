####
## Airflow v3 DAG for backfill purposes only (trigger dag from 01 and rest)
## Mario Caesar // caesarmario87@gmail.com
####

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

# Variables
job_name        = "weather_trigger_backfill"

# Default args
default_args = {
    "owner"           : "caesarmario87@gmail.com",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# Define DAGs
with DAG(
    dag_id          = f"99_dag_{job_name}",
    default_args    = default_args,
    schedule        = None,
    catchup         = False,
    tags            = ["backfill", "weather_data_engineering"],
) as dag:

    # Dummy Start
    task_start = EmptyOperator(
        task_id         = "task_start",
        dag             = dag
    )


    # Task to backfill
    @task
    def build_date_list(**kwargs) -> list[dict]:
        """
        Read start_date and end_date from dag_run.conf,
        defaulting to today if not provided, and generate list of conf dicts.
        """

        dr      = kwargs.get('dag_run')
        ts      = kwargs.get('ts')
        conf    = dr.conf or {}
        
        # Determine start and end dates
        sd_val  = conf.get('start_date') or ts.split('T')[0]
        ed_val  = conf.get('end_date') or sd_val

        sd      = datetime.fromisoformat(sd_val)
        ed      = datetime.fromisoformat(ed_val)
        days    = (ed - sd).days + 1

        return [
            {"exec_date": (sd + timedelta(days=i)).date().isoformat()}
            for i in range(days)
        ]

    # Generate list of conf dicts based on input
    date_confs = build_date_list()

    # Trigger child DAG per date in date_confs
    trigger_backfill = TriggerDagRunOperator.partial(
        task_id="trigger_backfill_01_dag",
        trigger_dag_id="01_dag_weather_generate_json_daily",
    ).expand(
        conf=date_confs
    )


    # Dummy End
    task_end = EmptyOperator(
        task_id         = "task_end",
        dag             = dag
    )

    # Define task dependencies
    task_start >> date_confs >> trigger_backfill >> task_end
