####
## Airflow v3 DAG weather alert notification daily [WIP 20250728]
## Mario Caesar // caesarmario87@gmail.com
####

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from utils.alerting.alert_utils import send_alert, send_weather_alert

import subprocess
import json

# -- DAG-level settings: job name, schedule, and credentials
job_name        = "weather_alert_notification"
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
    dag_id            = f"055_dag_{job_name}_{duration}",
    default_args      = default_args,
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["reporting", "weather_data_engineering", f"{duration}"]
)

# -- Function: run alert query script
def run_script(table, exec_date, **kwargs):
    """
    Execute xxx.py
    """
    # Credentials
    postgres_creds  = Variable.get("postgresql_creds")

    # Build command
    cmd = [
        "python", "scripts/alert_notification_to_chat.py",
        "--postgres_creds", postgres_creds,
        "--exec_date", exec_date
    ]

    # Execute script
    subprocess.run(cmd, check=True)


# -- Function: to sent alert to messaging apps
def alert_failure(context):
    """
    Sends a formatted alert message to the messaging platform.
    """
    creds         = json.loads(Variable.get("messaging_creds"))

    send_alert(creds=creds, alert_type="ERROR", context=context)