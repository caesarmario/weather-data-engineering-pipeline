####
## Airflow v3 DAG weather alert notification daily
## Mario Caesar // caesarmario87@gmail.com
####

from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from utils.alerting.alert_utils import send_alert, send_weather_alert
from utils.reporting_utils import get_weather_alerts_data

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
def get_alert_data(**kwargs):
    """
    Extract weather alert data from PostgreSQL and push to XCom.

    Parameters:
        kwargs (dict): Airflow context dictionary. Uses:
            - dag_run.conf['exec_date'] (optional): Override execution date.
            - data_interval_end: Default execution date if not passed via conf.
            - ti: TaskInstance for XCom push.
    """
    postgres_creds = json.loads(Variable.get("postgresql_creds"))
    exec_date      = kwargs['dag_run'].conf.get('exec_date') \
                    if kwargs.get('dag_run') and kwargs['dag_run'].conf.get('exec_date') \
                    else kwargs['data_interval_end'].strftime("%Y-%m-%d")
    
    alerts         = get_weather_alerts_data(exec_date, postgres_creds)
    print(f">> Returned weather alerts data for {exec_date}: {alerts[:2]} ")

    kwargs['ti'].xcom_push(key='alerts', value=alerts)
    kwargs['ti'].xcom_push(key='exec_date', value=exec_date)


# -- Function: to send weather alert message
def send_alerts(**kwargs):
    """
    Send weather alert message to messaging app using Telegram bot.

    Parameters:
        kwargs (dict): Airflow context dictionary. Uses:
            - ti: TaskInstance for XCom pull to retrieve 'alerts' and 'exec_date'.
    """
    messaging_creds = json.loads(Variable.get("messaging_creds"))

    alerts          = kwargs['ti'].xcom_pull(key='alerts', task_ids='extract_alert_data')
    exec_date       = kwargs['ti'].xcom_pull(key='exec_date', task_ids='extract_alert_data')
    send_weather_alert(creds=messaging_creds, exec_date=exec_date, alerts=alerts)


# -- Function: to sent failure alert to messaging apps
def alert_failure(context):
    """
    Sends a formatted alert message to the messaging platform.
    """
    creds         = json.loads(Variable.get("messaging_creds"))

    send_alert(creds=creds, alert_type="ERROR", context=context)


# -- Tasks: start, extract alert data, send alert data, end
# Dummy Start
task_start = EmptyOperator(
    task_id         = "task_start",
    dag             = dag
)

# Extract weather alert data
extract_alert_data = PythonOperator(
    task_id             = "extract_alert_data",
    python_callable     = get_alert_data,
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Send weather alert data
send_alert_data = PythonOperator(
    task_id             = "send_alert_data",
    python_callable     = send_alerts,
    on_failure_callback = alert_failure,
    dag                 = dag
)

# Dummy End
task_end = EmptyOperator(
    task_id = "task_end",
    dag     = dag
)

task_start >> extract_alert_data >> send_alert_data >> task_end