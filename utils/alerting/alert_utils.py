####
## Alerting utils script to send alert messages to messaging apps
## Mario Caesar // caesarmario87@gmail.com
####

# -- Importing Libraries
from datetime import datetime, timezone, timedelta
import requests


# -- Functions
# Function to send alert to messaging apps
def send_alert(creds, alert_type: str, context: dict):
    """
    Send alert message to messaging apps based on alert type and detail.

    Parameters:
        creds (str): json of messaging apps credentials (token, user id, and url)
        alert_type (str): Type of alert: "ERROR", "WARNING", "DQ_ISSUE"
        context (dict): Airflow context.
    """

    # Load from Airflow Variable
    token       = creds["MESSAGING_BOT_TOKEN"]
    chat_id     = creds["MESSAGING_USER_ID"]
    bot_url     = creds["MESSAGING_URL"]
    airflow_url = creds["AIRFLOW_URL"]

    # Load from Airflow Context
    ti              = context.get("task_instance")
    dag_id          = context.get("dag").dag_id
    exec_date       = context.get("execution_date") or context.get("logical_date")
    dag_run_id      = ti.run_id
    task_id         = ti.task_id

    timestamp       = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")

    # Local Airflow log URL
    log_url = f"{airflow_url}/dags/{dag_id}/runs/{dag_run_id}/tasks/{task_id}?try_number={ti.try_number}"

    # Set prefix & emoji based on type
    alert_prefix = {
        "ERROR": "🚨 *!! ERROR/JOB FAILURE DETECTED !!* 🚨",
        "WARNING": "⚠️ *!! WARNING !!* ⚠️",
        "DQ_ISSUE": "🧪 *!! DATA QUALITY ISSUE !!* 🧪",
    }.get(alert_type.upper(), "🔔 *!! ALERT !!* 🔔")

    # Final formatted message
    message = f"""{alert_prefix}
🗓️ `{timestamp} WIB`

📌 *DAG ID*         : `{dag_id}`
🔧 *Task ID*        : `{task_id}`
🌀 *Run ID*          : `{dag_run_id}`
⏱️ *Exec Date*    : `{exec_date}`

❌ *Exception*:
`{str(context.get("exception")) or getattr(ti, 'exception', None) or "No exception found."}`

🔗 Please check the complete logs [here]({log_url}).

👤 cc: [@caesarmario87](tg://user?id={chat_id})
"""

    url = f"{bot_url}{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"!! Failed to send alert: {e}")
