####
###### WIP ALERT
## Alerting utils script to send alert messages to messaging apps
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import requests
from datetime import datetime

# Function to send alert to messaging apps
def send_alert(creds, alert_type: str, context: dict):
    """
    Send alert message to messaging apps based on alert type and detail.

    Parameters:
        creds (str): json of messaging apps credentials (token, user id, and url)
        alert_type (str): Type of alert: "ERROR", "WARNING", "DQ_ISSUE"
        alert_detail (str): Detailed message to include in the alert
    """

    # Load from Airflow Variable
    token     = creds["MESSAGING_BOT_TOKEN"]
    chat_id   = creds["MESSAGING_USER_ID"]
    bot_url   = creds["MESSAGING_URL"]

    # Load from Airflow Context
    ti              = context.get("task_instance")
    dag_id          = context.get("dag").dag_id
    dag_run_id      = ti.run_id
    task_id         = ti.task_id

    timestamp       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(context) #### DELETE LATER

    # Local Airflow log URL
    log_url = f"http://localhost:8080/dags/{dag_id}/grid?dag_run_id={dag_run_id}&task_id={task_id}"


    # Set prefix & emoji based on type
    alert_prefix = {
        "ERROR": "🚨 *ERROR DETECTED!*",
        "WARNING": "⚠️ *WARNING!*",
        "DQ_ISSUE": "🧪 *DATA QUALITY ISSUE!*",
    }.get(alert_type.upper(), "🔔 *ALERT!*")

    # Final formatted message
    message = f"""!! TESTING !!
{alert_prefix} - `{timestamp}`

🗂️ *DAG:* `{dag_id}`
📌 *Task:* `{task_id}` | 🌀 *Run ID:* `{ti.run_id}`
🕒 *Execution:* `{context.get("execution_date")}`

❌ *Error:* \n`{str(context.get("exception"))}`

🔗 *Logs:* [View logs]({log_url})
Kindly check the error [@caesarmario87](tg://user?id={chat_id})!
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