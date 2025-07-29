####
## Alerting utils script to send alert messages to messaging apps
## Mario Caesar // caesarmario87@gmail.com
####

# -- Importing Libraries
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import requests


# -- Functions
# Function to send alert to messaging apps
def send_alert(creds: dict, alert_type: str, context: dict):
    """
    Send alert message to messaging apps based on alert type and detail.

    Parameters:
        creds (dict): json of messaging apps credentials (token, user id, and url)
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


def send_weather_alert(creds: dict, exec_date, alerts: list):
    """
    Send weather alert summary message to Telegram.

    Parameters:
        creds (dict): JSON object containing messaging credentials.
        exec_date (str): Execution date in 'YYYY-MM-DD' format.
        alerts (list): List of alert records retrieved from the database
    """
    token     = creds["MESSAGING_BOT_TOKEN"]
    chat_id   = creds["MESSAGING_USER_ID"]
    bot_url   = creds["MESSAGING_URL"]
    exec_date = datetime.strptime(exec_date, "%Y-%m-%d")
    date_str  = exec_date.strftime("%Y-%m-%d")

    title     = f"🌦️ *Weather Alerts Summary for {date_str}* 🌦️"

    if not alerts:
        message = f"{title}\n\n✅ No weather alerts detected for today."
    else:
        grouped_alerts = defaultdict(list)
        heat_detected = False

        for alert in alerts:
            loc     = alert['location_id']
            date    = str(alert['date'])
            temp    = alert['maxtemp_c']
            precip  = alert['totalprecip_mm']
            wind    = alert['maxwind_kph']

            is_heat  = temp > 35
            is_storm = precip > 20 or wind > 15

            if is_heat or is_storm:
                conds = []
                if is_heat:
                    conds.append(f"🔥 Heat ({temp:.1f}°C)")
                    heat_detected = True
                if is_storm:
                    conds.append(f"🌪️ Storm (🌧️ {precip:.2f}mm, 💨 {wind:.2f} kph)")

                grouped_alerts[date].append(f"• *{loc}* → {', '.join(conds)}")

        # Build message
        lines = [title, ""]

        if not grouped_alerts:
            lines.append("✅ No weather alerts detected above threshold.")
        else:
            for dt in sorted(grouped_alerts):
                lines.append(f"📅 *{dt}*")
                lines.extend(grouped_alerts[dt])
                lines.append("")

        if not heat_detected:
            lines.append("✅ No cities with 🔥 Extreme Heat today.")

        # Post Script - thresholds
        lines.extend([
            "",
            "_PS:_",
            "📌 *Thresholds:*",
            "• 🔥 Extreme Heat: Max Temp > 35°C",
            "• 🌪️ Storm: Precip > 20mm or Wind > 15 kph"
        ])

        message = "\n".join(lines).strip()

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
        print(f"!! Failed to send alert summary: {e}")