import os
from datetime import datetime
from airflow.providers.smtp.notifications.smtp import send_smtp_notification


error_emails = [
    e.strip()
    for e in os.environ.get("ERROR_EMAIL_ADDRESS", "").split(";")
    if e.strip()
]

email_notification_callback = send_smtp_notification(
    to="richard.bramley5@nhs.net",
    from_email="richard.a.bramley@uhl-tr.nhs.uk",
    subject="Airflow task failed",
)


default_dag_args = {
    "owner": "airflow",
    "on_failure_callback": email_notification_callback,
    "start_date": datetime(2020, 1, 1),
    "retries": 0,
}
