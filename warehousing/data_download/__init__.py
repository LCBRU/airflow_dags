from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from warehousing.data_download.download_to_mysql import download_mysql_backup_and_restore
from warehousing.data_download.edge_download import download_edge_studies
from tools import default_dag_args, error_emails
from airflow.providers.smtp.operators.smtp import EmailOperator


with DAG(
    dag_id="download_UOL_data",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_DOWNLOAD_UOL_DATA', None) or None,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):
    PythonOperator(
        task_id=f"download_mysql_backup_and_restore__uol_openspecimen",
        python_callable=download_mysql_backup_and_restore,
        op_kwargs={
            'destination_database': 'uol_openspecimen',
            'source_url': 'https://catissue-live.lcbru.le.ac.uk/publish/catissue.db',
        },
    )
    
    # PythonOperator(
    #     task_id=f"download_mysql_backup_and_restore__uol_survey_redcap",
    #     python_callable=download_mysql_backup_and_restore,
    #     op_kwargs={
    #         'destination_database': 'uol_survey_redcap',
    #         'source_url': 'https://redcap.lcbru.le.ac.uk/publish/redcap.db',
    #     },
    # )


with DAG(
    dag_id="download_external_data",
    default_args=default_dag_args,
    schedule=os.environ.get('SCHEDULE_DOWNLOAD_EXTERNAL_DATA', None) or None,
    catchup=False,
    start_date=datetime(2020, 1, 1),
):
        task_download_edge_studies = PythonOperator(
            task_id=f"download_edge_studies",
            python_callable=download_edge_studies,
            email=error_emails,
            email_on_failure=True,
        )

        print(task_download_edge_studies.email)
        print(task_download_edge_studies.email_on_failure)


with DAG(
    dag_id="test_email",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id="send_email",
        to=[
             "rabramley@gmail.com",
             "richard.bramley5@nhs.net",
             "rab63@leicester.ac.uk",
        ],
        from_email="richard.bramley5@nhs.net",
        subject="Airflow Email Test",
        html_content="<h3>Email from Airflow</h3>",
    )
