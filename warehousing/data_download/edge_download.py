import os
import logging
import tempfile
import csv
import datetime
from time import sleep
from airflow.operators.python_operator import PythonOperator
from warehousing.database import etl_central_session
from tools import create_sub_dag_task
from lbrc_selenium.selenium import get_selenium, NameSelector, IdSelector, CssSelector, XpathSelector
from selenium.webdriver.common.keys import Keys
from lbrc_edge import EdgeSiteStudy


def _download_edge_studies():
    logging.info("_download_edge_studies: Started")

    s = get_selenium(base_url=os.environ['AIRFLOW_VAR_EDGE_BASE_URL'])

    try:
        _login(s)
        _get_studies(s)
    finally:
        s.close()

    logging.info("_download_edge_studies: Ended")


def _login(selenium):
    logging.info("_login: Started")

    selenium.get("/")

    username = selenium.get_element(NameSelector("fldUsername"))
    password = selenium.get_element(NameSelector('fldPassword'))

    username.send_keys(os.environ['AIRFLOW_VAR_EDGE_USERNAME'])
    password.send_keys(os.environ['AIRFLOW_VAR_EDGE_PASSWORD'])
    password.send_keys(Keys.RETURN)

    selenium.wait_to_appear(IdSelector("headerOrganisationName"))

    logging.info("_login: Ended")


def _get_studies(selenium):
    logging.info("_get_studies: Started")

    download_file = tempfile.NamedTemporaryFile()

    _download_study_file(selenium, download_file.name)
    studies= _extract_study_details(selenium, download_file.name)
    _save_studies(studies)

    download_file.close()

    logging.info("_get_studies: Ended")


def _download_study_file(selenium, filename):
    logging.info("_download_studies: Started")

    selenium.get("ProjectAttributeReport")
    selenium.click_element(CssSelector('input[value="Load query"]'))
    selenium.click_element(XpathSelector('//a[@name="linkLoadQuery" and text()="BRC Report (Richard)"]'))
    selenium.click_element(CssSelector('input[value="Submit query"]'))
    sleep(15)
    selenium.click_element(IdSelector('butDownloadCsv'))
    sleep(30)

    selenium.download_file(filename)

    logging.info("_download_studies: Ended")


def _extract_study_details(selenium, download_filename):
    logging.info("_save_study_details: Started")

    try:
        with open(download_filename, encoding="utf-16-le") as csvfile:
            studies = []
            study_details = csv.DictReader(csvfile, delimiter=',', quotechar='"')

            for row in study_details:

                if row.get('Primary Clinical Management Areas', '').upper() not in ['CARDIOLOGY', 'VASCULAR SERVICES', 'CARDIAC SURGERY']:
                    continue

                e = EdgeSiteStudy(
                    project_id=_int_or_none(row['Project ID']),
                    iras_number=_string_or_none(row['IRAS Number']),
                    project_short_title=_string_or_none(row['Project Short title']),
                    primary_clinical_management_areas=_string_or_none(row['Primary Clinical Management Areas']),
                    project_site_status=_string_or_none(row['Project site status']),
                    project_site_rand_submission_date=_date_or_none(row['Project site R&D Submission Date']),
                    project_site_start_date_nhs_permission=_date_or_none(row['Project site Start date (NHS Permission)']),
                    project_site_date_site_confirmed=_date_or_none(row['Project site date site confirmed']),
                    project_site_planned_closing_date=_date_or_none(row['Project site Planned closing date']),
                    project_site_closed_date=_date_or_none(row['Project site Closed date']),
                    project_site_planned_recruitment_end_date=_date_or_none(row['Project site planned recruitment end date']),
                    project_site_actual_recruitment_end_date=_date_or_none(row['Project site actual recruitment end date']),
                    principal_investigator=_name_or_none(row['Principal Investigator']),
                    project_site_target_participants=_int_or_none(row['Project site target participants']),
                    recruited_org=_int_or_none(row['Recruited (org)']),
                    project_site_lead_nurses=_name_or_none(row['Project site lead nurse(s)']),
                    planned_start_date=_date_or_none(row['Planned Start Date']),
                    planned_end_date=_date_or_none(row['Planned End Date']),
                )

                e.calculate_values()

                studies.append(e)
        
        return studies
    finally:
        logging.info("_save_study_details: Ended")

def _save_studies(studies):
    logging.info("_save_study_details: Started")

    with etl_central_session() as session:
        logging.info("_save_study_details: Deleting old studies")

        session.query(EdgeSiteStudy).delete()

        logging.info("_save_study_details: Creating new studies")
        session.add_all(studies)

    logging.info("_save_study_details: Ended")

def _string_or_none(string_element):
    string_element = string_element.strip()
    if string_element:
        return string_element
    else:
        return None

def _name_or_none(string_element):
    string_element = string_element.strip()
    if string_element:
        name = ' '.join(reversed(
            [p.strip() for p in filter(lambda x: len(x) > 0, string_element.split(','))]
        )).strip()

        if name:
            return name


def _int_or_none(int_element):
    int_string = int_element.strip()
    if int_string:
        return int(int_string)
    else:
        return None


def _date_or_none(date_element):
    date_string = date_element.strip()
    if date_string:
        return datetime.datetime.strptime(date_string, "%d/%m/%Y").date()
    else:
        return None


def _boolean_or_none(boolean_element):
    boolean_string = boolean_element.strip().upper()
    if boolean_string in ['YES', 'TRUE', '1']:
        return True
    elif boolean_string in ['NO', 'FALSE', '0']:
        return False
    else:
        return None


def create_download_edge_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_edge_studies', run_on_failures=True)

    PythonOperator(
        task_id=f"download_edge_studies",
        python_callable=_download_edge_studies,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
