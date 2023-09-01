import itertools
import os
import logging
from airflow.operators.python_operator import PythonOperator
from warehousing.database import etl_central_session, Base
from tools import create_sub_dag_task
from lbrc_selenium.selenium import get_selenium, IdSelector, CssSelector
from selenium.webdriver.common.keys import Keys
from sqlalchemy import Column, Integer, String


class CrfmStudy(Base):
    __tablename__ = 'crfm_study'

    id = Column(Integer, primary_key=True)
    study_number = Column(String)
    protocol_number = Column(String)
    title = Column(String)
    ethics_number = Column(String)
    clinical_trial_gov = Column(String)
    isrctn = Column(String)
    iras_number = Column(String)
    nihr_crn_number = Column(String)
    rd_number = Column(String)
    who = Column(String)
    eudract = Column(String)
    status = Column(String)


def _download_crf_manager_studies():
    logging.info("_download_crf_manager_studies: Started")

    s = get_selenium(base_url=os.environ['AIRFLOW_VAR_CRFM_BASE_URL'])

    try:
        _login(s)
        _get_studies(s)
    finally:
        s.close()

    logging.info("_download_crf_manager_studies: Ended")


def _login(selenium):
    logging.info("_login: Started")

    selenium.get("Login.aspx")

    username = selenium.get_element(IdSelector("tbLogin"))
    password = selenium.get_element(IdSelector('tbPassword'))

    username.send_keys(os.environ['AIRFLOW_VAR_CRFM_USERNAME'])
    password.send_keys(os.environ['AIRFLOW_VAR_CRFM_PASSWORD'])
    password.send_keys(Keys.RETURN)

    selenium.wait_to_appear(CssSelector("div.pnl_primary_links"))

    logging.info("_login: Ended")


def _get_studies(selenium):
    logging.info("_get_studies: Started")

    studies = _extract_study_details(selenium)
    _save_studies(studies)

    logging.info("_get_studies: Ended")


def _extract_study_details(selenium):
    logging.info("_save_study_details: Started")

    studies = []

    selenium.get('Print/Print_List.aspx?dbid=crf_leicestercrf_live&areaID=44&type=Query&name=Default&vid=&iid=')
    selenium.wait_to_appear(CssSelector("div.printarea > div:nth-child(3)"))

    mapping = {
        'iras number:': 'iras_number',
        'ethics number:': 'ethics_number',
        'clinical trials gov:': 'clinical_trial_gov',
        'eudract:': 'eudract',
        'isrctn:': 'isrctn',
        'nihr crn number:': 'nihr_crn_number',
        'protocol number:': 'protocol_number',
        'r & d number:': 'rd_number',
        'who:': 'who',
    }

    for study_table in selenium.get_elements(CssSelector('div.printarea > div:nth-child(3) > table')):
        full_title = selenium.get_text(selenium.get_element(CssSelector('thead td'), element=study_table))

        title, study_number = itertools.islice(
            itertools.chain(
                reversed(full_title.split(':', 1)),
                itertools.repeat('', 2),
            ),
            2
        )

        logging.info(f"Getting study details for '{title}'; ID = {study_number}")

        details = {}

        for header, value in zip(
            selenium.get_elements(CssSelector('tbody tr td:first-child:not(:only-child)'), element=study_table),
            selenium.get_elements(CssSelector('tbody tr td:nth-child(2)'), element=study_table),
        ):
            normalised_header = selenium.get_text(header).strip().lower()
            if normalised_header in mapping.keys():
                details[mapping[normalised_header]] = selenium.get_text(value).strip()

        status = selenium.get_element(CssSelector('tbody tr:last-of-type td:last-of-type'), element=study_table)

        details['status'] = selenium.get_text(status).strip()


        studies.append(CrfmStudy(**details))
    
    return studies


def _save_studies(studies):
    logging.info("_save_study_details: Started")

    with etl_central_session() as session:
        logging.info("_save_study_details: Deleting old studies")

        session.query(CrfmStudy).delete()

        logging.info("_save_study_details: Creating new studies")
        session.add_all(studies)

    logging.info("_save_study_details: Ended")


def create_download_crf_manager_studies(dag):
    parent_subdag = create_sub_dag_task(dag, 'download_crfm_studies', run_on_failures=True)

    PythonOperator(
        task_id=f"download_crf_manager_studies",
        python_callable=_download_crf_manager_studies,
        dag=parent_subdag.subdag,
    )

    return parent_subdag
