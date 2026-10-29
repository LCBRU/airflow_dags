from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import DAG, task

from tools import default_dag_args, error_emails
from warehousing.schedules import SCHEDULE_BACKUP

CHECKS = [
    {"database": "redcap_uhl", "table": "redcap_log_eventsplitted"},
    {"database": "redcap_uhl", "table": "redcap_metadata"},
    {"database": "redcap_uhl", "table": "redcap_project_civicrm_case_type"},
    {"database": "redcap_uhl", "table": "redcap_projects"},
    {"database": "redcap_survey", "table": "redcap_log_eventsplitted"},
    {"database": "redcap_survey", "table": "redcap_data"},
    {"database": "redcap_survey", "table": "redcap_log_event"},
    {"database": "redcap_survey", "table": "redcap_metadata"},
    {"database": "redcap_uol", "table": "redcap_log_eventsplitted"},
    {"database": "redcap_uol", "table": "redcap_data"},
    {"database": "redcap_uol", "table": "redcap_events_metadata"},
    {"database": "redcap_uol", "table": "redcap_log_event"},
    {"database": "redcap_uol", "table": "redcap_log_eventsplitted"},
    {"database": "redcap_uol", "table": "redcap_metadata"},
]


STG_REDCAP_STANDARD_QUERY = """
    SELECT *
    FROM {database}.dbo.{table}
"""


STG_REDCAP_METADATA_QUERY = """
    SELECT
        [project_id],
        [field_name],
        [field_phi],
        [form_name],
        [form_menu_description],
        [field_order],
        [field_units],
        [element_preceding_header],
        [element_type],
        [element_label],
        [element_enum],
        [element_note],
        [element_validation_type],
        [element_validation_min],
        [element_validation_max],
        [element_validation_checktype],
        [branching_logic],
        [field_req],
        [edoc_id],
        [edoc_display_img],
        [custom_alignment],
        [stop_actions],
        [question_num],
        [grid_name],
        [misc]
    FROM {database}.dbo.{table}
"""


STG_REDCAP_EVENTS_METADATA_QUERY = """
    SELECT
        [event_id],
        [descrip]
    FROM {database}.dbo.{table}
"""


STG_REDCAP_DATA_QUERY = """
    SELECT
        [project_id],
        [event_id],
        [record],
        [field_name],
        [value]
    FROM {database}.dbo.{table}
"""


STG_REDCAP_PROJECTS_QUERY = """
    SELECT
        [project_id],
        [project_name],
        [app_title],
        [status],
        [creation_time],
        [production_time],
        [inactive_time],
        [created_by],
        [draft_mode],
        [surveys_enabled],
        [repeatforms],
        [scheduling],
        [purpose],
        [purpose_other],
        [show_which_records],
        [count_project],
        [investigators],
        [project_note],
        [online_offline],
        [auth_meth],
        [double_data_entry],
        [project_language],
        [project_encoding],
        [is_child_of],
        [date_shift_max],
        [institution],
        [site_org_type],
        [grant_cite],
        [project_contact_name],
        [project_contact_email],
        [headerlogo],
        [auto_inc_set],
        [custom_data_entry_note],
        [custom_index_page_note],
        [order_id_by],
        [custom_reports],
        [disable_data_entry],
        [google_translate_default],
        [require_change_reason],
        [dts_enabled],
        [project_pi_firstname],
        [project_pi_mi],
        [project_pi_lastname],
        [project_pi_email],
        [project_pi_alias],
        [project_pi_username],
        [project_pi_pub_exclude],
        [project_pub_matching_institution],
        [project_irb_number],
        [project_grant_number],
        [history_widget_enabled],
        [secondary_pk],
        [custom_record_label],
        [display_project_logo_institution],
        [imported_from_rs],
        [display_today_now_button],
        [auto_variable_naming],
        [randomization],
        [enable_participant_identifiers],
        [survey_email_participant_field],
        [survey_phone_participant_field],
        [data_entry_trigger_url],
        [template_id],
        [date_deleted],
        [data_resolution_enabled],
        [field_comment_edit_delete],
        [realtime_webservice_enabled],
        [realtime_webservice_offset_days],
        [realtime_webservice_offset_plusminus],
        [last_logged_event],
        [survey_queue_custom_text],
        [survey_auth_enabled],
        [survey_auth_field1],
        [survey_auth_event_id1],
        [survey_auth_field2],
        [survey_auth_event_id2],
        [survey_auth_field3],
        [survey_auth_event_id3],
        [survey_auth_min_fields],
        [survey_auth_apply_all_surveys],
        [survey_auth_custom_message],
        [survey_auth_fail_limit],
        [survey_auth_fail_window],
        [twilio_enabled],
        [twilio_account_sid],
        [twilio_auth_token],
        [twilio_from_number],
        [twilio_voice_language],
        [twilio_option_voice_initiate],
        [twilio_option_sms_initiate],
        [twilio_option_sms_invite_make_call],
        [twilio_option_sms_invite_receive_call],
        [twilio_option_sms_invite_web],
        [twilio_default_delivery_preference],
        [twilio_request_inspector_checked],
        [twilio_request_inspector_enabled],
        [twilio_append_response_instructions],
        [twilio_multiple_sms_behavior],
        [two_factor_exempt_project],
        [two_factor_force_project],
        [disable_autocalcs],
        [custom_public_survey_links]
    FROM {database}.dbo.{table}
"""





STG_REDCAP_LOG_EVENTSPLITTED_QUERY = """
    SELECT log_event_id, event_id, ts, user as c, event, pk, description, legacy, change_reason, project_id, Field_name, value
    FROM {database}.dbo.{table}
"""


DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY = """
    SELECT
        log_event_id,
        event_id,
        CONVERT(datetime, STUFF(STUFF(STUFF(STUFF(STUFF(CAST(ts AS varchar(14)),13,0,':'),11,0,':'),9,0,' '),7,0,'-'),5,0,'-')) as ts,
        user as c,
        event,
        pk,
        description,
        legacy,
        change_reason,
        project_id,
        Field_name,
        value
    FROM {database}.dbo.{table}
"""


STG_REDCAP_LOG_EVENT_QUERY = """
    SELECT
        log_event_id,
        project_id,
        ts,
        user as c,
        ip,
        page,
        event,
        object_type,
        pk,
        event_id,
        data_values,
        description,
        legacy,
        change_reason
    FROM {database}.dbo.{table}
"""


DATALAKE_REDCAP_LOG_EVENT_QUERY = """
    SELECT
        log_event_id,
        project_id,
        CONVERT(datetime, STUFF(STUFF(STUFF(STUFF(STUFF(CAST(ts AS varchar(14)),13,0,':'),11,0,':'),9,0,' '),7,0,'-'),5,0,'-')) as ts,
        user as c,
        ip,
        page,
        event,
        object_type,
        pk,
        event_id,
        data_values,
        description,
        legacy,
        change_reason
    FROM {database}.dbo.{table}
"""


TABLE_QUERIES = {
    "redcap_log_eventsplitted": {
        "staging_query": STG_REDCAP_LOG_EVENTSPLITTED_QUERY,
        "datalake_query": DATALAKE_REDCAP_LOG_EVENTSPLITTED_QUERY,
    },
    "redcap_metadata": {
        "staging_query": STG_REDCAP_METADATA_QUERY,
        "datalake_query": STG_REDCAP_METADATA_QUERY,
    },
    "redcap_project_civicrm_case_type": {
        "staging_query": STG_REDCAP_STANDARD_QUERY,
        "datalake_query": STG_REDCAP_STANDARD_QUERY,
    },
    "redcap_projects": {
        "staging_query": STG_REDCAP_PROJECTS_QUERY,
        "datalake_query": STG_REDCAP_PROJECTS_QUERY,
    },
    "redcap_data": {
        "staging_query": STG_REDCAP_DATA_QUERY,
        "datalake_query": STG_REDCAP_DATA_QUERY,
    },
    "redcap_log_event": {
        "staging_query": STG_REDCAP_LOG_EVENT_QUERY,
        "datalake_query": DATALAKE_REDCAP_LOG_EVENT_QUERY,
    },
    "redcap_events_metadata": {
        "staging_query": STG_REDCAP_EVENTS_METADATA_QUERY,
        "datalake_query": STG_REDCAP_EVENTS_METADATA_QUERY,
    },
}


DATABASE_XREF = {
    "redcap_uol": {
        "staging_database": "stg_uol_crf_redcap",
        "datalake_database": "datalake_uol_crf_redcap",
    },
    "redcap_uhl": {
        "staging_database": "stg_redcap",
        "datalake_database": "datalake_redcap6170_briccs",
    },
    "redcap_survey": {
        "staging_database": "stg_redcap_survey",
        "datalake_database": "datalake_uol_survey_redcap",
    }
}


def get_queries_text(database: str, table: str) -> tuple[str, str]:
    if table in TABLE_QUERIES:
        return (
            TABLE_QUERIES[table]["staging_query"].format(
                database=DATABASE_XREF[database]["staging_database"],
                table=table
            ),
            TABLE_QUERIES[table]["datalake_query"].format(
                database=DATABASE_XREF[database]["datalake_database"],
                table=table
            )
        )
    else:
        raise ValueError(f"No queries defined for {database}.{table}")


def except_query_has_differences(query1, query2):
    hook = DbApiHook.get_hook(conn_id="LEGACY_DWH")

    record = hook.get_first(
        f"""
        SELECT CASE
            WHEN EXISTS ({query1} EXCEPT {query2})
            THEN 1
            ELSE 0
        END AS mismatch
        """
    )

    return record[0] == 1


def queries_differ(query1, query2):
    return (
        except_query_has_differences(query1, query2)
        or except_query_has_differences(query2, query1)
    )


@task
def compare(config):
    database = config["database"]
    table = config["table"]

    query1, query2 = get_queries_text(database, table)

    different = queries_differ(query1, query2)

    return {
        "database": database,
        "table": table,
        "different": different,
    }


@task
def collect_differences(results):
    return [
        result
        for result in results
        if result.get("different", False)
    ]


@task.branch
def should_send_email(failures):
    if failures:
        return "send_email"
    return "no_email"


@task
def build_email(results):
    failures = [
        f"{r['database']}.{r['table']}"
        for r in results
        if r["different"]
    ]

    if not failures:
        return None

    html = """
    <h3>Differences found between Staging and DataLake</h3>
    <ul>
    """

    for failure in failures:
        html += f"<li>{failure}</li>"

    html += "</ul>"

    return html


@task
def no_email():
    pass


with DAG(
    dag_id="warehousing.migration.staging_vs_datalake_validation",
    schedule=SCHEDULE_BACKUP,
    default_args=default_dag_args,
) as dag:

    results = compare.expand(config=CHECKS)

    failures = collect_differences(results)

    email_body = build_email(failures)

    branch = should_send_email(failures)

    send_email = EmailOperator(
        task_id="send_email",
        to=error_emails,
        from_email="richard.a.bramley@uhl-tr.nhs.uk",
        subject="Differences Between DataLake and Staging",
        html_content=email_body,
        conn_id="smtp_default",
    )

    branch >> [send_email, no_email()]
