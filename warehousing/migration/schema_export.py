from __future__ import annotations

import hashlib
import shutil
from itertools import groupby
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import dag, task
from werkzeug.utils import secure_filename

from tools import default_dag_args, error_emails
from warehousing.schedules import SCHEDULE_SCHEMA_EXPORT


def get_hook(conn_id: str, database: str | None = None) -> DbApiHook:
    conn = BaseHook.get_connection(conn_id)
    hook = conn.get_hook()

    if not isinstance(hook, DbApiHook):
        raise TypeError(
            f"Connection '{conn_id}' resolved to "
            f"{type(hook).__name__}, not a DbApiHook."
        )

    if database is not None:
        hook.schema = database

    return hook

BACKUP_DIRECTORY = '/backup/dwh_schema/'

@task
def clear_output_directory(target_dir: str) -> None:
    target_dir = Path(target_dir)
    if target_dir.exists():
        shutil.rmtree(target_dir)

    target_dir.mkdir(parents=True, exist_ok=True)


@task
def create_archive(target_dir: str) -> str:
    archive_path = shutil.make_archive(
        target_dir,
        "zip",
        root_dir=target_dir,
    )

    return archive_path


@task
def zip_file_hash(target_dir: str) -> str:
    sha256 = hashlib.sha256()

    print(f"Calculating hash for {target_dir}")

    # create a repeatable zip for all the files in the directory recursively, sorted by name
    for file in sorted(Path(target_dir).glob("**/*")):
        if file.is_file():
            with open(file, "rb") as f:
                while chunk := f.read(8192):
                    sha256.update(chunk)

    result = sha256.hexdigest()

    print(f"Hash for {target_dir}: {result}")

    return result


@task
def previous_zip_file_hash(target_dir: str) -> str | None:
    previous_hash_file = Path(target_dir).with_suffix(".zip.previous.sha256")

    if not previous_hash_file.exists():
        return None

    return previous_hash_file.read_text(encoding="utf-8").strip().split()[0]


@task.branch
def choose_email_task(current_hash: str, previous_hash: str | None) -> str:
    print(f"Previous hash: {previous_hash}")
    print(f"Current hash: {current_hash}")

    if previous_hash is None:
        return "create_archive"

    if current_hash != previous_hash:
        return "create_archive"

    return "archive_unchanged"


@task
def archive_unchanged() -> None:
    pass


@task
def record_emailed_hash(target_dir: str, hash: str) -> None:
    previous_hash_file = Path(target_dir).with_suffix(".zip.previous.sha256")

    print(f"Recording hash {hash} to {previous_hash_file}")

    previous_hash_file.write_text(
        str(hash),
        encoding="utf-8",
    )


@task
def get_databases(conn_id: str) -> list[str]:

    hook = get_hook(conn_id)

    rows = hook.get_records("""
        SELECT name
        FROM sys.databases
        WHERE database_id > 4
            AND state_desc = 'ONLINE'
        """)

    return [row[0] for row in rows]


@task
def export_database(database: str, conn_id: str, target_dir: str) -> None:

    hook = get_hook(conn_id, database)

    output_path = Path(target_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    extract_tables(hook, output_path)
    export_primary_keys(hook, output_path)
    export_views(hook, output_path)
    export_stored_procedures(hook, output_path)
    export_triggers(hook, output_path)
    export_foreign_keys(hook, output_path)


def extract_tables(hook, output_directory):
    tables_directory = output_directory / "tables"
    tables_directory.mkdir(parents=True, exist_ok=True)

    tables = hook.get_records("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)

    for schema, table in tables:
        cols = hook.get_records("""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=%s
                    AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
                """,
                (schema, table),
            )

        ddl = f"\nCREATE TABLE [{schema}].[{table}] (\n"

        definitions = []

        for (col_name, dtype, char_max_len, numeric_precision, numeric_scale, is_nullable) in cols:
            if dtype in {"varchar", "nvarchar", "char", "nchar",}:
                if char_max_len == -1:
                    dtype += "(MAX)"
                else:
                    dtype += f"({char_max_len})"
            elif dtype in {"decimal", "numeric"}:
                dtype += f"({numeric_precision},{numeric_scale})"

            nullable = "NULL" if is_nullable == "YES" else "NOT NULL"

            definitions.append(f"[{col_name}] {dtype} {nullable}")

        ddl += ",\n".join(f"    {d}" for d in definitions)
        ddl += "\n);\nGO\n"

        with open(tables_directory / secure_filename(f"{schema}.{table}.sql"), "w", encoding="utf-8",) as f:
            f.write(ddl)


def export_primary_keys(hook, output_directory):
    primary_keys_directory = output_directory / "primary_keys"
    primary_keys_directory.mkdir(parents=True, exist_ok=True)

    records = hook.get_records("""
            SELECT
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.ORDINAL_POSITION
            """
        )

    pks = {}

    for schema, table, column, constraint_name in records:
        key = (schema, table, constraint_name)

        pks.setdefault(key, []).append(column)

    for key, cols in pks.items():        
        schema, table, constraint = key

        sql = f"""
            ALTER TABLE [{schema}].[{table}]
            ADD CONSTRAINT [{constraint}]
            PRIMARY KEY ({','.join(f'[{c}]' for c in cols)});
            GO
        """
        
        with open(primary_keys_directory / secure_filename(f"{schema}.{table}.sql"), "w", encoding="utf-8") as pk_f:
            pk_f.write(sql)


def export_views(hook, output_directory):
    views_directory = output_directory / "views"
    views_directory.mkdir(parents=True, exist_ok=True)

    views = hook.get_records("""
            SELECT s.name as schema_name, o.name as view_name, m.definition
            FROM sys.views o
            JOIN sys.schemas s
                ON o.schema_id=s.schema_id
            JOIN sys.sql_modules m
                ON o.object_id=m.object_id
            ORDER BY s.name,o.name
            """
        )

    for schema_name, view_name, definition in views:
        with open(views_directory / secure_filename(f"{schema_name}.{view_name}.sql"), "w", encoding="utf-8",) as f:
            f.write(f"\n-- Schema: {schema_name}\n")
            f.write(f"-- View: {view_name}\n")
            f.write(definition)
            f.write("\nGO\n")


def export_stored_procedures(hook, output_directory):
    stored_procedures_directory = output_directory / "stored_procedures"
    stored_procedures_directory.mkdir(parents=True, exist_ok=True)

    sps = hook.get_records("""
            SELECT s.name as schema_name, o.name as procedure_name, m.definition
            FROM sys.procedures o
            JOIN sys.schemas s
                ON o.schema_id=s.schema_id
            JOIN sys.sql_modules m
                ON o.object_id=m.object_id
            ORDER BY s.name,o.name
            """
        )

    for schema_name, procedure_name, definition in sps:
        with open(stored_procedures_directory / secure_filename(f"{schema_name}.{procedure_name}.sql"), "w", encoding="utf-8",) as f:
            f.write(f"\n-- Schema: {schema_name}\n")
            f.write(f"-- Procedure: {procedure_name}\n")
            f.write(definition)
            f.write("\nGO\n")


def export_triggers(hook, output_directory):
    triggers_directory = output_directory / "triggers"
    triggers_directory.mkdir(parents=True, exist_ok=True)

    triggers = hook.get_records("""
        SELECT
            t.name,
            m.definition
        FROM sys.triggers t
        JOIN sys.sql_modules m
            ON t.object_id=m.object_id
        WHERE t.parent_class = 1
        ORDER BY t.name
        """)

    for name, definition in triggers:
        with open(triggers_directory / secure_filename(f"{name}.sql"), "w", encoding="utf-8",) as f:
            f.write(f"\n-- Trigger: {name}\n")
            f.write(definition)
            f.write("\nGO\n")


def export_foreign_keys(hook, output_directory):
    foreign_keys_directory = output_directory / "foreign_keys"
    foreign_keys_directory.mkdir(parents=True, exist_ok=True)

    records = hook.get_records("""
        SELECT
            fk.name AS constraint_name,
            sch1.name AS schema_name,
            tab1.name AS table_name,
            col1.name AS column_name,
            sch2.name AS ref_schema_name,
            tab2.name AS ref_table_name,
            col2.name AS ref_column_name
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc
            ON fk.object_id = fkc.constraint_object_id
        JOIN sys.tables tab1
            ON fk.parent_object_id = tab1.object_id
        JOIN sys.schemas sch1
            ON tab1.schema_id = sch1.schema_id
        JOIN sys.columns col1
            ON fkc.parent_column_id = col1.column_id AND col1.object_id = tab1.object_id
        JOIN sys.tables tab2
            ON fk.referenced_object_id = tab2.object_id
        JOIN sys.schemas sch2
            ON tab2.schema_id = sch2.schema_id
        JOIN sys.columns col2
            ON fkc.referenced_column_id = col2.column_id AND col2.object_id = tab2.object_id
        ORDER BY sch1.name, tab1.name, fk.name, fkc.constraint_column_id;
    """)

    for constraint_name, schema_name, table_name, column_name, ref_schema_name, ref_table_name, ref_column_name in records:
        sql = f"""
            ALTER TABLE [{schema_name}].[{table_name}]
            ADD CONSTRAINT [{constraint_name}]
            FOREIGN KEY ([{column_name}])
            REFERENCES [{ref_schema_name}].[{ref_table_name}] ([{ref_column_name}]);
            GO
        """

        with open(foreign_keys_directory / secure_filename(f"{constraint_name}.sql"), "w", encoding="utf-8") as f:
            f.write(sql)


@task
def export_sqlserver_agent_jobs(target_dir, conn_id):
    target_dir = Path(target_dir)

    agent_jobs_directory = target_dir / "sqlserver_agent_jobs"
    agent_jobs_directory.mkdir(parents=True, exist_ok=True)

    hook = get_hook(conn_id)

    jobs = hook.get_records("""
        SELECT
            j.name AS job_name,
            s.step_id,
            s.step_name,
            s.subsystem,
            s.command,
            s.database_name,
            s.database_user_name,
            s.retry_attempts,
            s.retry_interval,
            s.on_success_action,
            s.on_success_step_id,
            s.on_fail_action,
            s.on_fail_step_id,
            s.output_file_name,
            p.name AS proxy_name
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobsteps s
            ON j.job_id = s.job_id
        LEFT JOIN msdb.dbo.sysproxies p
            ON s.proxy_id = p.proxy_id
        ORDER BY j.name, s.step_id;
    """)

    for job_name, job_steps in groupby(jobs, key=lambda x: x[0]):
        sql = f"""
        ----------------------------------------------
        -- Job: {job_name}
        ----------------------------------------------
        """

        for step in job_steps:
            (
                job_name,
                step_id,
                step_name,
                subsystem,
                command,
                database_name,
                database_user_name,
                retry_attempts,
                retry_interval,
                on_success_action,
                on_success_step_id,
                on_fail_action,
                on_fail_step_id,
                output_file_name,
                proxy_name
            ) = step
    
            sql = f"""
            -- Step {step_id}: {step_name}
            EXEC msdb.dbo.sp_add_jobstep
                @job_name = N'{job_name.replace("'", "''")}',
                @step_id = {step_id},
                @step_name = N'{step_name.replace("'", "''")}',
                @subsystem = N'{subsystem}',
                @command = N'{command.replace("'", "''")}',
                @database_name = N'{database_name or "master"}',
                @database_user_name = N'{database_user_name or ""}',
                @retry_attempts = {retry_attempts},
                @retry_interval = {retry_interval},
                @on_success_action = {on_success_action},
                @on_success_step_id = {on_success_step_id},
                @on_fail_action = {on_fail_action},
                @on_fail_step_id = {on_fail_step_id}"""

            if output_file_name:
                sql += f""",
    @output_file_name = N'{output_file_name.replace("'", "''")}'"""

            if proxy_name:
                sql += f""",
    @proxy_name = N'{proxy_name.replace("'", "''")}'"""

            sql += ";\nGO\n\n"

        with open(agent_jobs_directory / secure_filename(f"{job_name}.sql"), "w", encoding="utf-8") as f:
            f.write(sql)


def build_schema_export_dag(conn_id: str):

    @dag(
        dag_id=f"warehousing.migration.schema_export_{conn_id}",
        default_args=default_dag_args,
        schedule=SCHEDULE_SCHEMA_EXPORT,
        max_active_runs=1,
    )
    def schema_export():
        target_dir = str(Path(BACKUP_DIRECTORY) / conn_id)

        clear = clear_output_directory(target_dir)

        databases = get_databases(conn_id)

        exports = export_database.partial(
            conn_id=conn_id,
            target_dir=target_dir,
        ).expand(
            database=databases,
        )

        jobs = export_sqlserver_agent_jobs(target_dir, conn_id)

        hash = zip_file_hash(target_dir)
        previous_hash = previous_zip_file_hash(target_dir)
    
        branch = choose_email_task(hash, previous_hash)

        archive = create_archive(target_dir)

        email_archive = EmailOperator(
            task_id="email_changed_archive",
            to=error_emails,
            from_email="richard.a.bramley@uhl-tr.nhs.uk",
            subject=f"Schema Export for {conn_id} Changed",
            html_content=(f"<p>The schema export for <strong>{conn_id}</strong> has changed.</p>"),
            conn_id="smtp_default",
        )

        unchanged = archive_unchanged()

        record_hash = record_emailed_hash(target_dir, hash)

        clear >> jobs >> hash
        clear >> databases >> exports >> hash
        hash >> branch
        previous_hash >> branch
        branch >> [archive, unchanged]
        archive >> email_archive >> record_hash

    return schema_export()


build_schema_export_dag("DWH")
build_schema_export_dag("LEGACY_DWH")
