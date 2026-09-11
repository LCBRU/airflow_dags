from __future__ import annotations

from pathlib import Path

from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook


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

@dag(
    dag_id="schema_export",
    schedule=None,
)
def schema_export(conn_id: str):

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
    def export_database(database: str, conn_id: str, output_dir: str) -> str:

        hook = get_hook(conn_id, database)

        output_directory = Path(output_dir)/ conn_id / database
        output_directory.mkdir(parents=True, exist_ok=True)

        extract_tables(hook, output_directory)
        export_primary_keys(hook, output_directory)
        export_views(hook, output_directory)
        export_stored_procedures(hook, output_directory)
        export_triggers(hook, output_directory)
        export_foreign_keys(hook, output_directory)
        export_sqlserver_agent_jobs(hook, output_directory)

    def extract_tables(hook, output_directory):
        with open(output_directory / "tables.sql", "w", encoding="utf-8",) as f:
            tables = hook.get_records("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """
                )

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

                f.write(ddl)

    def export_primary_keys(hook, output_directory):
        with open(output_directory / "primary_keys.sql", "w", encoding="utf-8",) as f:
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
                f.write(sql)

    def export_views(hook, output_directory):
        with open(output_directory / "views.sql", "w", encoding="utf-8",) as f:
            views = hook.get_records("""
                    SELECT m.definition
                    FROM sys.views o
                    JOIN sys.schemas s
                        ON o.schema_id=s.schema_id
                    JOIN sys.sql_modules m
                        ON o.object_id=m.object_id
                    ORDER BY s.name,o.name
                    """
                )

            for definition, in views:
                f.write("\n")
                f.write(definition)
                f.write("\nGO\n")

    def export_stored_procedures(hook, output_directory):
        with open(output_directory / "stored_procedures.sql", "w", encoding="utf-8",) as f:
            sps = hook.get_records("""
                    SELECT m.definition
                    FROM sys.procedures o
                    JOIN sys.schemas s
                        ON o.schema_id=s.schema_id
                    JOIN sys.sql_modules m
                        ON o.object_id=m.object_id
                    ORDER BY s.name,o.name
                    """
                )

            for definition, in sps:
                f.write("\n")
                f.write(definition)
                f.write("\nGO\n")

    def export_triggers(hook, output_directory):
        with open(output_directory / "triggers.sql", "w", encoding="utf-8",) as f:
            triggers = hook.get_records("""
                SELECT
                    t.name,
                    m.definition
                FROM sys.triggers t
                JOIN sys.sql_modules m
                    ON t.object_id=m.object_id
                WHERE t.parent_class = 1
                ORDER BY t.name
                """
                )

            for name, definition in triggers:
                f.write(f"\n-- Trigger: {name}\n")
                f.write(definition)
                f.write("\nGO\n")

    conn_id = "{{ params.conn_id }}"

    export_database.partial(conn_id=conn_id, output_dir=BACKUP_DIRECTORY).expand(
        database=get_databases(conn_id),
    )

    def export_foreign_keys(hook, output_directory):
        with open(output_directory / "foreign_keys.sql", "w", encoding="utf-8",) as f:
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
                f.write(sql)

    def export_sqlserver_agent_jobs(hook, output_directory):
        with open(output_directory / "sqlserver_agent_jobs.sql", "w", encoding="utf-8") as f:

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

            current_job = None

            for (
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
                proxy_name,
            ) in jobs:

                if current_job != job_name:
                    f.write("\n" + "=" * 80 + "\n")
                    f.write(f"-- JOB: {job_name}\n")
                    f.write("=" * 80 + "\n")
                    current_job = job_name

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

                f.write(sql)

schema_export('DWH')
schema_export('LEGACY_DWH')
