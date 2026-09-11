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
            pks = hook.get_records("""
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

            for schema, table, column, constraint_name in pks:
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
                    SELECT m.definition
                    FROM sys.triggers o
                    JOIN sys.schemas s
                        ON o.schema_id=s.schema_id
                    JOIN sys.sql_modules m
                        ON o.object_id=m.object_id
                    ORDER BY s.name,o.name
                    """
                )

            for definition, in triggers:
                f.write("\n")
                f.write(definition)
                f.write("\nGO\n")

    conn_id = "{{ params.conn_id }}"

    export_database.partial(conn_id=conn_id, output_dir=BACKUP_DIRECTORY).expand(
        database=get_databases(conn_id),
    )


schema_export('DWH')