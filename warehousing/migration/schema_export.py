from __future__ import annotations

from pathlib import Path

from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook


def get_hook(conn_id: str) -> DbApiHook:
    """
    Generic DbApiHook resolver.

    Works for MsSqlHook, PostgresHook, MySqlHook, etc.
    provided the connection type has a registered provider.
    """
    conn = BaseHook.get_connection(conn_id)

    hook = conn.get_hook()

    if not isinstance(hook, DbApiHook):
        raise TypeError(
            f"Connection '{conn_id}' resolved to "
            f"{type(hook).__name__}, not a DbApiHook."
        )

    return hook

BACKUP_DIRECTORY = '/backup/dwh_schema/'

@dag(
    dag_id="schema_export",
    schedule=None,
    params={
        "conn_id": "DWH",
        "output_dir": BACKUP_DIRECTORY,
    },
)
def schema_export():

    @task
    def get_databases(conn_id: str) -> list[str]:

        hook = get_hook(conn_id)

        sql ="""
        SELECT name
        FROM sys.databases
        WHERE database_id > 4
          AND state_desc = 'ONLINE'
        ORDER BY name
        """

        rows = hook.get_records(sql)

        return [row[0] for row in rows]

    @task
    def export_database(
        database: str,
        conn_id: str,
        output_dir: str,
    ) -> str:

        hook = get_hook(conn_id)

        #
        # MsSqlHook supports schema override.
        # This creates a hook connected to the
        # target database.
        #
        db_hook = hook.__class__(
            mssql_conn_id=conn_id,
            schema=database,
        )

        outfile = (
            Path(output_dir)
            / f"{database}_Schema.sql"
        )

        outfile.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with db_hook.get_conn() as conn:
            cursor = conn.cursor()

            with open(
                outfile,
                "w",
                encoding="utf-8",
            ) as f:

                extract_tables(db_hook, cursor, f)
        return str(outfile)

    def extract_tables(hook, cursor, f):
        cursor.execute(
                    """
                    SELECT TABLE_SCHEMA,
                           TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE'
                    ORDER BY TABLE_SCHEMA,
                             TABLE_NAME
                    """
                )

        tables = cursor.fetchall()

        for schema, table in tables:
            cursor.execute(
                        """
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

            ddl = (
                        f"\nCREATE TABLE "
                        f"[{schema}].[{table}] (\n"
                    )

            definitions = []

            for (col_name, dtype, char_max_len, numeric_precision, numeric_scale, is_nullable) in cols:
                if dtype in {
                            "varchar",
                            "nvarchar",
                            "char",
                            "nchar",
                        }:
                    if (
                                char_max_len
                                == -1
                            ):
                        dtype += "(MAX)"
                    else:
                        dtype += (
                                    f"({char_max_len})"
                                )

                elif dtype in {
                            "decimal",
                            "numeric",
                        }:
                    dtype += (
                                f"({numeric_precision},"
                                f"{numeric_scale})"
                            )

                nullable = (
                            "NULL"
                            if is_nullable == "YES"
                            else "NOT NULL"
                        )

                definitions.append(
                            f"[{col_name}] "
                            f"{dtype} {nullable}"
                        )

            ddl += ",\n".join(
                        f"    {d}"
                        for d in definitions
                    )

            ddl += "\n);\nGO\n"

            f.write(ddl)

    conn_id = "{{ params.conn_id }}"
    output_dir = "{{ params.output_dir }}"

    databases = get_databases(conn_id)

    export_database.partial(
        conn_id=conn_id,
        output_dir=output_dir,
    ).expand(
        database=databases,
    )


schema_export()