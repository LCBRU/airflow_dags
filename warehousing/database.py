import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.ext.declarative import declarative_base
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.mysql_operator import MySqlOperator


Base = declarative_base()


@contextmanager
def etl_central_session():
    mysqlhook = MySqlHook(mysql_conn_id='ETL_CENTRAL')
    conn = mysqlhook.get_connection('ETL_CENTRAL')

    engine = create_engine(conn.get_uri())
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    try:
        yield session

    except Exception as e:
        session.rollback()
        session.close()
        raise e
    else:
        session.commit()
        session.close()
    finally:
        engine.dispose


class SqlConnection:
    def __init__(self, connection_name, schema=''):
        self._connection_name = connection_name
        self._schema = schema
    
    @contextmanager
    def query_dict(self, *args, **kwargs):
        logging.info("query_dict: Started")

        with self.query(*args, **kwargs) as cursor:

            schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))

            yield (dict(zip(schema, r)) for r in cursor)
            
        logging.info("query_dict: Ended")

    def execute(self, *args, **kwargs):
        logging.info("execute: Started")

        with self.query(*args, **kwargs) as cursor:
            pass

        logging.info("execute: Ended")

    def get_operator(self, dag, task_id, sql, params=None):
        pass

    def get_hook(self):
        pass

    @property
    def connection_details(self):
        return self.get_hook().get_connection(self._connection_name)

    @property
    def host(self):
        return self.connection_details.host

    @property
    def login(self):
        return self.connection_details.login

    @property
    def password(self):
        return self.connection_details.password

    @contextmanager
    def query(self, sql=None, file_path=None, parameters=None, context=None):
        logging.info("query: Started")

        if not parameters:
            parameters = {}

        conn = self.get_hook().get_conn()
        cursor = conn.cursor()

        if sql is None:
            logging.info(f'query running: {file_path}')
            with open(file_path) as sql_file:
                sql = sql_file.read()

        if context:
            sql = sql.format(**context)

        cursor.execute(sql, parameters)

        try:
            yield cursor

        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise e
        else:
            conn.commit()
            cursor.close()
            conn.close()
        finally:
            logging.info("query: Ended")


class MySqlConnection(SqlConnection):
    def get_operator(self, dag, task_id, sql, params=None):
        return MySqlOperator(
            task_id=task_id.replace(" ", "_"),
            mssql_conn_id=self._connection_name,
            sql=sql,
            autocommit=True,
            database=self._schema,
            dag=dag,
            params=params,
        )

    def get_hook(self):
        return MySqlHook(mysql_conn_id=self._connection_name, schema=self._schema)


class MsSqlConnection(SqlConnection):
    def get_operator(self, dag, task_id, sql, params=None):
        return MsSqlOperator(
            task_id=task_id.replace(" ", "_"),
            mssql_conn_id=self._connection_name,
            sql=sql,
            autocommit=True,
            database=self._schema,
            dag=dag,
            params=params,
        )

    def get_hook(self):
        return MsSqlHook(mssql_conn_id=self._connection_name, schema=self._schema)


DWH_CONNECTION_NAME = 'DWH'
SCH_WAREHOUSE_CENTRAL = 'warehouse_central'
SCH_WAREHOUSE_CONFIG = 'warehouse_config'
SCH_WAREHOUSE_MASTER = 'master'
SCH_DATALAKE_CIVICRM = 'datalake_civicrm'


class WarehouseConnection(MsSqlConnection):
    def __init__(self, schema=''):
        super().__init__(DWH_CONNECTION_NAME, schema)


class WarehouseCentralConnection(WarehouseConnection):
    def __init__(self):
        super().__init__(SCH_WAREHOUSE_CENTRAL)


class WarehouseConfigConnection(WarehouseConnection):
    def __init__(self):
        super().__init__(SCH_WAREHOUSE_CONFIG)


class WarehouseMasterConnection(WarehouseConnection):
    def __init__(self):
        super().__init__(SCH_WAREHOUSE_MASTER)


class DatalakeCiviCRMConnection(WarehouseConnection):
    def __init__(self):
        super().__init__(SCH_DATALAKE_CIVICRM)


LIVE_DB_CONNECTION_NAME = 'LIVE_DB'
REPLICANT_DB_CONNECTION_NAME = 'REPLICANT_DB'


class LiveDbConnection(MySqlConnection):
    def __init__(self, schema=''):
        super().__init__(LIVE_DB_CONNECTION_NAME, schema)


class ReplicantDbConnection(MySqlConnection):
    def __init__(self, schema=''):
        super().__init__(REPLICANT_DB_CONNECTION_NAME, schema)
