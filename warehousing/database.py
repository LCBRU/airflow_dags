import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.ext.declarative import declarative_base
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from contextlib import contextmanager


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


class MsSqlConnection:
    def __init__(self, connection_name, schema):
        self._connection_name = connection_name
        self._schema = schema
    
    @contextmanager
    def query_mssql_dict(self, *args, **kwargs):
        logging.info("query_mssql_dict: Started")

        with self.query_mssql(*args, **kwargs) as cursor:

            schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))

            yield (dict(zip(schema, r)) for r in cursor)
            
        logging.info("query_mssql_dict: Ended")


    def execute_mssql(self, *args, **kwargs):
        logging.info("execute_mssql: Started")

        with self.query_mssql(*args, **kwargs) as cursor:
            pass

        logging.info("execute_mssql: Ended")


    @contextmanager
    def query_mssql(self, sql=None, file_path=None, parameters=None):
        logging.info("query_mssql: Started")

        if not parameters:
            parameters = {}

        mysql = MsSqlHook(mssql_conn_id=self._connection_name, schema=self._schema)
        conn = mysql.get_conn()
        cursor = conn.cursor()

        if sql is not None:
            cursor.execute(sql, parameters)
        elif file_path is not None:
            logging.info(f'query_mssql running: {file_path}')
            with open(file_path) as sql_file:
                cursor.execute(sql_file.read(), parameters)

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
            logging.info("query_mssql: Ended")


DWH_CONNECTION_NAME = 'DWH'
SCH_WAREHOUSE_CENTRAL = 'warehouse_central'
SCH_WAREHOUSE_CONFIG = 'warehouse_config'
SCH_WAREHOUSE_MASTER = 'master'
SCH_DATALAKE_CIVICRM = 'datalake_civicrm'


class WarehouseConnection(MsSqlConnection):
    def __init__(self, schema):
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


@contextmanager
def query_mssql_dict(*args, **kwargs):
    logging.info("query_mssql_dict: Started")

    with query_mssql(*args, **kwargs) as cursor:

        schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))

        yield (dict(zip(schema, r)) for r in cursor)
        
    logging.info("query_mssql_dict: Ended")


def execute_mssql(*args, **kwargs):
    logging.info("execute_mssql: Started")

    with query_mssql(*args, **kwargs) as cursor:
        pass

    logging.info("execute_mssql: Ended")


@contextmanager
def query_mssql(connection_name, schema=None, sql=None, file_path=None, parameters=None):
    logging.info("query_mssql: Started")

    if not parameters:
        parameters = {}

    mysql = MsSqlHook(mssql_conn_id=connection_name, schema=schema)
    conn = mysql.get_conn()
    cursor = conn.cursor()

    if sql is not None:
        cursor.execute(sql, parameters)
    elif file_path is not None:
        with open(file_path) as sql_file:
            cursor.execute(sql_file.read(), parameters)

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
        logging.info("query_mssql: Ended")
