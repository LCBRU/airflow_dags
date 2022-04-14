from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.ext.declarative import declarative_base


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
