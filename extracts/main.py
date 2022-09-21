from extracts.easy_as_severe_screening import create_easy_as_severe_screening_dag
from tools import create_dag


dag = create_dag(title='exports', schedule_name='SCHEDULE_LOAD_WAREHOUSE')

easy_as_severe_screening = create_easy_as_severe_screening_dag(dag)
