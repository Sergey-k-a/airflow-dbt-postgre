from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
# import os
# import sys

# sys.path.append('/opt/airflow/scripts')
# # print(f"📁 Current sys.path: {sys.path}")
# # print(f"📁 Files in /opt/airflow/scripts: {os.listdir('/opt/airflow/scripts')}")
# from init_database import init_database as init_db_func

def tst():
    print('all right')

with DAG(
    'test_simple_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:
    
    start = DummyOperator(task_id='start')

    test = PythonOperator(
        task_id='test',
        python_callable=tst,
        trigger_rule='all_success'
    )

    end = DummyOperator(task_id='end')
    
    start >> test >> end