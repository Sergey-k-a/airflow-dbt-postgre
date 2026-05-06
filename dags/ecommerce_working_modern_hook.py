from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import sys
import os
import psycopg2
import subprocess
import socket 
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append('/opt/airflow/scripts')

def debug_execution(**kwargs):
    hostname = socket.gethostname()
    print(f"🖥️ Task executing on: {hostname}")

def cleanup_schemas(**kwargs):

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Очистка витрин перед запуском"""
    print("🧹 Cleaning previous marts and analytics schemas...")
    pg_hook = PostgresHook(postgres_conn_id = 'postgres_default')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    try:
        
        
        cur.execute("DROP SCHEMA IF EXISTS staging_staging CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS staging_marts CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS staging_analytics CASCADE;")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Schemas cleaned successfully")
    except Exception as e:
        if cur:
            cur.close()
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Cleanup failed: {e}")
        raise

def init_database(**kwargs):

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    # ДОБАВЬТЕ ОТЛАДКУ:
    print(f"📁 Current sys.path: {sys.path}")
    print(f"📁 Files in /opt/airflow/scripts: {os.listdir('/opt/airflow/scripts')}")

    """Инициализация БД"""
    print("🚀 Initializing database...")
    try:
        from init_database import init_database as init_db_func
        init_db_func()
        print("✅ Database initialized successfully")
    except ImportError as e:
        print(f"❌ Cannot import init_database: {e}")
        raise

def validate_source_data(**kwargs):
    """Валидация исходных данных"""

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")
    
    print("🔍 Validating source data quality...")
    
    conn = psycopg2.connect(
        host="postgres-dbt",
        #port=5432,
        database="dbt", 
        user="dbt", 
        password="dbt"
    )
    cur = conn.cursor()
    
    # Быстрая проверка наличия данных
    cur.execute("SELECT COUNT(*) FROM raw.customers")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("❌ No customers found")
    
    cur.execute("SELECT COUNT(*) FROM raw.orders WHERE status = 'completed'")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("❌ No completed orders found")
    
    cur.close()
    conn.close()
    print("✅ Source data validation passed")

def verify_dbt_success(**kwargs):
    
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Проверка что DBT создал все витрины"""
    print("🔍 Verifying DBT results...")
    
    conn = psycopg2.connect(
        host="postgres-dbt",
        #port=5432,
        database="dbt",
        user="dbt",
        password="dbt"
    )
    cur = conn.cursor()
    
    # Проверяем что DBT создал все основные витрины
    required_tables = [
        'dbt_marts.dim_customers',
        'dbt_marts.fct_daily_sales', 
        'dbt_analytics.daily_sales_summary'
    ]
    
    for table in required_tables:
        schema, table_name = table.split('.')
        cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}')")
        if not cur.fetchone()[0]:
            raise AirflowFailException(f"❌ DBT failed to create {table}")
    
    # Проверяем что в витринах есть данные
    cur.execute("SELECT COUNT(*) FROM dbt_marts.dim_customers")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("❌ dim_customers is empty")
    
    cur.close()
    conn.close()
    print("✅ DBT results verified - all tables created with data")


def smart_rollback(**kwargs):

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Умный откат - только при ошибках DBT задач"""
    print("🔍 Analyzing pipeline failures...")
    
    ti = kwargs['ti']
    
    # Получаем статусы выполнения ключевых задач
    dag_run = kwargs['dag_run']
    task_instances = dag_run.get_task_instances()

    # Получаем текущую задачу
    current_task_id = ti.task_id
    print(f"Current task: {current_task_id}")
    
    # Получаем ВСЕ задачи DAG'а
    all_tasks = dag_run.get_task_instances()
    print(f"all_tasks {all_tasks}")
    
    # Находим родителей текущей задачи
    dag = ti.task.dag
    current_task = dag.get_task(current_task_id)
    
    # Родители текущей задачи
    upstream_task_ids = [t.task_id for t in current_task.upstream_list]
    print(f"Parents: {upstream_task_ids}")
    # Для smart_rollback: ['dbt_run', 'dbt_test', 'verify_dbt']
    
    # Дети текущей задачи  
    downstream_task_ids = [t.task_id for t in current_task.downstream_list]
    print(f"Children: {downstream_task_ids}")
    
    # Определяем какие задачи упали
    failed_tasks = []
    dbt_tasks_failed = False
    other_tasks_failed = False
    
    for task_instance in task_instances:
        if task_instance.state == 'failed':
            failed_tasks.append(task_instance.task_id)
            
            # Проверяем тип упавшей задачи
            if any(dbt_keyword in task_instance.task_id for dbt_keyword in ['dbt', 'verify_dbt']):
                dbt_tasks_failed = True
                print(f"❌ DBT-related task failed: {task_instance.task_id}")
            else:
                other_tasks_failed = True
                print(f"⚠️ Non-DBT task failed: {task_instance.task_id}")
    
    print(f"📊 Failure analysis: {len(failed_tasks)} tasks failed")
    print(f"   - DBT tasks failed: {dbt_tasks_failed}")
    print(f"   - Other tasks failed: {other_tasks_failed}")
    
    # Логика умного отката
    if dbt_tasks_failed:
        print("🔄 Performing DBT rollback...")
        try:
            conn = psycopg2.connect(
                host="postgres-dbt",
                database="dbt", 
                user="dbt", 
                password="dbt"
            )
            cur = conn.cursor()
            
            # Удаляем только DBT схемы
            cur.execute("DROP SCHEMA IF EXISTS dbt_staging CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS dbt_marts CASCADE;") 
            cur.execute("DROP SCHEMA IF EXISTS dbt_analytics CASCADE;")
            
            conn.commit()
            cur.close()
            conn.close()
            
            print("✅ DBT rollback completed - DBT schemas removed")
            
        except Exception as e:
            print(f"❌ DBT rollback failed: {e}")
    
    elif other_tasks_failed:
        print("🟡 Skipping rollback - non-DBT failures detected")
        print("💡 Raw data preserved for investigation")
        
    else:
        print("ℹ️ No failures detected or rollback not required")

def send_success_notification(**kwargs):
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Уведомление об успешном выполнении"""
    print("🎉 PIPELINE SUCCESSFUL!")
    print("📊 Reports available in:")
    print("   - dbt_marts.dim_customers (Customer analytics)")
    print("   - dbt_analytics.daily_sales_summary (Sales reports)")

def send_failure_notification(**kwargs):
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")
    """Умное уведомление о провале"""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Анализируем какие задачи упали
    failed_tasks = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == 'failed':
            failed_tasks.append(task_instance.task_id)
    
    print("🚨 PIPELINE FAILED!")
    print(f"📋 Failed tasks: {', '.join(failed_tasks)}")
    
    if any('dbt' in task for task in failed_tasks):
        print("💡 DBT failure - schemas were rolled back")
    else:
        print("💡 Non-DBT failure - raw data preserved for investigation")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_robust_pipeline_3',
    default_args=default_args,
    description='Robust e-commerce pipeline with proper separation of concerns',
    #schedule_interval='@daily',
    schedule_interval = None,
    catchup=False,
    tags=['ecommerce', 'dbt'],
    max_active_runs = 1
) as dag:

    start = DummyOperator(task_id='start')
    
    # Очистка
    cleanup = PythonOperator(
        task_id='cleanup_schemas',
        python_callable=cleanup_schemas,
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    #Инициализация
    init_db = PythonOperator(
        task_id='initialize_database',
        python_callable=init_database,
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    # Валидация
    validate = PythonOperator(
        task_id='validate_source_data',
        python_callable=validate_source_data,
        provide_context=True,  # ✅ Важно для доступа к kwargs
        #queue='python_tasks'  # ← в python_tasks  если не одна очередь нужно добавить еще воркер
    )
    
    # DBT трансформация (все отчеты создаются здесь!)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='''
            echo "🖥️ Task dbt_run executing on: $(hostname)" &&
            cd /opt/airflow/dbt/ecommerce_analytics && 
            dbt run --profiles-dir .
        ''',
        queue='dbt_tasks'  # ← задача пойдет в очередь dbt_tasks
        #provide_context=True  # ✅ Важно для доступа к kwargs Не поддерживается в BashOperator
    ) 
    
    # DBT тестирование
    dbt_test = BashOperator(
        task_id='dbt_test',
         bash_command='''
            echo "🖥️ Task dbt_test executing on: $(hostname)" &&
            sleep 30 &&
            cd /opt/airflow/dbt/ecommerce_analytics && 
            dbt test --profiles-dir .
        ''',
        retries=0,  # ✅ Отключаем повторные попытки
        queue='dbt_tasks'  # ← задача пойдет в очередь dbt_tasks
        #provide_context=True  # ✅ Важно для доступа к kwargs Не поддерживается в BashOperator
    )
    
    # Проверка результатов DBT  # ✅ МЕНЯЕМ: trigger_rule='all_done' чтобы задача выполнялась ВСЕГДА
    verify_dbt = PythonOperator(
        task_id='verify_dbt_success',
        python_callable=verify_dbt_success,
        trigger_rule='all_success',
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    # Уведомления   # ✅ МЕНЯЕМ: trigger_rule='all_success # ← выполняется только если ВСЕ родители успешны
    success_notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    
    smart_rollback_task = PythonOperator(
        task_id='smart_rollback',
        python_callable=smart_rollback,
        trigger_rule='one_failed',
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    # ✅ ЗАМЕНЯЕМ на умное уведомление
    failure_notify = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        trigger_rule='one_failed',
        provide_context=True  # ✅ Важно для доступа к kwargs
    )
    
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done' 
    )
    #Когда у вас есть задача с trigger_rule='all_done' (как ваш end), Airflow выполняет ВСЕ задачи, которые могут быть выполнены, независимо от явных зависимостей.

    # Зависимости
    # start >> cleanup >> init_db >> validate >> dbt_run >> dbt_test >> verify_dbt
    #start >> cleanup >> init_db >> validate >> dbt_run >> dbt_test >> end
    
    #start >> cleanup >> init_db >> validate >> dbt_run >> dbt_test >> verify_dbt
    # Успешный путь
    #verify_dbt >> success_notify >> end
    
    # Путь ошибки
    #verify_dbt >> rollback >> failure_notify >> end



    # ОСНОВНОЙ ПОТОК
    start >> cleanup >> init_db >> validate >> dbt_run >> dbt_test >> verify_dbt

    # УСПЕШНАЯ ВЕТКА
    verify_dbt >> success_notify >> end

    # ВЕТКА ОШИБКИ С УМНЫМ ОТКАТОМ
    #[dbt_run, dbt_test, verify_dbt] >> smart_rollback_task >> failure_notify >> end

    # ВЕТКА ОШИБКИ - ВСЕ задачи ведут в оба места
    [dbt_run, dbt_test, verify_dbt] >> smart_rollback_task
    [dbt_run, dbt_test, verify_dbt] >> failure_notify

    smart_rollback_task >> end
    failure_notify >> end