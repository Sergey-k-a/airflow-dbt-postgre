from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import sys
import os
import psycopg2
import socket 

DB_CONFIG = {
    'host': 'postgres-dbt',
    'database': 'dbt',
    'user': 'dbt',
    'password': 'dbt'
}

sys.path.append('/opt/airflow/scripts')

def cleanup_schemas(**kwargs):

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Очистка витрин перед запуском"""
    print("🧹 Очистка витрин перед запуском...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("DROP SCHEMA IF EXISTS dbt_staging CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS dbt_marts CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS dbt_analytics CASCADE;")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Схемы очищены")
    except Exception as e:
        print(f"❌ Очистка упала: {e}")
        raise

def init_database(**kwargs):

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    # ОТЛАДКА:
    # print(f"📁 Current sys.path: {sys.path}")
    # print(f"📁 Files in /opt/airflow/scripts: {os.listdir('/opt/airflow/scripts')}")

    """Инициализация БД"""
    print("🚀 Инициализация базы данных...")
    try:
        from init_database import init_database as init_db_func
        init_db_func()
        print("✅ База данных успешно инициализирована.")
    except ImportError as e:
        print(f"❌ Невозможно импортировать init_database: {e}")
        raise

def validate_source_data(**kwargs):
    """Валидация исходных данных"""

    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")
    
    print("🔍 Проверка качества исходных данных...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Быстрая проверка наличия данных
    cur.execute("SELECT COUNT(*) FROM raw.customers")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("❌ Нет данных в таблице raw.customers")
    
    cur.execute("SELECT COUNT(*) FROM raw.orders WHERE status = 'completed'")
    if cur.fetchone()[0] == 0:
        raise AirflowFailException("❌ Нет завершенных заказов completed в raw.orders ")
    
    cur.close()
    conn.close()
    print("✅ Проверка исходных данных пройдена")

def verify_dbt_success(**kwargs):
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")
    print("🔍 Верификация dbt результатов...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    required_tables = {
        'dbt_marts.dim_customers': 'Customer dimension',
        'dbt_marts.fct_daily_sales': 'Daily sales fact',
        'dbt_marts.fct_orders_incremental': 'Orders fact',
        'dbt_analytics.daily_sales_summary': 'Sales summary',
        'dbt_analytics.customer_cohorts': 'Customer cohorts',
        'dbt_staging.stg_customers': 'Staging customers',
        'dbt_staging.stg_orders': 'Staging orders',
        'dbt_staging.stg_products': 'Staging products'
    }
    
    missing_tables = []
    empty_tables = []
    
    for table, description in required_tables.items():
        schema, table_name = table.split('.')
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            )
        """)
        
        if not cur.fetchone()[0]:
            missing_tables.append(f"  ❌ {table} ({description})")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            status = "✅" if count > 0 else "⚠️"
            print(f"  {status} {table}: {count} rows")
            if count == 0:
                empty_tables.append(table)
    
    cur.close()
    conn.close()
    
    if missing_tables:
        raise AirflowFailException(
            f"❌ Отсутствуют таблицы:\n" + "\n".join(missing_tables)
        )
    
    if empty_tables:
        print(f"⚠️ Пустые таблицы: {', '.join(empty_tables)}")
    
    print("✅ DBT результаты верифицированы")


def smart_rollback(**kwargs):
    """Откат DBT схем при ошибках."""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Получаем упавшие задачи
    failed_tasks = [
        t.task_id for t in dag_run.get_task_instances()
        if t.state == 'failed'
    ]
    
    dbt_failed = any('dbt' in t or 'verify_dbt' in t for t in failed_tasks)
    
    if not dbt_failed:
        print("🟡 Сбой, не связанный с DBT")
        return
    
    print("🔄 Откатываем изменения dbt")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for schema in ['dbt_staging', 'dbt_marts', 'dbt_analytics']:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        conn.commit()
        print("✅ DBT схемы удалены")
    except Exception as e:
        print(f"❌ Ошибка в откате изменений dbt: {e}")
    finally:
        cur.close()
        conn.close()

def send_success_notification(**kwargs):
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")

    """Уведомление об успешном выполнении"""
    print("🎉 PIPELINE успешен!")

def send_failure_notification(**kwargs):
    print(f"🖥️ Task {kwargs['ti'].task_id} executing on: {socket.gethostname()}")
    """Уведомление о провале"""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Анализируем какие задачи упали
    failed_tasks = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == 'failed':
            failed_tasks.append(task_instance.task_id)
    
    print("🚨 PIPELINE упал!")
    print(f"📋 Упавшие задачи: {', '.join(failed_tasks)}")
    
    if any('dbt' in task for task in failed_tasks):
        print("💡 Провал - схемы откачены")
    else:
        print("💡 Нет dbt провала - Исходные данные сохранены")

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
    'ecommerce_pipeline_modern',
    default_args=default_args,
    description='Robust e-commerce pipeline with proper separation of concerns',
    #schedule_interval='@daily',
    schedule_interval = None,
    catchup=False,
    tags=['ecommerce', 'dbt'],
) as dag:

    start = DummyOperator(task_id='start')
    
    # Очистка
    cleanup = PythonOperator(
        task_id='cleanup_schemas',
        python_callable=cleanup_schemas,
        provide_context=True  
    )
    
    #Инициализация
    init_db = PythonOperator(
        task_id='initialize_database',
        python_callable=init_database,
        provide_context=True  
    )
    
    # Валидация
    validate = PythonOperator(
        task_id='validate_source_data',
        python_callable=validate_source_data,
        provide_context=True,  
        #queue='python_tasks' 
    )
    
    # DBT трансформация 
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='''
            echo "🖥️ Task dbt_run executing on: $(hostname)" &&
            cd /opt/airflow/dbt/ecommerce_analytics_modern && 
            dbt build --profiles-dir .
        ''',
        queue='dbt_tasks',  # ← задача пойдет в очередь dbt_tasks
        retries=0
    ) 
    
    # DBT тестирование
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='''
            echo "🖥️ Task dbt_test executing on: $(hostname)" &&
            cd /opt/airflow/dbt/ecommerce_analytics_modern && 
            dbt test --profiles-dir .
        ''',
        retries=0,
        queue='dbt_tasks'
    )
    
    # Проверка результатов DBT 
    verify_dbt = PythonOperator(
        task_id='verify_dbt_success',
        python_callable=verify_dbt_success,
        trigger_rule='all_success',
        provide_context=True  
    )
    
    success_notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
        provide_context=True  # доступ к kwargs
    )
    
    
    smart_rollback_task = PythonOperator(
        task_id='smart_rollback',
        python_callable=smart_rollback,
        trigger_rule='one_failed',
        provide_context=True  # доступ к kwargs
    )
    
    # уведомление
    failure_notify = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        trigger_rule='one_failed',
        provide_context=True  # доступ к kwargs
    )
    
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done' 
    )
    
     # ОСНОВНОЙ ПОТОК
    start >> cleanup >> init_db >> validate >> dbt_run >> dbt_test >> verify_dbt

    # УСПЕШНАЯ ВЕТКА
    verify_dbt >> success_notify >> end

    # ВЕТКА ОШИБКИ
    [dbt_run, dbt_test, verify_dbt] >> smart_rollback_task >> failure_notify >> end