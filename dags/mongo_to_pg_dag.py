import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient

# Настройки подключений (внутри Docker-сети)
MONGO_URI = "mongodb://admin:admin_password@mongodb:27017/"
MONGO_DB = "etl_source"
PG_CONN_ID = "postgres_default" # Дефолтный коннекшн Airflow

default_args = {
    'owner': 's.tyurin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load_sessions(**kwargs):
    """
    Извлечение данных из MongoDB, трансформация и загрузка в PostgreSQL (схема ods).
    Включает дедупликацию (UPSERT) и приведение типов.
    """
    logger = logging.getLogger("airflow.task")
    
    # 1. Extract (MongoDB)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    sessions_data = list(db.UserSessions.find({}, {"_id": 0})) # Исключаем внутренний _id
    
    if not sessions_data:
        logger.info("Нет данных для репликации.")
        return

    # 2. Transform & Load (PostgreSQL)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    insert_query = """
        INSERT INTO ods.user_sessions (session_id, user_id, start_time, end_time, pages_visited, device, actions)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_id, start_time) DO UPDATE SET
            end_time = EXCLUDED.end_time,
            pages_visited = EXCLUDED.pages_visited,
            actions = EXCLUDED.actions;
    """
    
    records_to_insert = []
    for doc in sessions_data:
        # Трансформация: MongoDB хранит даты как строки (ISO), PG нужен timestamp. 
        # Массивы python корректно ложатся в массивы PG через драйвер psycopg2.
        records_to_insert.append((
            doc.get('session_id'),
            doc.get('user_id'),
            doc.get('start_time'),
            doc.get('end_time'),
            doc.get('pages_visited', []),
            doc.get('device'),
            doc.get('actions', [])
        ))

    # Выполняем батч-инсерт
    pg_hook.insert_rows(
        table="ods.user_sessions",
        rows=records_to_insert,
        target_fields=["session_id", "user_id", "start_time", "end_time", "pages_visited", "device", "actions"],
        replace=True,
        replace_index=["session_id", "start_time"]
    )
    logger.info(f"Успешно обработано {len(records_to_insert)} записей сессий.")

with DAG(
    'mongodb_to_postgres_etl',
    default_args=default_args,
    description='ETL пайплайн для итогового проекта',
    schedule_interval='@daily',
    catchup=False,
    tags=['hse_project', 'etl'],
) as dag:

    # Задача 1: ETL сессий (можно написать аналогичную для SupportTickets)
    etl_sessions_task = PythonOperator(
        task_id='extract_load_sessions',
        python_callable=extract_and_load_sessions,
    )

    # Задача 2: Построение Витрины 1 (Активность пользователей)
    create_mart_activity = PostgresOperator(
        task_id='create_mart_user_activity',
        postgres_conn_id=PG_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS marts.dm_user_activity;
            CREATE TABLE marts.dm_user_activity AS
            SELECT 
                user_id,
                COUNT(session_id) AS total_sessions,
                SUM(EXTRACT(EPOCH FROM (end_time::timestamp - start_time::timestamp))/60) AS total_minutes_spent,
                MODE() WITHIN GROUP (ORDER BY device) AS preferred_device
            FROM ods.user_sessions
            GROUP BY user_id;
        """,
    )

    # Задача 3: Построение Витрины 2 (Анализ действий - разворачиваем массив действий)
    create_mart_actions = PostgresOperator(
        task_id='create_mart_popular_actions',
        postgres_conn_id=PG_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS marts.dm_popular_actions;
            CREATE TABLE marts.dm_popular_actions AS
            SELECT 
                action,
                COUNT(*) as action_count
            FROM ods.user_sessions, 
            UNNEST(actions) AS action
            GROUP BY action
            ORDER BY action_count DESC;
        """,
    )

    # Настраиваем зависимости: сначала грузим сырые данные, потом параллельно строим витрины
    etl_sessions_task >> [create_mart_activity, create_mart_actions]
