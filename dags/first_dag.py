from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

def pobierz_i_zapisz_walute(symbol):
    # 1. Pobieramy dane z API
    url = f"http://api.nbp.pl/api/exchangerates/rates/a/{symbol}/?format=json"
    response = requests.get(url).json()
    kurs = response['rates'][0]['mid']
    data_kursu = response['rates'][0]['effectiveDate']

    # 2. Używamy Hooka, żeby wysłać dane do bazy
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    insert_sql = """
        INSERT INTO kursy_walut (waluta, kurs, data_kursu, data_zapisu) 
        VALUES (%s, %s, %s, NOW() + interval '1 hour')
        ON CONFLICT (waluta, data_kursu) 
        DO UPDATE SET 
            kurs = EXCLUDED.kurs,
            data_zapisu = EXCLUDED.data_zapisu;
    """
    pg_hook.run(insert_sql, parameters=(symbol, kurs, data_kursu))

with DAG(
    dag_id='nbp_to_postgres_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    # Zadanie 1: Tworzymy tabelę (Czysty SQL)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table_task',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS kursy_walut (
                id SERIAL PRIMARY KEY,
                waluta VARCHAR(10),
                kurs DECIMAL(10, 4),
                data_kursu DATE,
                data_zapisu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # Zadanie 2: Pobieramy i zapisujemy (Python)
    load_data = PythonOperator(
        task_id='pobierz_i_zapisz_task',
        python_callable=pobierz_i_zapisz_euro
    )


 # Zadanie 3: Transformacja - flaga "Drogo/Tanio"
    transform_data = SQLExecuteQueryOperator(
        task_id='transform_task',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS raport_kursow AS 
            SELECT 
                waluta, 
                kurs, 
                data_kursu,
                CASE WHEN kurs > 4.30 THEN 'DROGO' ELSE 'TANIO' END as status
            FROM kursy_walut;
        """
    )
    # Nowa kolejność zadań
    create_table >> load_data >> transform_data

    




    