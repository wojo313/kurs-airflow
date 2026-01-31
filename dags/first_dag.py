from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

# 1. Definiujemy funkcję, którą wykona Python
def pobierz_kurs_euro():
    url = "http://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json"
    response = requests.get(url)
    data = response.json()
    kurs = data['rates'][0]['mid']
    print(f"Aktualny kurs Euro to: {kurs} PLN")

# 2. Definiujemy strukturę DAGa
with DAG(
    dag_id='fisrt_dag_nbp', # Nazwa widoczna w panelu
    start_date=datetime(2024, 1, 1),
    schedule='@daily',    # Jak często ma się odpalać
    catchup=False                  # Nie nadrabiaj zaległych dni
) as dag:

    # 3. Definiujemy zadanie (Task)
    zadanie_pobierania = PythonOperator(
        task_id='pobierz_euro_task',
        python_callable=pobierz_kurs_euro
    )