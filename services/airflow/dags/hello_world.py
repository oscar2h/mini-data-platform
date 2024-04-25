from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hola_mundo():
    print("Hola Mundo")

dag = DAG(
    'hola_mundo',
    description='Mi primer DAG en Airflow',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
    catchup=False
)

tarea_hola_mundo = PythonOperator(
    task_id='tarea_hola_mundo',
    python_callable=hola_mundo,
    dag=dag
)