from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_csv_dag',
    default_args=default_args,
    description='DAG to download and move CSV file',
    schedule_interval='@daily',
)

# Define tasks
download_csv_task = BashOperator(
    task_id='download_csv',
    bash_command='wget -O /opt/airflow/Proyectos/input/Churn_Modelling-1.csv https://raw.githubusercontent.com/xploiterx/datasets/master/Proyect-0/CSV/Churn_Modelling-1.csv',
    dag=dag,
)

cp_csv_task = BashOperator(
    task_id='cp_csv',
    bash_command='cp /opt/airflow/Proyectos/input/Churn_Modelling-1.csv /opt/airflow/notebooks',
    dag=dag,
)


# Define task dependencies
download_csv_task >> cp_csv_task
