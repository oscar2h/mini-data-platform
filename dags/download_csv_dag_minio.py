import os
from datetime import datetime, timedelta
from minio import Minio

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Configuración de Minio
def get_minio_client():
    minio_client = Minio(
        endpoint="minio:9000",
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False
    )
    return minio_client

def upload_to_minio(**kwargs):
    minio_client = get_minio_client()
    bucket = 'csv-bucket'
    file_path = '/opt/airflow/notebooks/Churn_Modelling-1.csv'
    object_name = 'Churn_Modelling-1.csv'
    
    # Verificar si el bucket existe, si no, crearlo
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    
    # Subir el archivo
    minio_client.fput_object(bucket, object_name, file_path, content_type='application/csv')

# Configuración por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'download_csv_dag_minio',
    default_args=default_args,
    description='DAG to download, move, and upload CSV file to Minio',
    schedule_interval=None,
)

# Tarea para descargar el archivo CSV
download_csv_task = BashOperator(
    task_id='download_csv',
    bash_command='wget -O /opt/airflow/Proyectos/input/Churn_Modelling-1.csv https://raw.githubusercontent.com/xploiterx/datasets/master/Proyect-0/CSV/Churn_Modelling-1.csv',
    dag=dag,
)

# Tarea para copiar el archivo CSV a otro directorio
cp_csv_task = BashOperator(
    task_id='cp_csv',
    bash_command='cp /opt/airflow/Proyectos/input/Churn_Modelling-1.csv /opt/airflow/notebooks',
    dag=dag,
)

# Tarea para subir el archivo CSV a Minio
upload_csv_task = PythonOperator(
    task_id='upload_csv_to_minio',
    provide_context=True,
    python_callable=upload_to_minio,
    dag=dag,
)

# Definir las dependencias entre las tareas
download_csv_task >> cp_csv_task >> upload_csv_task