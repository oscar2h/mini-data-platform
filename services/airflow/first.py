from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'email' : ['airflow@xploiter.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),

}

def scrape():
    logging.info("perfoming scraping")

def process():
    logging.info("perfoming processing")

def save():
    logging.info("perfoming saving")

with DAG(
    'first',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['exmaple'],
) as dag:
    scrape_task = PythonOperator(task_id="scrape", python_callable=scrape)
    process_task = PythonOperator(task_id="process", python_callable=process)
    save_task = PythonOperator(task_id="save", python_callable=save)

    scrape_task >> process_task >> save_task




