#!/usr/bin/env python

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from utils import run_job

today = datetime.today()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': today - timedelta(days=2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('chained_job',
          schedule_interval='@once',
          default_args=default_args)


producer = PythonOperator(
    task_id='run_job_producer',
    python_callable=run_job,
    op_args=('75588', 300, '8edd9e11f4de44b39f666777ac79bfe1'),
    retries=1,
    dag=dag
)

consumer = PythonOperator(
    task_id='run_job_consumer',
    python_callable=run_job,
    op_args=('75588', 300, '8edd9e11f4de44b39f666777ac79bfe1'),
    retries=1,
    dag=dag
)


consumer.set_upstream(producer)