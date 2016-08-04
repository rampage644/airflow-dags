#!/usr/bin/env python

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, time

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

dag = DAG('simple_job',
          schedule_interval='@once',
          default_args=default_args)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = PythonOperator(
    task_id='run_job',
    python_callable=run_job,
    op_args=('75588', 300, '8edd9e11f4de44b39f666777ac79bfe1'),
    retries=1,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)