"""Dag to perform ETL job."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utilities.data_transform import data_transform
from utilities.wiki_file import extract_wiki_file
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 27),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
    }


with DAG('python_dag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=15)) as dag:

    t1 = PythonOperator(
            task_id='extract_wiki_file',
            python_callable=extract_wiki_file,
            dag=dag)

    t2 = PythonOperator(
            task_id='data_transform',
            python_callable=data_transform,
            dag=dag)
    
    t1 >> t2