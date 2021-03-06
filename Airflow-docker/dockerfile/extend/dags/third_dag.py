from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from load_db import load_data
from make_db import make_database



default_args = {
		'owner' : 'airflow',
		'depends_on_past' :False,
		'retries': 1,
		'retry_delay': timedelta(minutes=1)
		}


dag = DAG(
		dag_id='third_dag',
		default_args=default_args,
        start_date=datetime(2022,1,1),
        schedule_interval='* 1 * * * *',
         catchup=False)


task1 = BashOperator(
			task_id='get_data',
			bash_command='python3 ~/airflow/dags/get-data.py' ,
			dag=dag)

task2 = PythonOperator(
			task_id='make_db',
			PythonCallable=make_database ,
			dag=dag)

task3 = PythonOperator(
			task_id='load_db',
			PythonCallable=load_data,
			dag=dag)


task1 >> task2 >> task3

