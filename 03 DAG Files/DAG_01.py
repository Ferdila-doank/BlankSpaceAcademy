from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import bash_operator

from datetime import datetime

with DAG('DAG_01', description='Python DAG', schedule_interval='23 23 * * *', start_date=datetime(2021, 2, 22), catchup=False) as dag:
	python_task1 = bash_operator.BashOperator(
        task_id='python_task1',
        # This example runs a Python script from the data folder to prevent
        # Airflow from attempting to parse the script as a DAG.
        bash_command='python3 /home/airflow/gcs/data/GCP/AccessFolder.py',
	)