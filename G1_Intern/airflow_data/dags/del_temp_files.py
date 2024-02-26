from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import logging
import pendulum

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')

default_args = {
    "owner": "nbeyger",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 19, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "tags": ["bnm_de", "del_temp"],
}

path = '/tmp/airflow_staging'

@dag(default_args=default_args, description="Deletes temporary files from made in process of work of DAG", catchup=False, schedule_interval = timedelta(hours = 2))
def del_temp_files():
    bash_command = f'find {path} -name "*.csv" -type f -mmin +120 -delete'
    
    delete_temp_files = BashOperator(
        task_id='del_temp_files', 
        bash_command=bash_command)
        
        
dag_instance = del_temp_files()