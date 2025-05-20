import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Hisham',
    'start_date': dt.datetime(2025, 5, 20),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

with DAG('Scheduler_Traffict_Accident',
         default_args=default_args,
         schedule_interval='*/15 * * * *',
         catchup=False,
         ) as dag:

    Extract_Data = BashOperator(task_id='Extract_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Extract_Data.py')
    Transform_Data = BashOperator(task_id='Transform_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Transform_Data.py')
    Load_Data = BashOperator(task_id='Load_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Load_Data.py')
    
Extract_Data >> Transform_Data >> Load_Data
