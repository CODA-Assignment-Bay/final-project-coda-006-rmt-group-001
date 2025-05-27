import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'Hisham',
    'start_date': dt.datetime(2025, 5, 20),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

with DAG('Datamart_Traffict_Accident',
         default_args=default_args,
         schedule_interval='*/15 * * * *',
         catchup=False,
         ) as dag:

    Extract_Datamart_Data = BashOperator(task_id='Extract_Datamart_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Extract_Datamart_Data.py')
    Transform_Datamart_Data = BashOperator(task_id='Transform_Datamart_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Transform_Datamart_Data.py')
    Validation_Datamart_Data = BashOperator(task_id='Validation_Datamart_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/validation-datamart.py')
    Load_Datamart_Data = BashOperator(task_id='Load_Datamart_Data', bash_command='sudo -u airflow python /opt/airflow/scripts/Load_Datamart_Data.py')
    
    
Extract_Datamart_Data >> Transform_Datamart_Data >> Validation_Datamart_Data >> Load_Datamart_Data
