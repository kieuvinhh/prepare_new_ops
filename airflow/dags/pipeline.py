# Import necessary modules from Airflow
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from python_script.from_bronze_to_silver import process as process_bronze
from python_script.from_silver_to_gold import process as process_silver
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from python_script.push_file_into_warehouse import upload_files_to_adls_gen2

# Define the default arguments for the DAG
default_args = {
    'owner': 'vinh',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly',
}

# Define the DAG
with DAG(
    'end-to-end-pipeline',  # DAG ID
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',  # Cron expression or preset like @daily
    start_date=days_ago(1),      # Start date of the DAG
    catchup=False,               # If True, backfill missed intervals
) as dag:

    # Define tasks
    start = DummyOperator(
        task_id='start',  # Task ID
        dag=dag
    )

    process_bronze_to_silver = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=process_bronze,
        dag=dag
    )

    process_silver_to_gold = PythonOperator(
        task_id='process_silver_to_gold',
        python_callable=process_silver,
        dag=dag
    )


    def upload_files_to_adls():
        upload_files_to_adls_gen2(
            connection_string="DefaultEndpointsProtocol=https;AccountName=costcodatastoragegen2;AccountKey=XFCyRFgYIsUBcBequ4aNQdTiKaYE3P7WMOtzgdX31Li3Dtu9y8DGcc1fdjZa8EUz12ja+pvezqGQ+AStLB56SA==;EndpointSuffix=core.windows.net",
            container_name="costcocontainer",
            local_folder_path="/opt/airflow/gold",
            destination_path_in_blob="final_test"
        )


    upload_task = PythonOperator(
        task_id='upload_files',
        python_callable=upload_files_to_adls
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Set task dependencies
start >> process_bronze_to_silver >> process_silver_to_gold >> upload_task >> end
