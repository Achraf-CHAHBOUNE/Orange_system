from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
from utils.logger import setup_logging

# Initialize logger
logger = setup_logging('ETL_Pipeline', log_dir='./logs/Manager')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

def check_extraction_completion():
    last_extracted_path = '/opt/airflow/data/last_extracted.json'
    
    logger.info(f"Checking for last_extracted.json at {last_extracted_path}")
    
    # Check if file exists
    if not os.path.exists(last_extracted_path):
        logger.warning(f"File {last_extracted_path} does not exist. Assuming no tables to process.")
        print("No extraction data found. Proceeding with pipeline.")
        return
    
    # Check if file is empty
    if os.path.getsize(last_extracted_path) == 0:
        logger.warning(f"File {last_extracted_path} is empty. Assuming no tables to process.")
        print("Empty extraction data. Proceeding with pipeline.")
        return
    
    try:
        with open(last_extracted_path, 'r') as f:
            logger.info(f"Reading contents of {last_extracted_path}")
            content = f.read().strip()
            if not content:
                logger.warning(f"File {last_extracted_path} contains only whitespace. Assuming no tables to process.")
                print("Whitespace-only extraction data. Proceeding with pipeline.")
                return
            logger.info(f"File contents: {content}")
            f.seek(0)  # Reset file pointer for json.load
            data = json.load(f)
        
        logger.info(f"Loaded last_extracted.json: {data}")
        if not data:
            logger.warning(f"last_extracted.json is an empty dictionary. Assuming no tables to process.")
            print("No tables found in extraction data. Proceeding with pipeline.")
            return
        
        # Check if any tables were not fully extracted
        for table, info in data.items():
            if not info.get('completed', False):
                logger.error(f"Table {table} not fully extracted: {info}")
                raise ValueError(f"Table {table} not fully extracted")
        
        logger.info("All tables fully extracted")
        print("All tables fully extracted")
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse {last_extracted_path}: {str(e)}")
        raise ValueError(f"Failed to parse {last_extracted_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Extraction check failed: {str(e)}")
        raise Exception(f"Extraction check failed: {str(e)}")
    
    logger.info("Extraction check completed successfully")

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline triggering existing Docker containers',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['etl'],
) as dag:

    check_containers = BashOperator(
        task_id='check_containers',
        bash_command='docker ps | grep -q etl-extractor- && docker ps | grep -q etl-transformer- || { echo "Container check failed"; exit 1; } && echo "Containers etl-extractor- and etl-transformer- are running"',
        do_xcom_push=True,
        on_success_callback=lambda context: logger.info(f"check_containers succeeded: {context['task_instance'].xcom_pull(task_ids='check_containers')}"),
        on_failure_callback=lambda context: logger.error(f"check_containers failed: {context['exception']}"),
    )

    run_extractor = BashOperator(
        task_id='run_extractor',
        bash_command='docker exec etl-extractor-1 python /app/main.py && echo "Extractor execution completed"',
        do_xcom_push=True,
        on_success_callback=lambda context: logger.info(f"run_extractor succeeded: {context['task_instance'].xcom_pull(task_ids='run_extractor')}"),
        on_failure_callback=lambda context: logger.error(f"run_extractor failed: {context['exception']}"),
    )

    check_extraction = PythonOperator(
        task_id='check_extraction',
        python_callable=check_extraction_completion,
        on_success_callback=lambda context: logger.info("check_extraction succeeded"),
        on_failure_callback=lambda context: logger.error(f"check_extraction failed: {context['exception']}"),
    )

    run_transformer = BashOperator(
        task_id='run_transformer',
        bash_command='docker exec etl-transformer-1 python /app/main.py && echo "Transformer execution completed"',
        do_xcom_push=True,
        on_success_callback=lambda context: logger.info(f"run_transformer succeeded: {context['task_instance'].xcom_pull(task_ids='run_transformer')}"),
        on_failure_callback=lambda context: logger.error(f"run_transformer failed: {context['exception']}"),
    )

    clear_intermediate_table = BashOperator(
        task_id='clear_intermediate_table',
        bash_command='docker exec etl-mysql-1 mysql -uroot -p${MYSQL_ROOT_PASSWORD} -e "SET FOREIGN_KEY_CHECKS=0; DROP DATABASE IF EXISTS 5min_transform; CREATE DATABASE 5min_transform; SET FOREIGN_KEY_CHECKS=1;" && echo "Intermediate table cleared"',
        do_xcom_push=True,
        on_success_callback=lambda context: logger.info(f"clear_intermediate_table succeeded: {context['task_instance'].xcom_pull(task_ids='clear_intermediate_table')}"),
        on_failure_callback=lambda context: logger.error(f"clear_intermediate_table failed: {context['exception']}"),
    )

    check_containers >> run_extractor >> check_extraction >> run_transformer >> clear_intermediate_table