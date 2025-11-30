from airflow.decorators import dag, task
import pendulum
from pathlib import Path
import logging

logger = logging.getLogger("airflow.task")
folder = Path("/usr/local/airflow/daily_reports")

@dag(
    dag_id='files_task_dag',
    schedule="0 10 * * *",
    start_date= pendulum.datetime(2025, 1, 1, tz='Europe/Paris'),
    catchup=False 
)

def files_task_dag():
    @task
    def print_file_names():
        logger.info("All file names in the folder:")
        for path in folder.iterdir():  
            logger.info(path.stem)
        logger.info("Loop finished")
    @task        
    def print_file_path():
        for path in folder.iterdir():
            logger.info(path)
    
    print_file_names() >> print_file_path()

files_task_dag()
            
    