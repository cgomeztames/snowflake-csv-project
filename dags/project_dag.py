from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pathlib import Path
import pendulum
from include.python_codes.funtions.target import taget_env
from include.python_codes.load_to_stage import snow_data_load
from include.python_codes.customer_service import populate_customer_service
from include.python_codes.daily_delivery import populate_daily_delivery
from include.python_codes.vehicle_usage import populate_vehicle_usage
from include.python_codes.warehouse_inventory import populate_warehouse_inventory


folder = Path("/usr/local/airflow/include/daily_reports")
stage='daily_files'

# Dictionary with all tables
tables_dict = {
    "customer_service_report": populate_customer_service,
    "daily_delivery_report": populate_daily_delivery,
    "vehicle_usage_report": populate_vehicle_usage,
    "warehouse_inventory_report": populate_warehouse_inventory
}


DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/dbt_project")
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/project_env/bin/dbt")

SNOWFLAKE_CONNECTION ="snowflake_conn_id"

_proifile_mapping = SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONNECTION,
        profile_args={
            "database":taget_env['database'],
            "schema":taget_env['schema'],
            "warehouse":taget_env['warehouse']
        }
    )

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    models_relative_path="models"
)

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping= _proifile_mapping
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

@dag(
    dag_id='project_dag',
    schedule="0 3 * * *",
    start_date= pendulum.datetime(2025, 1, 1, tz='Europe/London'),
    catchup=False,
    template_searchpath="/usr/local/airflow/include/sql",
    
)
def project_dag():
    
    # Task: Create tables
    @task_group(group_id="creates_raw_tables_if_not_exists")
    def create_tables():
        tasks = []
        table_list = list(tables_dict.keys())
        for table in table_list:
            task = SQLExecuteQueryOperator(
                task_id=f"create_{table}_table",
                conn_id=SNOWFLAKE_CONNECTION,
                sql=f"create_{table}_table.sql",
                show_return_value_in_logs=True,
                params={
                    "database": taget_env['database'],
                    "schema": taget_env['schema']
                }
            )
            tasks.append(task)

        return tasks 
    
    # Task: Load to stage
    @task()
    def load_stage():
        """Uploads files to Snowflake stage."""
        snow_data_load(folder=folder, stage=stage)

    # Task: Populate tables
    @task_group(group_id="populate_reports")
    def populate_reports():
        tasks = []
        for table_name, populate_function in tables_dict.items():
            task = PythonOperator(
                task_id=f"populate_{table_name}_report",
                python_callable=populate_function,
                op_kwargs={
                    "stage": stage,
                    "stream": taget_env['stream'],
                    "table_name": table_name,
                    "file_format": taget_env['file_format'],
                },
            )

            # Add the task to the list of tasks
            tasks.append(task)

        return tasks
              
    # Task transform data in DBT
    
    transform_data_dbt = DbtTaskGroup(
            group_id='transform_data',
            project_config=_project_config,
            profile_config=_profile_config,
            execution_config=_execution_config
        )

    # Task 4 empty stream
    
    empty_snowflake_stream = SQLExecuteQueryOperator(
        task_id=f"empty_snowflake_stream_{taget_env['stream']}",
        conn_id=SNOWFLAKE_CONNECTION,
        sql=r"""
            use database {{ params.database }};
            use schema {{ params.schema }};
            create temporary table temp_tuncate_stream as
                select * 
                from 
                    {{ params.database }}.{{ params.schema }}.{{ params.stream }} 
                where 
                    1=0;
            """,
        show_return_value_in_logs=True,
        params={
            "database":taget_env['database'],
            "schema":taget_env['schema'],
            "stream":taget_env['stream']
        }
    )
    
    # Task dependency flow
    task_group_create_tables = create_tables()
    task_loaded_files = load_stage()
    task_group_populate_reports = populate_reports()
    empty_snowflake_stream << transform_data_dbt << task_group_populate_reports << task_loaded_files << task_group_create_tables


# Instantiate DAG
project_dag()