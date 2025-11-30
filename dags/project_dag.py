from airflow.decorators import dag, task
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
    catchup=False
)
def project_dag():
    
    # Task 1: Load to stage
    @task()
    def load_stage():
        """Uploads files to Snowflake stage."""
        snow_data_load(folder=folder, stage=stage)

    # Task 2: Populate tables
    @task()
    def populate_customer():
        populate_customer_service(
            stage=stage,
            stream=taget_env['stream'],
            table_name="customer_service_report",
            file_format=taget_env['file_format'],
        )

    @task()
    def populate_delivery():
        populate_daily_delivery(
            stage=stage,
            stream=taget_env['stream'],
            table_name="daily_delivery_report",
            file_format=taget_env['file_format'],
        )

    @task()
    def populate_vehicle():
        populate_vehicle_usage(
            stage=stage,
            stream=taget_env['stream'],
            table_name="vehicle_usage_report",
            file_format=taget_env['file_format'],
        )

    @task()
    def populate_inventory():
        populate_warehouse_inventory(
            stage=stage,
            stream=taget_env['stream'],
            table_name="warehouse_inventory_report",
            file_format=taget_env['file_format'],
        )
        
    # Task 3 transform data in DBT
    
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
    loaded_files = load_stage()
    populate_task_group = [populate_customer(), populate_delivery(), populate_vehicle(), populate_inventory()]
    empty_snowflake_stream << transform_data_dbt << populate_task_group << loaded_files


# Instantiate DAG
project_dag()