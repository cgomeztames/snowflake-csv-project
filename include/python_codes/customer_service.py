from include.python_codes.funtions.config import snowflake_connection
from include.python_codes.funtions.snow_functions import create_snowpark_conn, files_to_populate_table
from include.python_codes.funtions.target import taget_env
from snowflake.snowpark.functions import col, to_date, to_decimal


SNOW_ACCOUNT=snowflake_connection['account_name']
SNOW_USER=snowflake_connection['user']
SNOW_PASSWORD=snowflake_connection['password']
SNOW_ROLE=snowflake_connection['role']
SNOW_WAREHOUSE=taget_env['warehouse']
SNOW_DATABASE=taget_env['database']
SNOW_SCHEMA=taget_env['schema']



def snow_customer_service(connection, file_name, stage, file_format, table_name):
    stg_path = f"@{stage}/{file_name}"
    (
        connection.read
        .option("file_format", file_format)
        .option("skip_header", 1)
        .csv(stg_path)
        .select(
            to_date(col('"c1"')).alias("report_date"),
            col('"c2"').alias("ticket_id"),
            col('"c3"').alias("delivery_id"),
            col('"c4"').alias("customer_name"),
            col('"c5"').alias("issue_type"),
            to_decimal(col("$6"), 10, 2).alias("resolution_time_hours"),
            col('"c7"').alias("status"),
            col('"c8"').alias("agent_name"),
            col('"c9"').alias("agent_notes")
        )
        .write.mode('append').save_as_table(table_name)
    )

def populate_customer_service(
    stage:str,
    stream:str,
    table_name:str,
    file_format:str,
    account_name:str=SNOW_ACCOUNT, 
    user:str=SNOW_USER, 
    password:str=SNOW_PASSWORD, 
    role:str=SNOW_ROLE, 
    wh:str=SNOW_WAREHOUSE, 
    db:str=SNOW_DATABASE, 
    schema:str=SNOW_SCHEMA
):
    with create_snowpark_conn(account_name, user, password, role, wh, db, schema) as snow_conn:
        files = files_to_populate_table(connection=snow_conn, stream=stream, report_name=table_name)
        snow_conn.sql(f"truncate table {table_name};").collect()
        for file in files:
            snow_customer_service(connection=snow_conn, file_name=file, stage=stage, file_format=file_format, table_name=table_name)
            print(f"File {file} was uploaded successfully into {db}.{schema}.{table_name}")