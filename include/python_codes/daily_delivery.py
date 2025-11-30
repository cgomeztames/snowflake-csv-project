from include.python_codes.funtions.config import snowflake_connection
from include.python_codes.funtions.snow_functions import create_snowpark_conn, files_to_populate_table
from include.python_codes.funtions.target import taget_env
from snowflake.snowpark.functions import col, to_date


SNOW_ACCOUNT=snowflake_connection['account_name']
SNOW_USER=snowflake_connection['user']
SNOW_PASSWORD=snowflake_connection['password']
SNOW_ROLE=snowflake_connection['role']
SNOW_WAREHOUSE=taget_env['warehouse']
SNOW_DATABASE=taget_env['database']
SNOW_SCHEMA=taget_env['schema']



def snow_daily_delivery_report(connection, file_name, stage, file_format, table_name):
    stg_path = f"@{stage}/{file_name}"
    (
        connection.read
        .option("file_format", file_format)
        .option("skip_header", 1)
        .csv(stg_path)
        .select(
            to_date(col('"c1"')).alias("report_date"),
            col('"c2"').alias("delivery_id"),
            col('"c3"').cast("int").alias("order_number"),
            to_date(col('"c4"')).alias("shipment_date"),
            to_date(col('"c5"')).alias("expected_delivery_date"),
            to_date(col('"c6"')).alias("actual_delivery_date"),
            col('"c7"').alias("customer_name"),
            col('"c8"').alias("customer_address"),
            col('"c9"').alias("customer_city"),
            col('"c10"').alias("customer_country"),
            col('"c11"').cast("int").alias("postal_code"),
            col('"c12"').alias("product_name"),
            col('"c13"').alias("product_category"),
            col('"c14"').alias("product_subcategory"),
            col('"c15"').cast("int").alias("quantity"),
            col('"c16"').cast("float").alias("weight_kg"),
            col('"c17"').cast("float").alias("volume_cubic_meters"),
            col('"c18"').alias("vehicle_type"),
            col('"c19"').alias("vehicle_license_plate"),
            col('"c20"').alias("driver_name"),
            col('"c21"').alias("driver_license"),
            col('"c22"').alias("carrier_name"),
            col('"c23"').alias("origin_warehouse_name"),
            col('"c24"').alias("destination_city"),
            col('"c25"').alias("route_code"),
            col('"c26"').alias("status"),
            col('"c27"').cast("float").alias("delivery_time_hours"),
            col('"c28"').cast("float").alias("distance_km"),
            col('"c29"').cast("float").alias("fuel_used_liters"),
            col('"c30"').cast("float").alias("delivery_cost_usd"),
            col('"c31"').cast("float").alias("insurance_cost_usd"),
            col('"c32"').cast("float").alias("late_delivery_penalty_usd"),
            col('"c33"').alias("traffic_condition"),
            col('"c34"').alias("weather_condition"),
            col('"c35"').alias("driver_notes"),
            col('"c36"').alias("customer_feedback")
        )
        .write.mode('append').save_as_table(table_name)
    )

def populate_daily_delivery(
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
            snow_daily_delivery_report(connection=snow_conn, file_name=file, stage=stage, file_format=file_format, table_name=table_name)
            print(f"File {file} was uploaded successfully into {db}.{schema}.{table_name}")