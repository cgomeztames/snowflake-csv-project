from snowflake.snowpark import Session
import polars as pl

def create_snowpark_conn(
        account_name:str, 
        user:str, 
        password:str,
        role:str,
        wh:str,
        db:str,
        schema:str
    ):
    session_params={
        'account':account_name,
        'user':user,
        'password':password,
        'role': role,
        'warehouse': wh,
        'database': db,
        'schema': schema
    }
    new_session=Session.builder.configs(session_params).create()
    new_session.sql_simplifier_enable=True
    return new_session

def files_to_populate_table(connection, stream:str, report_name:str):
    connection.sql("alter stage daily_files refresh;").collect()
    stage_files = pl.from_pandas(connection.sql(f"select * from {stream} where metadata$action = 'INSERT'").to_pandas())
    files_to_load = (
        stage_files
        .filter(pl.col('RELATIVE_PATH').str.starts_with(report_name))
        .select('RELATIVE_PATH')
        .to_series()
        .to_list()
    )
    return files_to_load