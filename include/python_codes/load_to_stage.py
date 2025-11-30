from include.python_codes.funtions.config import snowflake_connection
from include.python_codes.funtions.snow_functions import create_snowpark_conn
from include.python_codes.funtions.target import taget_env
from pathlib import Path

SNOW_ACCOUNT=snowflake_connection['account_name']
SNOW_USER=snowflake_connection['user']
SNOW_PASSWORD=snowflake_connection['password']
SNOW_ROLE=snowflake_connection['role']
SNOW_WAREHOUSE=taget_env['warehouse']
SNOW_DATABASE=taget_env['database']
SNOW_SCHEMA=taget_env['schema']

def files_to_load(connection, folder_path:Path, stage):
    folder = folder_path
    files_list = [[str(file), f"{file.stem}{file.suffix}"]  for file in folder.iterdir() if file.is_file() and file.suffix == '.csv']
    stage_files = list(connection._list_files_in_stage(stage_location=stage))
    files_to_upload = [file for file in files_list if file[1] not in stage_files]
    return files_to_upload

def load_files_to_stage(connection, stage:str, path:str):
    connection.file.put(
        local_file_name=path, 
        stage_location=stage,
        auto_compress=False
    )
    print(f"'File '{Path(path).stem}' was uploaded successfully to the internal stage {stage}.")
    
def snow_data_load(
    folder:Path, 
    stage:str,
    account_name:str=SNOW_ACCOUNT, 
    user:str=SNOW_USER, 
    password:str=SNOW_PASSWORD, 
    role:str=SNOW_ROLE, 
    wh:str=SNOW_WAREHOUSE, 
    db:str=SNOW_DATABASE, 
    schema:str=SNOW_SCHEMA
):
    with create_snowpark_conn(account_name, user, password, role, wh, db, schema) as snow_conn:
        files_list = files_to_load(connection=snow_conn, folder_path=folder, stage=stage)
        for file in files_list:
            load_files_to_stage(connection=snow_conn, stage=stage, path=file[0])
        print('All files were succesfully loaded to the stage')
        
        snow_conn.sql("alter stage daily_files refresh;").collect()
        
        print("The stream has been refreshed")