## ----- Imports ----- 
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from python_codes.funtions.config import azure_connection

## ----- Functions ----- 
def blob_list(container_conn):
    blobs_in_az = container_conn.list_blobs()
    return [blob.name for blob in blobs_in_az]

def upload_blob_to_container(container_conn, file_path:Path, blob_name:str):
    with open(file_path, "rb") as data:
        container_conn.upload_blob(name=blob_name, data=data, overwrite=True)
    print(f"'File '{blob_name}' was uploaded successfully to the container")
    
## ----- Connect to Blob Service ----- 
blob_service_client = BlobServiceClient.from_connection_string(azure_connection['connection_string'])
container_client = blob_service_client.get_container_client(container=azure_connection['container_name'])

## ----- Extracts all file paths ----- 
folder = Path(r"D:\Portfolio projects\Snowflake warehouse from csv files\daily_reports")
files_list = [[file, f"{file.stem}{file.suffix}"]  for file in folder.iterdir() if file.is_file() and file.suffix == '.csv']

## ----- Identifies what has not been uploaded yet ----- 
uploaded_files = blob_list(container_conn=container_client)
files_to_upload = [file for file in files_list if file[1] not in uploaded_files]

## ----- Uploads files ----- 
for file in files_to_upload:
    upload_blob_to_container(container_conn=container_client, file_path=file[0], blob_name=file[1])