from azure.storage.blob import BlobServiceClient, ContainerClient
from src.connectors.data_connector import FileDataConnector
import os

class AzureBlobConnector(FileDataConnector):
    def __init__(self, connection_str: str, container: str) -> None:
        self.connection_str = connection_str
        self.container = container
        self.__connect(connection_str=connection_str, container=container)
        
    def __connect(self, connection_str: str, container: str):
        self.blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(connection_str)
        self.container_client: ContainerClient = self.blob_service_client.get_container_client(container)

    def list_files(self, prefix: str) -> list:
        blob_list = self.container_client.list_blobs(name_starts_with=prefix)
        return [ blob.name for blob in blob_list ]

    def __get_dowloands_path(self):
        return os.getenv("DOWNLOAD_PATH_FROM_ROOT") + "/"

    def download_file_if_not_exists(self, blob_name: str):
        local_file_name = self.__get_dowloands_path() + blob_name
        if os.path.exists(local_file_name):
            print(f"file {local_file_name} found locally")
            return local_file_name
        
        print(f"Downloading file: {blob_name}")
        blob_client_instance = self.blob_service_client.get_blob_client(self.container, blob_name, snapshot=None)
        blob_data = blob_client_instance.download_blob()
        os.makedirs(os.path.dirname(local_file_name), exist_ok=True)
        with open(local_file_name, "wb") as file:
            file.write(blob_data.readall())
            print(f"File {blob_name} downloaded")
            return file.name
    

