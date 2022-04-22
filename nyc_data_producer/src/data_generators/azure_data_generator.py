from typing import Dict, Iterator
import os

from src.connectors.data_connector import FileDataConnector

class AzureDataGenerator:
    def __init__(self, download_prefix: str,data_connector: FileDataConnector) -> None:
        self.data_connector: FileDataConnector = data_connector
        self.download_prefix = download_prefix

    def get_random_data_iterator(self) -> Iterator[Dict]:
        import random
        file_names = self.data_connector.list_files(self.download_prefix)
        while True:
            file_name = random.choice(file_names)
            print(f"Chose file {file_name}")
            iterator = self.__get_file_iterator(file_name)
            for data_object in iterator:
                yield data_object
            print(f"Finished streaming data from file {file_name}")
    
    def __get_file_iterator(self, filename: str) -> Iterator[Dict]:
        from csv import DictReader

        local_file_name = self.data_connector.download_file_if_not_exists(filename)
        with open(local_file_name, 'r') as downloaded_file:
            csv_dict_reader: DictReader = DictReader(downloaded_file)
            for row in csv_dict_reader:
                yield row

        

