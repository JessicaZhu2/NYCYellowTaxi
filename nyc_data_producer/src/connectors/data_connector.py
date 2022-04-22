from abc import ABC, abstractmethod


class FileDataConnector(ABC):

    @abstractmethod
    def list_files(self, prefix: str) -> list:
        pass

    @abstractmethod
    def download_file_if_not_exists(self, blob_name: str) -> str:
        pass