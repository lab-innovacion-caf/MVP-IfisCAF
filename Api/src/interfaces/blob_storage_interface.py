from abc import ABC, abstractmethod

class BlobStorageInterface(ABC):
    @abstractmethod
    def upload_blob(self,file_content, file_name, overwrite):
        pass

    def list_blobs(self):
        pass

    @abstractmethod
    def delete_blob(self, file_path):
        pass

    @abstractmethod
    def download_blob(self, file_path, container_name):
        pass