import logging
from azure.storage.blob import BlobServiceClient
from src.interfaces.blob_storage_interface import BlobStorageInterface
class BlobStorageRepository(BlobStorageInterface):
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name

    def upload_blob(self, file_content, file_name, overwrite=True):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)
            response = blob_client.upload_blob(file_content, overwrite=overwrite)
            logging.info("[BlobStorageRepository - upload] - blob_response: ", response)
        except Exception as e:
            logging.exception(f"Error when try upload file to blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - upload] - Error: {str(e)}")
    
    def list_blobs(self):
        try:
            container_client = self.blob_service_client.get_container_client(container=self.container_name)
            blobs = container_client.list_blobs()
            return blobs
        except Exception as e:
            logging.exception(f"Error when try get_container_client files to blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - get_folders_and_subfolders] - Error: {str(e)}")
        
    def delete_blob(self, file_path):
        try:
            get_blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_path)
            get_blob_client.delete_blob()
            logging.info(f"File: {file_path} was deleted successfully")
        except Exception as e:
            logging.exception(f"Error when try delete_blob files to blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - delete_blob] - Error: {str(e)}")        
        
    def download_blob(self, file_path, container_name):
        try:
            get_blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=file_path)
            response =  get_blob_client.download_blob().content_as_bytes()
            logging.info(f"File: {file_path} was download_blob successfully")
            return response
        except Exception as e:
            logging.exception(f"Error when try download_blob files to blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - download_blob] - Error: {str(e)}")        