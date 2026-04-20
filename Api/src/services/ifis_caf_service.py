import uuid
import json
import logging
from datetime import datetime

from src.utils import get_epoch_time
from src.const.const import TIME_ZONE
from src.interfaces.cosmosdb_interface import CosmosdbInterface
from src.interfaces.blob_storage_interface import BlobStorageInterface

class IfisCafService:
    def __init__(self, cosmosdb_repository: CosmosdbInterface, blob_storage_repository: BlobStorageInterface):
        self.cosmosdb_repository = cosmosdb_repository
        self.blob_storage_repository = blob_storage_repository

    def get_all(self, year):
        return self.cosmosdb_repository.get_all(year=year)
    
    def get_available_years(self, status: str):
        response = self.cosmosdb_repository.get_available_years(status)
        logging.info(response)
        years = [item["uploadYear"] for item in response if "uploadYear" in item]
        years.sort()
        return years
    
    def save(self, data):
        return self.cosmosdb_repository.save(data)

    def start_proyect(self, data):
        id = uuid.uuid4()
        data["id"] = str(id)
        # Crear la ruta para el archivo vacío (requests_gpt_paises.jsonl)
        empty_contries_path = f"{data['anio']}/requests_gpt_paises.jsonl"
        logging.info(f"empty_contries_path: {empty_contries_path}")
        # Obtener el cliente del segundo blob y subir un archivo vacío
        self.blob_storage_repository.upload_blob("", empty_contries_path)  # Archivo vacío


        empty_general_path = f"{data['anio']}/requests_gpt_general.jsonl"
        logging.info(f"Empty Path: {empty_general_path}")
        # Obtener el cliente del segundo blob y subir un archivo vacío
        self.blob_storage_repository.upload_blob("", empty_general_path) 

        parameters_path = f"{data['anio']}/ParametrosAnio.json"  
        logging.info(f"parameters_path: {parameters_path}")        
        self.blob_storage_repository.upload_blob(json.dumps(data,ensure_ascii=False), parameters_path)
        
        item_cosmos = {
            "countries":[],
            "status":"PROCESSING",
            "context": "En construcción",
            "uploadYear": data['anio'],
            "createdAt": get_epoch_time.get_epoch_time(TIME_ZONE),
            "updatedAt": get_epoch_time.get_epoch_time(TIME_ZONE),
            "id": str(id)
            }
        response = self.cosmosdb_repository.save(item_cosmos)
        return response
    
    def update(self, id: str, data):
        data['id'] = id
        data['updatedAt'] = datetime.now().timestamp()
        return self.cosmosdb_repository.update(id, data)
    
    def get_one_by_id(self, id: str):
        return self.cosmosdb_repository.get_one(id)