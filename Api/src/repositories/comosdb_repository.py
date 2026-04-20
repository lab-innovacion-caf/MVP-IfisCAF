import logging
from azure.cosmos import CosmosClient
from src.interfaces.cosmosdb_interface import CosmosdbInterface

class CosmosdbRepository(CosmosdbInterface):
    def __init__(self, connection_string, database_name, container_name):
        self.client = CosmosClient.from_connection_string(connection_string)
        self.container = self.client.get_database_client(database_name).get_container_client(container_name)        

    def save(self, data):
        try:
            # Limpiar los valores None en el data si es necesario
            prepare_data = {k: v for k, v in data.items() if v is not None}
            response = self.container.upsert_item(prepare_data)
            logging.info(f"[CosmosdbRepository - save] - cosmosDB_response: {response}")
            return response
        except Exception as e:
            logging.error(f"Error al ejecutar cosmosdb: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - save] - Error: {str(e)}")

    def get_all(self, year = None):
        try:
            if year is not None:
                query = f'SELECT * FROM c WHERE c["uploadYear"] = {year} ORDER BY c.createdAt DESC'
                # parameters = [{"name": "@anio", "value": anio}]
            else:
                query = "SELECT * FROM c ORDER BY c.createdAt DESC"
                
            logging.info(f"[CosmosdbRepository - get_all] - Query: {query}")
            items = list(self.container.query_items(query=query,enable_cross_partition_query=True))
            logging.info(f"[CosmosdbRepository - get_all] - cosmosDB_response: {items}")
            return items
        except Exception as e:
            logging.error(f"[CosmosdbRepository - get_all] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - get_all] - Error: {str(e)}")
    
    def get_available_years(self, status: str):
        try:
            query = 'SELECT DISTINCT c["uploadYear"] FROM c'
            if status:
                query += ' WHERE c["status"]= "FINISHED"'
            items = list(self.container.query_items(query=query, enable_cross_partition_query=True))
            return items
        except Exception as e:
            logging.error(f"[CosmosdbRepository - get_available_years] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - get_available_years] - Error: {str(e)}")

    def update(self, id:str, data):
        logging.info(f"id: {id}")
        
        try:
            #self.container.replace_item(item=id, body=data)
            response = self.container.upsert_item(body=data)
            logging.info("[CosmosdbRepository - update] - cosmosDB_response: ", response)
            return response
        except Exception as e:
            logging.error(f"[CosmosdbRepository - update] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - update] - Error: {str(e)}")
        
    def get_one(self, id: str):
        logging.info(f"id: {id}")
        try:
            items = self.container.read_item(item=id, partition_key=id)
            return items
        except Exception as e:
            logging.error(f"[CosmosdbRepository - get_one] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - get_one] - Error: {str(e)}")