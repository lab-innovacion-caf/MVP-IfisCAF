from azure.cosmos import CosmosClient
from src.interfaces.cosmosdb_logging_interface import CosmosdbLoggingInterface

class CosmosdbLoggingRepository(CosmosdbLoggingInterface):
    def __init__(self, connection_string, database_name, container_name):
        self.client = CosmosClient.from_connection_string(connection_string)
        self.container = self.client.get_database_client(database_name).get_container_client(container_name)        

    def save_log(self,data):
        try:
            response = self.container.upsert_item(data)
            return response
        except Exception as e:
            raise ValueError(f"[CosmosdbLoggingRepository - save] - Error: {str(e)}")

    def get_all(self):
        try:
            query = 'SELECT * FROM c'
            items = list(self.container.query_items(query=query,enable_cross_partition_query=True))
            return items
        except Exception as e:
          
            raise ValueError(f"[CosmosdbLoggingRepository - get_all] - Error: {str(e)}")
    