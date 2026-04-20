# Databricks notebook source
# MAGIC %pip install azure-identity azure-core azure-cosmos

# COMMAND ----------

dbutils.widgets.text("anio", "2024")

# COMMAND ----------

anio = dbutils.widgets.get("anio").split("/")[-1]
anio

# COMMAND ----------

from azure.cosmos import CosmosClient

# COMMAND ----------

# Credenciales de Cosmos DB
endpoint = 'https://cd-poc-ifis-cr.documents.azure.com:443/'
key = 'ilmNXXg4kDypH4hJyqNBXRr2HUSS3AlWHuhVFrzmQ2YCJf0CqrYqzXzXAsrsZwvvorN3DiAaljvSACDbbDv6MQ=='
database_name = 'IfisCAF'
container_name = 'Reportes'


# COMMAND ----------

clientCOSMOS = CosmosClient(endpoint, key)
database = clientCOSMOS.get_database_client(database_name)
container = database.get_container_client(container_name)

# COMMAND ----------

query = f"SELECT * FROM c WHERE c['AÑO DE CARGUE'] = {anio}"
items = list(container.query_items(query=query, enable_cross_partition_query=True))

if items:
    existing_item = items[0]  # Tomar el primer resultado 
    existing_item["status"] = "Proceso no completado"  # Actualiza o añade campos
    if(existing_item["CONTEXTO"] == "En construcción"):
        existing_item["CONTEXTO"] == "-"
    container.replace_item(item=existing_item, body=existing_item)
else:
    print("No se encontró el elemento.")

# COMMAND ----------

