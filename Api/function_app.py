import os
import json
import logging
import tempfile
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from src.const.const import API_URL_BASE
from src.repositories.comosdb_repository import CosmosdbRepository
from src.repositories.comosdb_logging_repository import CosmosdbLoggingRepository
from src.repositories.blob_storage_repository import BlobStorageRepository
from src.services.ifis_caf_service import IfisCafService
from src.services.logging_service import LoggingService
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

cosmosdb_connection_string = os.environ["COSMOS_DB_CONNECTION_STRING"]
cosmosdb_database_name = os.environ["COSMOS_DB_DATABASE"]
cosmosdb_container_name = os.environ["COSMOS_DB_CONTAINER"]

blob_connection_string = os.environ["BLOB_STORAGE_CONNECTION_STRING"]
blob_container_name = os.environ["BLOB_STORAGE_CONTAINER_NAME"]

storage_account = os.environ.get('storageaccount')
credential = os.environ.get('credential')

audits_api_url_base = os.environ["AUDTIS_API_URL_BASE"]

blob_service = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=credential
    )
blob_container = blob_service.get_container_client(blob_container_name)

cosmosdb_repository = CosmosdbRepository(
    connection_string = cosmosdb_connection_string,
    database_name = cosmosdb_database_name,
    container_name = cosmosdb_container_name
)
blob_storage_repository = BlobStorageRepository(
    connection_string = blob_connection_string,
    container_name = blob_container_name
)

ifis_caf_service = IfisCafService(cosmosdb_repository, blob_storage_repository)

logging_service = LoggingService(audits_api_url_base)

@app.route(route='analysis', methods=['GET'])
def get_analysis(req: func.HttpRequest) -> func.HttpResponse:
    try:
        year = req.params.get('anio')

        response = ifis_caf_service.get_all(year=year)
        if not isinstance(response, list):
            return func.HttpResponse(json.dumps({"success": False, "error": response}), mimetype="application/json", status_code=500)

        return func.HttpResponse(json.dumps({"success": True, "data": response}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_items] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route='years', methods=['GET'])
def get_available_years(req: func.HttpRequest) -> func.HttpResponse:
    try:
        status = req.params.get('status')
        response = ifis_caf_service.get_available_years(status)
        if not isinstance(response, list):
            return func.HttpResponse(json.dumps({"success": False, "error": response}), mimetype="application/json", status_code=500)

        return func.HttpResponse(json.dumps({"success": True, "data": response}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_available_years] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    
@app.route(route='upload', methods=['POST'])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    log = {
            "user": user,
            "action": "UPLOAD",
            "api": f"{API_URL_BASE}/upload",
            "request": json.dumps(req.form)
        }
    
    try:
        if 'files' not in req.files:
            return func.HttpResponse(json.dumps({"error": "No file part in the req"}), mimetype="application/json", status_code=400)

        files = req.files.getlist('files')
        if not files:
            return func.HttpResponse(json.dumps({"error": "No files selected for uploading"}), mimetype="application/json", status_code=400)


        tipo_documento = req.form.get('tipoDocumento')  
        if not tipo_documento:
            return func.HttpResponse(json.dumps({"error": "No document type provided"}), mimetype="application/json", status_code=400) 
        
        anio = req.form.get('anio') or "None"
        uploaded_files_info = []

        with tempfile.TemporaryDirectory() as temp_dir:
            for file in files:
                filename = file.filename
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)

                try:
                    # Subir el archivo (PDF o PDF convertido) a Blob Storage
                    pathtoblob = f"{anio}/{os.path.basename(file_path)}"  
                    print("Path: ", pathtoblob)
                    blob_client = blob_service.get_blob_client(container="source", blob=pathtoblob)

                    block_size = 4 * 1024 * 1024  
                    blocks = []
                    with open(file_path, "rb") as data:
                        block_id = 0
                        while True:
                            block = data.read(block_size)
                            if not block:
                                break
                            block_id_str = str(block_id).zfill(6) 
                            blob_client.stage_block(block_id_str, block)
                            blocks.append(block_id_str)
                            block_id += 1

                    blob_client.commit_block_list(blocks)

                    uploaded_files_info.append({
                        "message": "File uploaded successfully",
                        "path": pathtoblob,
                        "filename": filename,
                        "tipoDocumento": tipo_documento
                    })

                except Exception as e:
                    return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

        log["response"] = json.dumps(uploaded_files_info)
        log["isSuccess"] = True
        logging_service.save_log(log)     
        return func.HttpResponse(json.dumps(uploaded_files_info), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[upload] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route='analysis', methods=['POST'])
def save(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")
    data = req.get_json()
    log = {
            "user": user,
            "action": "CREATE",
            "api": f"{API_URL_BASE}/analysis",
            "request": json.dumps(data)
        }    
    try:
        response = ifis_caf_service.save(data)
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)   
        return func.HttpResponse(json.dumps({'mensaje': response}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[save] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route='analysis-processor', methods=['POST'])
def analysis_processor(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")
    data = req.get_json()
    log = {
            "user": user,
            "action": "TRIGGER_TO_DATA_FACTORY",
            "api": f"{API_URL_BASE}/analysis-processor",
            "request": json.dumps(data)
        } 
    try:
        response = ifis_caf_service.start_proyect(data)

        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)   
        return func.HttpResponse(json.dumps({'mensaje': "Proceso Inciado",'resultadocosmos':response}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"ERROR: - {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)      
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    
@app.route(route='analysis/{id}', methods=['PUT'])
def update(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")
    id = req.route_params.get("id")
    data = req.get_json()
    log = {
            "user": user,
            "action": "UPDATE",
            "api": f"{API_URL_BASE}/analysis/{id}",
            "request": json.dumps(data)
        }    
    try:
        if not id:
            return func.HttpResponse("El parámetro 'id' es requerido", status_code=400)

        if not data:
            return func.HttpResponse("No hay data para actualizar", status_code=400)
        
        response = ifis_caf_service.update(id, data)
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)   
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[update] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log) 
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

@app.route(route='analysis/{id}', methods=['GET'])
def get_analysis_by_id(req: func.HttpRequest) -> func.HttpResponse:
    try:
        id = req.route_params.get("id")

        if not id:
            return func.HttpResponse("El parámetro 'id' es requerido", status_code=400)
        
        response = ifis_caf_service.get_one_by_id(id)

        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_analysis_by_id] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)