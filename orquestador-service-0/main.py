# === 1. MAIN DEL ORQUESTADOR (FastAPI en puerto 8000) ===
from dotenv import load_dotenv
load_dotenv()
import requests
import os, uuid, json, base64
from typing import Annotated
from fastapi import FastAPI, UploadFile, Form, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL", "http://localhost:8002/trello")
GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL", "http://localhost:8003/gmail")
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL", "http://localhost:8004/drive")
CAVALI_SERVICE_URL = os.getenv("CAVALI_SERVICE_URL", "http://localhost:8005/validate")

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

@app.post("/submit-operation")
async def submit_operation(
    metadata_str: Annotated[str, Form(alias="metadata")],
    xml_files: Annotated[list[UploadFile], File(alias="xml_files")],
    pdf_files: Annotated[list[UploadFile], File(alias="pdf_files")],
    respaldo_files: Annotated[list[UploadFile], File(alias="respaldo_files")]
):
    try:
        metadata = json.loads(metadata_str)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="metadata no es JSON válido")

    operation_id = f"OP-{uuid.uuid4().hex[:8].upper()}"

    def upload_file(file: UploadFile, folder: str) -> str:
        unique_filename = f"{uuid.uuid4().hex[:8]}-{file.filename}"
        blob_path = f"{operation_id}/{folder}/{unique_filename}"
        blob = bucket.blob(blob_path)
        blob.upload_from_file(file.file)
        return f"gs://{BUCKET_NAME}/{blob_path}"

    xml_paths = [upload_file(f, "xml") for f in xml_files]
    pdf_paths = [upload_file(f, "pdf") for f in pdf_files]
    respaldo_paths = [upload_file(f, "respaldos") for f in respaldo_files]

    message_to_parser = {
        "operation_id": operation_id,
        "xml_paths": xml_paths
    }

    # === 1. Llama al PARSER local ===
    parser_response = requests.post("http://localhost:8001/parser", json=message_to_parser)
    parsed_data = parser_response.json()


    # === 2. Llama a GMAIL ===
    gmail_payload = {
        "operation_id": operation_id,
        "pdf_paths": pdf_paths,
        "parsed_invoice_data": parsed_data,
        "metadata": metadata  # contiene tasa, comisión, etc.
    }
    gmail_response = requests.post(GMAIL_SERVICE_URL, json=gmail_payload)


    # === 3. Llama a TRELLO ===
    trello_payload = {
        "operation_id": operation_id,
        "pdf_paths": pdf_paths,
        "respaldo_paths": respaldo_paths,
        "parsed_invoice_data": parsed_data
    }
    trello_response = requests.post(TRELLO_SERVICE_URL, json=trello_payload)

    # === 4. Llama a Cavli ====
    
    xml_files_data_for_cavali = []
    for xml_file in xml_files:
        await xml_file.seek(0)
        content_bytes = await xml_file.read()
        content_base64 = base64.b64encode(content_bytes).decode('utf-8')
        xml_files_data_for_cavali.append({
            "filename": xml_file.filename,
            "content_base64": content_base64
        })

    cavali_payload = {
        "operation_id": operation_id,
        "xml_files_data": xml_files_data_for_cavali
    }
    cavali_response = requests.post(CAVALI_SERVICE_URL, json=cavali_payload)


    # === 4. Llama a Drive ====
    all_files_to_archive = xml_paths + pdf_paths + respaldo_paths
    drive_payload = {
        "operation_id": operation_id,
        "file_paths": all_files_to_archive
    }
    drive_response = requests.post(DRIVE_SERVICE_URL, json=drive_payload)

    
    return {
        "message": "Operación procesada",
        "operation_id": operation_id,
        "parser_result": parsed_data,
        "trello_status": trello_response.status_code,
        "gmail_status": gmail_response.status_code,
        "drive_status": drive_response.status_code,
        "drive_folder_url": drive_response.json().get("drive_folder_url"),
        "cavali_status": cavali_response.status_code,
        "cavali_result": cavali_response.json()
    }
