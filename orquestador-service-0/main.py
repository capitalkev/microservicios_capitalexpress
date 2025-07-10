# orquestador-service/main.py
import uuid
import json, os
import base64
import requests
from typing import List, Annotated
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage

# Módulo de base de datos que ya tienes
from database import (
    crear_operaciones_agrupadas,
    update_log,
    get_operation_status_from_db,
    actualizar_url_drive  # <-- IMPORTAMOS LA NUEVA FUNCIÓN
)

# Carga las variables de entorno desde el archivo .env
load_dotenv()

app = FastAPI(title="Orquestador de Operaciones")

# --- Configuración de CORS ---
origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- URLs de los Microservicios (desde .env) ---
BUCKET_NAME = os.getenv("BUCKET_NAME")
PARSER_SERVICE_URL = os.getenv("PARSER_SERVICE_URL", "http://localhost:8001/parser")
GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL", "http://localhost:8003/gmail")
CAVALI_SERVICE_URL = os.getenv("CAVALI_SERVICE_URL", "http://localhost:8005/validate")
TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL", "http://localhost:8002/trello")
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL", "http://localhost:8004/drive")

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# --- Tareas en Segundo Plano ---
def run_background_tasks(
    op_id: str,
    facturas_de_moneda: list,
    xml_files_data_for_cavali: list,
    all_file_paths: list,
    pdf_paths: list,
    respaldo_paths: list,
    metadata: dict
):
    """
    Función más limpia que ejecuta los microservicios y actualiza los logs.
    """
    try:
        # === 1. GMAIL ===
        email_msg = f"Correos enviados a kevin.tupac@capitalexpress.cl."
        update_log(op_id, "Envío de Verificación", "procesando", "Enviando correos...")
        gmail_payload = {"operation_id": op_id, "parsed_invoice_data": {"results": facturas_de_moneda}, "pdf_paths": pdf_paths, "metadata": metadata}
        requests.post(GMAIL_SERVICE_URL, json=gmail_payload).raise_for_status()
        update_log(op_id, "Envío de Verificación", "ok", email_msg)

        # === 2. CAVALI ===
        update_log(op_id, "Bloqueo de Facturas en CAVALI", "procesando", "Validando facturas...")
        cavali_payload = {"operation_id": op_id, "xml_files_data": xml_files_data_for_cavali}
        requests.post(CAVALI_SERVICE_URL, json=cavali_payload).raise_for_status()

        # === 3. TRELLO ===
        update_log(op_id, "Carga en Trello", "procesando", "Creando tarjeta...")
        trello_payload = {"operation_id": op_id, "parsed_invoice_data": {"results": facturas_de_moneda}, "pdf_paths": pdf_paths, "respaldo_paths": respaldo_paths}
        requests.post(TRELLO_SERVICE_URL, json=trello_payload).raise_for_status()
        update_log(op_id, "Carga en Trello", "ok", "Tarjeta creada exitosamente.")

        # === 4. DRIVE ===
        update_log(op_id, "Archivado en Drive", "procesando", "Subiendo archivos a Drive...")
        drive_payload = {"operation_id": op_id, "file_paths": all_file_paths}
        drive_response = requests.post(DRIVE_SERVICE_URL, json=drive_payload)
        drive_response.raise_for_status()
        drive_url = drive_response.json().get("drive_folder_url")
        
        # Guardamos la URL de Drive en la base de datos
        if drive_url:
            actualizar_url_drive(op_id, drive_url)
        
        update_log(op_id, "Archivado en Drive", "ok", "Archivos guardados correctamente.")

    except Exception as e:
        # Registra un log de error si cualquier paso falla
        update_log(op_id, "Orquestador", "error", f"Error en tareas de fondo: {e}")

@app.get("/operation-group/{group_id}/status")
async def get_group_status(group_id: str):
    return get_operation_status_from_db(group_id)

@app.post("/submit-operation")
async def submit_operation(
    background_tasks: BackgroundTasks,
    metadata_str: Annotated[str, Form(alias="metadata")],
    xml_files: Annotated[List[UploadFile], File(alias="xml_files")],
    pdf_files: Annotated[List[UploadFile], File(alias="pdf_files")],
    respaldo_files: Annotated[List[UploadFile], File(alias="respaldo_files")]
):
    metadata = json.loads(metadata_str)
    
    # 1. SUBIR ARCHIVOS Y PARSEAR XML
    upload_id = f"UPLOAD-{uuid.uuid4().hex[:8].upper()}"
    def upload_file(file: UploadFile, folder: str) -> str:
        blob_path = f"{upload_id}/{folder}/{file.filename}"
        blob = bucket.blob(blob_path)
        file.file.seek(0)
        blob.upload_from_file(file.file)
        return f"gs://{BUCKET_NAME}/{blob_path}"

    xml_paths = [upload_file(f, "xml") for f in xml_files]
    pdf_paths = [upload_file(f, "pdf") for f in pdf_files]
    respaldo_paths = [upload_file(f, "respaldos") for f in respaldo_files]
    all_file_paths = xml_paths + pdf_paths + respaldo_paths

    parser_response = requests.post(PARSER_SERVICE_URL, json={"operation_id": upload_id, "xml_paths": xml_paths})
    parser_response.raise_for_status()
    parsed_data = parser_response.json()

    # 2. AGRUPAR FACTURAS POR MONEDA
    grouped_invoices = {}
    for result in parsed_data.get('results', []):
        moneda = result['parsed_invoice_data']['currency']
        if moneda not in grouped_invoices:
            grouped_invoices[moneda] = {'facturas': [], 'total_neto': 0.0}
        grouped_invoices[moneda]['facturas'].append(result)
        grouped_invoices[moneda]['total_neto'] += result['parsed_invoice_data']['net_amount']

    # 3. CREAR OPERACIONES EN LA BASE DE DATOS
    group_id = f"GRP-{uuid.uuid4().hex[:8].upper()}"
    operation_ids = crear_operaciones_agrupadas(group_id, grouped_invoices, metadata.get('user_email'))
    
    # 4. PREPARAR DATOS Y LANZAR TAREAS
    xml_files_data_for_cavali = []
    for xml_file in xml_files:
        content_bytes = await xml_file.read()
        await xml_file.seek(0)
        xml_files_data_for_cavali.append({"filename": xml_file.filename, "content_base64": base64.b64encode(content_bytes).decode('utf-8')})

    for op_id in operation_ids:
        moneda_actual = op_id.split('-')[-1]
        facturas_de_moneda = grouped_invoices[moneda_actual]['facturas']
        services_to_log = ["Envío de Verificación", "Bloqueo de Facturas en CAVALI", "Carga en Trello", "Archivado en Drive"]
        for service in services_to_log:
            update_log(op_id, service, "pendiente", "En espera...")
            
        background_tasks.add_task(
            run_background_tasks,
            op_id=op_id,
            facturas_de_moneda=facturas_de_moneda,
            xml_files_data_for_cavali=xml_files_data_for_cavali,
            all_file_paths=all_file_paths,
            pdf_paths=pdf_paths,
            respaldo_paths=respaldo_paths,
            metadata=metadata
        )

    return {"message": "Operación registrada y en proceso.", "group_id": group_id}