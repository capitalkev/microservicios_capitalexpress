# orquestador-service-0/main.py
import uuid
import json
import os
import base64
import requests
from typing import List, Annotated
from collections import defaultdict
from dotenv import load_dotenv

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from google.cloud import storage

# --- Importaciones de la aplicación ---
from database import get_db, engine
from repository import OperationRepository
import models

# --- Crear tablas en la BD al iniciar (si no existen) ---
models.Base.metadata.create_all(bind=engine)

# Cargar variables de entorno del archivo .env
load_dotenv()

# --- Inicialización de la Aplicación FastAPI ---
app = FastAPI(
    title="Orquestador de Operaciones Multi-Moneda",
    description="Orquesta el procesamiento de operaciones de factoring, creando lotes por moneda."
)

# --- Configuración de CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- URLs de los Microservicios ---
BUCKET_NAME = os.getenv("BUCKET_NAME")
PARSER_SERVICE_URL = "http://localhost:8001/parser"
CAVALI_SERVICE_URL = "http://localhost:8005/validate-invoices"
DRIVE_SERVICE_URL = "http://localhost:8004/archive-files"
GMAIL_SERVICE_URL = "http://localhost:8003/gmail"
TRELLO_SERVICE_URL = "http://localhost:8002/trello"

# --- Clientes de Google ---
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

@app.post("/submit-operation", summary="Registrar y Procesar Operación Multi-Moneda")
async def submit_multi_currency_operation(
    metadata_str: Annotated[str, Form(alias="metadata", description="Objeto JSON con metadatos de la operación")],
    xml_files: Annotated[List[UploadFile], File(alias="xml_files", description="Archivos XML de las facturas")],
    pdf_files: Annotated[List[UploadFile], File(alias="pdf_files", description="Archivos PDF de las facturas")],
    respaldo_files: Annotated[List[UploadFile], File(alias="respaldo_files", description="Archivos de respaldo (imágenes, docs)")],
    db: Session = Depends(get_db)
):
    """
    Endpoint principal que orquesta el flujo completo de una operación de factoring.
    """
    try:
        metadata = json.loads(metadata_str)
        upload_id = f"UPLOAD-{uuid.uuid4().hex[:8].upper()}"

        # --- 1. Subir todos los archivos a Google Cloud Storage ---
        def upload_file(file: UploadFile, folder: str) -> str:
            blob_path = f"{upload_id}/{folder}/{file.filename}"
            blob = bucket.blob(blob_path)
            file.file.seek(0)
            blob.upload_from_file(file.file)
            return f"gs://{BUCKET_NAME}/{blob_path}"

        xml_paths = [upload_file(f, "xml") for f in xml_files]
        pdf_paths = [upload_file(f, "pdf") for f in pdf_files]
        respaldo_paths = [upload_file(f, "respaldos") for f in respaldo_files]
        all_gcs_paths = xml_paths + pdf_paths + respaldo_paths

        parser_payload = {
            "operation_id": upload_id,
            "xml_paths": xml_paths
        }
        print("--- 📝 Enviando XMLs al servicio de Parser ---")
        # --- 2. Parsear XMLs para obtener los datos estructurados ---
        parser_response = requests.post(PARSER_SERVICE_URL, json=parser_payload)
        parser_response.raise_for_status()
        parsed_results = parser_response.json().get("results", [])

        invoices_data_with_filename = []
        for res in parsed_results:
            if res.get('status') == 'SUCCESS':
                data = res['parsed_invoice_data']
                data['xml_filename'] = os.path.basename(res['xml_path'])
                invoices_data_with_filename.append(data)

        if not invoices_data_with_filename:
            raise HTTPException(status_code=400, detail="No se pudo parsear ninguna factura válida de los XMLs.")

        # --- 3. Agrupar las facturas por moneda ---
        invoices_by_currency = defaultdict(list)
        for inv in invoices_data_with_filename:
            invoices_by_currency[inv['currency']].append(inv)

        repo = OperationRepository(db)
        created_operations = []

        # --- 4. Procesar cada grupo de moneda como una operación independiente ---
        for currency, invoices_in_group in invoices_by_currency.items():
            print(f"--- ⚙️  Procesando Lote para Moneda: {currency} ---")

            # --- 4.1. Validar en CAVALI ---
            xml_filenames_in_group = {inv['xml_filename'] for inv in invoices_in_group}
            xml_files_b64_group = []
            
            for xml_file in xml_files:
                await xml_file.seek(0)
                if xml_file.filename in xml_filenames_in_group:
                    content_bytes = await xml_file.read()
                    xml_files_b64_group.append({
                        "filename": xml_file.filename,
                        "content_base64": base64.b64encode(content_bytes).decode('utf-8')
                    })
            print("--- 📄 Enviando XMLs al servicio de CAVALI para validación ---")
            cavali_response = requests.post(CAVALI_SERVICE_URL, json={"xml_files_data": xml_files_b64_group})
            cavali_response.raise_for_status()
            cavali_results_json = cavali_response.json().get("results", {})
            
            if cavali_results_json:
                 first_result = next(iter(cavali_results_json.values()), {})
                 global_process_id = first_result.get("process_id")
                 cavali_results_json["global_process_id"] = global_process_id
            print("--- ✅ Validación en CAVALI completada ---")
            # --- 4.2. Generar ID y Archivar en Drive ---
            print("--- 📂 Generando ID de operación y archivando archivos en Google Drive ---")
            operation_id = repo.generar_siguiente_id_operacion()
            drive_response = requests.post(DRIVE_SERVICE_URL, json={"operation_id": operation_id, "gcs_file_paths": all_gcs_paths})
            print(drive_response.text)
            drive_response.raise_for_status()
            drive_folder_url = drive_response.json().get("drive_folder_url")
            
            print("--- 📂 Archivos archivados en Google Drive con éxito ---")
            print("")
            # --- 4.3. Guardar la operación en la BD ---
            repo.save_full_operation(operation_id, metadata, drive_folder_url, invoices_in_group, cavali_results_json)

            created_operations.append({
                "operation_id": operation_id, "currency": currency,
                "drive_url": drive_folder_url, "invoice_count": len(invoices_in_group)
            })
            
            # --- 4.4. Enviar notificaciones (Gmail y Trello) ---
            parser_results_for_group = [res for res in parsed_results if os.path.basename(res.get('xml_path', '')) in xml_filenames_in_group]
            
            # --- CÓDIGO DE GMAIL AÑADIDO AQUÍ ---
            try:
                # El payload necesita los datos parseados y las rutas de los PDFs
                gmail_payload = {
                    "parsed_invoice_data": {"results": parser_results_for_group},
                    "pdf_paths": pdf_paths
                }
                requests.post(GMAIL_SERVICE_URL, json=gmail_payload)
                print(f"--- ✉️  Notificación por Gmail enviada para op {operation_id}. ---")
            except Exception as e:
                print(f"ADVERTENCIA: Falló el envío de GMAIL para op {operation_id}. Error: {e}")
            
            try:
                trello_payload = {"operation_id": operation_id, "parsed_invoice_data": {"results": parser_results_for_group}, "pdf_paths": pdf_paths, "respaldo_paths": respaldo_paths}
                requests.post(TRELLO_SERVICE_URL, json=trello_payload)
            except Exception as e:
                print(f"ADVERTENCIA: Falló la creación en Trello para op {operation_id}. Error: {e}")

            print(f"--- ✅ Operación {operation_id} para {currency} finalizada con éxito. ---")

        return {
            "message": f"Proceso finalizado. Se crearon {len(created_operations)} operaciones.",
            "operations": created_operations
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=503,
            detail=f"Error de comunicación con un servicio interno: {e.response.text if e.response else str(e)}"
        )
    except Exception as e:
        print(f"ERROR INESPERADO: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado en la orquestación: {str(e)}"
        )