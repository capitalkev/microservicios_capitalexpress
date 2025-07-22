import uuid
import json
import os
import base64
import requests
from typing import List, Annotated
from collections import defaultdict
from dotenv import load_dotenv
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from google.cloud import storage
from database import get_db, engine
from repository import OperationRepository
import models

models.Base.metadata.create_all(bind=engine)

# Cargar variables de entorno
load_dotenv()

# --- Inicialización de FastAPI ---
app = FastAPI(
    title="Orquestador de Operaciones Multi-Moneda",
    description="Orquesta el procesamiento de operaciones de factoring."
)

# --- Configuración de CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "https://operaciones-peru.web.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- URLs de los Microservicios ---
BUCKET_NAME = os.getenv("BUCKET_NAME")
PARSER_SERVICE_URL = os.getenv("PARSER_SERVICE_URL")
TRELLO_SERVICE_URL = os.getenv("TRELLO_SERVICE_URL")
GMAIL_SERVICE_URL = os.getenv("GMAIL_SERVICE_URL")
DRIVE_SERVICE_URL = os.getenv("DRIVE_SERVICE_URL")
CAVALI_SERVICE_URL = os.getenv("CAVALI_SERVICE_URL")
EXCEL_SERVICE_URL = os.getenv("EXCEL_SERVICE_URL")

# --- Cliente de Google Storage ---
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

@app.post("/submit-operation", summary="Registrar y Procesar Operación")
async def submit_multi_currency_operation(
    metadata_str: Annotated[str, Form(alias="metadata")],
    xml_files: Annotated[List[UploadFile], File(alias="xml_files")],
    pdf_files: Annotated[List[UploadFile], File(alias="pdf_files")],
    respaldo_files: Annotated[List[UploadFile], File(alias="respaldo_files")],
    db: Session = Depends(get_db)
):
    try:
        metadata = json.loads(metadata_str)
        upload_id = f"OP-{datetime.now().strftime('%Y%m%d')}"
        # --- 1. Subir archivos a GCS ---
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

        # --- 2. Parsear XMLs ---
        parser_payload = {"operation_id": upload_id, "xml_paths": xml_paths}
        print("--- 📝 Enviando XMLs al servicio de Parser ---")
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
            raise HTTPException(status_code=400, detail="No se pudo parsear ninguna factura válida.")

        # --- 3. Actualizar y Consultar EXCEL por cada deudor ---
        print("--- 📊 Actualizando y consultando contactos en Google Sheets ---")
        invoices_by_debtor_ruc = defaultdict(list)
        for inv in invoices_data_with_filename:
            invoices_by_debtor_ruc[inv['debtor_ruc']].append(inv)

        correos_finales_por_ruc = {}

        for ruc, invoices in invoices_by_debtor_ruc.items():
            nombre_deudor = invoices[0]['debtor_name']
            correo_de_la_operacion = metadata.get('mailVerificacion', '').strip()

            excel_update_payload = {"ruc": ruc, "correo": correo_de_la_operacion, "nombre_deudor": nombre_deudor}
            try:
                requests.post(EXCEL_SERVICE_URL, json=excel_update_payload).raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Alerta: No se pudo actualizar el contacto para RUC {ruc}. Error: {e}")

            correos_del_excel = ""
            try:
                # Construye la URL para obtener correos, quitando la parte '/update-contact'
                base_excel_url = EXCEL_SERVICE_URL.replace('/update-contact', '')
                get_emails_url = f"{base_excel_url}/get-emails/{ruc}"
                emails_response = requests.get(get_emails_url)
                if emails_response.status_code == 200:
                    correos_del_excel = emails_response.json().get("emails", "")
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Alerta: No se pudieron obtener los correos para RUC {ruc}. Error: {e}")

            # Lógica para construir la lista final de correos
            lista_final = set(c.strip() for c in correos_del_excel.split(';') if c.strip())
            if correo_de_la_operacion:
                lista_final.add(correo_de_la_operacion)
            
            correos_finales_por_ruc[ruc] = ";".join(sorted(list(lista_final)))
            print(f"Lista de correos final para {ruc}: {correos_finales_por_ruc[ruc]}")

        # --- 4. Agrupar facturas por moneda ---
        invoices_by_currency = defaultdict(list)
        for inv in invoices_data_with_filename:
            invoices_by_currency[inv['currency']].append(inv)

        repo = OperationRepository(db)
        created_operations = []

        # --- 5. Procesar cada grupo de moneda como una operación independiente ---
        for currency, invoices_in_group in invoices_by_currency.items():
            print(f"--- ⚙️  Procesando Lote para Moneda: {currency} ---")
            
            # 5.1. Validar en CAVALI
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
            cavali_results_json = {}
            try:
                cavali_response = requests.post(CAVALI_SERVICE_URL, json={"xml_files_data": xml_files_b64_group})
                cavali_response.raise_for_status()
                cavali_results_json = cavali_response.json().get("results", {})
                print("--- ✅ Validación en CAVALI completada ---")
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Alerta: Falló la comunicación con el servicio de CAVALI. Error: {e}")
            print(f"------------------Resultados de CAVALI: {cavali_results_json}")
            # 5.2. Generar ID y Archivar en Drive
            operation_id = repo.generar_siguiente_id_operacion()
            drive_response = requests.post(DRIVE_SERVICE_URL, json={"operation_id": operation_id, "gcs_file_paths": all_gcs_paths})
            drive_response.raise_for_status()
            drive_folder_url = drive_response.json().get("drive_folder_url")
            print("--- 📂 Archivos archivados en Google Drive ---")

            # 5.3. Guardar operación en la BD
            repo.save_full_operation(
                operation_id, 
                metadata, 
                drive_folder_url, 
                invoices_in_group,
                cavali_results_json
            )

            created_operations.append({
                "operation_id": operation_id, "currency": currency,
                "drive_url": drive_folder_url, "invoice_count": len(invoices_in_group)
            })
            
            # 5.4. Enviar notificaciones
            parser_results_for_group = [res for res in parsed_results if os.path.basename(res.get('xml_path', '')) in xml_filenames_in_group]
            
            # Lógica para GMAIL
            try:
                ruc_deudor_grupo = invoices_in_group[0]['debtor_ruc']
                destinatarios_para_enviar = correos_finales_por_ruc.get(ruc_deudor_grupo)
                
                user_email = metadata.get('user_email', None)

                if destinatarios_para_enviar:
                    gmail_payload = {
                        "parsed_invoice_data": {"results": parser_results_for_group},
                        "pdf_paths": pdf_paths,
                        "recipient_emails": destinatarios_para_enviar,
                        "user_email": user_email
                    }
                    requests.post(GMAIL_SERVICE_URL, json=gmail_payload).raise_for_status()
                    print(f"--- ✉️  Notificación por Gmail enviada para op {operation_id}. ---")
                else:
                    print(f"ADVERTENCIA: No se enviarán correos para op {operation_id} porque no se encontraron correos para RUC {ruc_deudor_grupo}.")
            except Exception as e:
                print(f"ADVERTENCIA: Falló el envío de GMAIL para op {operation_id}. Error: {e}")

            cuentas_desembolso_data = metadata.get('cuentasDesembolso', [])
            cuenta_principal = cuentas_desembolso_data[0] if cuentas_desembolso_data else {}
            ## Lógica para Trello
            desembolso_banco = cuenta_principal.get('banco'),
            desembolso_tipo = cuenta_principal.get('tipo'),
            desembolso_moneda = cuenta_principal.get('moneda'),
            desembolso_numero = cuenta_principal.get('numero')
            solicitudAdelanto_obj = metadata.get('solicitudAdelanto', {})
            porcentajeAdelanto = solicitudAdelanto_obj.get('porcentaje', 0)
            
            # Lógica para TRELLO
            try:
                trello_payload = {
                    "operation_id": operation_id,
                    "client_name": invoices_in_group[0].get('client_name'),
                    "tasa": metadata.get('tasaOperacion', 'N/A'),
                    "comision": metadata.get('comision', 'N/A'),
                    "drive_folder_url": drive_folder_url,
                    "invoices": invoices_in_group,
                    "attachment_paths": pdf_paths + respaldo_paths,
                    "cavali_results": cavali_results_json,
                    "user_email": user_email,
                    "porcentajeAdelanto": porcentajeAdelanto,
                    "desembolso_numero": desembolso_numero,
                    "desembolso_moneda": desembolso_moneda,
                    "desembolso_tipo": desembolso_tipo,
                    "desembolso_banco": desembolso_banco
                }
                requests.post(TRELLO_SERVICE_URL, json=trello_payload)
                print(f"--- 🚀 Notificación a Trello enviada para op {operation_id}. ---")
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
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado en la orquestación: {str(e)}"
        )