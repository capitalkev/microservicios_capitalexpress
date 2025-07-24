import os
import requests
import time
import json
import logging
import traceback
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import List, Dict
from dotenv import load_dotenv
from google.cloud import storage

# --- Configuración del Logging ---
# Esto nos dará logs más detallados en Cloud Run
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar variables de entorno desde el archivo .env
load_dotenv()

app = FastAPI(
    title="Cavali Service (Modo Local/Cloud)",
    description="Servicio que se conecta a la API de Cavali y gestiona el token en GCS."
)

# --- Configuración de Cavali y GCS ---
CAVALI_CLIENT_ID = os.getenv("CAVALI_CLIENT_ID")
CAVALI_CLIENT_SECRET = os.getenv("CAVALI_CLIENT_SECRET")
CAVALI_SCOPE = os.getenv("CAVALI_SCOPE")
CAVALI_TOKEN_URL = os.getenv("CAVALI_TOKEN_URL")
CAVALI_API_KEY = os.getenv("CAVALI_API_KEY")
CAVALI_BLOCK_URL = os.getenv("CAVALI_BLOCK_URL")
CAVALI_STATUS_URL = os.getenv("CAVALI_STATUS_URL")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
TOKEN_FILE_NAME = "cavali_token.json"

storage_client = storage.Client()

def get_cavali_token():
    """
    Obtiene un token de Cavali, usando un archivo en GCS como caché.
    """
    if not GCS_BUCKET_NAME:
        logging.error("La variable de entorno GCS_BUCKET_NAME no está configurada.")
        raise HTTPException(status_code=500, detail="Configuración de GCS incompleta en el servidor.")

    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(TOKEN_FILE_NAME)

    try:
        if blob.exists():
            token_json = blob.download_as_string()
            token_data = json.loads(token_json)
            if token_data.get("expires_at", 0) > time.time() + 60:
                logging.info("Token válido obtenido desde GCS.")
                return token_data["access_token"]
            else:
                logging.warning("Token en GCS ha expirado.")
    except Exception as e:
        logging.error(f"No se pudo leer el token desde GCS, se solicitará uno nuevo. Error: {e}")

    logging.info("Solicitando nuevo token de Cavali...")
    data = {
        "grant_type": "client_credentials",
        "client_id": CAVALI_CLIENT_ID,
        "client_secret": CAVALI_CLIENT_SECRET,
        "scope": CAVALI_SCOPE,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(CAVALI_TOKEN_URL, data=data, headers=headers, timeout=30)
        response.raise_for_status()
        new_token_data = response.json()

        access_token = new_token_data["access_token"]
        expires_in = new_token_data.get("expires_in", 3600) 
        expires_at = time.time() + expires_in

        data_to_save = {
            "access_token": access_token,
            "expires_at": expires_at
        }
        blob.upload_from_string(
            json.dumps(data_to_save),
            content_type="application/json"
        )
        
        logging.info(f"Nuevo token de Cavali guardado en GCS: gs://{GCS_BUCKET_NAME}/{TOKEN_FILE_NAME}")
        return access_token
        
    except requests.RequestException as e:
        logging.error(f"Excepción detallada al obtener token de Cavali: {e}")
        raise HTTPException(status_code=502, detail=f"Error al obtener token de Cavali: {e}")

# --- Modelos Pydantic ---
class CavaliValidationRequest(BaseModel):
    xml_files_data: List[Dict[str, str]]

@app.post("/validate-invoices")
async def validate_invoices(request: CavaliValidationRequest):
    try:
        # LOG: Registrar el cuerpo de la solicitud para depuración (sin datos sensibles si es necesario)
        logging.info(f"Recibida solicitud para validar {len(request.xml_files_data)} facturas.")

        token = get_cavali_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-api-key": CAVALI_API_KEY,
            "Content-Type": "application/json",
        }

        invoice_xml_list = [{"name": f['filename'], "fileXml": f['content_base64']} for f in request.xml_files_data]
        payload_bloqueo = {"invoiceXMLDetail": {"invoiceXML": invoice_xml_list}}

        logging.info("Enviando solicitud de bloqueo a Cavali...")
        response_bloqueo = requests.post(CAVALI_BLOCK_URL, json=payload_bloqueo, headers=headers, timeout=60)
        response_bloqueo.raise_for_status()
        
        # LOG: Registrar la respuesta de Cavali para ver qué se recibió
        bloqueo_data = response_bloqueo.json()
        logging.info(f"Respuesta de bloqueo de Cavali: {json.dumps(bloqueo_data)}")

        id_proceso = bloqueo_data.get("response", {}).get("idProceso")

        if not id_proceso:
            logging.error(f"Cavali no retornó un idProceso. Respuesta completa: {bloqueo_data}")
            raise HTTPException(status_code=500, detail="Cavali no retornó un idProceso.")

        # Se recomienda un sleep mayor para dar tiempo a Cavali a procesar
        time.sleep(3) 
        
        logging.info(f"Consultando estado del proceso con id: {id_proceso}")
        payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
        response_estado = requests.post(CAVALI_STATUS_URL, json=payload_estado, headers=headers, timeout=30)
        response_estado.raise_for_status()

        # LOG: Registrar la respuesta de estado de Cavali
        cavali_response_data = response_estado.json()
        logging.info(f"Respuesta de estado de Cavali: {json.dumps(cavali_response_data)}")

        results_map = {}
        # Hacemos el acceso al JSON más seguro para evitar errores si las claves no existen
        process_detail = cavali_response_data.get("response", {}).get("Process")
        if not process_detail:
            logging.warning("La respuesta de Cavali no contiene la clave 'Process'.")
            return {"status": "PARTIAL_SUCCESS", "results": {}, "detail": "Cavali no devolvió detalles del proceso."}

        invoice_details = process_detail.get("ProcessInvoiceDetail", {}).get("Invoice", [])

        for invoice in invoice_details:
            nombre_archivo_original = "desconocido"
            for f in request.xml_files_data:
                if (str(invoice.get("ruc", "")) in f["filename"] and
                    invoice.get("serie", "") in f["filename"] and
                    str(invoice.get("numeration", "")) in f["filename"]):
                    nombre_archivo_original = f["filename"]
                    break
            results_map[nombre_archivo_original] = {
                "message": invoice.get("message"),
                "process_id": id_proceso,
                "result_code": invoice.get("resultCode")
            }
        
        logging.info("Proceso completado exitosamente.")
        return {"status": "SUCCESS", "results": results_map}
    
    except requests.RequestException as e:
        error_detail = e.response.text if e.response else str(e)
        logging.error(f"Error de comunicación con Cavali: {error_detail}")
        raise HTTPException(status_code=503, detail=f"Error de comunicación con Cavali: {error_detail}")
    
    except Exception as e:
        # ¡IMPORTANTE! Este log nos dará el traceback completo del error en Cloud Run.
        error_trace = traceback.format_exc()
        logging.error(f"Error interno no esperado en el servicio: {e}\nTRACEBACK:\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio: {str(e)}")


