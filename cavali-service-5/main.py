import os
import requests
import time
import json
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import List, Dict
from dotenv import load_dotenv
from google.cloud import storage

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
    Obtiene un token de Cavali, usando un archivo en GCS como caché
    para reutilizarlo entre ejecuciones y contenedores.
    """
    if not GCS_BUCKET_NAME:
        raise HTTPException(status_code=500, detail="La variable de entorno GCS_BUCKET_NAME no está configurada.")

    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(TOKEN_FILE_NAME)

    try:
        if blob.exists():
            token_json = blob.download_as_string()
            token_data = json.loads(token_json)
            if token_data.get("expires_at", 0) > time.time() + 60:
                print("Token válido obtenido desde GCS.")
                return token_data["access_token"]
            else:
                print("Token en GCS ha expirado.")
    except Exception as e:
        print(f"No se pudo leer el token desde GCS, se solicitará uno nuevo. Error: {e}")

    print("Solicitando nuevo token de Cavali...")
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
        
        print(f"Nuevo token de Cavali guardado en GCS: gs://{GCS_BUCKET_NAME}/{TOKEN_FILE_NAME}")
        return access_token
        
    except requests.RequestException as e:
        # Esta es la línea que nos dará el error detallado
        print(f"Excepción detallada al obtener token de Cavali: {e}")
        raise HTTPException(status_code=502, detail=f"Error al obtener token de Cavali: {e}")
    

@app.get('/test-ip')
def test_ip_endpoint():
    """
    Endpoint de prueba para verificar la IP de salida del servicio.
    """
    try:
        response = requests.get('https://ifconfig.me/ip', timeout=10)
        response.raise_for_status()
        public_ip = response.text.strip()
        print(f"VERIFICACIÓN DE IP DE SALIDA: Mi IP pública es {public_ip}")
        return Response(content=f"El servicio está saliendo a internet con la IP: {public_ip}", media_type="text/plain")
    except requests.exceptions.RequestException as e:
        print(f"Error al verificar la IP: {e}")
        return Response(content="Error al verificar la IP de salida.", status_code=500)


# --- Endpoints de la Lógica de Negocio ---
class CavaliValidationRequest(BaseModel):
    xml_files_data: List[Dict[str, str]]

@app.post("/validate-invoices")
async def validate_invoices(request: CavaliValidationRequest):
    try:
        token = get_cavali_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-api-key": CAVALI_API_KEY,
            "Content-Type": "application/json",
        }

        invoice_xml_list = [{"name": f['filename'], "fileXml": f['content_base64']} for f in request.xml_files_data]
        payload_bloqueo = {"invoiceXMLDetail": {"invoiceXML": invoice_xml_list}}

        response_bloqueo = requests.post(CAVALI_BLOCK_URL, json=payload_bloqueo, headers=headers, timeout=60)
        response_bloqueo.raise_for_status()
        id_proceso = response_bloqueo.json().get("response", {}).get("idProceso")

        if not id_proceso:
            raise HTTPException(status_code=500, detail="Cavali no retornó un idProceso.")

        time.sleep(1)
        payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
        response_estado = requests.post(CAVALI_STATUS_URL, json=payload_estado, headers=headers, timeout=30)
        response_estado.raise_for_status()
        cavali_response_data = response_estado.json()

        results_map = {}
        process_detail = cavali_response_data.get("response", {}).get("Process", {})
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
        return {"status": "SUCCESS", "results": results_map}
    except requests.RequestException as e:
        # ESTA ES LA LÍNEA QUE NOS DARÁ EL ERROR DETALLADO
        print(f"Excepción detallada de requests: {e}")
        error_detail = e.response.text if e.response else str(e)
        raise HTTPException(status_code=503, detail=f"Error de comunicación con Cavali: {error_detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio: {str(e)}")
