# cavali-service-5/main.py
import os
import requests
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Cavali Service (Lógica Pura)", description="Se conecta a la API de Cavali y devuelve el resultado.")

# --- Configuración de Cavali ---
CAVALI_CLIENT_ID = os.getenv("CAVALI_CLIENT_ID")
CAVALI_CLIENT_SECRET = os.getenv("CAVALI_CLIENT_SECRET")
CAVALI_SCOPE = os.getenv("CAVALI_SCOPE")
CAVALI_TOKEN_URL = os.getenv("CAVALI_TOKEN_URL")
CAVALI_API_KEY = os.getenv("CAVALI_API_KEY")
CAVALI_BLOCK_URL = os.getenv("CAVALI_BLOCK_URL")
CAVALI_STATUS_URL = os.getenv("CAVALI_STATUS_URL")

# --- Caché de Token (en memoria) ---
token_cache = {"access_token": None, "expires_at": 0}

def get_cavali_token():
    """Obtiene un token de Cavali, usando un caché para reutilizarlo."""
    if token_cache["access_token"] and token_cache["expires_at"] > time.time() + 60:
        return token_cache["access_token"]

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
        token_data = response.json()
        token_cache["access_token"] = token_data["access_token"]
        token_cache["expires_at"] = time.time() + token_data.get("expires_in", 3600)
        return token_cache["access_token"]
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Error al obtener token de Cavali: {e}")

class CavaliValidationRequest(BaseModel):
    xml_files_data: List[Dict[str, str]] # Lista de {filename, content_base64}

@app.post("/validate-invoices")
async def validate_invoices(request: CavaliValidationRequest):
    """
    Recibe un lote de XMLs, los valida en Cavali y devuelve el resultado detallado.
    """
    try:
        token = get_cavali_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "x-api-key": CAVALI_API_KEY,
            "Content-Type": "application/json",
        }

        invoice_xml_list = [
            {"name": f['filename'], "fileXml": f['content_base64']}
            for f in request.xml_files_data
        ]
        payload_bloqueo = {"invoiceXMLDetail": {"invoiceXML": invoice_xml_list}}

        # 1. Enviar lote a la primera API
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

        print(f"Resultados de Cavali extraídos: {invoice_details}")

        for invoice in invoice_details:

            nombre_archivo_original = "desconocido"
            for f in request.xml_files_data:
                 if str(invoice["ruc"]) in f["filename"] and \
                    invoice["serie"] in f["filename"] and \
                    str(invoice["numeration"]) in f["filename"]:
                    nombre_archivo_original = f["filename"]
                    break

            results_map[nombre_archivo_original] = {
                "message": invoice.get("message"),
                "process_id": id_proceso,
                "result_code": invoice.get("resultCode")
            }

        return {"status": "SUCCESS", "results": results_map}

    except requests.RequestException as e:
        error_detail = e.response.text if e.response else str(e)
        raise HTTPException(status_code=503, detail=f"Error de comunicación con Cavali: {error_detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de Cavali: {str(e)}")