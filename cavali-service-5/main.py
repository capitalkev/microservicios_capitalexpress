# cavali-service/main.py
import os

import requests
import time
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any
from dotenv import load_dotenv

# --- Carga de configuración y App ---
load_dotenv()
app = FastAPI(title="Cavali Service (con caché de token)", description="Microservicio para validar facturas con Cavali.")

# --- Configuración Global ---
CAVALI_CLIENT_ID = os.getenv("CAVALI_CLIENT_ID")
CAVALI_CLIENT_SECRET = os.getenv("CAVALI_CLIENT_SECRET")
CAVALI_SCOPE = os.getenv("CAVALI_SCOPE")
CAVALI_TOKEN_URL = os.getenv("CAVALI_TOKEN_URL")
CAVALI_API_KEY = os.getenv("CAVALI_API_KEY")
CAVALI_BLOCK_URL = "https://api.cavali.com.pe/factrack/v2/add-invoice-xml"
CAVALI_STATUS_URL = "https://api.cavali.com.pe/factrack/v2/get-process"

token_cache = {
    "access_token": None,
    "expires_at": 0
}

def obtener_token_cavali() -> str:
    """
    Obtiene un token de Cavali solo si el cacheado ha expirado.
    """

    if token_cache["access_token"] and token_cache["expires_at"] > time.time() + 60:
        print("Usando token de Cavali desde caché.")
        return token_cache["access_token"]

    data = {
        "grant_type": "client_credentials",
        "client_id": CAVALI_CLIENT_ID,
        "client_secret": CAVALI_CLIENT_SECRET,
        "scope": CAVALI_SCOPE
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    print("El token ha expirado o no existe. Solicitando uno nuevo...")
    response = requests.post(CAVALI_TOKEN_URL, data=data, headers=headers, timeout=30)
    response.raise_for_status()
    
    token_data = response.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", 3600)
    token_cache["access_token"] = access_token
    token_cache["expires_at"] = time.time() + expires_in
    print(f"Nuevo token obtenido. Válido por {expires_in // 60} minutos.")
    
    return access_token


def procesar_lote_cavali(lote: List[Dict[str, Any]], headers: Dict[str, str], num_lote: int) -> Dict[str, Any]:
    try:
        invoice_xml_list = [
            {
                "name": factura['filename'],
                "fileXml": factura['content_base64'],
                "additionalFieldOne": f"Lote {num_lote}",
                "additionalFieldTwo": "Enviado desde Orquestador v2"
            }
            for factura in lote
        ]
        numero_proceso = int(time.time()) + num_lote
        payload_bloqueo = {
            "processDetail": {"processNumber": numero_proceso},
            "invoiceXMLDetail": {"invoiceXML": invoice_xml_list}
        }

        print(f"Enviando Lote #{num_lote} a Cavali (Producción)...")
        response_bloqueo = requests.post(CAVALI_BLOCK_URL, json=payload_bloqueo, headers=headers, timeout=60)
        
        if response_bloqueo.status_code != 200:
             print(f"Error en bloqueo Lote #{num_lote}. Status: {response_bloqueo.status_code}. Response: {response_bloqueo.text}")
        response_bloqueo.raise_for_status()
        
        resultado_bloqueo = response_bloqueo.json()
        id_proceso = resultado_bloqueo.get("response", {}).get("idProceso")
        if not id_proceso:
            raise ValueError("Respuesta de Cavali no contiene 'idProceso'.")

        time.sleep(5)
        payload_estado = {"ProcessFilter": {"idProcess": id_proceso}}
        response_estado = requests.post(CAVALI_STATUS_URL, json=payload_estado, headers=headers, timeout=30)
        response_estado.raise_for_status()
        resultado_estado = response_estado.json()

        return {
            "bloqueo_resultado": resultado_bloqueo,
            "estado_resultado": resultado_estado
        }
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if e.response is not None:
            error_msg += f" | Response: {e.response.text}"
        return {"bloqueo_resultado": {"status": "error", "message": error_msg}, "estado_resultado": None}


class CavaliRequest(BaseModel):
    operation_id: str
    xml_files_data: List[Dict[str, str]]

@app.post("/validate")
async def validate_invoices_endpoint(request: CavaliRequest):
    token = obtener_token_cavali()
    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": CAVALI_API_KEY,
        "Content-Type": "application/json"
    }
    
    TAMANO_LOTE = int(os.getenv("CAVALI_BATCH_SIZE", 30))
    lotes = [request.xml_files_data[i:i + TAMANO_LOTE] for i in range(0, len(request.xml_files_data), TAMANO_LOTE)]
    resultados_finales = []
    
    for i, lote_actual in enumerate(lotes):
        resultado_lote = procesar_lote_cavali(lote_actual, headers, i + 1)
        resultados_finales.append(resultado_lote)

    return {
        "operation_id": request.operation_id,
        "status": "SUCCESS",
        "cavali_results": resultados_finales
    }

if __name__ == "__main__":
    import uvicorn
    print("Iniciando Cavali Service (con caché de token) en http://localhost:8005")
    uvicorn.run(app, host="0.0.0.0", port=8005)