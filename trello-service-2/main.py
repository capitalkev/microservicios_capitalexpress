import os
import requests
import datetime
import json
from fastapi import FastAPI, Request, HTTPException
from typing import List, Dict, Any
from dotenv import load_dotenv
from google.cloud import storage
from collections import defaultdict

# --- Carga de configuración ---
load_dotenv()
app = FastAPI(title="Trello Service (con Diagnósticos)")

# --- Clientes y Variables ---
TRELLO_API_KEY = os.getenv("TRELLO_API_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID = os.getenv("TRELLO_LIST_ID")
TRELLO_LABEL_VERIFICADA = os.getenv("TRELLO_LABEL_VERIFICADA")
TRELLO_LABEL_CAVALI = os.getenv("TRELLO_LABEL_CAVALI")
TRELLO_LABEL_HR = os.getenv("TRELLO_LABEL_HR")

storage_client = storage.Client()

# --- Funciones Auxiliares ---
def _format_number(num: float) -> str:
    return "{:,.2f}".format(num)

def _sanitize_name(name: str) -> str:
    return name.strip() if name else "—"

def download_blob_as_bytes(gs_path: str) -> bytes:
    """Descarga un archivo de GCS como bytes."""
    path_parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name, blob_path = path_parts[0], path_parts[1]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()

# --- Lógica Principal de Creación de Tarjeta (con Diagnósticos) ---
def process_operation_and_create_card(payload: Dict[str, Any]):
    print("--- 1. Iniciando procesamiento de tarjeta ---")

    # Extraer datos del payload
    operation_id = payload.get("operation_id")
    invoices = payload.get("invoices", [])

    # --- LOG DE DIAGNÓSTICO ---
    if not invoices:
        print("!!!!!!!! ERROR DE LÓGICA !!!!!!!!")
        print("La lista de 'invoices' en el payload está vacía. No se creará la tarjeta.")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return # Termina la función si no hay facturas

    # Extraer el resto de los datos
    client_name = payload.get("client_name")
    tasa = payload.get("tasa", "N/A")
    comision = payload.get("comision", "N/A")
    drive_folder_url = payload.get("drive_folder_url", "No disponible")
    attachment_paths = payload.get("attachment_paths", [])
    
    invoices_by_currency = defaultdict(list)
    for inv in invoices:
        invoices_by_currency[inv.get("currency", "PEN")].append(inv)
    
    label_ids_list = [TRELLO_LABEL_VERIFICADA, TRELLO_LABEL_CAVALI, TRELLO_LABEL_HR]
    id_labels_str = ",".join(filter(None, label_ids_list))

    for currency, invoices_in_group in invoices_by_currency.items():
        net_total = sum(inv.get("net_amount", 0.0) for inv in invoices_in_group)
        debtors_info = {inv['debtor_ruc']: inv['debtor_name'] for inv in invoices_in_group}
        
        debtors_str = ', '.join(_sanitize_name(name) for name in debtors_info.values() if name) or 'Ninguno'
        amount_str = f"{currency} {_format_number(net_total)}"
        current_date = datetime.datetime.now().strftime('%d.%m')
        
        card_title = (f"🤖 {current_date} // CLIENTE: {_sanitize_name(client_name)} // DEUDOR: {debtors_str} // MONTO: {amount_str} // OP: {operation_id[:8]}")
        debtors_markdown = '\n'.join(f"- RUC {ruc}: {_sanitize_name(name)}" for ruc, name in debtors_info.items()) or '- Ninguno'
        card_description = (
            f"**ID Operación:** {operation_id}\n\n"
            f"**Deudores:**\n{debtors_markdown}\n\n"
            f"**Tasa:** {tasa}\n"
            f"**Comisión:** {comision}\n"
            f"**Monto Operación:** {amount_str}\n\n"
            f"**Carpeta Drive:** {drive_folder_url}"
        )

        auth_params = {'key': TRELLO_API_KEY, 'token': TRELLO_TOKEN}
        card_payload = {
            'idList': TRELLO_LIST_ID,
            'name': card_title,
            'desc': card_description,
            'idLabels': id_labels_str
        }
        
        # --- LOG DE DIAGNÓSTICO ---
        print("\n--- 2. Preparando para llamar a la API de Trello ---")
        print(f"    URL: https://api.trello.com/1/cards")
        print(f"    API Key: {str(TRELLO_API_KEY)[:4]}... (oculto)")
        print(f"    List ID: {TRELLO_LIST_ID}")
        print(f"    Card Title: {card_title}")
        print("---------------------------------------------------\n")

        url_card = "https://api.trello.com/1/cards"
        response = requests.post(url_card, params=auth_params, json=card_payload)
        
        # Esta línea es crucial. Si Trello devuelve un error, detendrá el programa aquí.
        response.raise_for_status() 
        
        card_id = response.json()["id"]
        print(f"--- 3. Tarjeta creada exitosamente con ID: {card_id} ---")

        # Lógica para adjuntar archivos
        url_attachment = f"https://api.trello.com/1/cards/{card_id}/attachments"
        for path in attachment_paths:
            try:
                file_bytes = download_blob_as_bytes(path)
                filename = os.path.basename(path)
                files = {"file": (filename, file_bytes)}
                requests.post(url_attachment, params=auth_params, files=files)
            except Exception as e:
                print(f"ADVERTENCIA: No se pudo adjuntar el archivo {path}. Error: {e}")

# --- Endpoint HTTP ---
@app.post("/trello")
async def handle_trello_request(request: Request):
    try:
        payload = await request.json()
        # Log del payload completo que se recibe
        print("\n--- PAYLOAD RECIBIDO EN TRELLO SERVICE ---")
        print(json.dumps(payload, indent=2))
        print("------------------------------------------\n")
        
        process_operation_and_create_card(payload)
        return {"status": "SUCCESS", "message": "Proceso de Trello iniciado."}

    except requests.exceptions.HTTPError as e:
        # Este bloque se activará si raise_for_status() falla
        print(f"!!!!!!!! ERROR DE API TRELLO !!!!!!!!")
        print(f"Status Code: {e.response.status_code}")
        print(f"Respuesta de Trello: {e.response.text}")
        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        raise HTTPException(status_code=e.response.status_code, detail=f"Error de API Trello: {e.response.text}")
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")