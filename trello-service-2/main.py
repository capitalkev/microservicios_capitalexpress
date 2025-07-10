# 8002

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, HTTPException
from datetime import datetime
from google.cloud import storage
import os
import requests

app = FastAPI()

# Variables de entorno
TRELLO_API_KEY = os.getenv("TRELLO_API_KEY")
TRELLO_TOKEN = os.getenv("TRELLO_TOKEN")
TRELLO_BOARD_ID = os.getenv("TRELLO_BOARD_ID")
TRELLO_LIST_ID = os.getenv("TRELLO_LIST_ID")
GCP_BUCKET_NAME = os.getenv("BUCKET_NAME")
TRELLO_LABEL_VERIFICADA = os.getenv("TRELLO_LABEL_VERIFICADA")
TRELLO_LABEL_CAVALI = os.getenv("TRELLO_LABEL_CAVALI")
TRELLO_LABEL_HR = os.getenv("TRELLO_LABEL_HR")

# Cliente de GCS
storage_client = storage.Client()

def download_blob_as_bytes(gs_path: str) -> bytes:
    path_parts = gs_path.replace("gs://", "").split("/", 1)
    bucket_name, blob_path = path_parts[0], path_parts[1]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()

def create_trello_card(title: str, description: str) -> str:
    url = "https://api.trello.com/1/cards"
    params = {
        "key": TRELLO_API_KEY,
        "token": TRELLO_TOKEN,
        "idList": TRELLO_LIST_ID,
        "name": title,
        "desc": description,
        "idLabels": f"{TRELLO_LABEL_VERIFICADA},{TRELLO_LABEL_CAVALI},{TRELLO_LABEL_HR}"
    }
    response = requests.post(url, params=params)
    print("[TRELLO] Respuesta:", response.text)
    if response.status_code != 200:
        raise Exception("Error al crear tarjeta Trello")
    return response.json()["id"]

def attach_file_to_card(card_id: str, file_bytes: bytes, filename: str):
    url = f"https://api.trello.com/1/cards/{card_id}/attachments"
    params = {
        "key": TRELLO_API_KEY,
        "token": TRELLO_TOKEN
    }
    files = {
        "file": (filename, file_bytes)
    }
    response = requests.post(url, params=params, files=files)
    if response.status_code != 200:
        raise Exception("Error al adjuntar archivo a Trello")

@app.post("/trello")
async def create_trello_card_endpoint(request: Request):
    try:
        payload = await request.json()
        print("[TRELLO] Payload recibido:", payload)

        op_id = payload.get("operation_id")
        pdf_paths = payload.get("pdf_paths", [])
        respaldo_paths = payload.get("respaldo_paths", [])
        tasa = payload.get("tasa", "-")
        comision = payload.get("comision", "-")

        parser_result = payload.get("parsed_invoice_data", {})
        results = parser_result.get("results", [])
        if not results:
            raise HTTPException(status_code=400, detail="No se encontraron resultados en parsed_invoice_data")

        # Agrupar por moneda
        grouped_by_currency = {}
        for result in results:
            data = result["parsed_invoice_data"]
            currency = data.get("currency", "PEN")
            if currency not in grouped_by_currency:
                grouped_by_currency[currency] = []
            grouped_by_currency[currency].append(data)

        responses = []

        for currency, docs in grouped_by_currency.items():
            net_total = sum(doc.get("net_amount", 0.0) for doc in docs)
            debtor_set = set((doc.get("debtor_ruc"), doc.get("debtor_name")) for doc in docs)
            debtor_lines = [f"RUC {ruc}: {name}" for ruc, name in debtor_set]
            parsed = docs[0]

            title = f"🤖 {datetime.today().strftime('%d.%m')} // CLIENTE: {parsed['client_name']} // DEUDOR: {parsed['debtor_name']} // {currency} EN {net_total:,.2f} // OP: {op_id}"
            description = f"ID Operación: {op_id}\nDeudores:\n\n" + "\n".join(debtor_lines) + f"\n\nTasa: {tasa}%\nComisión: {comision}\nMonto Operación: {currency} {net_total:,.2f}"

            card_id = create_trello_card(title, description)

            for path in pdf_paths + respaldo_paths:
                try:
                    file_bytes = download_blob_as_bytes(path)
                    filename = os.path.basename(path)
                    attach_file_to_card(card_id, file_bytes, filename)
                except Exception as e:
                    print(f"[TRELLO] Error adjuntando {path}: {e}")

            responses.append({
                "status": "ok",
                "currency": currency,
                "title": title,
                "card_id": card_id
            })

        return responses

    except Exception as e:
        print("[TRELLO] Error inesperado:", str(e))
        raise HTTPException(status_code=400, detail=str(e))