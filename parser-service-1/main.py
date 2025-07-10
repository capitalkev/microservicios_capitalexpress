## 8001
import os
import json
from fastapi import FastAPI, Request, HTTPException
from google.cloud import storage
from parser import extract_invoice_data

app = FastAPI(title="Parser Service")

# Configuración del bucket
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "operaciones-peru")
BUCKET_NAME = os.getenv("BUCKET_NAME", "tu-bucket-pruebas")  # asegúrate de definir esto en tu .env o directamente aquí

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)


def read_xml_from_gcs(gcs_path):
    parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name, file_path = parts
    blob = storage_client.bucket(bucket_name).blob(file_path)
    return blob.download_as_bytes()


@app.post("/parser")
async def receive_parser_request(command: dict):
    op_id = command.get("operation_id")
    xml_paths = command.get("xml_paths") or []

    if not op_id or not xml_paths:
        raise HTTPException(status_code=400, detail="Faltan campos requeridos (operation_id, xml_paths)")

    print(f"[Parser] Procesando operación: {op_id} con {len(xml_paths)} XMLs")

    results = []

    for idx, xml_path in enumerate(xml_paths):
        print(f"[Parser] Procesando XML {idx+1}: {xml_path}")
        if not xml_path:
            results.append({
                "status": "ERROR",
                "xml_index": idx + 1,
                "xml_path": None,
                "error_message": "Ruta del XML no proporcionada."
            })
            continue

        try:
            xml_bytes = read_xml_from_gcs(xml_path)
            invoice_data = extract_invoice_data(xml_bytes)
            result = {
                "operation_id": op_id,
                "status": "SUCCESS",
                "xml_index": idx + 1,
                "xml_path": xml_path,
                "parsed_invoice_data": invoice_data
            }
            print(f"[Parser] Datos extraídos correctamente: {invoice_data}")
        except Exception as e:
            result = {
                "operation_id": op_id,
                "status": "ERROR",
                "xml_index": idx + 1,
                "xml_path": xml_path,
                "error_message": str(e)
            }
            print(f"[Parser] Error al procesar XML {idx+1}: {e}")

        results.append(result)

    return {"message": f"Procesados {len(xml_paths)} archivos", "results": results}
