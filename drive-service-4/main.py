# drive_service/main.py
import os
import io
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- Carga de configuración ---
load_dotenv()
app = FastAPI(title="Drive Service", description="Microservicio para archivar archivos en Google Drive.")

# --- Constantes y Configuración de Autenticación ---
# Scopes para Google Drive: Permiso completo sobre los archivos que la app cree.
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

# --- Clientes de Google ---
storage_client = storage.Client()

# --- Modelo Pydantic para validación de datos ---
class DriveRequest(BaseModel):
    operation_id: str
    file_paths: list[str]


def get_drive_service():
    """
    Construye el cliente de servicio de Drive, manejando la autenticación
    de la misma forma que el gmail-service.
    """
    creds = None
    # El archivo token.json almacena los tokens de acceso y actualización del usuario.
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # Si no hay credenciales (válidas), permite que el usuario inicie sesión.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Guarda las credenciales para la próxima ejecución
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
            
    return build('drive', 'v3', credentials=creds)

def archive_operation_files(drive_service, operation_id: str, gcs_paths: list) -> str:
    """Crea una carpeta para la operación y sube los archivos desde GCS a Google Drive."""
    print(f"[{operation_id}] Iniciando archivado en Google Drive...")
    folder_name = f"Operacion_{operation_id}"

    # 1. Crear la carpeta en Google Drive
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [DRIVE_PARENT_FOLDER_ID]
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id, webViewLink').execute()
    folder_id = folder.get('id')
    folder_url = folder.get('webViewLink')
    print(f"[{operation_id}] Carpeta creada: {folder_url}")

    # 2. Subir cada archivo a la nueva carpeta
    for gcs_path in gcs_paths:
        try:
            filename = os.path.basename(gcs_path)
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            file_content_bytes = blob.download_as_bytes()
            
            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaIoBaseUpload(io.BytesIO(file_content_bytes), mimetype='application/octet-stream', resumable=True)
            drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"[{operation_id}] Archivo '{filename}' subido correctamente.")
        except Exception as e:
            print(f"ADVERTENCIA: Falló la subida de '{gcs_path}'. Error: {e}")
            
    return folder_url

# --- Endpoint Principal del Servicio ---
@app.post("/drive", status_code=200)
async def handle_archive_request(data: DriveRequest):
    """Recibe la solicitud del orquestador y ejecuta el proceso de archivado."""
    op_id = data.operation_id
    file_paths = data.file_paths

    if not file_paths:
        return {"status": "NO_FILES", "operation_id": op_id, "detail": "No se proporcionaron archivos para archivar."}

    try:
        service = get_drive_service()
        folder_url = archive_operation_files(service, op_id, file_paths)
        
        print(f"[{op_id}] Proceso de archivado completado con éxito.")
        return {
            "status": "SUCCESS",
            "operation_id": op_id,
            "drive_folder_url": folder_url
        }
    except Exception as e:
        print(f"[ERROR][{op_id}] {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Ejecución del Servidor ---
if __name__ == "__main__":
    import uvicorn
    print("Iniciando Drive Service en http://localhost:8004")
    uvicorn.run(app, host="0.0.0.0", port=8004)