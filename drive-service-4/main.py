# drive-service-4/main.py
import os
import io
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

load_dotenv()
app = FastAPI(title="Drive Service")

# --- Configuración de Google ---
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "client_secret.json")
TOKEN_FILE = "token.json"
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

storage_client = storage.Client()

class ArchiveRequest(BaseModel):
    operation_id: str # Se usará para el nombre de la carpeta
    gcs_file_paths: list[str]

def get_drive_service():
    """Construye y retorna un cliente de servicio de Google Drive autenticado."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

@app.post("/archive-files")
async def archive_files(request: ArchiveRequest):
    """
    Crea una carpeta en Drive para la operación y sube los archivos desde GCS.
    """
    if not request.gcs_file_paths:
        raise HTTPException(status_code=400, detail="No se proporcionaron archivos para archivar.")

    try:
        drive_service = get_drive_service()
        folder_name = f"Operacion_{request.operation_id}"

        # 1. Crear la carpeta en Drive
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [DRIVE_PARENT_FOLDER_ID]
        }
        folder = drive_service.files().create(body=folder_metadata, fields='id, webViewLink').execute()
        folder_id = folder.get('id')
        folder_url = folder.get('webViewLink')

        # 2. Subir cada archivo a la nueva carpeta
        for gcs_path in request.gcs_file_paths:
            try:
                bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
                blob = storage_client.bucket(bucket_name).blob(blob_name)
                file_bytes = blob.download_as_bytes()
                
                file_metadata = {'name': os.path.basename(gcs_path), 'parents': [folder_id]}
                media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype='application/octet-stream')
                drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            except Exception as e:
                print(f"ADVERTENCIA: Falló la subida de '{gcs_path}' a Drive. Error: {e}")
        
        return {"status": "SUCCESS", "drive_folder_url": folder_url}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el servicio de Drive: {str(e)}")