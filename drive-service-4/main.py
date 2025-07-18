# drive-service/main.py
import os
import io
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from google.cloud import storage
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# --- Configuración ---
app = FastAPI(
    title="Servicio de Google Drive",
    description="Crea carpetas y sube archivos desde Cloud Storage a Google Drive."
)

# Carga las variables de entorno
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")
SERVICE_ACCOUNT_FILE = 'service_account.json' # El nombre del archivo de clave
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- Clientes de Google ---
# Usa las credenciales del entorno de Cloud Run para GCS
storage_client = storage.Client()

# Usa la clave JSON específica para la API de Drive
try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
except FileNotFoundError:
    drive_service = None
    print("ADVERTENCIA: No se encontró el archivo service_account.json. El servicio de Drive no funcionará.")
except Exception as e:
    drive_service = None
    print(f"ADVERTENCIA: No se pudo inicializar el servicio de Drive. Error: {e}")


# --- Modelos de Datos ---
class ArchiveRequest(BaseModel):
    operation_id: str = Field(..., description="ID de la operación, se usará para el nombre de la carpeta.")
    gcs_file_paths: list[str] = Field(..., description="Lista de rutas de archivos en GCS a archivar.")


# --- Endpoint ---
@app.post("/archive-files")
async def archive_files(request: ArchiveRequest):
    """
    Crea una carpeta en Drive para la operación y sube los archivos desde GCS.
    """
    if not drive_service:
        raise HTTPException(status_code=500, detail="El servicio de Google Drive no está inicializado correctamente.")
    if not request.gcs_file_paths:
        raise HTTPException(status_code=400, detail="No se proporcionaron rutas de archivos para archivar.")

    # 1. Crear la carpeta para la operación en la Unidad Compartida
    try:
        folder_metadata = {
            'name': f"Operacion_{request.operation_id}",
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [DRIVE_PARENT_FOLDER_ID]
        }
        # 'supportsAllDrives=True' es crucial para Unidades Compartidas
        folder = drive_service.files().create(
            body=folder_metadata,
            fields='id, webViewLink',
            supportsAllDrives=True
        ).execute()
        folder_id = folder.get('id')
        folder_url = folder.get('webViewLink')
        print(f"Carpeta creada con éxito en Drive. URL: {folder_url}")

    except HttpError as e:
        print(f"ERROR FATAL al crear carpeta en Drive: {e.content}")
        raise HTTPException(status_code=500, detail=f"No se pudo crear la carpeta en Drive: {e.content}")
    except Exception as e:
        print(f"ERROR FATAL Inesperado al crear carpeta: {e}")
        raise HTTPException(status_code=500, detail=f"Error inesperado al crear carpeta: {str(e)}")

    # 2. Subir cada archivo a la nueva carpeta
    successful_uploads = 0
    for gcs_path in request.gcs_file_paths:
        try:
            # Descargar archivo de GCS
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            file_bytes = blob.download_as_bytes()
            
            # Subir archivo a Drive
            file_metadata = {
                'name': os.path.basename(gcs_path),
                'parents': [folder_id]
            }
            media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype='application/octet-stream', resumable=True)
            
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True # También necesario aquí
            ).execute()
            successful_uploads += 1

        except Exception as e:
            # Si un archivo falla, solo se imprime una advertencia y se continúa con el siguiente
            print(f"ADVERTENCIA: Falló la subida de '{gcs_path}' a Drive. Error: {e}")
            
    print(f"Proceso de subida finalizado. {successful_uploads}/{len(request.gcs_file_paths)} archivos subidos.")
    
    return {"status": "SUCCESS", "drive_folder_url": folder_url, "files_uploaded": successful_uploads}