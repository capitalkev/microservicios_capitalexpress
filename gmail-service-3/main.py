# gmail-service-3/main.py

import os
import base64
import mimetypes
from fastapi import FastAPI, Request, HTTPException
from email.message import EmailMessage
from dotenv import load_dotenv
from collections import defaultdict

# --- Importamos pandas ---
import pandas as pd

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build

from utils import download_blob_as_bytes

load_dotenv()

# ... (Configuración de SCOPES, etc., no cambia) ...
RECIPIENT = os.getenv("GMAIL_RECIPIENT")
SENDER = os.getenv("GMAIL_SENDER")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
TOKEN_FILE = "token.json"
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/devstorage.read_only'
]

app = FastAPI()

def get_google_creds():
    # ... (esta función se mantiene igual)
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
    return creds

def create_html_body_with_pandas(cliente_nombre, cliente_ruc, facturas_grupo):
    """
    Crea el cuerpo HTML del correo usando Pandas para la tabla.
    """
    df = pd.DataFrame(facturas_grupo)

    # Formatear columnas para una mejor visualización
    df['monto_total'] = df.apply(lambda row: f"{row.get('moneda', '')} {float(row.get('monto_total', 0)):,.2f}", axis=1)
    df['monto_neto'] = df.apply(lambda row: f"{row.get('moneda', '')} {float(row.get('monto_neto', 0)):,.2f}", axis=1)
    
    # Renombrar columnas para la tabla
    df_display = df.rename(columns={
        'rucDeudor': 'RUC Deudor',
        'nombreDeudor': 'Nombre Deudor',
        'documento': 'Documento',
        'monto_total': 'Monto Factura',
        'monto_neto': 'Monto Neto',
        'dueDate': 'Fecha de Pago'
    })

    display_columns = ['RUC Deudor', 'Nombre Deudor', 'Documento', 'Monto Factura', 'Monto Neto', 'Fecha de Pago']
    tabla_html = df_display[display_columns].to_html(index=False, border=0, justify='center', classes='invoices_table')

    # Construir el HTML completo del correo
    mensaje_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 14px; color: #333; line-height: 1.6; }}
            .email-container {{ max-width: 700px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9; }}
            table.invoices_table {{ border-collapse: collapse; width: 100%; margin: 25px 0; }}
            th, td {{ text-align: left; padding: 12px; border-bottom: 1px solid #eee; }}
            th {{ background-color: #f2f2f2; font-weight: 600; color: #555; }}
            .highlight {{ font-weight: 600; color: #0056b3; }}
            .disclaimer {{ font-style: italic; color: #777; font-size: 11px; margin-top: 30px; border-top: 1px solid #eee; padding-top: 15px; }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <p>Estimados señores,</p>
            <p>
                Por medio de la presente, les informamos que los señores de 
                <span class="highlight">{cliente_nombre}</span> (RUC: {cliente_ruc}) nos han transferido la(s) siguiente(s)
                factura(s) negociable(s). Agradeceríamos su amable confirmación.
            </p>
            <h3>Detalle de las facturas:</h3>
            {tabla_html}
            <p class="disclaimer"><strong>Cláusula Legal:</strong> Sin perjuicio de lo anteriormente mencionado... (etc.)</p>
        </div>
    </body>
    </html>
    """
    return mensaje_html

@app.post("/gmail")
async def send_verification_email(request: Request):
    try:
        data = await request.json()
        pdf_paths = data.get("pdf_paths", [])
        parser_data = data.get("parsed_invoice_data", {}).get("results", [])

        if not parser_data:
            return {"status": "SKIPPED", "message": "No hay datos para enviar correo."}

        creds = get_google_creds()
        
        facturas_por_deudor = defaultdict(list)
        for item in parser_data:
            factura = item.get("parsed_invoice_data", {})
            facturas_por_deudor[factura['debtor_ruc']].append({
                "rucDeudor": factura['debtor_ruc'],
                "nombreDeudor": factura['debtor_name'],
                "documento": factura['document_id'],
                "monto_total": factura['total_amount'],
                "monto_neto": factura['net_amount'],
                "moneda": factura['currency'],
                "dueDate": factura['due_date'][:10] if factura.get('due_date') else 'N/A'
            })
        
        cliente_nombre = parser_data[0].get("parsed_invoice_data", {}).get("client_name", "N/A")
        cliente_ruc = parser_data[0].get("parsed_invoice_data", {}).get("client_ruc", "N/A")

        for ruc_deudor, facturas_grupo in facturas_por_deudor.items():
            
            html_body = create_html_body_with_pandas(cliente_nombre, cliente_ruc, facturas_grupo)

            message = EmailMessage()
            message.set_content(html_body, subtype='html')
            message['To'] = RECIPIENT
            message['From'] = SENDER
            message['Subject'] = f"Conformidad de Facturas del Proveedor: {cliente_nombre}"

            for pdf_path in pdf_paths:
                try:
                    pdf_bytes = download_blob_as_bytes(pdf_path, creds)
                    filename = os.path.basename(pdf_path)
                    maintype, subtype = (mimetypes.guess_type(filename)[0] or "application/octet-stream").split('/')
                    message.add_attachment(pdf_bytes, maintype=maintype, subtype=subtype, filename=filename)
                except Exception as e:
                    print(f"ADVERTENCIA: No se pudo adjuntar el archivo {pdf_path}. Error: {e}")

            service = build('gmail', 'v1', credentials=creds)
            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            create_message_request = {'raw': encoded_message}
            service.users().messages().send(userId='me', body=create_message_request).execute()

        return {"status": "SUCCESS", "message": f"Correo(s) de verificación enviado(s) a {RECIPIENT}."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de Gmail: {str(e)}")