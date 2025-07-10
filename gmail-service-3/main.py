#8003

import os
import base64
import mimetypes
from fastapi import FastAPI, Request
from email.message import EmailMessage
from dotenv import load_dotenv

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build

from utils import download_blob_as_bytes

# Carga variables de entorno
load_dotenv()

# Constantes desde .env
RECIPIENT = os.getenv("GMAIL_RECIPIENT")
SENDER = os.getenv("GMAIL_SENDER")
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
TOKEN_FILE = "token.json"

# FastAPI
app = FastAPI()

# Scopes necesarios para enviar correos
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Dev: OAuth con token.json
def get_gmail_service_local():
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
    return build('gmail', 'v1', credentials=creds)

service = get_gmail_service_local()

# Función para limpiar caracteres HTML
def sanitize_name(name):
    return name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

@app.post("/gmail")
async def enviar_correo(request: Request):
    data = await request.json()
    print("[GMAIL] Payload:", data)

    pdf_paths = data.get("pdf_paths", [])
    parser_data = data.get("parsed_invoice_data", {}).get("results", [])

    facturas_por_deudor = {}
    for factura in parser_data:
        f = factura["parsed_invoice_data"]
        key = f["debtor_ruc"]
        if key not in facturas_por_deudor:
            facturas_por_deudor[key] = {"nombreDeudor": f["debtor_name"], "facturas": []}
        facturas_por_deudor[key]["facturas"].append({
            "rucDeudor": f["debtor_ruc"],
            "nombreDeudor": f["debtor_name"],
            "documento": f["document_id"],
            "monto_total": f["total_amount"],
            "monto_neto": f["net_amount"],
            "moneda": f["currency"],
            "dueDate": f["due_date"][:10]
        })

    nombre_cliente = parser_data[0]["parsed_invoice_data"]["client_name"]
    ruc_cliente = parser_data[0]["parsed_invoice_data"]["client_ruc"]

    for ruc, info in facturas_por_deudor.items():
        nombre_deudor = info["nombreDeudor"]
        facturas = info["facturas"]

        tabla_html = f"""
            <p><strong>Cliente:</strong> {sanitize_name(nombre_cliente)}</p>
            <p><strong>RUC Cliente:</strong> {ruc_cliente}</p>
            <table border="1" cellpadding="6" style="border-collapse: collapse;">
            <tr style="background-color:#f2f2f2;">
            <th>RUC Deudor</th>
            <th>Nombre Deudor</th>
            <th>Documento</th>
            <th>Monto Factura</th>
            <th>Monto Neto a pagar</th>
            <th>Fecha de Pago</th>
            </tr>
        """

        for f in facturas:
            fecha_pago = f["dueDate"]
            try:
                fecha_formateada = f"{fecha_pago[8:10]}-{fecha_pago[5:7]}-{fecha_pago[0:4]}"
            except:
                fecha_formateada = fecha_pago
            tabla_html += f"""
                <tr>
                    <td>{f['rucDeudor']}</td>
                    <td>{sanitize_name(f['nombreDeudor'])}</td>
                    <td>{f['documento']}</td>
                    <td>{f['moneda']} {f['monto_total']:.2f}</td>
                    <td>{f['moneda']} {f['monto_neto']:.2f}</td>
                    <td>{fecha_formateada}</td>
                </tr>
            """
        tabla_html += "</table>"

        asunto = f"Conformidad de facturas de su proveedor {sanitize_name(nombre_cliente)}"
        cuerpo = f"""
            <p><strong>Estimados señores:</strong></p>
            <p>Por medio de la presente informamos que los señores {sanitize_name(nombre_cliente)} nos están cediendo la(s) factura(s) negociable(s) que se adjunta(n), por lo que solicitamos se sirvan confirmar la veracidad de los mismos, en indicar si:</p>
            <ol>
              <li>¿Factura recepcionada con sus productos o servicios correctamente?</li>
              <li>Fecha programada para el pago.</li>
              <li>Monto Neto a pagar de Detracciones, Retenciones, Garantías u otros.</li>
            </ol>
            <p><strong>Detalle de las facturas:</strong></p>
            {tabla_html}
            <p>Con su confirmación, procederemos a anotar en nuestra cuenta en CAVALI para que nos brinden su conformidad.</p>
            <p><em>“Sin perjuicio de lo anteriormente mencionado (...)</em></p>
        """

        message = EmailMessage()
        message.set_content(cuerpo, subtype='html')
        message['To'] = RECIPIENT
        message['From'] = SENDER
        message['Subject'] = asunto

        for pdf_path in pdf_paths:
            try:
                pdf_bytes = download_blob_as_bytes(pdf_path)
                filename = os.path.basename(pdf_path)
                content_type, _ = mimetypes.guess_type(filename)
                message.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=filename)
            except Exception as e:
                print(f"[GMAIL] Error adjuntando {pdf_path}: {e}")

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw_message}).execute()

    return {"status": "sent"}
