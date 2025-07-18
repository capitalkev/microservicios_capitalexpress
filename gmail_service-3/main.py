import os
import base64
import mimetypes
from fastapi import FastAPI, Request, HTTPException
from email.message import EmailMessage
from dotenv import load_dotenv
from collections import defaultdict
import pandas as pd
from pydantic import BaseModel
from typing import List, Optional

# --- Importaciones de Google ---
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build

load_dotenv()

# --- Configuración ---
USER_TOKEN_FILE = 'token.json'
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SENDER_USER_ID = 'kevin.gianecchine@capitalexpress.cl'
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/devstorage.read_only']

# Lista fija de correos que siempre recibirán una copia
FIXED_CC_LIST = [
    'kevin.gianecchine@capitalexpress.cl',
    'jenssy.huaman@capitalexpress.pe',
    'jakeline.quispe@capitalexpress.pe',
    'jhonny.celay@capitalexpress.pe',
    'kevin.tupac@capitalexpress.cl'
]

app = FastAPI(title="Servicio de Gmail Híbrido Avanzado")

class InvoiceData(BaseModel):
    document_id: Optional[str] = None
    issue_date: Optional[str] = None
    due_date: Optional[str] = None
    currency: Optional[str] = None
    total_amount: Optional[float] = 0.0
    net_amount: Optional[float] = 0.0
    debtor_name: Optional[str] = None
    debtor_ruc: Optional[str] = None
    client_name: Optional[str] = None
    client_ruc: Optional[str] = None

# --- Funciones de Autenticación ---
def get_user_credentials():
    creds = None
    if os.path.exists(USER_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(USER_TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request as GoogleRequest
            creds.refresh(GoogleRequest())
        else:
            raise Exception("No se encontraron credenciales de usuario válidas.")
    return creds

def get_storage_client():
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
         raise Exception("No se encontró el archivo de cuenta de servicio.")
    sa_creds = ServiceAccountCredentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return storage.Client(credentials=sa_creds)

# --- Función para Crear el HTML ---
def create_html_body(invoice_data_list: List[InvoiceData]) -> str:
    if not invoice_data_list:
        return "<p>No hay datos de facturas para procesar.</p>"

    first_invoice = invoice_data_list[0]
    client_name = first_invoice.client_name
    client_ruc = first_invoice.client_ruc
    
    data_for_df = [invoice.dict() for invoice in invoice_data_list]
    df = pd.DataFrame(data_for_df)

    # Formatear columnas
    df['total_amount'] = df.apply(lambda row: f"{row.get('currency', '')} {float(row.get('total_amount', 0)):,.2f}".strip(), axis=1)
    df['net_amount'] = df.apply(lambda row: f"{row.get('currency', '')} {float(row.get('net_amount', 0)):,.2f}".strip(), axis=1)
    df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce').dt.strftime('%d/%m/%Y')
    
    df_display = df.rename(columns={
        'debtor_ruc': 'RUC Deudor', 'debtor_name': 'Nombre Deudor',
        'document_id': 'Documento', 'total_amount': 'Monto Factura',
        'net_amount': 'Monto Neto', 'due_date': 'Fecha de Pago'
    })
    
    display_columns = ['RUC Deudor', 'Nombre Deudor', 'Documento', 'Monto Factura', 'Monto Neto', 'Fecha de Pago']
    tabla_html = df_display[display_columns].to_html(index=False, border=1, justify='left', classes='invoice_table')

    mensaje_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, Helvetica, sans-serif; font-size: 13px; color: #000; }}
            .container {{ max-width: 800px; }}
            p, li {{ line-height: 1.5; }}
            ol {{ padding-left: 30px; }}
            table.invoice_table {{ width: 100%; border-collapse: collapse; margin-top: 15px; margin-bottom: 20px; }}
            table.invoice_table th, table.invoice_table td {{ border: 1px solid #777; padding: 6px; text-align: left; font-size: 12px; }}
            table.invoice_table th {{ background-color: #f0f0f0; font-weight: bold; }}
            .disclaimer {{ font-style: italic; font-size: 11px; margin-top: 25px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <p>Estimados señores,</p>
            <p>Por medio de la presente, les informamos que los señores de <strong>{client_name}</strong>, nos han transferido la(s) siguiente(s) factura(s) negociable(s). Solicitamos su amable confirmación sobre los siguientes puntos:</p>
            <ol>
                <li>¿La(s) factura(s) ha(n) sido recepcionada(s) conforme con sus productos o servicios?</li>
                <li>¿Cuál es la fecha programada para el pago de la(s) misma(s)?</li>
                <li>Por favor, confirmar el Monto Neto a pagar, considerando detracciones, retenciones u otros descuentos.</li>
            </ol>
            <p><strong>Detalle de las facturas:</strong></p>
            <p>
                Cliente: {client_name}<br>
                RUC Cliente: {client_ruc}
            </p>
            {tabla_html}
            <p>Agradecemos de antemano su pronta respuesta. Con su confirmación, procederemos a la anotación en cuenta en CAVALI.</p>
            <p class="disclaimer">"Sin perjuicio de lo anteriormente mencionado, nos permitimos recordarles que toda acción tendiente a simular la emisión de la referida factura negociable o letra para obtener un beneficio a título personal o a favor de la otra parte de la relación comercial, teniendo pleno conocimiento de que la misma no proviene de una relación comercial verdadera, se encuentra sancionada penalmente como delito de estafa en nuestro ordenamiento jurídico.
            <br>Asimismo, en caso de que vuestra representada cometa un delito de forma conjunta y/o en contubernio con el emitente de la factura, dicha acción podría tipificarse como delito de asociación ilícita para delinquir, según el artículo 317 del Código Penal, por lo que nos reservamos el derecho de iniciar las acciones penales correspondientes en caso resulte necesario."</p>
        </div>
    </body>
    </html>
    """
    return mensaje_html

# --- Endpoint Principal ---
@app.post("/gmail")
async def send_verification_email(request: Request):
    try:
        user_creds = get_user_credentials()
        storage_client = get_storage_client()
        gmail_service = build('gmail', 'v1', credentials=user_creds)

        data = await request.json()
        pdf_paths = data.get("pdf_paths", [])
        parser_results = data.get("parsed_invoice_data", {}).get("results", [])
        
        # --- Lógica de Destinatarios ---
        emails_from_excel = data.get("recipient_emails")
        user_email = data.get("user_email")

        if not parser_results or not emails_from_excel:
            return {"status": "SKIPPED", "message": "Faltan datos de parser o correos del Excel."}

        # Construir la lista final de CC
        cc_list = set(FIXED_CC_LIST)
        if user_email:
            cc_list.add(user_email)
        
        cc_string = ",".join(sorted(list(cc_list)))

        # Procesar facturas
        invoice_models = [InvoiceData(**res['parsed_invoice_data']) for res in parser_results if res.get('status') == 'SUCCESS']
        if not invoice_models:
            return {"status": "SKIPPED", "message": "No hay facturas válidas para enviar."}

        facturas_por_deudor = defaultdict(list)
        for invoice in invoice_models:
            facturas_por_deudor[invoice.debtor_ruc].append(invoice)

        for ruc_deudor, facturas_grupo in facturas_por_deudor.items():
            html_body = create_html_body(facturas_grupo)

            message = EmailMessage()
            message.add_alternative(html_body, subtype='html')
            
            # Asignación de Destinatarios
            message['To'] = emails_from_excel
            message['Cc'] = cc_string
            message['Subject'] = f"Confirmación de Facturas Negociables - {facturas_grupo[0].client_name}"

            # Adjuntar archivos PDF
            for pdf_path in pdf_paths:
                try:
                    bucket_name, blob_name = pdf_path.replace("gs://", "").split("/", 1)
                    blob = storage_client.bucket(bucket_name).blob(blob_name)
                    pdf_bytes = blob.download_as_bytes()
                    maintype, subtype = (mimetypes.guess_type(os.path.basename(pdf_path))[0] or "application/octet-stream").split('/')
                    message.add_attachment(pdf_bytes, maintype=maintype, subtype=subtype, filename=os.path.basename(pdf_path))
                except Exception as e:
                    print(f"ADVERTENCIA al adjuntar {pdf_path}: {e}")

            encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            create_message_request = {'raw': encoded_message}
            gmail_service.users().messages().send(userId=SENDER_USER_ID, body=create_message_request).execute()
            print(f"Correo para deudor {ruc_deudor} enviado a: {emails_from_excel} con CC a: {cc_string}")

        return {"status": "SUCCESS", "message": "Correos de notificación enviados."}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno en el servicio de Gmail: {str(e)}")