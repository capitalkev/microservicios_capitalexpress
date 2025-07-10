# database.py
import os
import json
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector

# --- Conexión Segura y Centralizada a Cloud SQL ---
# Esta es la forma moderna y recomendada por Google para conectar.
connector = Connector()

def get_db_connection():
    """
    Crea y retorna una conexión a la base de datos de Cloud SQL.
    Utiliza las variables de entorno para los detalles de la conexión.
    """
    try:
        # Crea un "motor" de conexión que gestiona un pool de conexiones.
        engine = create_engine(
            "postgresql+pg8000://",
            creator=lambda: connector.connect(
                os.getenv("DB_INSTANCE_CONNECTION_NAME"), # ej: "proyecto:region:instancia"
                "pg8000",
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                db=os.getenv("DB_NAME")
            ),
            pool_pre_ping=True,  # Verifica que la conexión esté viva antes de usarla.
            pool_recycle=3600,   # Recicla conexiones cada hora para evitar timeouts.
        )
        return engine.connect()
    except Exception as e:
        print(f"CRITICAL [DB Connection]: No se pudo crear la conexión a la base de datos. Error: {e}")
        # Lanza la excepción para que el servicio que la llama sepa que no puede continuar.
        raise

def crear_o_actualizar_empresa(conn, ruc: str, razon_social: str):
    """
    (Función interna) Crea una empresa si no existe, basado en el RUC.
    Reutiliza una conexión existente para ser más eficiente dentro de una transacción.
    """
    stmt = text("""
        INSERT INTO empresas (ruc, razon_social) VALUES (:ruc, :razon_social)
        ON CONFLICT (ruc) DO NOTHING;
    """)
    conn.execute(stmt, {"ruc": ruc, "razon_social": razon_social})

def crear_operaciones_agrupadas(group_id: str, grouped_invoices: dict, email_usuario: str):
    """
    Crea los registros principales en la BD, generando una operación distinta para cada moneda.
    Retorna la lista de IDs de operación únicos que se crearon.
    """
    operation_ids = []
    try:
        with get_db_connection() as conn:
            for moneda, facturas_info in grouped_invoices.items():
                op_id = f"{group_id}-{moneda}"
                operation_ids.append(op_id)
                
                # Crear o verificar la existencia de las empresas (cliente y deudores)
                primera_factura = facturas_info['facturas'][0]['parsed_invoice_data']
                crear_o_actualizar_empresa(conn, primera_factura['client_ruc'], primera_factura['client_name'])

                # 1. Crear la operación principal para esta moneda
                op_stmt = text("INSERT INTO operaciones (id, group_id, cliente_ruc, email_usuario, monto_operacion, moneda) VALUES (:id, :group_id, :cliente_ruc, :email, :monto, :moneda)")
                conn.execute(op_stmt, {"id": op_id, "group_id": group_id, "cliente_ruc": primera_factura['client_ruc'], "email": email_usuario, "monto": facturas_info['total_neto'], "moneda": moneda})

                # 2. Insertar todas las facturas de esta moneda
                factura_stmt = text("INSERT INTO facturas (id_operacion, numero_documento, deudor_ruc) VALUES (:op_id, :num_doc, :deudor_ruc)")
                for factura_result in facturas_info['facturas']:
                    factura = factura_result['parsed_invoice_data']
                    crear_o_actualizar_empresa(conn, factura['debtor_ruc'], factura['debtor_name'])
                    conn.execute(factura_stmt, {"op_id": op_id, "num_doc": factura['document_id'], "deudor_ruc": factura['debtor_ruc']})
            
            conn.commit() # Confirma todas las inserciones como una única transacción
        print(f"[DB] Operaciones creadas para el grupo {group_id}: {operation_ids}")
        return operation_ids
    except Exception as e:
        print(f"CRITICAL [DB Create Ops]: No se pudieron crear las operaciones para el grupo {group_id}. Error: {e}")
        return []

def actualizar_factura_cavali(op_id: str, numero_documento: str, mensaje: str, id_proceso: str):
    """
    Actualiza una factura específica con el resultado del procesamiento de Cavali.
    Esta función es usada por el 'cavali-service'.
    """
    with get_db_connection() as conn:
        stmt = text("""
            UPDATE facturas
            SET mensaje_cavali = :mensaje, id_proceso_cavali = :id_proceso
            WHERE id_operacion = :op_id AND numero_documento = :numero_documento;
        """)
        conn.execute(stmt, {"op_id": op_id, "numero_documento": numero_documento, "mensaje": mensaje, "id_proceso": id_proceso})
        conn.commit()

def update_log(op_id: str, service_name: str, status: str, message: str, details: list = None):
    """
    Crea o actualiza un log para el modal de progreso del frontend.
    """
    try:
        with get_db_connection() as conn:
            stmt = text("""
                INSERT INTO operation_logs (operation_id, service_name, status, message, details)
                VALUES (:op_id, :service_name, :status, :message, :details)
                ON CONFLICT (operation_id, service_name) DO UPDATE SET
                    status = EXCLUDED.status, message = EXCLUDED.message, details = EXCLUDED.details, timestamp = NOW();
            """)
            conn.execute(stmt, {"op_id": op_id, "service_name": service_name, "status": status, "message": message, "details": json.dumps(details) if details else None})
            conn.commit()
    except Exception as e:
        print(f"ERROR [DB Log]: No se pudo actualizar el log para {op_id} en {service_name}. Error: {e}")

def get_operation_status_from_db(group_id: str) -> dict:
    """
    Obtiene el estado de todos los logs para un grupo de operaciones,
    formateado para que el frontend lo pueda consumir fácilmente.
    """
    try:
        with get_db_connection() as conn:
            op_stmt = text("SELECT id, moneda FROM operaciones WHERE group_id = :group_id")
            op_results = conn.execute(op_stmt, {"group_id": group_id}).fetchall()
            if not op_results: return {}

            op_ids = [row[0] for row in op_results]
            log_stmt = text("SELECT operation_id, service_name, status, message, details FROM operation_logs WHERE operation_id = ANY(:op_ids) ORDER BY service_name, timestamp")
            log_results = conn.execute(log_stmt, {"op_ids": op_ids}).fetchall()
            
            status_map = {row[1]: {} for row in op_results}
            for log in log_results:
                op_id, service_name, status, message, details = log
                moneda = op_id.split('-')[-1]
                service_key = service_name.lower().replace(" ", "_") # Ej: 'Envío de Verificación' -> 'envio_de_verificacion'
                status_map[moneda][service_key] = {"status": status, "message": message, "details": details or []}
            return status_map
    except Exception as e:
        print(f"ERROR [DB Get Status]: No se pudo obtener el estado para el grupo {group_id}. Error: {e}")
        return {}
    
def actualizar_url_drive(op_id: str, url: str):
    """
    Guarda la URL de la carpeta de Google Drive en el registro de la operación.
    """
    with get_db_connection() as conn:
        stmt = text("""
            UPDATE operaciones SET url_carpeta_drive = :url
            WHERE id = :op_id;
        """)
        conn.execute(stmt, {"op_id": op_id, "url": url})
        conn.commit()