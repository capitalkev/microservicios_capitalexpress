# orquestador-service-0/repository.py
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import func
from datetime import datetime
from models import Operacion, Factura, Empresa

class OperationRepository:
    def __init__(self, db: Session):
        self.db = db

    def _find_or_create_company(self, ruc: str, name: str) -> Optional[Empresa]:
        if not ruc or not name: return None
        empresa = self.db.query(Empresa).filter(Empresa.ruc == ruc).first()
        if not empresa:
            empresa = Empresa(ruc=ruc, razon_social=name)
            self.db.add(empresa)
            self.db.flush()
        return empresa

    def generar_siguiente_id_operacion(self) -> str:
        """Genera un ID de operación único y secuencial para el día actual."""
        today_str = datetime.now().strftime('%Y%m%d')
        id_prefix = f"OP-{today_str}-"
        last_id_today = self.db.query(func.max(Operacion.id)).filter(Operacion.id.like(f"{id_prefix}%")).scalar()
        next_number = int(last_id_today.split('-')[-1]) + 1 if last_id_today else 1
        return f"{id_prefix}{next_number:03d}"

    def save_full_operation(self, operation_id: str, metadata: dict, drive_url: str, invoices_data: List[Dict], cavali_results_map: Dict) -> str:
        """
        Guarda una operación completa para UNA SOLA MONEDA.
        """
        if not invoices_data:
            raise ValueError("No se puede guardar una operación sin datos de facturas.")

        client_ruc = invoices_data[0].get('client_ruc')
        client_name = invoices_data[0].get('client_name')
        primer_cliente = self._find_or_create_company(client_ruc, client_name)

        # La sumatoria ahora es correcta porque todas las facturas son de la misma moneda.
        monto_sumatoria = sum(inv.get('total_amount', 0) for inv in invoices_data)
        moneda_operacion = invoices_data[0].get('currency')
        email = metadata.get('user_email', 'unknown@example.com')
        nombre_ejecutivo = email.split('@')[0].replace('.', ' ').title()

        db_operacion = Operacion(
            id=operation_id,
            cliente_ruc=primer_cliente.ruc,
            email_usuario=email,
            nombre_ejecutivo=nombre_ejecutivo,
            url_carpeta_drive=drive_url,
            monto_sumatoria_total=monto_sumatoria,
            moneda_sumatoria=moneda_operacion
        )
        self.db.add(db_operacion)
        self.db.flush()

        for inv in invoices_data:
            deudor = self._find_or_create_company(inv.get('debtor_ruc'), inv.get('debtor_name'))
            # El 'id_proceso_cavali' ahora es el mismo para todo el lote
            cavali_data = cavali_results_map.get(inv.get('xml_filename'), {})
            
            db_factura = Factura(
                id_operacion=operation_id,
                numero_documento=inv.get('document_id'),
                deudor_ruc=deudor.ruc if deudor else None,
                fecha_emision=datetime.fromisoformat(inv.get('issue_date')) if inv.get('issue_date') else None,
                fecha_vencimiento=datetime.fromisoformat(inv.get('due_date')) if inv.get('due_date') else None,
                moneda=inv.get('currency'),
                monto_total=inv.get('total_amount'),
                monto_neto=inv.get('net_amount'),
                mensaje_cavali=cavali_data.get("message"),
                id_proceso_cavali=cavali_results_map.get("global_process_id") 
            )
            self.db.add(db_factura)
            
        # Hacemos commit al final del proceso de guardado para esta operación
        self.db.commit()
        return operation_id