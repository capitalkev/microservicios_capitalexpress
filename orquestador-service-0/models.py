# app/infrastructure/persistence/models.py
from sqlalchemy import Column, String, Float, ForeignKey, Integer, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# --- CORRECCIÓN AQUÍ ---
# Se quita el punto antes de 'database' para que sea un import absoluto.
from database import Base

class Empresa(Base):
    __tablename__ = "empresas"
    ruc = Column(String(15), primary_key=True, index=True)
    razon_social = Column(String(255))

class Operacion(Base):
    __tablename__ = "operaciones"
    id = Column(String(255), primary_key=True)
    cliente_ruc = Column(String(15), ForeignKey("empresas.ruc"), nullable=False)
    email_usuario = Column(String(255))
    nombre_ejecutivo = Column(Text)
    url_carpeta_drive = Column(Text)
    monto_sumatoria_total = Column(Float, server_default='0')
    moneda_sumatoria = Column(String(10))
    fecha_creacion = Column(DateTime(timezone=True), server_default=func.now())
    facturas = relationship("Factura", back_populates="operacion")

class Factura(Base):
    __tablename__ = "facturas"
    id = Column(Integer, primary_key=True)
    id_operacion = Column(String(255), ForeignKey("operaciones.id"), nullable=False)
    numero_documento = Column(String(255), nullable=False, index=True)
    deudor_ruc = Column(String(15), ForeignKey("empresas.ruc"), nullable=False)
    fecha_emision = Column(DateTime(timezone=True))
    fecha_vencimiento = Column(DateTime(timezone=True))
    moneda = Column(String(10))
    monto_total = Column(Float)
    monto_neto = Column(Float)
    mensaje_cavali = Column(Text)
    id_proceso_cavali = Column(String(255))
    
    # Relaciones actualizadas
    operacion = relationship("Operacion", back_populates="facturas")
    deudor = relationship("Empresa")