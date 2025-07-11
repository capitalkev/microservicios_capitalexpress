# orquestador-service-0/database.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# --- Importaciones añadidas ---
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector

# --- CORRECCIÓN CLAVE: Cargar las variables de entorno AQUÍ ---
# Esto asegura que las variables estén disponibles tan pronto como se importe este archivo.
load_dotenv()

# --- Conector de Cloud SQL ---
connector = Connector()

# --- Función para obtener la conexión a la base de datos ---
def get_db_connection():
    """
    Crea y retorna un motor de conexión a la base de datos de Cloud SQL.
    """
    instance_connection_name = os.getenv("DB_INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")

    # Validar que las variables de entorno se hayan cargado
    if not instance_connection_name:
        raise ValueError("La variable de entorno DB_INSTANCE_CONNECTION_NAME no está definida.")
    if not db_user:
        raise ValueError("La variable de entorno DB_USER no está definida.")

    engine = create_engine(
        "postgresql+pg8000://",
        creator=lambda: connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name
        ),
        pool_pre_ping=True,
    )
    return engine

# --- Configuración de la Sesión de SQLAlchemy ---
# Estas líneas ahora se ejecutarán DESPUÉS de que las variables de entorno hayan sido cargadas.
engine = get_db_connection()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Dependencia para inyectar en las rutas de FastAPI ---
def get_db():
    """
    Abre y cierra una sesión de base de datos por cada petición.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()