import gspread
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

# --- Modelo de Datos de Entrada ---
class Contacto(BaseModel):
    ruc: str
    correo: str
    nombre_deudor: Optional[str] = None

# --- Inicialización de FastAPI y Google Sheets ---
app = FastAPI(
    title="Microservicio de Google Sheets (Versión Estable)",
    description="Actualiza o crea contactos de deudores en una hoja de cálculo."
)

credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
if not credentials_file:
    raise RuntimeError("La variable de entorno GOOGLE_SHEETS_CREDENTIALS no está definida.")

try:
    gc = gspread.service_account(filename=credentials_file)
    sh = gc.open("Contactos verificaciones")
    worksheet = sh.worksheet("CORREOS")
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar Google Sheets: {e}")


# --- Endpoints ---

@app.post("/update-contact", summary="Actualizar o Crear Contacto")
def update_contact(contacto: Contacto):
    """
    Busca un RUC. Si lo encuentra, actualiza los correos. Si no, crea una nueva fila.
    """
    try:
        # Método estable: Obtener todos los valores y buscar en memoria.
        all_rows = worksheet.get_all_values()
        
        for i, row in enumerate(all_rows):
            # Compara el RUC en la primera columna (índice 0)
            if row and row[0] == contacto.ruc:
                fila_num = i + 1  # Las filas en gspread son 1-indexadas
                
                # --- LÓGICA SI EL RUC YA EXISTE ---
                correos_actuales_str = worksheet.cell(fila_num, 3).value or ""
                correo_nuevo = contacto.correo.strip()
                
                if not correo_nuevo:
                    return {"status": "SUCCESS", "message": "RUC encontrado, sin correo nuevo para añadir."}

                lista_de_correos = {c.strip() for c in correos_actuales_str.split(';') if c.strip()}
                if correo_nuevo in lista_de_correos:
                    return {"status": "SUCCESS", "message": f"El correo '{correo_nuevo}' ya existía."}
                
                lista_de_correos.add(correo_nuevo)
                correos_actualizados = ";".join(sorted(lista_de_correos))
                worksheet.update_cell(fila_num, 3, correos_actualizados)

                return {"status": "SUCCESS", "message": f"Contacto para RUC {contacto.ruc} actualizado."}

        # --- LÓGICA SI EL RUC NO SE ENCONTRÓ ---
        if not contacto.nombre_deudor:
            raise HTTPException(
                status_code=400, 
                detail=f"RUC '{contacto.ruc}' no existe y se necesita 'nombre_deudor' para crearlo."
            )
            
        nueva_fila = [contacto.ruc, contacto.nombre_deudor, contacto.correo.strip()]
        worksheet.append_row(nueva_fila)
        
        return {"status": "CREATED", "message": f"Nuevo contacto para RUC {contacto.ruc} creado."}

    except Exception as e:
        print(f"ERROR: {e}")
        raise HTTPException(status_code=500, detail=f"Error inesperado en excel-service: {str(e)}")


@app.get("/get-emails/{ruc}", summary="Obtener correos de un RUC")
def get_emails(ruc: str):
    """
    Busca un RUC y devuelve la cadena de correos asociada.
    """
    try:
        celda = worksheet.find(ruc, in_column=1)
        correos = worksheet.cell(celda.row, 3).value or ""
        return {"ruc": ruc, "emails": correos}
    except gspread.exceptions.CellNotFound:
        raise HTTPException(status_code=404, detail=f"No se encontró el RUC '{ruc}'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado al obtener correos: {str(e)}")

