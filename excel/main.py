import gspread
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

# --- Modelo de Datos de Entrada (Actualizado) ---
class Contacto(BaseModel):
    ruc: str
    correo: str
    nombre_deudor: Optional[str] = None # Es opcional por si solo se actualiza

# --- Inicialización ---
app = FastAPI(
    title="Microservicio de Google Sheets",
    description="Actualiza o crea contactos de deudores en una hoja de cálculo."
)

# --- Autenticación ---
# Lee la ruta del archivo desde una variable de entorno
credentials_file = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
if not credentials_file:
    raise RuntimeError("La variable de entorno GOOGLE_SHEETS_CREDENTIALS no está definida.")

try:
    gc = gspread.service_account(filename=credentials_file)
    sh = gc.open("Contactos verificaciones")
    worksheet = sh.worksheet("CORREOS")
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar Google Sheets: {e}")


@app.post("/update-contact", summary="Actualizar o Crear Contacto en Google Sheets")
def update_contact(contacto: Contacto):
    """
    Busca un RUC en la columna 1.
    - Si lo encuentra, actualiza la lista de correos en la columna 3.
    - Si no lo encuentra, añade una nueva fila con RUC, Nombre y Correo.
    """
    try:
        # Obtenemos todos los valores de la hoja de una sola vez para ser más eficientes
        all_rows = worksheet.get_all_values()
        
        # Iteramos sobre cada fila para buscar el RUC
        for i, row in enumerate(all_rows):
            # Comparamos el RUC en la primera columna (índice 0)
            if row and row[0] == contacto.ruc:
                fila_num = i + 1  # Las filas en gspread son 1-indexadas
                print(f"RUC {contacto.ruc} encontrado en la fila {fila_num}. Actualizando correos.")
                
                # --- LÓGICA SI EL RUC YA EXISTE ---
                correos_actuales_str = worksheet.cell(fila_num, 3).value or ""
                correo_nuevo = contacto.correo.strip()
                
                if not correo_nuevo:
                    return {"status": "SUCCESS", "message": "RUC encontrado, pero no se proporcionó correo para actualizar."}

                # Evitar duplicados
                lista_de_correos = {c.strip() for c in correos_actuales_str.split(';') if c.strip()}
                if correo_nuevo in lista_de_correos:
                    return {"status": "SUCCESS", "message": f"El correo '{correo_nuevo}' ya existía para el RUC {contacto.ruc}."}
                
                lista_de_correos.add(correo_nuevo)
                correos_actualizados = ";".join(sorted(lista_de_correos))
                worksheet.update_cell(fila_num, 3, correos_actualizados)

                return {"status": "SUCCESS", "message": f"Contacto para RUC {contacto.ruc} actualizado exitosamente."}

        # --- LÓGICA SI EL RUC NO SE ENCONTRÓ (el bucle for terminó) ---
        print(f"RUC {contacto.ruc} no encontrado. Creando nueva fila.")
        
        if not contacto.nombre_deudor:
            raise HTTPException(
                status_code=400, 
                detail=f"El RUC '{contacto.ruc}' no existe y no se proporcionó 'nombre_deudor' para crearlo."
            )
            
        nueva_fila = [
            contacto.ruc,
            contacto.nombre_deudor,
            contacto.correo.strip()
        ]
        worksheet.append_row(nueva_fila)
        
        return {"status": "CREATED", "message": f"Nuevo contacto para RUC {contacto.ruc} creado exitosamente."}

    except Exception as e:
        # Captura cualquier otro error inesperado
        print(f"ERROR: Ocurrió un error inesperado: {e}")
        raise HTTPException(status_code=500, detail=f"Error inesperado al procesar la hoja de cálculo: {str(e)}")

@app.get("/get-emails/{ruc}", summary="Obtener correos de un RUC")
def get_emails(ruc: str):
    """
    Busca un RUC en la hoja y devuelve la cadena de correos asociada.
    """
    try:
        celda_encontrada = worksheet.find(ruc, in_column=1)
        # Lee el valor de la columna 3 (correos) en la fila encontrada
        correos = worksheet.cell(celda_encontrada.row, 3).value or ""
        return {"ruc": ruc, "emails": correos}
    except gspread.exceptions.CellNotFound:
        raise HTTPException(status_code=404, detail=f"No se encontró el RUC '{ruc}' para obtener correos.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado al obtener correos: {str(e)}")
