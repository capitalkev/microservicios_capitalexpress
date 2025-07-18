# generar_token.py
import os
from google_auth_oauthlib.flow import InstalledAppFlow

# --- Configuración ---
# Asegúrate de que tu archivo de credenciales se llame 'credentials.json'
# y esté en la misma carpeta que este script.
CLIENT_SECRET_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/devstorage.read_only']

def main():
    """
    Inicia el flujo de autenticación para obtener el consentimiento del usuario
    y guardar las credenciales en un archivo token.json.
    """
    print("Iniciando el proceso de autenticación...")

    if not os.path.exists(CLIENT_SECRET_FILE):
        print(f"Error: No se encontró el archivo '{CLIENT_SECRET_FILE}'.")
        print("Por favor, asegúrate de que esté en la misma carpeta que este script.")
        return

    # Inicia el flujo de la aplicación instalada
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)

    # Esto abrirá automáticamente una ventana en tu navegador
    print("Se abrirá una ventana en tu navegador para que inicies sesión y des tu permiso.")
    creds = flow.run_local_server(port=0)

    # Guarda las credenciales (incluido el refresh_token) en token.json
    # para que la aplicación del servidor pueda usarlas en el futuro.
    token_path = 'token.json'
    with open(token_path, 'w') as token_file:
        token_file.write(creds.to_json())

    print("-" * 50)
    print(f"¡Éxito! Se ha creado el archivo '{token_path}' en esta carpeta.")
    print("Este es el archivo que debes usar en tu microservicio de Gmail.")
    print("-" * 50)

if __name__ == '__main__':
    # Instalar la dependencia necesaria
    os.system('pip install -r requirements.txt')
    main()