import os
from azure.storage.blob import BlobServiceClient

# Configuración: usa tu cadena de conexión de Azure Storage
AZURE_STORAGE_CONNECTION_STRING = ("DefaultEndpointsProtocol=https;AccountName=leyesmexicanas;AccountKey=5wEgXq3BbGp7zbM2uVV3rAwQg3coh04yxcshcRntZAMX+OhCSTep8ORrkdkk5xaI6dqem+LinW6R+AStfmXvGQ==;EndpointSuffix=core.windows.net")
# DefaultEndpointsProtocol=https;AccountName=leyesmexicanas;AccountKey=5wEgXq3BbGp7zbM2uVV3rAwQg3coh04yxcshcRntZAMX+OhCSTep8ORrkdkk5xaI6dqem+LinW6R+AStfmXvGQ==;EndpointSuffix=core.windows.net
# Nombre del contenedor donde subirás los JSON
CONTAINER_NAME = "jurisprudenciasjson"   # cámbialo si usas otro

# Carpeta local donde están tus archivos JSON
LOCAL_FOLDER = r"C:\Users\pro02\Documents\divididos\quinta2"  # cámbialo a la ruta real

# Carpeta lógica en el contenedor
REMOTE_FOLDER = "QuintaEpoca"

# Conexión al servicio de blobs
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Crea el contenedor si no existe
try:
    container_client.create_container()
    print(f"✔ Contenedor '{CONTAINER_NAME}' creado")
except Exception:
    print(f"ℹ Contenedor '{CONTAINER_NAME}' ya existe")

# Recorre todos los archivos JSON locales
for file_name in os.listdir(LOCAL_FOLDER):
    if file_name.lower().endswith(".json"):
        local_path = os.path.join(LOCAL_FOLDER, file_name)

        # Blob path → "undecima/nombre.json"
        blob_path = f"{REMOTE_FOLDER}/{file_name}"

        try:
            blob_client = container_client.get_blob_client(blob_path)

            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f"✔ Subido {file_name} a {blob_path}")
        except Exception as e:
            print(f"✗ Error subiendo {file_name}: {e}")
