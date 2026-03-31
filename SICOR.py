import os
import re
import time
import json
import fitz
import logging
import requests
import pdfplumber
from selenium import webdriver
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from selenium.webdriver.chrome.options import Options

# ---------- CONFIGURACIÓN ----------
chrome_options = Options()
download_dir = "C:\\Users\\pro02\\Downloads"
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("--headless")  # Ejecutar sin ventana (opcional)
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.binary_location = r"C:\Users\pro02\Downloads\GoogleChromePortable64\App\Chrome-bin\chrome.exe"

# Configuración de ChromeDriver
chrome_driver_path = 'C:\\WebDriver\\chromedriver.exe'
service = Service(chrome_driver_path)
chrome_options = Options()
chrome_options.add_argument("--headless")  # Opcional: ejecuta en segundo plano
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = r"C:\Users\pro02\Downloads\GoogleChromePortable64\App\Chrome-bin\chrome.exe"  # Ajusta la ruta

# Iniciar el navegador
service = Service(ChromeDriverManager().install())

# Iniciar el navegador
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get('https://sicor.poderjudicialdf.gob.mx/')
wait = WebDriverWait(driver, 10)  # Aumentamos el tiempo de espera

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
wait = WebDriverWait(driver, 5)

# ---------- GOOGLE DRIVE ----------
base_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(base_dir, 'service_account.json')

gauth = GoogleAuth()
scope = ['https://www.googleapis.com/auth/drive']
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)
drive = GoogleDrive(gauth)

# ID de carpeta destino (opcional)
folder_id = "1wzwTJLRlSm7HtjP_uwdbnIg-OJXBEYBg"

# Ingresar usuario y contraseña
usuario = "marco_alv7"
contrasena = "marcoalvarez"

driver.find_element(By.NAME, "usuario").send_keys(usuario)
driver.find_element(By.NAME, "contrasena").send_keys(contrasena)
time.sleep(1)

#prceso principal
# Hacer clic en el botón de ingresar
driver.find_element(By.ID, "submit").click()
time.sleep(5)  # Esperar a que cargue la siguiente página

# Seleccionar "Consulta expediente"
consulta_expediente = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Consulta expediente')]")))
driver.execute_script("arguments[0].click();", consulta_expediente)
time.sleep(25)

#-----------esta es la chida------------------
# Buscar y hacer clic en el enlace con 'verDetalle' sin abrir en una nueva pestaña
enlace_ver_detalle = WebDriverWait(driver, 15).until(
    EC.presence_of_element_located((By.XPATH, "//a[contains(@onclick, 'verDetalle')]"))
)
driver.execute_script("arguments[0].click();", enlace_ver_detalle)
logging.info("Se hizo clic en 'verDetalle' sin abrir en otra página.")
time.sleep(25)  # Esperar a que cargue la información del expediente

# ---------- VERIFICAR Y SUBIR A DRIVE ----------
def verificar_descarga_completa(descarga_path, timeout=90):
    tiempo_inicio = time.time()
    archivo_descargado = None

    while time.time() - tiempo_inicio < timeout:
        archivos = os.listdir(descarga_path)
        archivos_validos = [f for f in archivos if not f.endswith(".crdownload") and not f.endswith(".tmp")]

        if archivos_validos:
            archivo_descargado = max(
                [os.path.join(descarga_path, f) for f in archivos_validos],
                key=os.path.getctime
            )
            logging.info(f"Archivo detectado: {archivo_descargado}")
            # Verificamos que tenga tamaño distinto de cero
            if os.path.getsize(archivo_descargado) > 0:
                return archivo_descargado
        time.sleep(3)

    return None
import glob

# -------------------------------
# FASE 1: Scraping y descarga
# -------------------------------

# Ruta real donde se descargan los archivos
ruta_descargas = "C:\\Users\\pro02\\Downloads"
extensiones_validas = ["*.pdf"]  # Puedes agregar más tipos si necesitas

# Obtener el listado inicial de archivos antes de iniciar las descargas
archivos_antes = set(glob.glob(f"{ruta_descargas}\\*.pdf"))

abrir_acuerdos = WebDriverWait(driver, 25).until(
    EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@onclick, 'abrirAcuerdo')]"))
)

archivos_descargados = []

for idx, abrir_acuerdo in enumerate(abrir_acuerdos):
    try:
        logging.info(f"🔍 Procesando expediente {idx + 1}...")
        abrir_acuerdo.click()

        # Esperar a que aparezca un nuevo archivo PDF en la carpeta
        tiempo_espera = 30
        tiempo_transcurrido = 0
        nuevo_archivo = None

        while tiempo_transcurrido < tiempo_espera:
            time.sleep(1)
            tiempo_transcurrido += 1
            archivos_despues = set(glob.glob(f"{ruta_descargas}\\*.pdf"))
            nuevos = archivos_despues - archivos_antes
            if nuevos:
                nuevo_archivo = nuevos.pop()
                archivos_descargados.append(nuevo_archivo)
                archivos_antes = archivos_despues  # Actualizar para la siguiente vuelta
                logging.info(f"📥 Archivo descargado: {nuevo_archivo}")
                break

        if not nuevo_archivo:
            logging.warning(f"⚠️ No se detectó archivo nuevo tras descargar el expediente {idx + 1}")

    except Exception as e:
        logging.warning(f"⚠️ No se pudo procesar el expediente {idx + 1}: {e}")


# -------------------------------
# FASE 2: Subida a Google Drive
# -------------------------------

try:
    if archivos_descargados:
        logging.info(f"🚀 Iniciando subida de {len(archivos_descargados)} archivos a Google Drive...")

        for archivo_path in archivos_descargados:
            try:
                nombre_archivo = os.path.basename(archivo_path)

                archivo_drive = drive.CreateFile({
                    'title': nombre_archivo,
                    'parents': [{'id': folder_id}] if folder_id else []
                })
                archivo_drive.SetContentFile(archivo_path)
                archivo_drive.Upload()
                logging.info(f"📤 Archivo subido a Google Drive: {nombre_archivo}")

                # os.remove(archivo_path)  # Opcional: eliminar archivo local

            except Exception as e:
                logging.error(f"❌ Error al subir archivo {archivo_path} a Google Drive: {e}")
    else:
        logging.info("📁 No hay archivos para subir a Google Drive.")

except Exception as e:
    logging.error(f"❌ Error durante la subida a Google Drive: {e}")
