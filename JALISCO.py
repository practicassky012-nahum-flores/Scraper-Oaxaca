import os
import re
import glob
import time
import pyodbc 
import logging
import requests
import traceback
import pandas as pd
from datetime import datetime
from selenium import webdriver
from google.oauth2 import service_account
from selenium.webdriver.common.by import By
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException

# Configuración de ChromeDriver
chrome_driver_path = 'C:\\WebDriver\\chromedriver.exe'
service = Service(chrome_driver_path)
chrome_options = Options()
chrome_options.add_argument("--headless")  # Opcional: ejecuta en segundo plano
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--log-level=3')  # Suprime la mayoría de los logs
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = r"C:\\Users\\pro02\\Downloads\\GoogleChromePortable64\\App\\Chrome-bin\\chrome.exe"  # Ajusta la ruta

# Iniciar el navegador
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

driver.get('http://www.ordenjuridico.gob.mx/ambest.php#gsc.tab=14')

wait = WebDriverWait(driver, 10)  # Aumenté el tiempo de espera para elementos

# Ruta base para guardar archivos
ruta_base_guardado = "C:\\Users\\pro02\\Documents\\azurite\\JALISCO DOF"
os.makedirs(ruta_base_guardado, exist_ok=True)

# Google Sheets credentials
SERVICE_ACCOUNT_FILE = "gcredential.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1PlZX_p7PDcV6Enz26v5ezZHaw-54aKJjabTShKj9zp8'  # Asegúrate de que este ID sea correcto
SHEET_NAME = 'JALISCO'

# Credenciales para Google Sheets
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service_sheets = build('sheets', 'v4', credentials=creds)

# Función para limpiar nombres de archivo
def limpiar_nombre_archivo(nombre_original, max_length=100):
    try:
        """Limpia caracteres no válidos y trunca nombres largos."""
        logging.info(f"limpiando archivo: {nombre_original}")
        nombre_limpio = re.sub(r'[<>:"/\\|?*]', '', nombre_original)
        logging.info("Limpieza de archivo hecha de forma correcta")
        return nombre_limpio[:max_length]
    except Exception as e:
        logging.error(f"Hubo un problema al hacer la limpieza del archivo: {nombre_original}, error: {e}")
 
# Función para descargar el archivo
def descargar_ordenamiento(url, ruta_guardado, nombre_archivo, extension):
    try:
        nombre_archivo = limpiar_nombre_archivo(nombre_archivo)
        nombre_archivo = f"{nombre_archivo}.{extension}"
        ruta_completa = os.path.join(ruta_guardado, nombre_archivo)

        if os.path.exists(ruta_completa):
            logging.warning(f"El archivo ya existe: {nombre_archivo}. Se omite la descarga.")
            return True

        respuesta = requests.get(url)
        if respuesta.status_code == 200:
            with open(ruta_completa, 'wb') as archivo:
                archivo.write(respuesta.content)
            logging.info(f"Descargado exitosamente: {nombre_archivo}")
            return True
        else:
            logging.error(f"Error al descargar {nombre_archivo}. Código de estado: {respuesta.status_code}")
            return False
    except Exception as e:
        logging.error(f"Error descargando {nombre_archivo}: {e}")
        return False

# Función para obtener datos de Google Sheets
def obtener_datos_sheets():
    try:
        sheet = service_sheets.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:E").execute()
        return result.get('values', [])
    except HttpError as err:
        logging.error(f"Error al obtener datos de Google Sheets: {err}")
        return []

# Función para obtener el siguiente ID en Google Sheets
def get_next_id(values):
    if not values or len(values) < 2:
        return 1
    last_row = values[-1]
    try:
        last_id = int(last_row[0])
    except (ValueError, IndexError):
        logging.error("Error al analizar el último ID")
        return 1
    return last_id + 1

# Función para guardar los datos en Google Sheets
def guardar_en_google_sheets(nombre, fecha, tipo, estatus):
    try:

        datos_existentes = obtener_datos_sheets()
        # Verificar si el dato ya existe en Google Sheets
        for fila in datos_existentes:
            if len(fila) > 1 and fila[1] == nombre:
                logging.info(f"El archivo '{nombre}' ya existe en Google Sheets.")
                return

        # Recuperar los valores existentes de la hoja de cálculo
        sheet = service_sheets.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:D").execute()
        values = result.get('values', [])
        
        # Obtenemos el próximo ID basado en la última fila
        next_id = get_next_id(values)

        # Crear nueva fila con los datos: nombre, fecha, tipo, estatus
        nueva_fila = [next_id, nombre, fecha, tipo]
        cuerpo = {
            'values': [nueva_fila]
        }
        
        # Insertamos la nueva fila en la hoja
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:D",  # Usamos las primeras 4 columnas: ID, Nombre, Fecha, Tipo, Estatus
            valueInputOption='USER_ENTERED',
            body=cuerpo
        ).execute()
        logging.info(f'Datos de {nombre} almacenados exitosamente en Google Sheets.')
    except HttpError as err:
        logging.error(f"Error al almacenar los datos: {err}")
    except Exception as e:
        logging.error(f"Error general al guardar en Google Sheets: {e}")


try:
    # Selección del enlace
    enlace_jalisco = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//a[@href="./estatal.php?liberado=no&edo=14" and @class="notas_rapidas"]'))
    )
    enlace_jalisco.click()
    logging.info("Enlace de Jalisco seleccionado exitosamente.")

    select_element = wait.until(EC.presence_of_element_located((By.NAME, "catTipo")))
    select = Select(select_element)
    select.select_by_visible_text("Todos los ordenamientos")
    logging.info("Tipo seleccionado: Todos los ordenamientos.")


#agregado
    elementos = wait.until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.txt_gral a, a[href*='javascript:void(window.open']"))
    )
    logging.info(f"Se encontraron {len(elementos)} elementos para procesar.")


    for elemento in elementos:
        # Obtener texto del enlace
        nombre_ordenamiento = limpiar_nombre_archivo(elemento.text.strip())
        elemento.click()

        # Cambiar a ventana emergente---verificando....
        driver.switch_to.window(driver.window_handles[-1])
        logging.info(f"Procesando: {nombre_ordenamiento}")

        for extension in ["doc", "pdf"]:
            try:
                enlace_descarga = wait.until(
                    EC.presence_of_element_located( #estoooooooo se modifico y ya hace la descarga
                    (By.XPATH, "//a[contains(@href, '.doc') or contains(@href, '.pdf')]")
                ))
                descargar_ordenamiento(enlace_descarga.get_attribute('href'),
                    ruta_base_guardado, nombre_ordenamiento, extension)
                
            except Exception as e:
                logging.warning(f"No se encontró enlace para {extension} en {nombre_ordenamiento}. Error: {e}")

# Almacenar en Google Sheets
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        guardar_en_google_sheets(nombre_ordenamiento, fecha, "Tipo predeterminado", "Estatus predeterminado") #modificar solo eso TIPO

        # Cerrar la ventana emergente y volver a la principal
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

except Exception as e:
    logging.error(f"Error general: {e}")
finally:
    driver.quit()
