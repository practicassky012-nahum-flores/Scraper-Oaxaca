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

driver.get('http://compilacion.ordenjuridico.gob.mx/poderes2.php?edo=23')
wait = WebDriverWait(driver, 10)  # Aumentamos el tiempo de espera

# Ruta base para guardar archivos
ruta_base_guardado = "C:\\Users\\pro02\\Documents\\azurite\\QUINTANA ROO DOF 3"
os.makedirs(ruta_base_guardado, exist_ok=True)

# Google Sheets credentials
SERVICE_ACCOUNT_FILE = "gcredential.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1PlZX_p7PDcV6Enz26v5ezZHaw-54aKJjabTShKj9zp8'  # Asegúrate de que este ID sea correcto
SHEET_NAME = 'QUINT.ROO'

# Credenciales para Google Sheets
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service_sheets = build('sheets', 'v4', credentials=creds)

if not all([SERVICE_ACCOUNT_FILE, SPREADSHEET_ID]):
    logging.error("Faltan variables de entorno requeridas. Verifica la configuración.")
    exit(1)

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

# Función para descargar archivos
def descargar_ordenamiento(url, ruta_guardado, nombre_archivo, extension):
    """Descarga un archivo desde un enlace y lo guarda localmente."""
    try:
        logging.info(f"Descargando ordenamiento: {nombre_archivo}")
        nombre_archivo = limpiar_nombre_archivo(nombre_archivo)
        nombre_base = nombre_archivo
        nombre_archivo = f"{nombre_archivo}.{extension}"
        ruta_completa = os.path.join(ruta_guardado, nombre_archivo)

        # Si el archivo ya existe, agregar un número al final
        contador = 1
        while os.path.exists(ruta_completa):
            logging.info(f"El archivo ya existe: {nombre_archivo}. Agregando número al final...")
            nombre_archivo = f"{nombre_base}_{contador}.{extension}"
            ruta_completa = os.path.join(ruta_guardado, nombre_archivo)
            contador += 1

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
        nueva_fila = [next_id, nombre, fecha, tipo, estatus]
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

#prceso principal
try:
    # Selección del enlace
    #enlace_aguascalientes = wait.until(
    #    EC.element_to_be_clickable((By.XPATH, '//a[@href="./estatal.php?liberado=si&edo=19" and @class="notas_rapidas"]'))
    #)
    #enlace_aguascalientes.click()
    #logging.info("Enlace de Jalisco seleccionado exitosamente.")

    # Cambio al iframe
    #iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
    #driver.switch_to.frame(iframe)

    select_element = wait.until(EC.presence_of_element_located((By.NAME, "catTipo")))
    select = Select(select_element)
    select.select_by_visible_text("Todos los ordenamientos")
    driver.switch_to.default_content()

    # Cambio al iframe de resultados
    #iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='compilacion.ordenjuridico.gob.mx']")))
    #driver.switch_to.frame(iframe)

    # Procesar filas de la tabla
    filas = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.txt_gral")))
    for fila in filas:
        tipo = fila.find_element(By.XPATH, ".//td[2]").text.strip()
        estatus = fila.find_element(By.XPATH, ".//td[3]").text.strip()
        tipo = limpiar_nombre_archivo(tipo)
        estatus = limpiar_nombre_archivo(estatus)

        # Crear carpeta
        ruta_tipo_estatus = os.path.join(ruta_base_guardado, tipo, estatus)
        os.makedirs(ruta_tipo_estatus, exist_ok=True)

        # Procesar enlace
        enlace = fila.find_element(By.XPATH, ".//a[contains(@href, 'fichaOrdenamiento2.php')]")
        nombre_ordenamiento = limpiar_nombre_archivo(enlace.text.strip())
        enlace.click()

        # Verificar ventana emergente
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
        else:
            continue

        for extension in ["doc", "pdf"]:
            try:
                enlace_descarga = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, f"//a[contains(@href, 'obtenerdoc.php') and contains(@href, '.{extension}')]") #no hace bien las descargas
                    )
                )
                descargar_ordenamiento(enlace_descarga.get_attribute('href'), ruta_tipo_estatus, nombre_ordenamiento, extension)
            except Exception:
                logging.warning(f"No se encontró enlace para {extension} en {nombre_ordenamiento}.")

# Almacenar en Google Sheets
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        guardar_en_google_sheets(nombre_ordenamiento, fecha, tipo, estatus)

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        #driver.switch_to.frame(iframe)

 # Después de procesar todas las filas, buscar el botón "siguiente"
        try:
            logging.info("Buscando el botón 'siguiente' para continuar con la siguiente página...")
            boton_siguiente = driver.find_element(By.ID, "forwardbutton")

            # Verificar si el botón está visible
            style_attr = boton_siguiente.get_attribute("style")
            if boton_siguiente.is_displayed() and style_attr and "visibility: visible" in style_attr:
                logging.info("Haciendo clic en el botón 'siguiente' para ir a la próxima página...")
                boton_siguiente.click()
                time.sleep(3)  # Esperar a que cargue la siguiente página
            else:
                logging.info("El botón 'siguiente' no está visible. Fin de la paginación.")
                break
        except Exception as e:
            logging.info(f"No se encontró el botón 'siguiente' o se llegó a la última página. Error: {e}")
            break

except Exception as e:
    logging.error(f"Error general: {e}")
finally:
    driver.quit()