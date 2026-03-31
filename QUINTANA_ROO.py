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

driver.get('http://www.ordenjuridico.gob.mx/ambest.php#gsc.tab=23')
wait = WebDriverWait(driver, 10)  # Aumentamos el tiempo de espera

# Ruta base para guardar archivos
ruta_base_guardado = "C:\\Users\\pro02\\Documents\\azurite\\QUINTANA ROO DOF 2"
os.makedirs(ruta_base_guardado, exist_ok=True)

# Google Sheets credentials
SERVICE_ACCOUNT_FILE = "gcredential.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1PlZX_p7PDcV6Enz26v5ezZHaw-54aKJjabTShKj9zp8'  # Asegúrate de que este ID sea correcto
SHEET_NAME = 'QUINT.ROO'

# Credenciales para Google Sheets
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service_sheets = build('sheets', 'v4', credentials=creds)

#-------------------------------------------------------------
def limpiar_texto(texto):
    """
   Limpia el texto eliminando espacios en blanco extra, líneas vacías repetidas
    y referencias a imágenes o enlaces. Además, corrige tildes en palabras que lo requieran.
    """

    # Eliminar líneas vacías adicionales
    texto = re.sub(r'\n\s*\n+', '\n', texto)
    
    # Eliminar caracteres no imprimibles y binarios
    texto = ''.join(c for c in texto if c.isprintable() or c.isspace())

    # Elimina espacios al inicio y final de cada línea
    texto = "\n".join(linea.strip() for linea in texto.split("\n"))

    return texto.strip()

#-------------------------------------------------------------


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
def guardar_en_google_sheets(nombre = None, fecha = None, tipo = None, estatus = None):
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


# Proceso principal
try:
    logging.info("Verificando enlace de cmdx.")
    enlace_aguascalientes = wait.until(
    EC.element_to_be_clickable((By.XPATH, '//a[@href="./estatal.php?liberado=no&edo=23" and @class="notas_rapidas"]'))
    )
    enlace_aguascalientes.click()
    logging.info("Enlace de cmdx seleccionado exitosamente.")

    select_element = wait.until(EC.presence_of_element_located((By.NAME, "catTipo")))
    select = Select(select_element)
    select.select_by_visible_text("Todos los ordenamientos")
    driver.switch_to.default_content()
    logging.info("seleccionado todos los ordenamientos exitosamente")

    # Bucle de paginación
    fila_inicio = 273  # Empezar desde la fila 200
    primera_pagina = True  # Bandera para controlar la primera página

    while True:
        # Esperar que la tabla esté presente
        tabla = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tabla_gral")))

        # Extraer todas las filas con enlaces
        filas = tabla.find_elements(By.TAG_NAME, "tr")

        # En la primera página, empezar desde la fila 200; en las siguientes, desde la fila 1
        if primera_pagina:
            filas_a_procesar = filas[fila_inicio:]
            primera_pagina = False
            logging.info(f"Primera página: Iniciando desde la fila {fila_inicio}")
        else:
            filas_a_procesar = filas[1:]  # Saltar solo el encabezado en páginas siguientes
            logging.info("Página siguiente: Procesando todas las filas")

        for fila in filas_a_procesar:
            try:
                logging.info("no se pudo")
                # Intentar obtener el enlace utilizando un XPath más seguro
                enlace = fila.find_element(By.XPATH, ".//a[contains(@href, 'fichaOrdenamiento.php')]")
                url = enlace.get_attribute('href')
                nombre = enlace.text.strip()
                logging.info(f"Procesando archivo: {nombre}, URL: {url}")

                # Hacer clic en el enlace si se encuentra correctamente
                enlace.click()
                # Verificar ventana emergente
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                else:
                    continue

                            # Descargar todos los enlaces disponibles
                enlaces_descarga = driver.find_elements(By.XPATH, "//a[contains(@href, 'obtenerdoc.php')] | //a/img[contains(@src, 'PDF-icono.gif')]/..")
                for enlace_descarga in enlaces_descarga:
                    try:
                        # Hacer clic en la imagen si es un enlace ESTO PONER EN EL DE CDMX--------------------------------------
                        if enlace_descarga.find_element(By.TAG_NAME, "img"):
                            enlace_descarga.click()

                        href = enlace_descarga.get_attribute('href')
                        extension = href.split('.')[-1]  # Extraer la extensión del archivo
                        descargar_ordenamiento(href, ruta_base_guardado, nombre, extension)
                    except Exception as e:
                        logging.warning(f"No se pudo descargar el archivo desde {href}. Error: {e}")

#-------------------------------------------------------------------------
                def guardar_archivos(respuesta_de_solicitud, ruta_leyes):
                    """
                    Guarda el archivo descargado como .txt y lo limpia de espacios innecesarios.
                    """
                    output_directory = os.path.dirname(ruta_leyes)

                    if not os.path.exists(output_directory):
                        os.makedirs(output_directory)

                    try:
                        # Guardar el archivo temporalmente como texto
                        temp_path = ruta_leyes + ".temp"
                        with open(temp_path, "w", encoding='utf-8', errors='ignore') as file:
                            file.write(respuesta_de_solicitud.text)

                        # Leer el contenido y limpiarlo
                        with open(temp_path, "r", encoding='utf-8', errors='ignore') as file:
                            contenido = file.read()
                            contenido_limpio = limpiar_texto(contenido)

                        # Guardar el archivo limpio
                        with open(ruta_leyes, "w", encoding='utf-8') as file:
                            file.write(contenido_limpio)

                        # Eliminar archivo temporal
                        os.remove(temp_path)

                        logging.info(f"Archivo limpio y guardado en {ruta_leyes}")

                    except Exception as e:
                        logging.error(f"Hubo un error al guardar el archivo: {e}")

#---------------------------------------------------------------------
                # Almacenar en Google Sheets
                fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                guardar_en_google_sheets(nombre, fecha, "Tipo_Ordenamiento", "Estatus")

                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except Exception as e:
                logging.warning(f"Error al procesar fila: {e}")

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



