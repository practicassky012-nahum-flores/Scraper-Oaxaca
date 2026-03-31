import os
import re
import logging
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de ChromeDriver
chrome_driver_path = 'C:\\WebDriver\\chromedriver.exe'
service = Service(chrome_driver_path)
chrome_options = Options()
chrome_options.add_argument('--ignore-certificate-errors')

# Iniciar el navegador
driver = webdriver.Chrome(service=service, options=chrome_options)
#driver.get('http://www.ordenjuridico.gob.mx/estatal.php?liberado=si&edo=22#gsc.tab=0')
driver.get('http://www.ordenjuridico.gob.mx/ambest.php#gsc.tab=0')
wait = WebDriverWait(driver, 5)

# Ruta base para guardar archivos
ruta_base_guardado = "C:\\Users\\pro02\\Documents\\azurite\\queretaro"
os.makedirs(ruta_base_guardado, exist_ok=True)

# Accesos
SERVICE_ACCOUNT_FILE = "gcredential.json"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1PlZX_p7PDcV6Enz26v5ezZHaw-54aKJjabTShKj9zp8'
SHEET_NAME = 'PRU'

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
def guardar_en_google_sheets(nombre = None, fecha = None, publicacion = None, tipo = None, estatus = None):
    try:
        # Recuperar los valores existentes de la hoja de cálculo
        sheet = service_sheets.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_NAME}!A:E").execute()
        values = result.get('values', [])
        
        # Obtenemos el próximo ID basado en la última fila
        next_id = get_next_id(values)

        # Crear nueva fila con los datos: nombre, fecha, tipo, estatus
        nueva_fila = [next_id, nombre, tipo, estatus, fecha, publicacion]    #estatus es la fecha del archivo y fecha la fecha de descarga
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
    logging.info("Verificando enlace de QUERETARO.")
    enlace_aguascalientes = wait.until(
    EC.element_to_be_clickable((By.XPATH, '//a[@href="./estatal.php?liberado=si&edo=22" and @class="notas_rapidas"]'))
    )
    enlace_aguascalientes.click()
    logging.info("Enlace de Aguascalientes seleccionado exitosamente.")

    # Cambio al iframe
    iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
    driver.switch_to.frame(iframe)

    select_element = wait.until(EC.presence_of_element_located((By.NAME, "catTipo")))
    select = Select(select_element)
    select.select_by_visible_text("Todos los ordenamientos")
    driver.switch_to.default_content()
    # Cambio al iframe de resultados
    iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='compilacion.ordenjuridico.gob.mx']")))
    driver.switch_to.frame(iframe)
    
    # Procesar filas de la tabla
    filas = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.txt_gral")))
    
    for fila in filas:
        logging.info(f"Procesando archivo: {fila}")
        tipo = fila.find_element(By.XPATH, ".//td[2]").text.strip()
        estatus = fila.find_element(By.XPATH, ".//td[3]").text.strip()
        publicacion = fila.find_element(By.XPATH, ".//td[4]").text.strip()#agregado

        #publicacion = limpiar_nombre_archivo(publicacion)

        tipo = limpiar_nombre_archivo(tipo)
        estatus = limpiar_nombre_archivo(estatus)

        # Crear carpeta para guardar archivos
        ruta_tipo_estatus = os.path.join(ruta_base_guardado, tipo, estatus) #publicacion
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
                        (By.XPATH, f"//a[contains(@href, 'obtenerdoc.php') and contains(@href, '.{extension}')]")
                    )
                )
                descargar_ordenamiento(enlace_descarga.get_attribute('href'), ruta_tipo_estatus, nombre_ordenamiento, extension)
            except Exception:
                logging.warning(f"No se encontró enlace para {extension} en {nombre_ordenamiento}.")

        # Almacenar en Google Sheets
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        guardar_en_google_sheets(nombre_ordenamiento, publicacion, fecha, tipo, estatus)

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        driver.switch_to.frame(iframe)

except Exception as e:
    logging.error(f"Error general: {e}")

finally:
    driver.quit()