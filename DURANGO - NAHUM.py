# -*- coding: utf-8 -*-
"""
Scraper y Extractor de Legislación Vigente - Congreso de Durango
Descarga PDFs, extrae texto y metadatos, genera JSONs individuales
"""

import os
import re
import json
import time
import requests
from pathlib import Path
from urllib.parse import unquote

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# PDF
try:
    import pdfplumber
    PDFPLUMBER_DISPONIBLE = True
except ImportError:
    PDFPLUMBER_DISPONIBLE = False

# OCR
try:
    import pytesseract
    from pdf2image import convert_from_path
    OCR_DISPONIBLE = True
except ImportError:
    OCR_DISPONIBLE = False


# ============================================================
# CONFIGURACIÓN
# ============================================================

URL_LEGISLACION = "https://congresodurango.gob.mx/trabajo-legislativo/legislacion-estatal/"
CARPETA_PDFS = "pdfs"
CARPETA_TEXTO = "textos"
CARPETA_JSON = "json"
FUENTE_OFICIAL = "Congreso del Estado de Durango"

# Umbral para detectar PDF escaneado (caracteres mínimos por página)
UMBRAL_CARACTERES_ESCANEADO = 100


# ============================================================
# FUNCIONES DE DETECCIÓN DE METADATOS
# ============================================================

def detectar_ordenamiento(texto):
    """
    Detecta el tipo de ordenamiento legal basándose en palabras clave.
    Retorna: Ley | Reglamento | Código | Decreto | Marco Jurídico | Otro
    """
    texto_upper = texto.upper()[:5000]  # Solo revisar inicio del documento

    # Orden de prioridad para detección
    if re.search(r'\bCÓDIGO\b|\bCODIGO\b', texto_upper):
        return "Código"
    if re.search(r'\bREGLAMENTO\b', texto_upper):
        return "Reglamento"
    if re.search(r'\bDECRETO\b', texto_upper):
        return "Decreto"
    if re.search(r'\bLEY\b', texto_upper):
        return "Ley"
    if re.search(r'\bMARCO\s+JUR[ÍI]DICO\b', texto_upper):
        return "Marco Jurídico"

    return "Otro"


def detectar_status(texto):
    """
    Detecta el status del documento legal.
    Retorna: Vigente | Abrogado | Derogado | Desconocido
    """
    texto_upper = texto.upper()

    if re.search(r'\bABROGAD[OA]\b', texto_upper):
        return "Abrogado"
    if re.search(r'\bDEROGAD[OA]\b', texto_upper):
        return "Derogado"
    if re.search(r'\bVIGENTE\b', texto_upper):
        return "Vigente"

    # Por defecto, si viene del sitio de legislación vigente
    return "Vigente"


def extraer_fecha_publicacion(texto):
    """
    Extrae la fecha de publicación del Periódico Oficial.
    Busca patrones cerca de 'publicad' o 'Periódico Oficial'.
    Retorna: fecha en formato DD/MM/AAAA o None
    """
    texto_lower = texto.lower()[:10000]

    # Patrón: "publicado el 15 de enero de 2020"
    patron_texto = r'publicad[oa]?\s+(?:en\s+)?(?:el\s+)?(?:periódico\s+oficial\s+)?(?:el\s+)?(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de(?:l)?\s+(\d{4})'

    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }

    match = re.search(patron_texto, texto_lower)
    if match:
        dia = match.group(1).zfill(2)
        mes = meses.get(match.group(2), '01')
        anio = match.group(3)
        return f"{dia}/{mes}/{anio}"

    # Patrón alternativo: "Periódico Oficial No. 123, de fecha 15/01/2020"
    patron_fecha = r'periódico\s+oficial[^.]*?(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})'
    match = re.search(patron_fecha, texto_lower)
    if match:
        dia = match.group(1).zfill(2)
        mes = match.group(2).zfill(2)
        anio = match.group(3)
        return f"{dia}/{mes}/{anio}"

    # Patrón: buscar fecha cerca de "P.O." o "P. O."
    patron_po = r'p\.?\s*o\.?[^.]*?(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de(?:l)?\s+(\d{4})'
    match = re.search(patron_po, texto_lower)
    if match:
        dia = match.group(1).zfill(2)
        mes = meses.get(match.group(2), '01')
        anio = match.group(3)
        return f"{dia}/{mes}/{anio}"

    return None


def extraer_fecha_reforma(texto):
    """
    Extrae la fecha de la última reforma.
    Busca patrones cerca de 'reform' o 'última modificación'.
    Retorna: fecha en formato DD/MM/AAAA o None
    """
    texto_lower = texto.lower()

    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }

    # Buscar todas las fechas de reforma y tomar la más reciente
    patron_reforma = r'(?:última\s+)?reform[aáo][^.]*?(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de(?:l)?\s+(\d{4})'

    matches = re.findall(patron_reforma, texto_lower)

    if matches:
        # Convertir a fechas y encontrar la más reciente
        fechas = []
        for match in matches:
            try:
                dia = int(match[0])
                mes = int(meses.get(match[1], 1))
                anio = int(match[2])
                fechas.append((anio, mes, dia, f"{match[0].zfill(2)}/{meses[match[1]]}/{match[2]}"))
            except:
                continue

        if fechas:
            fechas.sort(reverse=True)
            return fechas[0][3]

    # Patrón alternativo: "modificación del 15/01/2020"
    patron_mod = r'(?:última\s+)?modificaci[oó]n[^.]*?(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})'
    match = re.search(patron_mod, texto_lower)
    if match:
        dia = match.group(1).zfill(2)
        mes = match.group(2).zfill(2)
        anio = match.group(3)
        return f"{dia}/{mes}/{anio}"

    return None


def detectar_materia(texto):
    """
    Clasifica el documento por materia jurídica.
    Retorna: Array de materias identificadas (solo las que corresponden)
    """
    texto_upper = texto.upper()
    
    # Análisis del título y primeras líneas para mejor detección
    texto_analisis = texto_upper[:2000]  # Solo primeras 2000 caracteres
    texto_completo = texto_upper
    
    materias_encontradas = []
    
    # PRIORIDAD 1: Términos muy específicos (orden de prioridad)
    if re.search(r'\bCONSTITUC[IÓ]N\s+POL[ÍI]TICA\b|\bCARTA\s+MAGNA\b', texto_analisis):
        materias_encontradas.append('Constitucional')
    if re.search(r'\bC[ÓO]DIGO\s+(CIVIL|FAMILIAR|FAMILIA|SUCESIONES)\b|\bLEY\s+(CIVIL|FAMILIAR)\b', texto_analisis):
        materias_encontradas.append('Civil')
    if re.search(r'\bC[ÓO]DIGO\s+PENAL\b|\bLEY\s+PENAL\b|\bC[ÓO]DIGO\s+(PROC\s*)+PENAL\b', texto_analisis):
        materias_encontradas.append('Penal')
    if re.search(r'\bLEY\s+FISCAL\b|\bC[ÓO]DIGO\s+FISCAL\b|\bIMPUESTO\s+Sobre\s+LA\s+Renta\b', texto_analisis):
        materias_encontradas.append('Fiscal')
    if re.search(r'\bLEY\s+LABORAL\b|\bC[ÓO]DIGO\s+LABORAL\b|\bJORNADA\b', texto_analisis):
        materias_encontradas.append('Laboral')
    if re.search(r'\bLEY\s+ELECTORAL\b|\bC[ÓO]DIGO\s+ELECTORAL\b|\bINSTITUTO\s+ELECTORAL\b', texto_analisis):
        materias_encontradas.append('Electoral')
    
    # PRIORIDAD 2: Términos específicos de título
    if re.search(r'\bADMINISTRACI[OÓ]N\s+(P[UÚ]BLICA|ESTATAL|MUNICIPAL)\b', texto_analisis):
        materias_encontradas.append('Administrativo')
    if re.search(r'\bDERECHOS\s+HUMANOS\b|\bDEFENSOR[ÍI]A\s+DEL\s+PUEBLO\b', texto_analisis):
        materias_encontradas.append('Derechos Humanos')
    if re.search(r'\bTRANSPARENCIA\b|\bACCESO\s+A\s+LA\s+INFORMACI[OÓ]N\b', texto_analisis):
        materias_encontradas.append('Transparencia')
    if re.search(r'\bIGUALDAD\s+DE\s+G[ÉE]NERO\b|\bVIOLENCIA\s+(DE\s+G[ÉE]NERO|CONTRA\s+LAS\s+MUJERES)\b', texto_analisis):
        materias_encontradas.append('Género')
    if re.search(r'\bDERECHOS\s+DE\s+(LA\s+INFANCIA|NIÑOS|ADOLESCENTES)\b', texto_analisis):
        materias_encontradas.append('Derechos de la Niñez')
    
    # PRIORIDAD 3: Palabras clave específicas (evitar falsos positivos)
    if not any(m in materias_encontradas for m in ['Civil', 'Familiar']):
        if re.search(r'\bMATRIMONIO\b|\bDIVORCIO\b|\bSUCESI[OÓ]N\b|\bPATRIMONIO\s+FAMILIAR\b', texto_completo):
            materias_encontradas.append('Civil')
    
    if not any(m in materias_encontradas for m in ['Penal']):
        if re.search(r'\bDELITO\b|\bCRIMEN\b|\bSANCI[OÓ]N\s+(PENAL|PRIVATIVA\s+DE\s+LIBERTAD)\b|\bEJECUCI[OÓ]N\s+DE\s+PENAS\b', texto_completo):
            materias_encontradas.append('Penal')
    
    if not any(m in materias_encontradas for m in ['Fiscal']):
        if re.search(r'\bTRIBUTARI[OA]\b|\bHACIENDA\s+(P[UÚ]BLICA|ESTATAL)\b|\bERARIO\b|\bRENTA\b', texto_completo):
            materias_encontradas.append('Fiscal')
    
    if not any(m in materias_encontradas for m in ['Laboral']):
        if re.search(r'\bTRABAJADOR\b|\bEMPLEADOR\b|\bSINDICAL\b|\bCONTRATO\s+LABORAL\b|\bJORNADA\b|\bSALARIO\b', texto_completo):
            materias_encontradas.append('Laboral')
    
    if not any(m in materias_encontradas for m in ['Administrativo']):
        if re.search(r'\bSERVIDOR\s+P[UÚ]BLICO\b|\bPROCEDIMIENTO\s+ADMINISTRATIVO\b|\bACTO\s+ADMINISTRATIVO\b', texto_completo):
            materias_encontradas.append('Administrativo')
    
    # PRIORIDAD 4: Materias más específicas
    if re.search(r'\bSISTEMA\s+DE\s+SALUD\b|\bSEGURIDAD\s+SOCIAL\b|\bHOSPITAL\b|\bMEDICINA\b', texto_completo):
        materias_encontradas.append('Salud')
    
    if re.search(r'\bSISTEMA\s+EDUCATIVO\b|\bESCUELA\s+(P[UÚ]BLICA|PRIVADA)\b|\bUNIVERSIDAD\b', texto_completo):
        materias_encontradas.append('Educación')
    
    if re.search(r'\bPROTECCI[OÓ]N\s+(CIVIL|AMBIENTAL|DE\s+DATOS)\b|\bEMERGENCIAS?\b|\bDEASTRES?\s+NATURALES?\b', texto_completo):
        if 'Civil' not in materias_encontradas:
            materias_encontradas.append('Protección Civil')
    
    if re.search(r'\bSEGURIDAD\s+P[UÚ]BLICA\b|\bPOLIC[ÍI]A\b|\bPREVENCI[OÓ]N\s+DEL\s+DELITO\b', texto_completo):
        materias_encontradas.append('Seguridad Pública')
    
    if re.search(r'\bMUNICIPIO\b|\bAYUNTAMIENTO\b|\bCABILDO\b|\bGOBIERNO\s+MUNICIPAL\b', texto_completo):
        materias_encontradas.append('Municipal')
    
    # PRIORIDAD 5: Materias más generales (si no hay nada más específico)
    if not materias_encontradas:
        if re.search(r'\bTRANSPORTE\b|\bVIALIDAD\b|\bMOVILIDAD\b', texto_completo):
            materias_encontradas.append('Movilidad')
        
        if re.search(r'\bMEDIO\s+AMBIENTE\b|\bECOL[OÓ]GICO\b|\bRECURSOS\s+NATURALES\b', texto_completo):
            materias_encontradas.append('Ambiental')
        
        if re.search(r'\bNOTAR[ÍI]A\b|\bFEDATARIO\b|\bESCRITURA\s+P[UÚ]BLICA\b', texto_completo):
            materias_encontradas.append('Notariado')
        
        if re.search(r'\bAUDITOR[ÍI]A\b|\bFISCALIZACI[OÓ]N\b|\bRENDICI[OÓ]N\s+DE\s+CUENTAS\b', texto_completo):
            materias_encontradas.append('Auditoría')
    
    # ÚLTIMO RECURSO: Si no se detectó nada específico
    if not materias_encontradas:
        materias_encontradas.append("General")

    return materias_encontradas


def detectar_escaneado(ruta_pdf, texto_extraido):
    """
    Detecta si el PDF es un documento escaneado.
    Criterio: poco texto extraíble en relación a las páginas.
    """
    if not texto_extraido or texto_extraido.strip() == "":
        return True

    try:
        with pdfplumber.open(ruta_pdf) as pdf:
            num_paginas = len(pdf.pages)
            if num_paginas == 0:
                return True

            caracteres_por_pagina = len(texto_extraido) / num_paginas
            return caracteres_por_pagina < UMBRAL_CARACTERES_ESCANEADO
    except:
        return True


def detectar_tablas(ruta_pdf):
    """
    Detecta si el PDF contiene tablas usando pdfplumber.
    """
    try:
        with pdfplumber.open(ruta_pdf) as pdf:
            for pagina in pdf.pages[:10]:  # Revisar primeras 10 páginas
                tablas = pagina.find_tables()
                if tablas:
                    return True
        return False
    except:
        return False


# ============================================================
# FUNCIONES DE OCR
# ============================================================

def extraer_texto_ocr(ruta_pdf):
    """
    Extrae texto de un PDF escaneado usando OCR (Tesseract).
    """
    if not OCR_DISPONIBLE:
        return None

    try:
        # Convertir PDF a imágenes
        imagenes = convert_from_path(ruta_pdf, dpi=300)

        texto_paginas = []
        for i, imagen in enumerate(imagenes):
            # Extraer texto con Tesseract (español)
            texto = pytesseract.image_to_string(imagen, lang='spa')
            if texto.strip():
                texto_paginas.append(texto)

        if texto_paginas:
            return '\n\n'.join(texto_paginas)

    except Exception as e:
        print(f"    Error en OCR: {str(e)[:50]}")

    return None


# ============================================================
# FUNCIONES DEL SCRAPER
# ============================================================

def crear_carpetas():
    """Crea las carpetas necesarias"""
    for carpeta in [CARPETA_PDFS, CARPETA_TEXTO, CARPETA_JSON]:
        if not os.path.exists(carpeta):
            os.makedirs(carpeta)


def configurar_driver():
    """Configura y retorna el driver de Chrome"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def limpiar_nombre_archivo(nombre):
    """Limpia el nombre para que sea válido como nombre de archivo"""
    # Primero eliminar saltos de línea
    nombre_limpio = nombre.replace('\n', ' ').replace('\r', ' ')

    caracteres_invalidos = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in caracteres_invalidos:
        nombre_limpio = nombre_limpio.replace(char, '_')

    nombre_limpio = re.sub(r'\s+', ' ', nombre_limpio)
    nombre_limpio = re.sub(r'_+', '_', nombre_limpio)
    nombre_limpio = nombre_limpio.strip('_- ')

    if len(nombre_limpio) > 200:
        nombre_limpio = nombre_limpio[:200]

    return nombre_limpio


def seleccionar_100_entries(driver):
    """Selecciona mostrar 100 elementos en la tabla"""
    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'select[name$="_length"]'))
        )
        select = Select(select_element)
        select.select_by_value("100")
        print("  Seleccionado: Mostrar 100 elementos")
        time.sleep(2)
        return True
    except Exception as e:
        try:
            select_element = driver.find_element(By.CSS_SELECTOR, 'select.form-control')
            select = Select(select_element)
            select.select_by_value("100")
            time.sleep(2)
            return True
        except:
            return False


def extraer_pdfs_de_tabla(driver):
    """Extrae los enlaces PDF junto con el nombre del item de cada fila"""
    pdfs = []
    filas = driver.find_elements(By.CSS_SELECTOR, 'table tbody tr')

    for fila in filas:
        try:
            enlace_pdf = fila.find_element(By.CSS_SELECTOR, 'a[href$=".pdf"]')
            href = enlace_pdf.get_attribute("href")

            if not href or ".pdf" not in href.lower():
                continue

            nombre_item = None

            try:
                primera_celda = fila.find_element(By.CSS_SELECTOR, 'td:first-child')
                nombre_item = primera_celda.text.strip()
            except:
                pass

            if not nombre_item:
                try:
                    for tag in ['h4', 'h5', 'strong', 'b']:
                        try:
                            elemento = fila.find_element(By.TAG_NAME, tag)
                            nombre_item = elemento.text.strip()
                            if nombre_item:
                                break
                        except:
                            continue
                except:
                    pass

            if not nombre_item:
                texto_fila = fila.text.strip()
                nombre_item = texto_fila.split('\n')[0].strip()

            if not nombre_item:
                nombre_item = unquote(href.split("/")[-1].replace(".pdf", ""))

            nombre_archivo = limpiar_nombre_archivo(nombre_item) + ".pdf"

            pdfs.append({
                "url": href,
                "nombre": nombre_archivo,
                "titulo": nombre_item
            })

        except:
            continue

    return pdfs


def hacer_click_next(driver):
    """Hace clic en el botón Next"""
    selectores_next = [
        'a.paginate_button.next:not(.disabled)',
        '#DataTables_Table_0_next:not(.disabled)',
        '.dataTables_paginate a.next:not(.disabled)',
    ]

    for selector in selectores_next:
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, selector)
            if next_btn.is_displayed() and next_btn.is_enabled():
                clase = next_btn.get_attribute("class") or ""
                if "disabled" not in clase:
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2)
                    return True
        except:
            continue
    return False


def hay_pagina_siguiente(driver):
    """Verifica si hay una página siguiente disponible"""
    selectores = ['a.paginate_button.next', '#DataTables_Table_0_next']

    for selector in selectores:
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, selector)
            clase = next_btn.get_attribute("class") or ""
            if "disabled" not in clase:
                return True
        except:
            continue
    return False


def obtener_todos_los_pdfs(driver):
    """Obtiene todos los enlaces a PDFs navegando por todas las páginas"""
    print(f"\n  Accediendo a: {URL_LEGISLACION}")
    driver.get(URL_LEGISLACION)

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)

    seleccionar_100_entries(driver)
    time.sleep(2)

    todos_los_pdfs = []
    urls_vistas = set()
    pagina_actual = 1

    while True:
        print(f"\n  Extrayendo PDFs de la página {pagina_actual}...")

        pdfs_pagina = extraer_pdfs_de_tabla(driver)

        nuevos = 0
        for pdf in pdfs_pagina:
            if pdf["url"] not in urls_vistas:
                urls_vistas.add(pdf["url"])
                todos_los_pdfs.append(pdf)
                nuevos += 1

        print(f"    Encontrados {nuevos} PDFs nuevos")
        print(f"    Total acumulado: {len(todos_los_pdfs)} PDFs")

        if hay_pagina_siguiente(driver):
            if hacer_click_next(driver):
                pagina_actual += 1
            else:
                break
        else:
            break

    return todos_los_pdfs


def descargar_pdf(url, nombre_archivo, numero, total):
    """Descarga un PDF individual"""
    ruta_completa = os.path.join(CARPETA_PDFS, nombre_archivo)

    if os.path.exists(ruta_completa):
        print(f"  [{numero}/{total}] Ya existe: {nombre_archivo[:50]}")
        return True

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()

        with open(ruta_completa, "wb") as archivo:
            for chunk in response.iter_content(chunk_size=8192):
                archivo.write(chunk)

        print(f"  [{numero}/{total}] Descargado: {nombre_archivo[:50]}")
        return True

    except Exception as e:
        print(f"  [{numero}/{total}] Error: {str(e)[:30]}")
        return False


# ============================================================
# FUNCIONES DEL EXTRACTOR
# ============================================================

def extraer_texto_pdf(ruta_pdf):
    """Extrae texto de un PDF usando pdfplumber"""
    try:
        texto_paginas = []

        with pdfplumber.open(ruta_pdf) as pdf:
            total_paginas = len(pdf.pages)

            for pagina in pdf.pages:
                try:
                    texto = pagina.extract_text()
                    if texto:
                        texto_paginas.append(texto)
                except:
                    continue

        if texto_paginas:
            contenido = '\n\n'.join(texto_paginas)
            contenido = re.sub(r'\n{3,}', '\n\n', contenido)
            contenido = re.sub(r' +', ' ', contenido)
            return contenido, total_paginas

        return "", total_paginas if 'total_paginas' in dir() else 0

    except Exception as e:
        return "", 0


def procesar_documento(ruta_pdf, url, titulo):
    """
    Procesa un documento PDF y extrae todos los metadatos.
    Retorna un diccionario con la estructura JSON definida.
    """
    nombre_archivo = Path(ruta_pdf).stem

    # Extraer texto con pdfplumber
    texto, num_paginas = extraer_texto_pdf(ruta_pdf)

    # Detectar si es escaneado
    es_escaneado = detectar_escaneado(ruta_pdf, texto)

    # Si es escaneado e intentar OCR
    if es_escaneado and OCR_DISPONIBLE:
        print(f"    Documento escaneado, aplicando OCR...")
        texto_ocr = extraer_texto_ocr(ruta_pdf)
        if texto_ocr:
            texto = texto_ocr
            es_escaneado = True
        else:
            es_escaneado = True

    # Si no hay texto, marcar contenido vacío
    if not texto or texto.strip() == "":
        texto = "[No se pudo extraer texto del documento]"
        es_escaneado = True

    # Detectar tablas
    tiene_tablas = detectar_tablas(ruta_pdf)

    # Extraer metadatos del contenido
    ordenamiento = detectar_ordenamiento(titulo + " " + texto)
    status = detectar_status(texto)
    fecha_publicacion = extraer_fecha_publicacion(texto)
    fecha_ultima_reforma = extraer_fecha_reforma(texto)
    materia = detectar_materia(texto)

    # Construir documento JSON
    documento = {
        "titulo": titulo,
        "contenido": texto,
        "jurisdiccion": "Estado de Durango",
        "ordenamiento": ordenamiento,
        "fuente_oficial": FUENTE_OFICIAL,
        "status": status,
        "fecha_publicacion": fecha_publicacion,
        "fecha_ultima_reforma": fecha_ultima_reforma,
        "numero_paginas": num_paginas,
        "es_escaneado": es_escaneado,
        "tiene_tablas": tiene_tablas,
        "url": url,
        "materia": materia
    }

    return documento


def guardar_texto(contenido, titulo, numero, total):
    """Guarda el contenido en un archivo de texto"""
    # Limpiar saltos de línea y caracteres inválidos
    nombre_seguro = titulo.replace('\n', ' ').replace('\r', ' ')
    nombre_seguro = re.sub(r'[<>:"/\\|?*]', '_', nombre_seguro)
    nombre_seguro = re.sub(r'\s+', ' ', nombre_seguro)
    nombre_seguro = re.sub(r'_+', '_', nombre_seguro).strip('_- ')[:150]

    digitos = max(3, len(str(total)))
    nombre_archivo = f"{numero:0{digitos}d}_{nombre_seguro}.txt"
    ruta = os.path.join(CARPETA_TEXTO, nombre_archivo)

    with open(ruta, 'w', encoding='utf-8') as f:
        f.write(f"TITULO: {titulo}\n")
        f.write("=" * 60 + "\n\n")
        f.write(contenido)

    return nombre_archivo


def guardar_json(documento, numero, total):
    """Guarda el documento como JSON individual"""
    titulo = documento["titulo"]
    # Limpiar saltos de línea y caracteres inválidos
    nombre_seguro = titulo.replace('\n', ' ').replace('\r', ' ')
    nombre_seguro = re.sub(r'[<>:"/\\|?*]', '_', nombre_seguro)
    nombre_seguro = re.sub(r'\s+', ' ', nombre_seguro)
    nombre_seguro = re.sub(r'_+', '_', nombre_seguro).strip('_- ')[:150]

    digitos = max(3, len(str(total)))
    nombre_archivo = f"{numero:0{digitos}d}_{nombre_seguro}.json"
    ruta = os.path.join(CARPETA_JSON, nombre_archivo)

    with open(ruta, 'w', encoding='utf-8') as f:
        json.dump(documento, f, ensure_ascii=False, indent=2)

    return nombre_archivo


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("SCRAPER Y EXTRACTOR DE LEGISLACION VIGENTE")
    print("Congreso del Estado de Durango")
    print("=" * 60)

    crear_carpetas()

    # ========== FASE 1: DESCARGAR PDFs ==========
    print("\n" + "=" * 60)
    print("FASE 1: DESCARGA DE PDFs")
    print("=" * 60)

    print("\nIniciando navegador...")
    driver = configurar_driver()

    pdfs_info = []  # Guardar info de PDFs para fase 2

    try:
        pdfs = obtener_todos_los_pdfs(driver)

        if not pdfs:
            print("No se encontraron PDFs")
            return

        print(f"\n  Total de PDFs encontrados: {len(pdfs)}")
        print(f"  Carpeta de destino: {os.path.abspath(CARPETA_PDFS)}")
        print("-" * 60)

        exitosos = 0
        for i, pdf in enumerate(pdfs, 1):
            if descargar_pdf(pdf["url"], pdf["nombre"], i, len(pdfs)):
                exitosos += 1
                pdfs_info.append(pdf)
            time.sleep(0.3)

        print(f"\n  Descarga completada: {exitosos}/{len(pdfs)} PDFs")

    finally:
        driver.quit()
        print("  Navegador cerrado")

    # ========== FASE 2: EXTRAER CONTENIDO Y METADATOS ==========
    print("\n" + "=" * 60)
    print("FASE 2: EXTRACCION DE CONTENIDO Y METADATOS")
    print("=" * 60)

    if not PDFPLUMBER_DISPONIBLE:
        print("\nERROR: pdfplumber no instalado")
        print("Ejecuta: pip install pdfplumber")
        return

    # Mapear PDFs descargados con su info
    pdf_map = {pdf["nombre"]: pdf for pdf in pdfs_info}

    # Buscar PDFs descargados
    archivos_pdf = sorted(Path(CARPETA_PDFS).glob("*.pdf"), key=lambda x: x.name.lower())

    if not archivos_pdf:
        print("No se encontraron PDFs para extraer")
        return

    print(f"\n  PDFs a procesar: {len(archivos_pdf)}")
    print(f"  Carpeta de texto: {os.path.abspath(CARPETA_TEXTO)}")
    print(f"  Carpeta de JSON: {os.path.abspath(CARPETA_JSON)}")

    if OCR_DISPONIBLE:
        print("  OCR: Disponible (Tesseract)")
    else:
        print("  OCR: No disponible (instalar pytesseract y pdf2image)")

    print("-" * 60)

    total = len(archivos_pdf)
    stats = {
        "procesados": 0,
        "escaneados": 0,
        "con_tablas": 0,
        "por_ordenamiento": {},
        "por_materia": {}
    }

    for i, archivo in enumerate(archivos_pdf, 1):
        nombre_corto = archivo.name[:40] + "..." if len(archivo.name) > 40 else archivo.name
        print(f"\n  [{i}/{total}] {nombre_corto}")

        # Obtener info del PDF
        pdf_data = pdf_map.get(archivo.name, {})
        url = pdf_data.get("url", "")
        titulo = pdf_data.get("titulo", archivo.stem)

        # Procesar documento
        documento = procesar_documento(str(archivo), url, titulo)

        # Guardar texto
        nombre_txt = guardar_texto(documento["contenido"], titulo, i, total)
        print(f"    TXT: {nombre_txt[:45]}...")

        # Guardar JSON individual
        nombre_json = guardar_json(documento, i, total)
        print(f"    JSON: {nombre_json[:45]}...")

        # Estadísticas
        stats["procesados"] += 1
        if documento["es_escaneado"]:
            stats["escaneados"] += 1
        if documento["tiene_tablas"]:
            stats["con_tablas"] += 1

        ord_tipo = documento["ordenamiento"]
        stats["por_ordenamiento"][ord_tipo] = stats["por_ordenamiento"].get(ord_tipo, 0) + 1

        # Manejar múltiples materias
        for mat_tipo in documento["materia"]:
            stats["por_materia"][mat_tipo] = stats["por_materia"].get(mat_tipo, 0) + 1

        print(f"    {documento['numero_paginas']} págs | {documento['ordenamiento']} | {documento['materia']}")

    # ========== RESUMEN FINAL ==========
    print("\n" + "=" * 60)
    print("PROCESO COMPLETADO")
    print("=" * 60)
    print(f"\n  PDFs descargados:   {CARPETA_PDFS}/")
    print(f"  Textos extraidos:   {CARPETA_TEXTO}/")
    print(f"  JSONs individuales: {CARPETA_JSON}/")
    print(f"\n  Total archivos procesados: {stats['procesados']}")
    print(f"  Documentos escaneados:     {stats['escaneados']}")
    print(f"  Documentos con tablas:     {stats['con_tablas']}")

    print("\n  Por tipo de ordenamiento:")
    for tipo, cantidad in sorted(stats["por_ordenamiento"].items(), key=lambda x: -x[1]):
        print(f"    - {tipo}: {cantidad}")

    print("\n  Por materia:")
    for materia, cantidad in sorted(stats["por_materia"].items(), key=lambda x: -x[1]):
        print(f"    - {materia}: {cantidad}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
