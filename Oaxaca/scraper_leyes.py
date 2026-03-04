#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║     SCRAPER v8 — Leyes de Oaxaca & Leyes Federales                 ║
║     + OCR (pdfplumber / PaddleOCR / Tesseract) + JSON enriquecido ║
╚══════════════════════════════════════════════════════════════════════╝

Novedades v8:
  ✓ Título real extraído de la primera página del PDF (una sola pasada)
  ✓ Corrección de encoding para PDFs con fuentes no estándar
  ✓ JSON enriquecido: jurisdiccion, ordenamiento, fuente_oficial, status
  ✓ Nuevos campos: numero_paginas, es_escaneado, tiene_tablas
  ✓ Captura de fecha_pub y fecha_reforma desde tabla HTML
  ✓ Renombrado automático del JSON y PDF con el título real tras OCR
  ✓ OCR_WORKERS=1 para evitar crashes de pdfplumber en Windows

Dependencias:
  pip install pdfplumber pytesseract Pillow paddleocr paddlepaddle
  Tesseract binario (opcional): https://github.com/UB-Mannheim/tesseract/wiki
  PaddleOCR descarga modelos (~500 MB) la primera vez que se usa.
"""

import json
import re
import time
import warnings
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

# ── OCR imports (opcionales) ───────────────────────────────────────────────────
try:
    import pdfplumber
    _PDFPLUMBER_OK = True
except ImportError:
    _PDFPLUMBER_OK = False
    print("⚠ pdfplumber no instalado. Ejecuta: pip install pdfplumber")

try:
    import pytesseract
    _TESSERACT_OK = True
except ImportError:
    _TESSERACT_OK = False

try:
    from paddleocr import PaddleOCR
    import numpy as np
    _PADDLE_OK = True
except ImportError:
    _PADDLE_OK = False

_paddle_instance = None  # singleton lazy — el modelo (~500 MB) se carga una sola vez

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Configuración ──────────────────────────────────────────────────────────────
BASE_DIR       = Path("Leyes de Oaxaca")
BASE_DIR_TEXTO = Path("Textos de Oaxaca")
MAX_WORKERS    = 10
OCR_WORKERS    = 1      # 1 hilo evita crashes de pdfplumber en Windows
DL_TIMEOUT     = 60

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

HEADERS_BASE = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

SECCIONES = {
    "Federal": {
        "url":      "https://www.diputados.gob.mx/LeyesBiblio/index.htm",
        "pdf_base": "https://www.diputados.gob.mx/LeyesBiblio/",
        "tipo":     "federal",
    },
    "Estatal": {
        "url":            "https://www.congresooaxaca.gob.mx/legislaciones/legislacion_estatal.html",
        "tipo":           "oaxaca",
        "truncar_oaxaca": True,
    },
    "Municipal": {
        "url":            "https://www.congresooaxaca.gob.mx/legislaciones/legislacion_municipal.html",
        "tipo":           "oaxaca",
        "truncar_oaxaca": True,
    },
    "Marco Normativo": {
        "url":            "https://www.congresooaxaca.gob.mx/legislaciones/marco-normativo.html",
        "tipo":           "oaxaca",
        "truncar_oaxaca": False,
    },
}


# ── Utilidades de texto ────────────────────────────────────────────────────────

def sanitizar(texto: str, max_len: int = 160) -> str:
    """Nombre de archivo válido conservando acentos UTF-8."""
    texto = re.sub(r'\(Reformad[ao].*',            '', texto, flags=re.IGNORECASE | re.DOTALL)
    texto = re.sub(r'\(.*?[Ll]egislatura.*?\)',    '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Dd]ecreto.*?\)',        '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Aa]probad[ao].*?\)',    '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Rr]esoluci[oó]n.*?\)', '', texto, flags=re.DOTALL)
    for src, dst in [('\u2013', '-'), ('\u2014', '-'),
                     ('\u2018', ''),  ('\u2019', ''),
                     ('\u201c', ''),  ('\u201d', ''),
                     ('\u00b7', '.'), ('\u00ba', ''), ('\u00aa', '')]:
        texto = texto.replace(src, dst)
    texto = re.sub(r'[<>:"/\\|?*]', ' ', texto)
    texto = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', texto)
    texto = re.sub(r"[^\w\s\-\.\,\(\)]", ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip().rstrip('.,')
    return texto[:max_len] if texto else "sin_titulo"


def truncar_en_oaxaca(titulo: str) -> str:
    idx = titulo.lower().find("oaxaca")
    if idx != -1:
        return titulo[: idx + len("oaxaca")].strip().rstrip(".,")
    return titulo


def aplicar_nombre(num: int, titulo: str, seccion: str) -> str:
    """Nombre base del archivo según las reglas de cada sección."""
    if seccion in ("Estatal", "Municipal"):
        titulo = truncar_en_oaxaca(titulo)
    return f"{num:03d} - {sanitizar(titulo)}"


# ── URLs de descarga ───────────────────────────────────────────────────────────

def preparar_urls(href: str, page_url: str, pdf_base: str = "") -> tuple[str, str]:
    if href.startswith("http"):
        primary = href
    elif pdf_base and not href.startswith(".."):
        primary = urljoin(pdf_base, href.lstrip("./"))
    else:
        primary = urljoin(page_url, href)
    m = re.match(r'^(https?://)([^/]+)/(docs\d+\.congresooaxaca\.gob\.mx/.+)$', primary)
    alt = (m.group(1) + m.group(3)) if m else ""
    return primary, alt


def es_pdf(href: str, img_src: str = "") -> bool:
    h = href.lower()
    i = img_src.lower()
    if h.endswith((".doc", ".docx", ".xls", ".xlsx")):
        return False
    if "_mov" in h or "pdf_mov" in h:
        return False
    return h.endswith(".pdf") or "pdf" in i


# ── Clasificación por materia ──────────────────────────────────────────────────

REGLAS_MATERIA: list[tuple[str, list[str]]] = [
    ("constitucional",          ["constitución", "constitucional"]),
    ("penal",                   ["código penal", "penal", "delito", "crimen",
                                  "readaptación social", "reinserción social",
                                  "ejecución de pena"]),
    ("civil",                   ["código civil", "familiar", "sucesiones",
                                  "arrendamiento", "propiedad privada", "notariado familiar"]),
    ("electoral",               ["electoral", "elecciones", "partido político",
                                  "sufragio", "electorales", "instituto electoral"]),
    ("fiscal",                  ["fiscal", "hacienda", "presupuesto", "impuesto",
                                  "tributario", "finanzas públicas", "ingresos del estado",
                                  "egresos", "deuda pública"]),
    ("laboral",                 ["laboral", "ley del trabajo", "empleo",
                                  "trabajador", "sindicato", "contrato colectivo",
                                  "servicio civil", "burocracia"]),
    ("derechos humanos",        ["derechos humanos", "derechos fundamentales",
                                  "comisión de derechos"]),
    ("transparencia",           ["transparencia", "acceso a la información",
                                  "datos personales", "protección de datos",
                                  "archivos", "gobierno abierto"]),
    ("género",                  ["género", "igualdad de género", "violencia contra la mujer",
                                  "mujeres", "feminicidio", "paridad",
                                  "hostigamiento sexual", "acoso sexual",
                                  "igualdad entre mujeres y hombres"]),
    ("niñez",                   ["niño", "niña", "niñez", "adolescente",
                                  "infancia", "menor de edad", "menores"]),
    ("salud",                   ["salud", "sanitario", "sanitaria", "médico",
                                  "médica", "enfermería", "hospital",
                                  "farmacéutico", "adicciones"]),
    ("educación",               ["educación", "educativa", "educativo",
                                  "universidad", "escolar", "escuela",
                                  "enseñanza", "bachillerato", "normal"]),
    ("cultura",                 ["cultura", "cultural", "patrimonio",
                                  "museo", "arte", "artístico", "lenguas indígenas",
                                  "patrimonio histórico"]),
    ("desarrollo social",       ["desarrollo social", "asistencia social",
                                  "bienestar social", "pobreza", "marginación",
                                  "asistencia pública"]),
    ("urbanismo",               ["desarrollo urbano", "ordenamiento territorial",
                                  "asentamiento", "vivienda", "construcción",
                                  "obra pública", "catastro"]),
    ("ambiental",               ["ambiente", "ambiental", "ecología", "ecológico",
                                  "recursos naturales", "forestal", "fauna",
                                  "flora", "biodiversidad", "sustentable",
                                  "residuos", "cambio climático"]),
    ("protección civil",        ["protección civil", "emergencia", "desastre",
                                  "riesgo", "gestión de riesgos", "bomberos"]),
    ("seguridad pública",       ["seguridad pública", "policía", "prevención del delito",
                                  "seguridad ciudadana", "fuerza pública",
                                  "guardia nacional"]),
    ("ciencia y tecnología",    ["ciencia", "tecnología", "innovación",
                                  "investigación científica", "tecnológico",
                                  "inteligencia artificial"]),
    ("deporte",                 ["deporte", "deportivo", "cultura física",
                                  "actividad física", "recreación", "olimpiada"]),
    ("municipal",               ["municipal", "municipio", "municipios",
                                  "cabildo", "ayuntamiento"]),
    ("turismo",                 ["turismo", "turística", "turístico"]),
    ("comunicación social",     ["radio", "televisión", "medios de comunicación",
                                  "comunicación social", "prensa", "periodismo"]),
    ("movilidad",               ["transporte", "vialidad", "tránsito",
                                  "tráfico", "movilidad", "carretera",
                                  "autopista", "aeropuerto"]),
    ("agrario",                 ["agrario", "agrícola", "rural", "campo",
                                  "campesino", "ganadero", "pesca",
                                  "silvícola", "ejido"]),
    ("económico",               ["económico", "economía", "competitividad",
                                  "industria", "comercio", "empresa",
                                  "inversión", "fomento económico"]),
    ("anticorrupción",          ["anticorrupción", "combate a la corrupción",
                                  "contraloría", "sistema anticorrupción"]),
    ("notariado",               ["notarial", "notario", "fedatario",
                                  "fe pública", "correduría"]),
    ("registral",               ["registro público", "registral",
                                  "registro civil", "registro de la propiedad"]),
    ("energía",                 ["energía", "energético", "eléctrico",
                                  "electricidad", "hidrocarburos", "gas"]),
    ("migratorio",              ["migración", "migrante", "migratorio",
                                  "refugiado", "desplazado", "apátrida"]),
    ("ética",                   ["ética", "código de conducta",
                                  "conducta de servidores", "conflicto de interés",
                                  "declaración patrimonial"]),
    ("parlamentario",           ["congreso", "legislativo", "parlamentario",
                                  "diputado", "senado", "cámara de diputados",
                                  "poder legislativo"]),
    ("auditoría",               ["auditoría", "fiscalización",
                                  "rendición de cuentas", "cuenta pública",
                                  "órgano de fiscalización", "auditoría superior"]),
    ("judicial",                ["poder judicial", "tribunal", "juzgado",
                                  "magistrado", "supremo tribunal", "juicio de amparo"]),
    ("participación ciudadana", ["participación ciudadana", "referéndum",
                                  "plebiscito", "consulta popular",
                                  "iniciativa popular"]),
    ("administrativo",          ["orgánica", "administrativa", "administrativo",
                                  "organización gubernamental",
                                  "servicio profesional de carrera",
                                  "servidor público", "función pública"]),
]


def clasificar_materia(titulo: str) -> list[str]:
    t = titulo.lower()
    encontradas = [m for m, kws in REGLAS_MATERIA if any(k in t for k in kws)]
    return encontradas if encontradas else ["general"]


def capitalizar_materia(materia: str) -> str:
    return " ".join(p.capitalize() for p in materia.split())


# ── Corrección de encoding ────────────────────────────────────────────────────

_CORRECCIONES_ENCODING: list[tuple[str, str]] = [
    ("CDIGO",         "Código"),
    ("CONSTITUCIN",   "Constitución"),
    ("LEGISLACIN",    "Legislación"),
    ("ADMINISTRACIN", "Administración"),
    ("REGLAMENTACIN", "Reglamentación"),
    ("REGULACIN",     "Regulación"),
    ("ORGANIZACIN",   "Organización"),
    ("COMUNICACIN",   "Comunicación"),
    ("EDUCACIN",      "Educación"),
    ("HABITACIN",     "Habitación"),
    ("INFORMACIN",    "Información"),
    ("PARTICIPACIN",  "Participación"),
    ("PROTECCIN",     "Protección"),
    ("SANCIN",        "Sanción"),
    ("FUNCIN",        "Función"),
    ("RELACIN",       "Relación"),
    ("GESTIN",        "Gestión"),
    ("CREACIN",       "Creación"),
    ("NACIN",         "Nación"),
    ("ACCIN",         "Acción"),
    ("APLICACIN",     "Aplicación"),
    ("PUBLICACIN",    "Publicación"),
    ("MODIFICACIN",   "Modificación"),
    ("DISPOSICIN",    "Disposición"),
    ("OBLIGACIN",     "Obligación"),
    ("DEFINICIN",     "Definición"),
    ("CONTAMINACIN",  "Contaminación"),
    ("PREVENCIN",     "Prevención"),
    ("ATENCIN",       "Atención"),
    ("CONSTRUCCIN",   "Construcción"),
    ("GENERACIN",     "Generación"),
    ("COOPERACIN",    "Cooperación"),
    ("RESOLUCIN",     "Resolución"),
    ("SELECCIN",      "Selección"),
    ("CONTRIBUCIN",   "Contribución"),
    ("EJECUCIN",      "Ejecución"),
    ("INSTITUCIN",    "Institución"),
    ("PBLICA",        "Pública"),
    ("PBLICO",        "Público"),
    ("MXICO",         "México"),
    ("REPBLICA",      "República"),
    ("DEMOCRTICA",    "Democrática"),
    ("JURDICA",       "Jurídica"),
    ("JURDICO",       "Jurídico"),
    ("ECONMICA",      "Económica"),
    ("ECONMICO",      "Económico"),
    ("ORGNICA",       "Orgánica"),
    ("ORGNICO",       "Orgánico"),
    ("POLTICA",       "Política"),
    ("POLTICO",       "Político"),
    ("TCNICA",        "Técnica"),
    ("TCNICO",        "Técnico"),
    ("HISTRICA",      "Histórica"),
    ("HISTRICO",      "Histórico"),
    ("TURSTICA",      "Turística"),
    ("TURSTICO",      "Turístico"),
    ("ENERGTICA",     "Energética"),
    ("ENERGTICO",     "Energético"),
    ("PERIDICO",      "Periódico"),
    ("MDICO",         "Médico"),
    ("MDICA",         "Médica"),
    ("AGRICOLA",      "Agrícola"),
    ("SILVICOLA",     "Silvícola"),
    ("CRITICA",       "Crítica"),
]

_RE_CORRECCIONES = [
    (re.compile(r'\b' + re.escape(m) + r'\b', re.IGNORECASE), b)
    for m, b in _CORRECCIONES_ENCODING
]


def corregir_encoding_comun(texto: str) -> str:
    """Corrige palabras legales mexicanas con encoding de fuente corrupto."""
    for patron, correcto in _RE_CORRECCIONES:
        texto = patron.sub(correcto, texto)
    return texto


# ── Title case inteligente para español ───────────────────────────────────────

_MINUSCULAS_ES = {
    "de", "del", "la", "los", "las", "el", "en", "con", "por", "para",
    "a", "ante", "bajo", "y", "o", "e", "u", "al", "que", "sobre",
    "sin", "sus", "su", "entre",
}


def _title_case_es(texto: str) -> str:
    palabras = texto.split()
    resultado = []
    for i, pal in enumerate(palabras):
        if i == 0 or pal.lower() not in _MINUSCULAS_ES:
            resultado.append(pal[0].upper() + pal[1:].lower() if pal else pal)
        else:
            resultado.append(pal.lower())
    return " ".join(resultado)


# ── Extracción unificada: título + texto en una sola pasada ──────────────────

_KEYWORDS_TITULO_LEGAL = [
    "LEY", "CÓDIGO", "CODIGO", "CONSTITUCIÓN", "CONSTITUCION",
    "REGLAMENTO", "DECRETO", "ACUERDO", "NORMA", "LINEAMIENTO",
    "CONVENCIÓN", "CONVENCION", "TRATADO", "CIRCULAR",
]


def _filtrar_lineas_titulo(lineas: list[str]) -> list[str]:
    """Devuelve el bloque de 1-5 líneas que forman el título legal."""
    resultado: list[str] = []
    en_titulo = False
    for linea in lineas:
        linea_up = linea.upper()
        es_kw = any(kw in linea_up for kw in _KEYWORDS_TITULO_LEGAL)
        if es_kw and not en_titulo:
            en_titulo = True
            resultado.append(linea)
        elif en_titulo:
            if (len(linea) > 5
                    and not re.match(r'^(Art[íi]culo|ARTÍCULO|Art\.)', linea, re.IGNORECASE)
                    and not re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', linea)
                    and not re.match(r'^(Página|PÁGINA|\d+\s*$)', linea, re.IGNORECASE)):
                resultado.append(linea)
                if len(resultado) >= 5:
                    break
            else:
                break
    return resultado


def _titulo_desde_texto(texto_p0: str) -> str:
    """Extrae título de texto pdfplumber aplicando correcciones de encoding."""
    texto_corr = corregir_encoding_comun(texto_p0)
    lineas = [l.strip() for l in texto_corr.splitlines() if l.strip()][:25]
    tl = _filtrar_lineas_titulo(lineas)
    if tl:
        return _title_case_es(re.sub(r'\s+', ' ', " ".join(tl)).strip())
    primeras = [l for l in lineas if len(l) > 10][:3]
    if primeras:
        return _title_case_es(re.sub(r'\s+', ' ', " ".join(primeras)).strip())
    return ""


def _get_paddle() -> "PaddleOCR":
    """Singleton lazy de PaddleOCR (carga el modelo una sola vez)."""
    global _paddle_instance
    if _paddle_instance is None:
        _paddle_instance = PaddleOCR(
            use_angle_cls=True,   # corrige páginas rotadas
            lang="es",            # español
            use_gpu=False,
            show_log=False,
        )
    return _paddle_instance


def _paddle_ocr_pagina(img) -> str:
    """
    Recibe una imagen PIL y devuelve el texto extraído por PaddleOCR,
    ordenado de arriba-abajo / izquierda-derecha.
    Solo incluye fragmentos con confianza > 0.5.
    """
    ocr = _get_paddle()
    img_arr = np.array(img)
    resultado = ocr.ocr(img_arr, cls=True)
    if not resultado or not resultado[0]:
        return ""
    fragmentos = []
    for linea in resultado[0]:
        bbox, (texto, confianza) = linea
        if confianza > 0.5 and texto.strip():
            y_top = bbox[0][1]   # coordenada vertical del borde superior
            x_left = bbox[0][0]
            fragmentos.append((y_top, x_left, texto.strip()))
    fragmentos.sort(key=lambda t: (t[0], t[1]))
    return "\n".join(t[2] for t in fragmentos)


def _procesar_pdf_completo(pdf_path: Path) -> dict:
    """
    Abre pdfplumber UNA SOLA VEZ y extrae en la misma pasada:
      titulo_real, texto, numero_paginas, es_escaneado, tiene_tablas, metodo
    """
    _VACIO = {
        "titulo_real": "", "texto": "", "numero_paginas": 0,
        "es_escaneado": False, "tiene_tablas": False,
        "metodo": "pdfplumber no instalado",
    }
    if not _PDFPLUMBER_OK:
        return _VACIO

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            num_paginas = len(pdf.pages)
            if num_paginas == 0:
                return {**_VACIO, "metodo": "PDF vacío"}

            partes: list[str] = []
            pags_ocr = 0
            tiene_tablas = False
            titulo_real = ""

            for i, page in enumerate(pdf.pages):
                texto_pag = (page.extract_text() or "").strip()

                # Heurística rápida de tablas (primeras 5 páginas)
                if i < 5 and not tiene_tablas:
                    lineas_pag = texto_pag.splitlines()
                    if sum(1 for l in lineas_pag if l.count("  ") >= 2 or "\t" in l) >= 3:
                        tiene_tablas = True

                # Página 0: extraer título
                # FIX: la imagen se genera UNA sola vez y se reutiliza para texto OCR
                if i == 0:
                    img = None
                    try:
                        if _PADDLE_OK or _TESSERACT_OK:
                            img = page.to_image(resolution=200).original
                    except Exception as exc_img:
                        warnings.warn(f"No se pudo convertir página 0 a imagen: {exc_img}")

                    if _PADDLE_OK and img is not None:
                        try:
                            raw_paddle = _paddle_ocr_pagina(img)
                            lineas_ocr = [l.strip() for l in raw_paddle.splitlines() if l.strip()][:25]
                            tl = _filtrar_lineas_titulo(lineas_ocr)
                            if tl:
                                titulo_real = _title_case_es(
                                    re.sub(r'\s+', ' ', " ".join(tl)).strip()
                                )
                        except (RuntimeError, ValueError, AttributeError) as exc_paddle:
                            warnings.warn(f"PaddleOCR falló en título: {exc_paddle}")
                    elif _TESSERACT_OK and img is not None:
                        try:
                            raw_ocr = pytesseract.image_to_string(
                                img, lang="spa", config="--psm 6"
                            )
                            lineas_ocr = [l.strip() for l in raw_ocr.splitlines() if l.strip()][:25]
                            tl = _filtrar_lineas_titulo(lineas_ocr)
                            if tl:
                                titulo_real = _title_case_es(
                                    re.sub(r'\s+', ' ', " ".join(tl)).strip()
                                )
                        except (RuntimeError, OSError, AttributeError) as exc_tess:
                            warnings.warn(f"Tesseract falló en título: {exc_tess}")

                    if not titulo_real and texto_pag:
                        titulo_real = _titulo_desde_texto(texto_pag)

                # Texto de la página
                # FIX: img ya existe para página 0; solo se genera de nuevo en páginas siguientes
                if len(texto_pag) >= 30:
                    partes.append(texto_pag)
                elif _PADDLE_OK:
                    try:
                        if i != 0:
                            img = page.to_image(resolution=200).original
                        if img is not None:
                            texto_ocr = _paddle_ocr_pagina(img)
                            if texto_ocr:
                                partes.append(f"[Pág {i+1} — OCR]\n{texto_ocr}")
                                pags_ocr += 1
                    except (RuntimeError, ValueError, AttributeError) as exc_paddle:
                        warnings.warn(f"PaddleOCR falló en pág {i+1}: {exc_paddle}")
                elif _TESSERACT_OK:
                    try:
                        if i != 0:
                            img = page.to_image(resolution=200).original
                        if img is not None:
                            texto_ocr = pytesseract.image_to_string(
                                img, lang="spa", config="--psm 3"
                            ).strip()
                            if texto_ocr:
                                partes.append(f"[Pág {i+1} — OCR]\n{texto_ocr}")
                                pags_ocr += 1
                    except (RuntimeError, OSError, AttributeError) as exc_tess:
                        warnings.warn(f"Tesseract falló en pág {i+1}: {exc_tess}")

            contenido = "\n\n".join(partes)
            es_escaneado = pags_ocr / num_paginas > 0.5
            # FIX: motor refleja el que realmente se usó, no el que está instalado
            if pags_ocr == 0:
                metodo = "pdfplumber"
            elif _PADDLE_OK:
                metodo = f"pdfplumber+paddle ({pags_ocr} págs OCR)"
            else:
                metodo = f"pdfplumber+tesseract ({pags_ocr} págs OCR)"

            return {
                "titulo_real":    titulo_real,
                "texto":          contenido,
                "numero_paginas": num_paginas,
                "es_escaneado":   es_escaneado,
                "tiene_tablas":   tiene_tablas,
                "metodo":         metodo,
            }

    except Exception as e:
        return {**_VACIO, "metodo": f"error: {str(e)[:80]}"}


# ── Metadatos legales ─────────────────────────────────────────────────────────

_ORDENAMIENTOS: list[tuple[str, str]] = [
    ("constitución", "Constitución"),
    ("código",       "Código"),
    ("reglamento",   "Reglamento"),
    ("decreto",      "Decreto"),
    ("acuerdo",      "Acuerdo"),
    ("convención",   "Convención"),
    ("tratado",      "Tratado"),
    ("circular",     "Circular"),
    ("lineamiento",  "Lineamiento"),
    ("norma",        "Norma"),
    ("ley",          "Ley"),
]


def detectar_ordenamiento(titulo: str, tipo_tabla: str = "") -> str:
    t = titulo.lower()
    for kw, label in _ORDENAMIENTOS:
        if kw in t:
            return label
    return tipo_tabla if tipo_tabla else "Ley"


def obtener_jurisdiccion(nombre_seccion: str) -> str:
    return f"Congreso del Estado de Oaxaca - {nombre_seccion}"


def obtener_fuente_oficial(nombre_seccion: str) -> str:
    if nombre_seccion == "Federal":
        return "Cámara de Diputados del H. Congreso de la Unión"
    return "Congreso del Estado de Oaxaca"


def detectar_status(contenido: str) -> str:
    muestra = contenido[:2000].upper()
    if "ABROGADO" in muestra:
        return "Abrogado"
    if "DEROGADO" in muestra:
        return "Derogado"
    return "Vigente"


# ── Guardar JSON enriquecido ──────────────────────────────────────────────────

def guardar_json_texto(doc: dict, nombre_seccion: str,
                       extraccion: dict, titulo_real: str,
                       json_path: Path) -> None:
    titulo_final = titulo_real if titulo_real else doc["titulo"]
    texto        = extraccion["texto"]
    materias     = [capitalizar_materia(m) for m in clasificar_materia(titulo_final)]

    datos = {
        "titulo":               titulo_final,
        "contenido":            texto,
        "jurisdiccion":         obtener_jurisdiccion(nombre_seccion),
        "ordenamiento":         detectar_ordenamiento(titulo_final, nombre_seccion),
        "fuente_oficial":       obtener_fuente_oficial(nombre_seccion),
        "status":               detectar_status(texto),
        "fecha_publicacion":    doc.get("fecha_pub", ""),
        "fecha_ultima_reforma": doc.get("fecha_reforma", ""),
        "numero_paginas":       extraccion["numero_paginas"],
        "es_escaneado":         extraccion["es_escaneado"],
        "tiene_tablas":         extraccion["tiene_tablas"],
        "url":                  doc.get("url", ""),
        "materia":              materias,
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)


# ── Extracción + guardado + renombrado ────────────────────────────────────────

def _extraer_y_guardar(pdf_path: Path, json_path: Path,
                       doc: dict, nombre_seccion: str) -> tuple[bool, str]:
    """Extrae título + texto, guarda JSON y lo renombra inmediatamente con el título real."""
    resultado   = _procesar_pdf_completo(pdf_path)
    titulo_real = resultado.pop("titulo_real", "")
    guardar_json_texto(doc, nombre_seccion, resultado, titulo_real, json_path)

    # Renombrar JSON y PDF con el título correcto que quedó en el JSON
    titulo_final = titulo_real if titulo_real else doc["titulo"]
    stem_nuevo   = f"{doc['num']:03d} - {sanitizar(titulo_final)}"
    nuevo_json   = json_path.parent / f"{stem_nuevo}.json"
    if nuevo_json != json_path and not nuevo_json.exists():
        try:
            json_path.rename(nuevo_json)
            nuevo_pdf = pdf_path.parent / f"{stem_nuevo}.pdf"
            if not nuevo_pdf.exists():
                pdf_path.rename(nuevo_pdf)
        except Exception:
            pass

    return bool(resultado["texto"].strip()), resultado["metodo"]


def _any_file(carpeta: Path, num: int, ext: str) -> "Path | None":
    """Busca cualquier archivo '{num:03d} - *.ext' sin importar el título (post-renombrado)."""
    hits = sorted(carpeta.glob(f"{num:03d} - *.{ext}"))
    return hits[0] if hits else None


def procesar_textos_seccion(nombre: str, docs: list, carpeta_pdfs: Path) -> None:
    """Para cada PDF descargado, extrae texto, guarda JSON y renombra."""
    if not _PDFPLUMBER_OK:
        print("  ⚠ pdfplumber no disponible — omitiendo extracción de texto.")
        return

    json_dir = BASE_DIR_TEXTO / nombre
    json_dir.mkdir(parents=True, exist_ok=True)

    tareas: list[tuple] = []
    omitidos = 0

    for d in docs:
        base      = aplicar_nombre(d["num"], d["titulo"], nombre)
        json_path = json_dir / f"{base}.json"

        # Skip si ya existe cualquier JSON con ese número (incluso renombrado por OCR)
        if _any_file(json_dir, d["num"], "json"):
            omitidos += 1
            continue

        # Usar el PDF real (cualquier nombre con ese número, incluso renombrado)
        pdf_path = _any_file(carpeta_pdfs, d["num"], "pdf")
        if pdf_path is None or pdf_path.stat().st_size < 1024:
            continue

        tareas.append((pdf_path, json_path, d, nombre))

    if omitidos:
        print(f"  ↩  {omitidos} JSON ya existen — omitidos")
    if not tareas:
        print("  ✓  Sin textos nuevos que extraer.")
        return

    print(f"  → Extrayendo texto de {len(tareas)} PDFs ({OCR_WORKERS} hilo)\n")

    ok = fail = 0
    with ThreadPoolExecutor(max_workers=OCR_WORKERS) as ex:
        futuros = {
            ex.submit(_extraer_y_guardar, pdf_p, json_p, doc, nom): pdf_p.name
            for pdf_p, json_p, doc, nom in tareas
        }
        for fut in as_completed(futuros):
            nombre_pdf = futuros[fut]
            try:
                exito, metodo = fut.result()
                if exito:
                    ok += 1
                    print(f"  ✓  {nombre_pdf}  [{metodo}]", flush=True)
                else:
                    fail += 1
                    print(f"  ⚠  {nombre_pdf}  [sin texto — {metodo}]", flush=True)
            except Exception as e:
                fail += 1
                print(f"  ✗  {nombre_pdf}  [{e}]", flush=True)

    print(f"\n  OCR [{nombre}]: {ok} con texto, {fail} vacíos/error\n")


# ── Detección de columnas en tabla HTML ───────────────────────────────────────

KEYWORDS_OK = ["DENOMINACION", "TITULO DE LA LEY", "TITULO", "LEY", "NORMA", "NOMBRE"]
KEYWORDS_NO = ["TIPO", "FECHA", "DESCARGA", "PUBLICACION", "REFORMA", "NO."]
DATE_RE     = re.compile(r'^\d{2,4}[-/]\d{2}[-/]\d{2,4}$')


def detectar_col_titulo(headers: list, rows_sample: list) -> int:
    for kw in KEYWORDS_OK:
        for i, h in enumerate(headers):
            h_up = h.upper()
            if kw in h_up and not any(k in h_up for k in KEYWORDS_NO):
                return i
    col_lens: dict = {}
    for row in rows_sample:
        for i, txt in enumerate(row):
            if i == 0 or DATE_RE.match(txt) or not txt:
                continue
            col_lens.setdefault(i, []).append(len(txt))
    best, best_avg = 1, 0
    for ci, lens in col_lens.items():
        avg = sum(lens) / len(lens)
        if avg > best_avg:
            best_avg, best = avg, ci
    return best


def _find_col(headers: list, keywords: list) -> int | None:
    for kw in keywords:
        for i, h in enumerate(headers):
            if kw in h.upper():
                return i
    return None


# ── Selección de tabla ─────────────────────────────────────────────────────────

def elegir_tabla(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None
    if len(tables) == 1:
        return tables[0]
    scored = []
    for t in tables:
        tbody = t.find("tbody") or t
        score = sum(
            1 for tr in tbody.find_all("tr")
            if tr.find_all("td") and re.sub(r'\D', '', tr.find_all("td")[0].get_text())
        )
        scored.append((score, len(tbody.find_all("tr")), t))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return scored[0][2]


# ── Parser de tabla HTML ───────────────────────────────────────────────────────

def parse_tabla(soup: BeautifulSoup, page_url: str,
                seccion: str, pdf_base: str = "") -> list:
    table = elegir_tabla(soup)
    if not table:
        print("  [debug] No se encontró <table> en el HTML")
        return []

    headers = []
    thead = table.find("thead")
    if thead:
        for c in thead.find_all(["th", "td"]):
            headers.append(c.get_text(" ", strip=True))
    else:
        first_tr = table.find("tr")
        if first_tr:
            for c in first_tr.find_all(["th", "td"]):
                headers.append(c.get_text(" ", strip=True))

    tbody  = table.find("tbody") or table
    all_tr = tbody.find_all("tr")

    raw_rows = []
    seen: set = set()

    for tr in all_tr:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue
        num_txt = re.sub(r'\D', '', cells[0].get_text())
        if not num_txt:
            continue
        num = int(num_txt)
        if num in seen:
            continue
        seen.add(num)

        cell_texts = [c.get_text(" ", strip=True).split("\n")[0].strip() for c in cells]
        links = []
        for a in tr.find_all("a", href=True):
            img     = a.find("img")
            img_src = img.get("src", "") if img else ""
            links.append({"href": a["href"], "imgSrc": img_src})

        raw_rows.append((num, cell_texts, links))

    if not raw_rows:
        print(f"  [debug] {len(all_tr)} <tr> pero 0 pasaron el filtro de número")
        for i, tr in enumerate(all_tr[:5]):
            cells = tr.find_all("td")
            print(f"  [debug] tr[{i}] {len(cells)} celdas: "
                  f"{[c.get_text(' ', strip=True)[:25] for c in cells]}")
        return []

    # Detectar columna de título
    if seccion == "federal":
        col_titulo = 1
        col_pub = col_reform = col_tipo = None
    else:
        samples    = [r[1] for r in raw_rows[:12]]
        col_titulo = detectar_col_titulo(headers, samples)
        col_label  = headers[col_titulo] if col_titulo < len(headers) else "?"
        print(f"  → Headers   : {headers}")
        print(f"  → Col titulo: {col_titulo} → '{col_label}'")

        col_pub    = _find_col(headers, ["PUBLICACION", "PUBLICACIÓN", "FECHA PUB"])
        col_reform = _find_col(headers, ["REFORMA", "REFORMADO", "ÚLT. REFORMA", "ULT. REFORMA"])
        col_tipo   = _find_col(headers, ["TIPO"])

        if col_pub    is not None: print(f"  → Col pub   : {col_pub} → '{headers[col_pub]}'")
        if col_reform is not None: print(f"  → Col reform: {col_reform} → '{headers[col_reform]}'")
        if col_tipo   is not None: print(f"  → Col tipo  : {col_tipo} → '{headers[col_tipo]}'")

    for num, cells, links in raw_rows[:2]:
        print(f"    [debug] #{num:03d} | cells={cells} | links={len(links)}")

    docs = []
    for num, cell_texts, links in raw_rows:
        if col_titulo >= len(cell_texts):
            continue
        titulo = cell_texts[col_titulo]
        if not titulo:
            continue

        pdf_primary = pdf_alt = ""
        for lnk in links:
            if not es_pdf(lnk["href"], lnk["imgSrc"]):
                continue
            pdf_primary, pdf_alt = preparar_urls(lnk["href"], page_url, pdf_base)
            break

        if pdf_primary:
            doc = {"num": num, "titulo": titulo, "url": pdf_primary, "url_alt": pdf_alt}
            if seccion != "federal":
                doc["fecha_pub"]    = cell_texts[col_pub]    if col_pub    is not None and col_pub    < len(cell_texts) else ""
                doc["fecha_reforma"] = cell_texts[col_reform] if col_reform is not None and col_reform < len(cell_texts) else ""
                doc["tipo"]         = cell_texts[col_tipo]   if col_tipo   is not None and col_tipo   < len(cell_texts) else ""
            docs.append(doc)
        else:
            print(f"    ⚠ #{num:03d} '{titulo[:55]}' — sin enlace PDF")

    return docs


# ── Obtener HTML ───────────────────────────────────────────────────────────────

def get_html_requests(url: str, session: requests.Session) -> str | None:
    try:
        r = session.get(url, timeout=30, verify=False)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception as e:
        print(f"  ⚠ requests falló: {e}")
        return None


def get_html_selenium(url: str) -> str | None:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait, Select
        from selenium.common.exceptions import NoSuchElementException, TimeoutException
    except ImportError:
        print("  ✗ selenium no instalado")
        return None

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={UA}")
    opts.add_argument("--ignore-certificate-errors")

    try:
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    except Exception:
        try:
            driver = webdriver.Chrome(options=opts)
        except Exception as e:
            print(f"  ✗ No se pudo iniciar Chrome: {e}")
            return None

    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 35).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "table tr")) >= 3)
        except TimeoutException:
            return None

        time.sleep(2)

        for css in [".dataTables_length select", "select[name*='DataTables']", "select[name*='_length']"]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, css)
                s  = Select(el)
                for op in s.options:
                    val = op.get_attribute("value") or ""
                    if val in ("-1", "All", "all") or "all" in op.text.lower():
                        s.select_by_value(val)
                        time.sleep(4)
                        break
                else:
                    s.select_by_index(len(s.options) - 1)
                    time.sleep(3)
            except NoSuchElementException:
                continue

        try:
            driver.execute_script(
                "if(typeof $!=='undefined'&&$.fn&&$.fn.DataTable){"
                "var t=$.fn.dataTable.tables(true);"
                "if(t&&t.length)$(t).DataTable().page.len(-1).draw();}"
            )
            time.sleep(4)
        except Exception:
            pass

        prev = 0
        for _ in range(10):
            n = len(driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
            if n > 0 and n == prev:
                break
            prev = n
            time.sleep(1)

        print(f"  → Selenium: {prev} filas en tbody")
        return driver.page_source
    finally:
        driver.quit()


# ── Scraper principal ──────────────────────────────────────────────────────────

_oaxaca_warmup_done = False


def scrape_seccion(nombre: str, cfg: dict, session: requests.Session) -> list:
    global _oaxaca_warmup_done
    url      = cfg["url"]
    pdf_base = cfg.get("pdf_base", "")
    tipo     = cfg["tipo"]

    if tipo == "oaxaca" and not _oaxaca_warmup_done:
        try:
            session.get("https://www.congresooaxaca.gob.mx/", timeout=10, verify=False)
            _oaxaca_warmup_done = True
        except Exception:
            pass

    print(f"  Cargando (requests): {url}")
    html = get_html_requests(url, session)

    if html:
        for parser in ("lxml", "html.parser"):
            try:
                soup = BeautifulSoup(html, parser)
                docs = parse_tabla(soup, url, tipo, pdf_base)
                if docs:
                    print(f"  → {len(docs)} docs encontrados (requests/{parser})")
                    return docs
            except Exception as e:
                print(f"  ⚠ {parser}: {e}")
        print("  ⚠ requests: 0 filas con PDF. Intentando Selenium…")

    print(f"  Cargando (Selenium): {url}")
    html = get_html_selenium(url)
    if not html:
        print("  ✗ Selenium también falló.")
        return []

    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html, parser)
            docs = parse_tabla(soup, url, tipo, pdf_base)
            if docs:
                print(f"  → {len(docs)} docs encontrados (Selenium/{parser})")
                return docs
        except Exception as e:
            print(f"  ⚠ {parser}: {e}")

    print("  ✗ 0 documentos encontrados.")
    return []


# ── Descarga concurrente ───────────────────────────────────────────────────────

def _descargar_pdf(urls: list, dest: Path,
                   session: requests.Session, referer: str) -> tuple[bool, str]:
    dl_headers = {
        **HEADERS_BASE,
        "Referer": referer,
        "Accept":  "application/pdf,application/octet-stream,*/*",
    }
    last_err = "Sin URLs"
    for url in urls:
        if not url:
            continue
        try:
            r = session.get(url, headers=dl_headers, timeout=DL_TIMEOUT, stream=True, verify=False)
            r.raise_for_status()
            ct = r.headers.get("Content-Type", "")
            if "text/html" in ct.lower():
                last_err = f"HTML devuelto ({url[:60]}…)"
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                for chunk in r.iter_content(65_536):
                    if chunk:
                        f.write(chunk)
            if dest.stat().st_size < 1024:
                dest.unlink(missing_ok=True)
                last_err = f"Archivo vacío desde {url[:60]}…"
                continue
            return True, url
        except requests.exceptions.HTTPError as e:
            last_err = f"HTTP {e.response.status_code} ({url[:60]}…)"
        except requests.exceptions.ConnectionError:
            last_err = f"Error de conexión ({url[:60]}…)"
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:60]}"
    return False, last_err


def descargar_seccion(nombre: str, docs: list, carpeta: Path,
                      session: requests.Session):
    carpeta.mkdir(parents=True, exist_ok=True)
    referer  = next((cfg["url"] for n, cfg in SECCIONES.items() if n == nombre),
                    "https://www.congresooaxaca.gob.mx/")
    tareas   = []
    omitidos = 0

    for d in docs:
        base = aplicar_nombre(d["num"], d["titulo"], nombre)
        dest = carpeta / f"{base}.pdf"
        # Skip si ya existe cualquier PDF con ese número (incluso renombrado por OCR)
        existing = _any_file(carpeta, d["num"], "pdf")
        if existing and existing.stat().st_size > 1024:
            omitidos += 1
            continue
        urls = [u for u in [d.get("url", ""), d.get("url_alt", "")] if u]
        tareas.append((urls, dest, base))

    if omitidos:
        print(f"  ↩  {omitidos} ya descargados — omitidos")
    if not tareas:
        print("  ✓  Sin descargas nuevas.")
    else:
        print(f"  → Descargando {len(tareas)} PDFs ({MAX_WORKERS} hilos)\n")
        ok = fail = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futuros = {
                ex.submit(_descargar_pdf, urls, dest, session, referer): base
                for urls, dest, base in tareas
            }
            for fut in as_completed(futuros):
                base = futuros[fut]
                try:
                    exito, info = fut.result()
                except Exception as e:
                    exito, info = False, str(e)
                if exito:
                    ok += 1
                    print(f"  ✓  {base}.pdf")
                else:
                    fail += 1
                    print(f"  ✗  {base}.pdf  ← {info}")

        nums_esperados = {d["num"] for d in docs}
        pdfs_ok = [f for f in carpeta.iterdir()
                   if f.suffix.lower() == ".pdf" and f.stat().st_size > 1024]
        mapa: dict = {}
        for f in pdfs_ok:
            m = re.match(r'^(\d+)\s*-', f.name)
            if m:
                mapa.setdefault(int(m.group(1)), []).append(f.name)

        presentes = set(mapa.keys())
        faltantes = sorted(nums_esperados - presentes)
        dupls     = {n: fs for n, fs in mapa.items() if len(fs) > 1}
        extra     = sorted(presentes - nums_esperados)

        print(f"\n  {'─'*58}")
        print(f"  Validación [{nombre}]")
        print(f"  Esperados:{len(nums_esperados)} | Disco:{len(pdfs_ok)} | "
              f"OK:{ok} | Fallo:{fail} | Omitido:{omitidos}")
        if faltantes:
            print(f"  ✗ Faltantes ({len(faltantes)}): {faltantes}")
        else:
            print(f"  ✓ Sin faltantes — todos presentes")
        if dupls:
            for n, fs in sorted(dupls.items()):
                print(f"  ⚠ Duplicado #{n:03d}: {fs}")
        else:
            print(f"  ✓ Sin duplicados")
        if extra:
            print(f"  ⚠ Números extra: {extra}")
        print(f"  {'─'*58}\n")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("═" * 66)
    print("  SCRAPER v8 — Leyes de Oaxaca  (requests + BeautifulSoup + OCR)")
    print("  JSON enriquecido: jurisdiccion, ordenamiento, status, fechas")
    print("═" * 66)
    if not _PDFPLUMBER_OK:
        print("  ⚠ pdfplumber no instalado — se omitirá extracción de texto")
        print("    Instala con: pip install pdfplumber pytesseract Pillow")
    if _PADDLE_OK:
        print("  ✓ PaddleOCR disponible (motor principal para PDFs escaneados)")
    elif _TESSERACT_OK:
        print("  ✓ Tesseract disponible (fallback OCR)")
    else:
        print("  ℹ Sin OCR — título vía pdfplumber + correcciones de encoding")
        print("    Para PDFs escaneados: pip install paddleocr paddlepaddle")

    BASE_DIR.mkdir(exist_ok=True)
    BASE_DIR_TEXTO.mkdir(exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS_BASE)

    for nombre, cfg in SECCIONES.items():
        print(f"\n{'═' * 66}")
        print(f"  SECCIÓN : {nombre}")
        print(f"  URL     : {cfg['url']}")
        print(f"{'─' * 66}")

        carpeta = BASE_DIR / nombre

        try:
            docs = scrape_seccion(nombre, cfg, session)
            docs.sort(key=lambda d: d["num"])

            if docs:
                descargar_seccion(nombre, docs, carpeta, session)
                print(f"\n  {'─'*66}")
                print(f"  EXTRACCIÓN DE TEXTO [{nombre}]")
                print(f"  {'─'*66}")
                procesar_textos_seccion(nombre, docs, carpeta)
            else:
                print("  ! Sin documentos PDF encontrados.")

        except Exception:
            import traceback
            print(f"  ✗ Error en [{nombre}]:")
            traceback.print_exc()

    print(f"\n{'═' * 66}")
    print(f"  COMPLETADO")
    print(f"  PDFs   → {BASE_DIR.resolve()}")
    print(f"  Textos → {BASE_DIR_TEXTO.resolve()}")
    print(f"{'═' * 66}")


if __name__ == "__main__":
    main()
