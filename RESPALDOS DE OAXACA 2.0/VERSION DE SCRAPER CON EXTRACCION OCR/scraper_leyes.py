                #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║      SCRAPER v6 — Leyes de Oaxaca & Leyes Federales        ║
╚══════════════════════════════════════════════════════════════╝

Correcciones v6:
  ✓ URLs de descarga: el servidor real es www.congresooaxaca.gob.mx
    (el path /docs66.congresooaxaca.gob.mx/... funciona como proxy)
    → NO se transforma el hostname; se usa la URL tal como la
      resuelve urljoin()
  ✓ Downloads usan la MISMA sesión del scraping (con cookies Oaxaca)
  ✓ verify=False para certificados SSL del servidor de documentos
  ✓ Fallback a la URL con subdominio corregido si la primaria falla
  ✓ sanitizar() conserva acentos UTF-8 (á, é, ñ…)
    solo elimina caracteres inválidos en Windows/Linux
  ✓ Nombres: dobles espacios, puntos finales y símbolos raros limpiados
"""

import re
import time
import warnings
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

# Silenciar advertencias de SSL (verify=False)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Configuración ──────────────────────────────────────────────────────────────
BASE_DIR    = Path("Leyes de Oaxaca")
MAX_WORKERS = 10
DL_TIMEOUT  = 60

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
    """
    Genera un nombre de archivo válido CONSERVANDO los acentos UTF-8.
    Solo elimina caracteres que no son válidos en Windows/Linux.
    """
    # ── Eliminar notas de reforma entre paréntesis (texto muy largo) ──────────
    texto = re.sub(r'\(Reformad[ao].*',          '', texto, flags=re.IGNORECASE | re.DOTALL)
    texto = re.sub(r'\(.*?[Ll]egislatura.*?\)',  '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Dd]ecreto.*?\)',      '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Aa]probad[ao].*?\)',  '', texto, flags=re.DOTALL)
    texto = re.sub(r'\(.*?[Rr]esoluci[oó]n.*?\)', '', texto, flags=re.DOTALL)

    # ── Reemplazar guiones tipográficos y comillas tipográficas ───────────────
    for src, dst in [('\u2013', '-'), ('\u2014', '-'),   # en-dash, em-dash
                     ('\u2018', ''),  ('\u2019', ''),    # ' '
                     ('\u201c', ''),  ('\u201d', ''),    # " "
                     ('\u00b7', '.'), ('\u00ba', ''),    # · º
                     ('\u00aa', '')]:                    # ª
        texto = texto.replace(src, dst)

    # ── Caracteres inválidos en Windows: < > : " / \ | ? * ───────────────────
    texto = re.sub(r'[<>:"/\\|?*]', ' ', texto)

    # ── Eliminar caracteres de control ────────────────────────────────────────
    texto = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', texto)

    # ── Eliminar cualquier otro símbolo que no sea letra/número/espacio/-.(),──
    # \w en Python 3 incluye letras Unicode: á, é, ñ, etc. se conservan
    texto = re.sub(r"[^\w\s\-\.\,\(\)]", ' ', texto)

    # ── Limpiar espacios múltiples y puntos/comas al final ────────────────────
    texto = re.sub(r'\s+', ' ', texto).strip().rstrip('.,')
    return texto[:max_len] if texto else "sin_titulo"


def truncar_en_oaxaca(titulo: str) -> str:
    """Recorta el título hasta e incluyendo 'Oaxaca'."""
    idx = titulo.lower().find("oaxaca")
    if idx != -1:
        return titulo[: idx + len("oaxaca")].strip().rstrip(".,")
    return titulo


def aplicar_nombre(num: int, titulo: str, seccion: str) -> str:
    """Nombre base del archivo según las reglas de cada sección."""
    if seccion == "Federal":
        if num != 1:
            titulo = titulo + " del Estado de Oaxaca"
    elif seccion in ("Estatal", "Municipal"):
        titulo = truncar_en_oaxaca(titulo)
    # Marco Normativo: nombre tal cual (columna DENOMINACION)
    return f"{num:03d} - {sanitizar(titulo)}"


# ── URLs de descarga ───────────────────────────────────────────────────────────

def preparar_urls(href: str, page_url: str, pdf_base: str = "") -> tuple[str, str]:
    """
    Retorna (url_primaria, url_alternativa) para intentar la descarga.

    Para Oaxaca, el href es como '../../docs66.congresooaxaca.gob.mx/ruta/file.pdf'.
    La URL que resuelve el NAVEGADOR es:
      https://www.congresooaxaca.gob.mx/docs66.congresooaxaca.gob.mx/ruta/file.pdf
    Esa URL primaria funciona porque el servidor www actúa como proxy.
    La alternativa apunta al subdominio docs66 directamente.
    """
    if href.startswith("http"):
        primary = href
    elif pdf_base and not href.startswith(".."):
        # Para Federal: resolver relativo a pdf_base
        primary = urljoin(pdf_base, href.lstrip("./"))
    else:
        # Para Oaxaca: resolver relativo a la página (da www.congresooaxaca/docs66...)
        primary = urljoin(page_url, href)

    # URL alternativa: mover el hostname incrustado al host real
    import re as _re
    m = _re.match(
        r'^(https?://)([^/]+)/(docs\d+\.congresooaxaca\.gob\.mx/.+)$',
        primary
    )
    if m:
        alt = m.group(1) + m.group(3)   # https://docs66.congresooaxaca.gob.mx/...
    else:
        alt = ""

    return primary, alt


def es_pdf(href: str, img_src: str = "") -> bool:
    h = href.lower()
    i = img_src.lower()
    if h.endswith((".doc", ".docx", ".xls", ".xlsx")):
        return False
    if "_mov" in h or "pdf_mov" in h:
        return False
    return h.endswith(".pdf") or "pdf" in i


# ── Detección de columna de título ────────────────────────────────────────────

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


# ── Selección de la tabla de datos ────────────────────────────────────────────

def elegir_tabla(soup: BeautifulSoup):
    """Elige la tabla con más filas que tienen número en la primera celda."""
    tables = soup.find_all("table")
    if not tables:
        return None
    if len(tables) == 1:
        return tables[0]

    scored = []
    for t in tables:
        tbody = t.find("tbody") or t
        score = 0
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if cells and re.sub(r'\D', '', cells[0].get_text()):
                score += 1
        scored.append((score, len(tbody.find_all("tr")), t))

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return scored[0][2]


# ── Parser de tabla ────────────────────────────────────────────────────────────

def parse_tabla(soup: BeautifulSoup, page_url: str,
                seccion: str, pdf_base: str = "") -> list:
    table = elegir_tabla(soup)
    if not table:
        print("  [debug] No se encontró <table> en el HTML")
        return []

    # Headers
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

    # Filas
    tbody  = table.find("tbody") or table
    all_tr = tbody.find_all("tr")

    raw_rows = []
    seen:  set = set()

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
        print(f"  [debug] {len(all_tr)} <tr> en tbody pero 0 pasaron el filtro de número")
        for i, tr in enumerate(all_tr[:5]):
            cells = tr.find_all("td")
            print(f"  [debug] tr[{i}] {len(cells)} celdas: "
                  f"{[c.get_text(' ', strip=True)[:25] for c in cells]}")
        return []

    # Detectar columna de título
    if seccion == "federal":
        col_titulo = 1
    else:
        samples    = [r[1] for r in raw_rows[:12]]
        col_titulo = detectar_col_titulo(headers, samples)
        col_label  = headers[col_titulo] if col_titulo < len(headers) else "?"
        print(f"  → Headers   : {headers}")
        print(f"  → Col titulo: {col_titulo} → '{col_label}'")

    # Debug primeras 2 filas
    for num, cells, links in raw_rows[:2]:
        print(f"    [debug] #{num:03d} | cells={cells} | links={len(links)}")

    # Armar documentos
    docs = []
    for num, cell_texts, links in raw_rows:
        if col_titulo >= len(cell_texts):
            continue
        titulo = cell_texts[col_titulo]
        if not titulo:
            continue

        # Buscar PDF entre todos los links de la fila
        pdf_primary = pdf_alt = ""
        for lnk in links:
            href    = lnk["href"]
            img_src = lnk["imgSrc"]
            if not es_pdf(href, img_src):
                continue
            pdf_primary, pdf_alt = preparar_urls(href, page_url, pdf_base)
            break

        if pdf_primary:
            docs.append({
                "num":     num,
                "titulo":  titulo,
                "url":     pdf_primary,
                "url_alt": pdf_alt,
            })
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
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=opts)
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

        for css in [".dataTables_length select",
                    "select[name*='DataTables']",
                    "select[name*='_length']"]:
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


def scrape_seccion(nombre: str, cfg: dict,
                   session: requests.Session) -> list:
    global _oaxaca_warmup_done
    url      = cfg["url"]
    pdf_base = cfg.get("pdf_base", "")
    tipo     = cfg["tipo"]

    # Calentar sesión en el portal Oaxaca (obtener cookies)
    if tipo == "oaxaca" and not _oaxaca_warmup_done:
        try:
            session.get("https://www.congresooaxaca.gob.mx/", timeout=10, verify=False)
            _oaxaca_warmup_done = True
        except Exception:
            pass

    # ── Intento 1: requests ───────────────────────────────────────────────────
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

    # ── Intento 2: Selenium ───────────────────────────────────────────────────
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
                   session: requests.Session,
                   referer: str) -> tuple[bool, str]:
    """
    Intenta descargar desde cada URL en 'urls'.
    Usa la sesión compartida (tiene cookies Oaxaca).
    verify=False para certificados SSL del servidor de documentos.
    """
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
            r = session.get(url, headers=dl_headers,
                            timeout=DL_TIMEOUT, stream=True, verify=False)
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

            size = dest.stat().st_size
            if size < 1024:
                dest.unlink(missing_ok=True)
                last_err = f"Archivo vacío desde {url[:60]}…"
                continue

            return True, url   # Retorna la URL que funcionó

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

    # Referer para descargas: la URL de la sección
    referer = next(
        (cfg["url"] for n, cfg in SECCIONES.items() if n == nombre),
        "https://www.congresooaxaca.gob.mx/"
    )

    tareas   = []
    omitidos = 0

    for d in docs:
        base = aplicar_nombre(d["num"], d["titulo"], nombre)
        dest = carpeta / f"{base}.pdf"
        if dest.exists() and dest.stat().st_size > 1024:
            omitidos += 1
            continue
        # Lista de URLs a intentar: primaria, luego alternativa
        urls = [u for u in [d.get("url", ""), d.get("url_alt", "")] if u]
        tareas.append((urls, dest, base))

    if omitidos:
        print(f"  ↩  {omitidos} ya descargados — omitidos")
    if not tareas:
        print("  ✓  Sin descargas nuevas.")
        return

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

    # ── Validación: 1 PDF por número ─────────────────────────────────────────
    nums_esperados = {d["num"] for d in docs}
    pdfs_ok = [
        f for f in carpeta.iterdir()
        if f.suffix.lower() == ".pdf" and f.stat().st_size > 1024
    ]
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
    print("═" * 62)
    print("  SCRAPER v6 — Leyes de Oaxaca  (requests + BeautifulSoup)")
    print("═" * 62)

    BASE_DIR.mkdir(exist_ok=True)

    # Una sola sesión compartida para scraping Y descargas
    session = requests.Session()
    session.headers.update(HEADERS_BASE)

    for nombre, cfg in SECCIONES.items():
        print(f"\n{'═' * 62}")
        print(f"  SECCIÓN : {nombre}")
        print(f"  URL     : {cfg['url']}")
        print(f"{'─' * 62}")

        carpeta = BASE_DIR / nombre

        try:
            docs = scrape_seccion(nombre, cfg, session)
            docs.sort(key=lambda d: d["num"])

            if docs:
                descargar_seccion(nombre, docs, carpeta, session)
            else:
                print("  ! Sin documentos PDF encontrados.\n"
                      "    Revisa la salida [debug] de arriba.")

        except Exception:
            import traceback
            print(f"  ✗ Error en [{nombre}]:")
            traceback.print_exc()

    print(f"\n{'═' * 62}")
    print(f"  COMPLETADO — {BASE_DIR.resolve()}")
    print(f"{'═' * 62}")


if __name__ == "__main__":
    main()
