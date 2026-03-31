import time
import logging
import hashlib
import requests

from sqlalchemy import create_engine, text

# ——— Configuración inicial ———
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reemplaza por tu cadena real
DATABASE_URL = "postgresql+psycopg2://user:pass@host:port/dbname"
engine = create_engine(DATABASE_URL)
conn = engine.connect()


estatus = [
    "Vigente"
    "Derogada"
    "Abrogada"
    "Suspendida"
    "Transitoria"
    "Promulgada"
    "Publicada"
    "No vigente"
]

materias = [
    "Constitucional"
    "Administrativa"
    "Laboral"
    "Civil"
    "Común"
    "Penal"
    "Electoral"
    "Corporativo"
    "Mercantil"
    "Aduanero"
    "Ambiental"
    "Fiscal"
    "Notarial"
    "Agrario"
    "Juicio De Amparo"
    "Procesal"
    "Internacional Privado"
    "Internacional Público"
    "Migratorio"
    "Seguridad Social"
    "Militar"
    "Propiedad Intelectual Industrial"
    "Financiero"
    "Salud"
    "Propiedad Intelectual De Autor"
    "Sucesiones"
    "Inmobiliario"
    "Educativo"
    "Deportivo"
    "Cultural"
    "Género"
    "Antidiscriminación"
    "Informático"
    "Telecomunicaciones"
    "Espacial"
    "Energía"
]


tipos_de_ley = [
    "Constitución",
    "Tratado",
    "Código",
    "Ley",
    "Decreto",
    "Reglamento",
    "Norma",
    "Acto Administrativo",
    "Jurisprudencia",
    "Costumbre / Principio / Otro",
    "Acto Administrativo",
    "Decreto",
    "Jurisprudencia"
    ]

tipos_de_subley = [
    "Constitución Política de los Estados Unidos Mexicanos"
    "Tratado Internacional"
    "Código Civil"
    "Código Penal"
    "Ley Orgánica"
    "Ley Reglamentaria"
    "Ley Ordinaria"
    "Decreto Legislativo"
    "Decreto Presidencial"
    "Reglamento"
    "Norma Oficial Mexicana"
    "Norma Técnica"
    "Acuerdo"
    "Circular"
    "Lineamiento"
    "Instructivo"
    "Directriz"
    "Jurisprudencia"
    "Criterio Administrativo"
    "Costumbre"
    "Principio General del Derecho"
    "Uso y Costumbre"
    "Bando Municipal"
    "Acuerdo Municipal"
    "Norma Administrativa Municipal"
    "Decreto Administrativo"
    "Tesis"
]






# ——— Mapeos de catálogo (sólo si tus scraped_items traen nombres en lugar de IDs) ———
# Si ya tienes materia_id, estado_vigencia_id, etc. en scraped_items, puedes saltarte esta sección.
materias = {
    desc.lower().strip(): mid
    for mid, desc in conn.execute(text(
        "SELECT materia_id, descripcion FROM legislacion.materia"
    )).all()
}
estados = {
    desc.lower().strip(): eid
    for eid, desc in conn.execute(text(
        "SELECT estado_vigencia_id, descripcion FROM legislacion.estados_de_vigencia"
    )).all()
}
tipos_ley = {
    desc.lower().strip(): tid
    for tid, desc in conn.execute(text(
        "SELECT tipo_ley_id, descripcion FROM legislacion.tipos_de_ley"
    )).all()
}
jurisdicciones = {
    desc.lower().strip(): jid
    for jid, desc in conn.execute(text("""
        SELECT jurisdiccion_id, pais || '|' || coalesce(estado_provincia,'') || '|' || coalesce(municipio,'')
        FROM legislacion.jurisdicciones"
    """)).all()
}

# ——— Sentencias preparadas ———

# -- Se llama a través de: SELECT legislacion.upsert_ley(
#   p_titulo                       TEXT,
#   p_materia_desc                 TEXT,
#   p_estado_vigencia_desc         TEXT,
#   p_tipo_ley_desc                TEXT,
#   p_jurisdiccion_desc            TEXT,        -- formato: "pais" o "pais|estado" o "pais|estado|municipio (México|Ciudad de México|Coyoacán)"
#   p_fecha_publicacion            DATE,        -- FECHA DE ÚLTIMA PUBLICACIÓN (REFORMA, DEROGACIÓN, ETC.)
#   p_numero_oficial               VARCHAR(50)  DEFAULT NULL,
#   p_fecha_entrada_vigor          DATE         DEFAULT NULL,
#   p_fecha_fin_vigor              DATE         DEFAULT NULL,
#   p_fuente_url                   VARCHAR( 500) DEFAULT 'https://www.ordenjuridico.gob.mx/leyes.php',
#   p_prev_estado_vigencia_desc    TEXT         DEFAULT 'NO VIGENTE'
# -- )

sql_upsert_ley = text("""
  SELECT ley_id, is_new
    FROM legislacion.upsert_ley(
      :titulo, :materia_id, :estado_id, :tipo_id, :jurisd_id,
      :numero_oficial, :f_pub, :f_ini, :f_fin, :fuente_url
    )
""")

sql_upsert_doc = text("""
  SELECT documento_id, is_new
    FROM legislacion.upsert_doc_ley(
      :ley_id, :nombre, :ruta, :mime, :size, :version, :hash
    )
""")

# ——— Bucle principal ———
for item in scraped_items:
    # 2. Variables para la invocación
    titulo                    = "Código Civil"
    materia_desc              = "penal"
    estado_vigencia_desc      = "vigente"
    tipo_ley_desc             = "código"
    jurisdiccion_desc         = "México|Ciudad de México|Venustiano Carranza"
    fecha_publicacion         = "2025-04-12"
    numero_oficial            = "DL-2025-007"
    fecha_entrada_vigor       = "2025-05-01"
    fecha_fin_vigor           = "2030-05-01"
    fuente_url                = "https://dof.gob.mx/2025/CodigoCivil.pdf"
    prev_estado_vigencia_desc = "no vigente"

    # 3. SQL con parámetros nombrados
    sql = text("""
    SELECT *
    FROM legislacion.upsert_ley(
        p_titulo                       => :titulo,
        p_materia_desc                 => :materia_desc,
        p_estado_vigencia_desc         => :estado_vigencia_desc,
        p_tipo_ley_desc                => :tipo_ley_desc,
        p_jurisdiccion_desc            => :jurisdiccion_desc,
        p_fecha_publicacion            => :fecha_publicacion,
        p_numero_oficial               => :numero_oficial,
        p_fecha_entrada_vigor          => :fecha_entrada_vigor,
        p_fecha_fin_vigor              => :fecha_fin_vigor,
        p_fuente_url                   => :fuente_url,
        p_prev_estado_vigencia_desc    => :prev_estado_vigencia_desc
    );
    """)

    # 4. Parámetros para ejecutar
    params = {
        "titulo":                     titulo,
        "materia_desc":               materia_desc,
        "estado_vigencia_desc":       estado_vigencia_desc,
        "tipo_ley_desc":              tipo_ley_desc,
        "jurisdiccion_desc":          jurisdiccion_desc,
        "fecha_publicacion":          fecha_publicacion,
        "numero_oficial":             numero_oficial,
        "fecha_entrada_vigor":        fecha_entrada_vigor,
        "fecha_fin_vigor":            fecha_fin_vigor,
        "fuente_url":                 fuente_url,
        "prev_estado_vigencia_desc":  prev_estado_vigencia_desc,
    }

    # 5. Ejecutar y obtener resultados
    with engine.begin() as conn:
        out_ley_id, is_new, updated = conn.execute(sql, params).one()

    if not is_new:
        logger.info(f"→ Ley ‘{item['titulo']}’ (ID={ley_id}) ya existe, omito PDF.")
        continue

    # 3) Descarga + subida del PDF (hasta 3 reintentos)
    pdf_bytes = None
    for intento in range(3):
        try:
            resp = requests.get(item["pdf_url"], timeout=30)
            resp.raise_for_status()
            pdf_bytes = resp.content
            break
        except Exception as e:  
            logger.warning(f"Intento {intento+1}/3 descarga PDF fallido: {e}")
            time.sleep(1)
    if pdf_bytes is None:
        logger.error(f"✗ No pude descargar PDF para ‘{item['titulo']}’, salto.")
        continue

    # 4) Subida a Azure Blob
    try:
        blob_url = azure_upload(pdf_bytes, item["blob_name"])
    except TemporaryAzureError as e:
        logger.error(f"✗ Azure upload falló para ‘{item['titulo']}’: {e}")
        continue

    # 5) Calcular hash y tamaño
    sha256 = hashlib.sha256(pdf_bytes).digest()
    size   = len(pdf_bytes)

    # 6) Registrar documento en la BD
    doc_id, doc_is_new = conn.execute(sql_upsert_doc, {
        "ley_id":  ley_id,
        "nombre":  item["blob_name"],
        "ruta":    blob_url,
        "mime":    resp.headers.get("Content-Type"),
        "size":    size,
        "version": item.get("version"),
        "hash":    sha256
    }).one()

    logger.info(f"✔ Documento Ley(ID={ley_id}) → Doc(ID={doc_id}, nuevo={doc_is_new})")

    # 7) Commit (puedes hacerlo por lotes)
    conn.commit()

# ——— Limpieza ———
conn.close()
engine.dispose()
