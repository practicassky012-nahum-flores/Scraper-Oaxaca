# CLAUDE.md — Scrapers de legislación mexicana (federal + estatal)

## Qué es este proyecto
Scrapers que descargan PDFs de congresos mexicanos (federales y estatales), extraen texto con OCR, y generan JSONs estructurados para ingesta en la API de normatividad (PostgreSQL).

## Fuentes de datos
- **Congresos estatales** (32 estados): alojan legislación estatal Y copias de legislación federal
- **Congreso federal** (Cámara de Diputados): diputados.gob.mx — legislación federal oficial
- **DOF / SIDOF**: Diario Oficial de la Federación (pendiente)
- Un mismo sitio web puede tener documentos de distintas jurisdicciones

## Schema del JSON de salida (OBLIGATORIO)
Cada PDF procesado genera exactamente UN archivo JSON con estos 12 campos:

```json
{
  "titulo": "string — nombre completo del documento legal",
  "tipo_ordenamiento": "string — solo: Ley | Código | Reglamento | Decreto | Constitución | Acuerdo | NOM | Otro",
  "jurisdiccion": "string — Federal | nombre del estado (ej: Oaxaca, Tabasco)",
  "materia": ["array de strings — civil, penal, administrativo, fiscal, laboral, etc."],
  "fecha_publicacion": "string ISO — YYYY-MM-DD o null",
  "es_vigente": "boolean o null — true | false | null",
  "status_texto": "string — Vigente | Abrogada | Derogada | null",
  "contenido": "string — texto completo extraído del PDF",
  "url_fuente": "string — URL de donde se descargó el PDF, o null",
  "es_escaneado": "boolean — true si requirió OCR",
  "calidad_ocr": "number 0-100 o null — null si no fue escaneado",
  "archivo_origen": "string — nombre del PDF sin ruta"
}
```

## Reglas irrompibles

### Jurisdicción
- La jurisdicción refleja el ALCANCE de la ley, NO el sitio web de donde se scrapeó
- Leyes FEDERALES → `"Federal"` (aunque estén en un sitio estatal)
  - Ejemplos: Código Civil Federal, Ley Agraria, Ley General de Salud
  - Pista: títulos con "Federal", "General", publicados en el DOF
- Leyes ESTATALES → nombre del estado: `"Oaxaca"`, `"Tabasco"`, etc.
- Leyes MUNICIPALES → nombre del estado (la jurisdicción es estatal, no municipal)
- NO usar "Estado de Oaxaca", ni abreviaturas
- Si un sitio estatal tiene sección "Legislación Federal" → todos esos docs llevan `"Federal"`

### Tipo de ordenamiento
- Clasificar por el TÍTULO del documento, no por palabras internas
- "constitucional" en el texto NO significa tipo "Constitución"
- Si no es claro, usar "Otro"

### Vigencia
- NO inferir de palabras internas del texto
- Una ley que menciona "derogaciones internas" NO está derogada
- Si no hay indicador explícito de abrogación/derogación → `null`
- Para secciones de leyes abrogadas del sitio → forzar `"Abrogada"`

### Fechas
- Siempre formato ISO: `"2015-06-20"`
- Si hay múltiples fechas, usar la de PUBLICACIÓN ORIGINAL
- Si no se puede parsear → `null`

### Materia
- SIEMPRE array, nunca string
- Ejemplo correcto: `["civil", "laboral"]`

### Título
- Nombre completo del documento legal
- NO puede ser solo una fecha ("12/01/2026")
- NO puede ser genérico ("DIARIO DE LOS DEBATES")

## Stack de OCR (orden de prioridad)
1. pdfplumber — texto directo de PDFs no escaneados
2. PaddleOCR — PDFs escaneados, mejor precisión
3. Tesseract — fallback final, resolución 200 DPI

## Pistas para distinguir federal vs estatal
- "Federal" o "General" en el título → federal
- "del Estado de [nombre]" → estatal
- Publicado en DOF → federal
- Publicado en Periódico Oficial del estado → estatal
- Sección "Legislación Federal" del sitio → federal
- Sección "Legislación Estatal" del sitio → estatal
