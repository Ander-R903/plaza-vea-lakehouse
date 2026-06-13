# Plaza Vea Pricing — Lakehouse Pipeline

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Azure Functions](https://img.shields.io/badge/Azure_Functions-0062AD?style=for-the-badge&logo=azurefunctions&logoColor=white)](https://azure.microsoft.com/en-us/products/functions)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)

Pipeline de datos para extraer, transformar y analizar precios del catálogo de Plaza Vea Perú (~21,000 productos · 16 categorías) con ejecución batch diaria sobre Databricks Lakehouse en Azure.

---

## Evolución del proyecto

| | v1 — Local | v2 — Cloud (actual) |
|---|---|---|
| **Cómputo** | Local · Python scripts | Databricks · PySpark |
| **Storage** | SQL Server local | ADLS Gen2 · Delta Lake |
| **Modelo** | Star Schema SQL Server | Star Schema Delta · Unity Catalog |
| **Orquestación** | Manual · CLI | Lakeflow Jobs · Timer 06:00 UTC |
| **Analítica** | Excel · Pandas | Power BI · Direct Lake |
| **Escala** | ~21k productos / run | ~21k productos / día · histórico acumulado |

La versión local se puede encontrar en: [plaza-vea-pricing](https://github.com/tuusuario/plaza-vea-pricing)

---

## Arquitectura

> diagrama aquí — reemplazar con imagen final

```
VTEX API → Azure Function → ADLS Gen2 Landing
                                    ↓
                          Lakeflow Job (Databricks)
                                    ↓
                    Bronze → Silver → Gold (Star Schema)
                                    ↓
                           Power BI · Direct Lake
```

---

## Stack técnico

**Ingesta**
- Azure Functions (Python) — scraping concurrente de la API pública de Plaza Vea
- ADLS Gen2 — landing zone particionada por `batch_id=YYYY-MM-DD`
- Unity Catalog External Volume

**Pipeline**
- Databricks Lakeflow Jobs — orquestación con `p_batch_id` via taskValues
- PySpark · Delta Lake ACID — procesamiento batch incremental
- Medallion Architecture — Bronze / Silver / Gold

**Modelo**
- Star Schema · Periodic Snapshot
- 6 dimensiones · `fact_precio_snapshot`
- Unity Catalog — lineaje y RBAC

**Analítica**
- Power BI Direct Lake — sin mover datos, sin Import
- ~$12 USD/mes (Jobs Compute + ADLS)

---

## Estructura del repositorio

```
plaza-vea-lakehouse/
├── azure-function/
│   ├── HttpTrigger/         # Trigger HTTP de Azure Functions
│   ├── scraper.py           # Scraping concurrente por categoría
│   ├── config.py            # Configuración de la API (IDs no incluidos)
│   ├── host.json
│   └── requirements.txt
│
├── 00-common/
│   ├── 01-environment-config.py   # Configuración centralizada de paths y tablas
│   └── 02-bronze-helpers.py       # Helpers de normalización VTEX
│
├── 01-setup/
│   └── 01_Project_Environment.sql # DDL Unity Catalog (requiere configuración)
│
├── 02-bronze/
│   └── 01-ingest-jsons.py         # Ingesta incremental · replaceWhere batch_id
│
├── 03-silver/
│   └── 01-transform-products.py   # MERGE upsert · 36 columnas · dedup
│
├── 04-gold/
│   └── 01-gold.py                 # Star Schema · 6 dims · fact INSERT-ONLY
│
├── 05-analytics/
│   └── 02_validar.py              # Validaciones post-carga
│
└── 06-orchestration/
    ├── 00_call_azure_function.py  # T01 · llama Azure Function
    ├── 01.Identify_Next_Batch.py  # T02 · verifica batch_control
    ├── 02.Create_New_Batch.py     # T04 · registra IN_PROGRESS
    ├── 03.Complete_Batch.py       # T08 · marca COMPLETED
    └── 04-mark-failed-batch.py    # T09 · marca FAILED · guarda error
```

---

## Setup

### 1. Clonar el repositorio

```bash
git clone https://github.com/tuusuario/plaza-vea-lakehouse.git
```

### 2. Configurar Unity Catalog

Edita `01-setup/01_Project_Environment.sql` y reemplaza los placeholders:

```sql
-- Reemplaza <ADLS_ACCOUNT> con el nombre de tu cuenta de ADLS Gen2
URL 'abfss://<CONTAINER>@<ADLS_ACCOUNT>.dfs.core.windows.net/'
```

Luego ejecuta el notebook en tu workspace de Databricks.

### 3. Configurar Azure Function

Agrega en Application Settings de tu Function App:

```
ADLS_CONNECTION_STRING = <tu connection string>
AZURE_FUNCTION_URL     = <url de tu function>
```

### 4. Obtener IDs de categoría

Los IDs de la API de Plaza Vea no están incluidos. Para obtenerlos:

1. Abre DevTools (F12) → pestaña Network
2. Navega por las categorías de Plaza Vea
3. Busca requests a `/api/catalog_system/pub/products/search`
4. Extrae el ID del parámetro `fq=C:/{id}/`
5. Completa el diccionario `CATEGORIAS` en `azure-function/config.py`

---

## Decisiones técnicas

| Decisión | Implementación |
|---|---|
| `replaceWhere` vs overwrite completo | Sobreescribe solo la partición `batch_id` del día. Histórico intacto. |
| MERGE key compuesta | `productid + ingesta_date` — cada día tiene su snapshot independiente |
| `dim_etiquetado` JUNK pre-poblada | 32 combinaciones Ley 30021 (2³×4) — elimina NULLs en fact |
| INNER JOINs en fact | RI 100% garantizada — perdidos van a `gold_auditoria_perdidos` |

---

## Resultados

| Métrica | Valor |
|---|---|
| Productos por batch | ~21,358 |
| Categorías | 16 |
| Registros Silver | 17,023 (may-29) · 17,812 (jun-02) |
| Facts cargadas | 34,835 · 2 fechas |
| Duración promedio | ~11 min/batch |
| Costo estimado | ~$12 USD/mes |

---

## Hallazgos técnicos

Durante el desarrollo se identificaron y resolvieron 8 problemas de calidad de datos en producción, incluyendo discrepancias en cálculo de precios, duplicación por múltiples secciones VTEX, arrays vacíos y productos promocionales distorsionando métricas.

Ver documentación completa → [DATA_QUALITY.md](https://github.com/Ander-R903/plaza-vea-lakehouse/blob/main/DATA_QUALITY.md)


## Mejoras futuras

- [ ] Migrar secretos a Azure Key Vault
- [ ] Scraping paralelo entre categorías *(fix aplicado en v2.1)*
- [ ] Alertas por email en fallo de batch
- [ ] Tests unitarios para transformaciones Silver