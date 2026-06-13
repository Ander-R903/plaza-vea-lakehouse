# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer — Plaza Vea Pricing
# MAGIC
# MAGIC Carga incremental por batch_id.
# MAGIC Cada ejecucion procesa exactamente un batch sin tocar el historico.
# MAGIC
# MAGIC - Fuente:  plazavea_dev.silver.silver_productos
# MAGIC - Destino: plazavea_dev.gold.gold_dim_* · gold_fact_precio_snapshot
# MAGIC
# MAGIC Modelo estrella tipo periodic snapshot.
# MAGIC Grano: un registro por producto por dia de ingesta.
# MAGIC
# MAGIC Estrategia de carga:
# MAGIC - Dimensiones usan MERGE INTO filtrando por batch_id.
# MAGIC - Fact usa INSERT INTO verificando que el batch no exista previamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Receive batch_id parameter

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

if not v_batch_id:
    import datetime
    v_batch_id = datetime.datetime.now().strftime("%Y-%m-%d")

print(f"Procesando batch_id: {v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from itertools import product as iterproduct

TABLA_SILVER = get_silver_table()
TABLA_GOLD   = get_gold_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Read from Silver (only current batch) and validate

# COMMAND ----------

df_silver = spark.table(TABLA_SILVER) \
    .filter(F.col("batch_id") == v_batch_id)

total_silver = df_silver.count()
print(f"Filas en Silver para batch_id={v_batch_id}: {total_silver}")

if total_silver == 0:
    raise Exception(f"No hay datos en Silver para batch_id={v_batch_id}")

null_productid = df_silver.filter(F.col("productid").isNull()).count()
null_fecha     = df_silver.filter(F.col("ingesta_date").isNull()).count()

if null_productid > 0:
    raise Exception(f"Existen {null_productid} valores nulos en productid")
if null_fecha > 0:
    raise Exception(f"Existen {null_fecha} valores nulos en ingesta_date")

print("Validacion OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Gold tables if not exist

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_fecha (
    fecha_key        BIGINT GENERATED ALWAYS AS IDENTITY,
    fecha            DATE,
    dia              INT,
    mes              INT,
    nombre_mes       STRING,
    trimestre        INT,
    anio             INT,
    dia_semana       STRING,
    es_fin_de_semana BOOLEAN
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_marca (
    marca_key BIGINT GENERATED ALWAYS AS IDENTITY,
    brandid   STRING,
    brand     STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_canal (
    canal_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    canal_venta STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_categoria (
    categoria_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    categoryid          STRING,
    categoria_principal STRING,
    subcategoria        STRING,
    grupo_producto      STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_etiquetado (
    etiquetado_key BIGINT GENERATED ALWAYS AS IDENTITY,
    alto_sodio     BOOLEAN,
    alto_azucar    BOOLEAN,
    alto_saturadas BOOLEAN,
    num_octogonos  INT
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_dim_producto (
    producto_key             BIGINT GENERATED ALWAYS AS IDENTITY,
    productid                STRING,
    sku                      STRING,
    productname              STRING,
    tipo_especifico_producto STRING,
    fecha_lanzamiento        DATE,
    vendedor                 STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA_GOLD}.gold_fact_precio_snapshot (
    fecha_key                 BIGINT,
    producto_key              BIGINT,
    marca_key                 BIGINT,
    canal_key                 BIGINT,
    categoria_key             BIGINT,
    etiquetado_key            BIGINT,
    precio_regular            DOUBLE,
    precio_online             DOUBLE,
    precio_tarjeta_sip        DOUBLE,
    full_price                DOUBLE,
    descuento_base_pct        INT,
    descuento_tarjeta_sip_pct INT,
    cantidad_disponible       INT,
    unit_multiplier           DOUBLE,
    en_promocion              BOOLEAN,
    precio_promo_exacto       BOOLEAN,
    precio_valido             BOOLEAN,
    disponible                BOOLEAN,
    retira_hoy                BOOLEAN,
    llega_manana              BOOLEAN,
    envio_domicilio           BOOLEAN,
    recojo_tienda             BOOLEAN
) USING DELTA
""")

print("Tablas Gold verificadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. DIM_FECHA
# MAGIC
# MAGIC MERGE sobre fecha. Solo procesa fechas del batch actual.
# MAGIC Si la fecha ya existe no se toca.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_fecha AS target
USING (
    SELECT DISTINCT
        ingesta_date                      AS fecha,
        DAYOFMONTH(ingesta_date)          AS dia,
        MONTH(ingesta_date)               AS mes,
        DATE_FORMAT(ingesta_date, 'MMMM') AS nombre_mes,
        QUARTER(ingesta_date)             AS trimestre,
        YEAR(ingesta_date)                AS anio,
        DATE_FORMAT(ingesta_date, 'EEEE') AS dia_semana,
        DAYOFWEEK(ingesta_date) IN (1, 7) AS es_fin_de_semana
    FROM {TABLA_SILVER}
    WHERE batch_id = '{v_batch_id}'
) AS source
ON target.fecha = source.fecha
WHEN NOT MATCHED THEN INSERT
    (fecha, dia, mes, nombre_mes, trimestre, anio, dia_semana, es_fin_de_semana)
VALUES
    (source.fecha, source.dia, source.mes, source.nombre_mes,
     source.trimestre, source.anio, source.dia_semana, source.es_fin_de_semana)
""")

print(f"DIM_FECHA: {spark.table(f'{TABLA_GOLD}.gold_dim_fecha').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. DIM_MARCA
# MAGIC
# MAGIC MERGE sobre brandid. Solo procesa marcas del batch actual.
# MAGIC Las marcas existentes no se tocan.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_marca AS target
USING (
    SELECT DISTINCT brandid, brand
    FROM {TABLA_SILVER}
    WHERE batch_id = '{v_batch_id}'
) AS source
ON target.brandid = source.brandid
WHEN NOT MATCHED THEN INSERT (brandid, brand)
VALUES (source.brandid, source.brand)
""")

print(f"DIM_MARCA: {spark.table(f'{TABLA_GOLD}.gold_dim_marca').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. DIM_CANAL

# COMMAND ----------

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_canal AS target
USING (
    SELECT DISTINCT canal_venta
    FROM {TABLA_SILVER}
    WHERE batch_id = '{v_batch_id}'
) AS source
ON target.canal_venta = source.canal_venta
WHEN NOT MATCHED THEN INSERT (canal_venta)
VALUES (source.canal_venta)
""")

print(f"DIM_CANAL: {spark.table(f'{TABLA_GOLD}.gold_dim_canal').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. DIM_CATEGORIA
# MAGIC
# MAGIC Clave compuesta: categoryid + subcategoria + grupo_producto.
# MAGIC seccion excluido de la clave porque es metadata de ingesta,
# MAGIC no un atributo de la categoria segun VTEX.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_categoria AS target
USING (
    SELECT DISTINCT
        categoryid,
        categoria_principal,
        subcategoria,
        grupo_producto
    FROM {TABLA_SILVER}
    WHERE batch_id = '{v_batch_id}'
      AND categoryid IS NOT NULL
) AS source
ON  target.categoryid     = source.categoryid
AND target.subcategoria   = source.subcategoria
AND target.grupo_producto = source.grupo_producto
WHEN NOT MATCHED THEN INSERT
    (categoryid, categoria_principal, subcategoria, grupo_producto)
VALUES
    (source.categoryid, source.categoria_principal,
     source.subcategoria, source.grupo_producto)
""")

print(f"DIM_CATEGORIA: {spark.table(f'{TABLA_GOLD}.gold_dim_categoria').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. DIM_ETIQUETADO
# MAGIC
# MAGIC Junk dimension pre-poblada con las 32 combinaciones posibles
# MAGIC de octogonos segun la Ley 30021 (2^3 x 4 conteos).
# MAGIC Se pre-pobla una sola vez, el MERGE no inserta duplicados.

# COMMAND ----------

combinaciones = list(iterproduct([True, False], [True, False], [True, False], [0, 1, 2, 3]))
df_etiq = spark.createDataFrame(
    combinaciones,
    ["alto_sodio", "alto_azucar", "alto_saturadas", "num_octogonos"]
)
df_etiq.createOrReplaceTempView("etiq_source")

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_etiquetado AS target
USING etiq_source AS source
ON  target.alto_sodio     = source.alto_sodio
AND target.alto_azucar    = source.alto_azucar
AND target.alto_saturadas = source.alto_saturadas
AND target.num_octogonos  = source.num_octogonos
WHEN NOT MATCHED THEN INSERT
    (alto_sodio, alto_azucar, alto_saturadas, num_octogonos)
VALUES
    (source.alto_sodio, source.alto_azucar,
     source.alto_saturadas, source.num_octogonos)
""")

print(f"DIM_ETIQUETADO: {spark.table(f'{TABLA_GOLD}.gold_dim_etiquetado').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. DIM_PRODUCTO
# MAGIC
# MAGIC MERGE sobre productid. Solo inserta productos nuevos.
# MAGIC Los existentes no se tocan para preservar la surrogate key historica.

# COMMAND ----------

spark.sql(f"""
MERGE INTO {TABLA_GOLD}.gold_dim_producto AS target
USING (
    SELECT DISTINCT
        productid, sku, productname,
        tipo_especifico_producto, fecha_lanzamiento, vendedor
    FROM {TABLA_SILVER}
    WHERE batch_id = '{v_batch_id}'
) AS source
ON target.productid = source.productid
WHEN NOT MATCHED THEN INSERT
    (productid, sku, productname, tipo_especifico_producto,
     fecha_lanzamiento, vendedor)
VALUES
    (source.productid, source.sku, source.productname,
     source.tipo_especifico_producto, source.fecha_lanzamiento,
     source.vendedor)
""")

print(f"DIM_PRODUCTO: {spark.table(f'{TABLA_GOLD}.gold_dim_producto').count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. FACT_PRECIO_SNAPSHOT
# MAGIC
# MAGIC Verifica si el batch ya tiene snapshot en la fact antes de insertar.
# MAGIC Garantiza idempotencia: correr dos veces no duplica datos.
# MAGIC INNER JOINs garantizan integridad referencial.
# MAGIC Registros sin dimension van a la tabla de auditoria.

# COMMAND ----------

ya_existe_en_fact = False
try:
    ya_existe_en_fact = spark.sql(f"""
        SELECT COUNT(*) as c
        FROM {TABLA_GOLD}.gold_fact_precio_snapshot f
        JOIN {TABLA_GOLD}.gold_dim_fecha df
            ON f.fecha_key = df.fecha_key
        WHERE df.fecha = '{v_batch_id}'
    """).collect()[0][0] > 0
except Exception:
    ya_existe_en_fact = False

if ya_existe_en_fact:
    print(f"Snapshot de {v_batch_id} ya existe en Fact. Saltando.")
    dbutils.notebook.exit(f"batch_id={v_batch_id} ya en Fact")

# Registrar la temp view del batch actual
spark.table(TABLA_SILVER) \
    .filter(F.col("batch_id") == v_batch_id) \
    .createOrReplaceTempView("silver_batch")

spark.sql(f"""
INSERT INTO {TABLA_GOLD}.gold_fact_precio_snapshot
SELECT
    df.fecha_key,
    p.producto_key,
    m.marca_key,
    c.canal_key,
    cat.categoria_key,
    e.etiquetado_key,
    s.precio_regular,
    s.precio_online,
    s.precio_tarjeta_sip,
    s.full_price,
    s.descuento_base_pct,
    s.descuento_tarjeta_sip_pct,
    s.cantidad_disponible,
    s.unit_multiplier,
    s.en_promocion,
    s.precio_promo_exacto,
    s.precio_valido,
    s.disponible,
    s.retira_hoy,
    s.llega_manana,
    s.envio_domicilio,
    s.recojo_tienda
FROM silver_batch s
INNER JOIN {TABLA_GOLD}.gold_dim_fecha df
    ON df.fecha = s.ingesta_date
INNER JOIN {TABLA_GOLD}.gold_dim_producto p
    ON p.productid = s.productid
INNER JOIN {TABLA_GOLD}.gold_dim_marca m
    ON m.brandid = s.brandid
INNER JOIN {TABLA_GOLD}.gold_dim_canal c
    ON c.canal_venta = s.canal_venta
INNER JOIN {TABLA_GOLD}.gold_dim_categoria cat
    ON  cat.categoryid     = s.categoryid
    AND cat.subcategoria   = s.subcategoria
    AND cat.grupo_producto = s.grupo_producto
INNER JOIN {TABLA_GOLD}.gold_dim_etiquetado e
    ON  e.alto_sodio     = COALESCE(s.alto_sodio,     false)
    AND e.alto_azucar    = COALESCE(s.alto_azucar,    false)
    AND e.alto_saturadas = COALESCE(s.alto_saturadas, false)
    AND e.num_octogonos  = COALESCE(s.num_octogonos,  0)
""")

print(f"Fact cargada para batch_id={v_batch_id}")

# Auditoria de registros sin dimension
df_perdidos = spark.sql(f"""
    SELECT s.productid, s.productname, s.categoryid,
           s.subcategoria, s.grupo_producto,
           s.canal_venta, s.brandid, s.ingesta_date
    FROM silver_batch s
    LEFT JOIN {TABLA_GOLD}.gold_dim_producto p  ON p.productid     = s.productid
    LEFT JOIN {TABLA_GOLD}.gold_dim_marca    m  ON m.brandid       = s.brandid
    LEFT JOIN {TABLA_GOLD}.gold_dim_canal    c  ON c.canal_venta   = s.canal_venta
    LEFT JOIN {TABLA_GOLD}.gold_dim_categoria cat
        ON  cat.categoryid     = s.categoryid
        AND cat.subcategoria   = s.subcategoria
        AND cat.grupo_producto = s.grupo_producto
    WHERE p.producto_key   IS NULL
       OR m.marca_key      IS NULL
       OR c.canal_key      IS NULL
       OR cat.categoria_key IS NULL
""")

perdidos = df_perdidos.count()
if perdidos > 0:
    print(f"Advertencia: {perdidos} registros sin dimension")
    df_perdidos.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{TABLA_GOLD}.gold_auditoria_perdidos")
    print(f"Guardados en {TABLA_GOLD}.gold_auditoria_perdidos")
else:
    print("Integridad referencial: OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Execution Summary

# COMMAND ----------

print("=" * 55)
print("RESUMEN EJECUCION GOLD")
print("=" * 55)

for tabla, nombre in [
    ("gold_dim_fecha",            "DIM_FECHA"),
    ("gold_dim_marca",            "DIM_MARCA"),
    ("gold_dim_canal",            "DIM_CANAL"),
    ("gold_dim_categoria",        "DIM_CATEGORIA"),
    ("gold_dim_etiquetado",       "DIM_ETIQUETADO"),
    ("gold_dim_producto",         "DIM_PRODUCTO"),
    ("gold_fact_precio_snapshot", "FACT_SNAPSHOT"),
]:
    count = spark.table(f"{TABLA_GOLD}.{tabla}").count()
    print(f"  {nombre:<20} {count} registros")

fact_batch = spark.sql(f"""
    SELECT COUNT(*) as c
    FROM {TABLA_GOLD}.gold_fact_precio_snapshot f
    JOIN {TABLA_GOLD}.gold_dim_fecha df ON f.fecha_key = df.fecha_key
    WHERE df.fecha = '{v_batch_id}'
""").collect()[0][0]

print(f"\n  Batch procesado:     {v_batch_id}")
print(f"  Registros en fact:   {fact_batch}")
print(f"  Registros perdidos:  {perdidos}")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Debug — Validacion final del batch

# COMMAND ----------



# COMMAND ----------

