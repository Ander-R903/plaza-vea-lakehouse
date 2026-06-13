# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Plaza Vea Pricing
# MAGIC # 
# MAGIC ### **Ingesta incremental particionada por batch_id**
# MAGIC # 
# MAGIC - Cada ejecucion procesa exactamente un batch sin tocar el historico
# MAGIC - Fuente: /Volumes/plazavea_dev/landing/files/batch_id={batch_id}/
# MAGIC - Destino: plazavea_dev.bronze.bronze_productos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Import Bronze Helpers

# COMMAND ----------

# MAGIC %run "../00-common/02-bronze-helpers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Receive batch_id parameter

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

if not v_batch_id:
    import datetime
    v_batch_id = datetime.datetime.now().strftime("%Y-%m-%d")

print(f"Procesando batch_id: {v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Build paths using config

# COMMAND ----------

LANDING_PATH = get_landing_path(v_batch_id)
NORM_PATH = get_norm_path(v_batch_id)
TABLA = get_bronze_table()

print(f"Landing:      {LANDING_PATH}")
print(f"Normalizados: {NORM_PATH}")
print(f"Tabla:        {TABLA}")

# COMMAND ----------

from pyspark.sql import functions as F
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Check if batch already exists (idempotency)

# COMMAND ----------

ya_existe = False
try:
    ya_existe = spark.table(TABLA) \
        .filter(F.col("batch_id") == v_batch_id) \
        .limit(1).count() > 0
except Exception:
    ya_existe = False

if ya_existe:
    print(f"batch_id={v_batch_id} ya existe en Bronze. Saltando procesamiento.")
    dbutils.notebook.exit(f"batch_id={v_batch_id} ya procesado")

print(f"batch_id={v_batch_id} no existe. Continuando ingesta.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Read JSON files from landing

# COMMAND ----------

try:
    archivos = dbutils.fs.ls(LANDING_PATH)
    json_files = [f.path for f in archivos if f.name.endswith(".json")]
    print(f"Archivos encontrados: {len(json_files)}")
except Exception as e:
    print(f"No se encontro la carpeta: {LANDING_PATH}")
    print(f"Asegurate de que Azure Functions creo los JSONs antes de ejecutar.")
    raise

if len(json_files) == 0:
    raise Exception(f"No hay archivos JSON en {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Normalize and save temporally

# COMMAND ----------

total_productos = 0

for ruta in json_files:
    resultado = leer_y_normalizar(ruta)
    
    categoria = ruta.split("/")[-1].replace(".json", "")
    
    for p in resultado:
            p["seccion"] = categoria
    
    dbutils.fs.put(
        f"{NORM_PATH}{categoria}.json",
        json.dumps(resultado),
        overwrite=True
    )
    
    total_productos += len(resultado)
    print(f"{categoria}: {len(resultado)} productos")

print(f"\nTotal productos normalizados: {total_productos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Read with Spark and add metadata

# COMMAND ----------

df_raw = spark.read.option("multiline", "true").json(NORM_PATH)

df_bronze = df_raw \
    .withColumn("ingesta_date", F.to_date(F.lit(v_batch_id))) \
    .withColumn("batch_id",     F.lit(v_batch_id))

print(f"Filas leidas:  {df_bronze.count()}")
print(f"Columnas:      {len(df_bronze.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Write to Bronze with incremental logic

# COMMAND ----------

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"batch_id = '{v_batch_id}'") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true")\
    .saveAsTable(TABLA)

print(f"Bronze actualizado para batch_id={v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Verify result

# COMMAND ----------

registros_batch = spark.table(TABLA).filter(F.col("batch_id") == v_batch_id).count()
print(f"Registros en batch_id={v_batch_id}: {registros_batch}")

total_bronze = spark.table(TABLA).count()
print(f"Total historico en Bronze: {total_bronze}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Preview data

# COMMAND ----------

display(spark.table(TABLA).filter(F.col("batch_id") == v_batch_id).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Execution Summary

# COMMAND ----------

print("=" * 50)
print("RESUMEN EJECUCION BRONZE")
print("=" * 50)
print(f"Batch ID:           {v_batch_id}")
print(f"Archivos JSON:      {len(json_files)}")
print(f"Productos totales:  {total_productos}")
print(f"Registros en tabla: {registros_batch}")
print(f"Tabla destino:      {TABLA}")
print(f"Particion escrita:  batch_id={v_batch_id}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. SQL Preview

# COMMAND ----------

# %sql
# SELECT 
#     productid,
#     productname,
#     canal_venta,
#     tipo_especifico_producto,
#     tipodeoctogono,
#     seccion,
#     ingesta_date,
#     items
# FROM plazavea_dev.bronze.bronze_productos
# LIMIT 5

# COMMAND ----------

