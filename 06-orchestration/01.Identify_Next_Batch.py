# Databricks notebook source
# MAGIC %md
# MAGIC ## Identify Next Batch
# MAGIC # 
# MAGIC Identifica el siguiente lote a procesar escaneando la carpeta landing
# MAGIC y comparando con la tabla de control de lotes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Get landing batches

# COMMAND ----------

landing_batches = sorted([
    file.name.rstrip("/")
    for file in dbutils.fs.ls(LANDING_BASE)
    if file.isDir() and file.name.startswith("batch_id=")
])

# Limpiar el prefijo "batch_id=" para obtener solo el valor
landing_batches_clean = [b.replace("batch_id=", "") for b in landing_batches]

print(f"Landing batches encontrados: {landing_batches_clean}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Get tracked batches from control table

# COMMAND ----------

control_table = get_control_table()
tracked_batches = []

if spark.catalog.tableExists(control_table):
    tracked_batches = [
        row.batch_id
        for row in (
            spark.table(control_table)
                 .filter(F.col("status").isin("COMPLETED"))
                 .select("batch_id")
                 .distinct()
                 .collect()
        )
    ]
    print(f"Tracked batches: {tracked_batches}")
else:
    print(f"Tabla {control_table} no existe. Se creara al procesar el primer lote.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Identify next batch

# COMMAND ----------

# Identificar lotes no procesados
new_batches = sorted(list(set(landing_batches_clean) - set(tracked_batches)))
next_batch = new_batches[0] if new_batches else None

print(f"Landing batches:     {landing_batches_clean}")
print(f"Tracked batches:     {tracked_batches}")
print(f"Next batch to process: {next_batch}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Set task values for downstream tasks

# COMMAND ----------

if next_batch is None:
    dbutils.jobs.taskValues.set(key="p_batch_id", value="")
    dbutils.jobs.taskValues.set(key="has_batch", value=False)
    print("No hay lotes para procesar. Pipeline finalizado.")
else:
    dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch)
    dbutils.jobs.taskValues.set(key="has_batch", value=True) 
    print(f"Siguiente lote a procesar: {next_batch}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Summary

# COMMAND ----------

print("=" * 50)
print("IDENTIFY NEXT BATCH - RESUMEN")
print("=" * 50)
print(f"Landing base:       {LANDING_BASE}")
print(f"Control table:      {control_table}")
print(f"Batches encontrados: {len(landing_batches_clean)}")
print(f"Batches procesados:  {len(tracked_batches)}")
print(f"Siguiente batch:     {next_batch}")
print("=" * 50)