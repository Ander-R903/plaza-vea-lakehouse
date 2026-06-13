# Databricks notebook source
# MAGIC %md
# MAGIC # Crear Nuevo Lote
# MAGIC  
# MAGIC Marca el batch_id como IN_PROGRESS en la tabla de control.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get control table name

# COMMAND ----------

control_table = get_control_table()
print(f"Tabla de control: {control_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Receive batch_id parameter

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")

# COMMAND ----------

v_batch_id = dbutils.widgets.get("p_batch_id")

print(f"Batch ID recibido: {v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Validate batch_id

# COMMAND ----------

if not v_batch_id:
    raise Exception("Error: No se recibio batch_id. La tarea anterior debe proporcionar p_batch_id.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Mark batch as IN_PROGRESS

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

in_progress_df = (
    spark.createDataFrame(
        [Row(batch_id=v_batch_id, status="IN_PROGRESS")]
    )
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
    .withColumn("start_time", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write to control table

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists(control_table):
    delta_table = DeltaTable.forName(spark, control_table)
    (delta_table.alias("t")
        .merge(
            in_progress_df.alias("s"),
            "t.batch_id = s.batch_id"
        )
        .whenNotMatchedInsert(values={
            "batch_id": "s.batch_id",
            "status": "s.status",
            "created_timestamp": "s.created_timestamp",
            "updated_timestamp": "s.updated_timestamp",
            "start_time": "s.start_time"
        })
        .execute()
    )
else:
    in_progress_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(control_table)

print(f"Lote {v_batch_id} marcado como IN_PROGRESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verify

# COMMAND ----------

print("=" * 50)
print("CREAR NUEVO LOTE - RESUMEN")
print("=" * 50)
print(f"Tabla de control: {control_table}")
print(f"Batch ID:         {v_batch_id}")
print(f"Estado:           IN_PROGRESS")
print("=" * 50)