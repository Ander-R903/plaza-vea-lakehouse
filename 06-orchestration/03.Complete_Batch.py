# Databricks notebook source
# MAGIC %md
# MAGIC ## Completar Lote
# MAGIC  Marca el batch_id como COMPLETED en la tabla de control.

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
# MAGIC ### 5. Mark batch as COMPLETED

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

delta_table = DeltaTable.forName(spark, control_table)

source_df = (
    spark.createDataFrame([(v_batch_id,)], ["batch_id"])
        .withColumn("status", F.lit("COMPLETED"))
        .withColumn("updated_timestamp", F.current_timestamp())
        .withColumn("end_time", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Merge to update status

# COMMAND ----------

(delta_table.alias("t")
    .merge(
        source_df.alias("s"),
        "t.batch_id = s.batch_id AND t.status = 'IN_PROGRESS'"
    )
    .whenMatchedUpdate(set={
        "status": "s.status",
        "updated_timestamp": "s.updated_timestamp",
        "end_time": "s.end_time"
    })
    .execute()
)
print(f"Lote {v_batch_id} marcado como COMPLETED")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verify

# COMMAND ----------

print("=" * 50)
print("COMPLETAR LOTE - RESUMEN")
print("=" * 50)
print(f"Tabla de control: {control_table}")
print(f"Batch ID:         {v_batch_id}")
print(f"Estado anterior:  IN_PROGRESS")
print(f"Estado nuevo:     COMPLETED")
print("=" * 50)

# COMMAND ----------

