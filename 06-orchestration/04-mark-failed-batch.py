# Databricks notebook source
# MAGIC %md
# MAGIC ### Marcar Lote como FAILED

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")

# COMMAND ----------

v_batch_id = dbutils.widgets.get("p_batch_id")

print(f"Marcando lote {v_batch_id} como FAILED")

# COMMAND ----------

from pyspark.sql import functions as F

control_table = get_control_table()

spark.sql(f"""
UPDATE {control_table}
SET status = 'FAILED', 
    updated_timestamp = CURRENT_TIMESTAMP(),
    error_message = 'Pipeline fallo durante el procesamiento de datos'
WHERE batch_id = '{v_batch_id}' AND status = 'IN_PROGRESS'
""")

print(f"Lote {v_batch_id} marcado como FAILED")