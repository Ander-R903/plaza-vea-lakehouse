# Databricks notebook source
# MAGIC %md 
# MAGIC # Create Control Tables

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{CONTROL_SCHEMA}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{CONTROL_SCHEMA}.batch_control (
    batch_id STRING,
    status STRING,
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message STRING
)
""")


