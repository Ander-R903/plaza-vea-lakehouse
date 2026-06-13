-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS plazavea_ext_location
URL 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-vea-ac`)
COMMENT 'External location for the plazavea container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS plazavea_dev
MANAGED LOCATION 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/bronze/'
COMMENT 'This is the main catalog for the Plaza Vea project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Schemas (landing, bronze, silver, gold)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS plazavea_dev.landing;
CREATE SCHEMA IF NOT EXISTS plazavea_dev.bronze
    MANAGED LOCATION 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS plazavea_dev.silver
    MANAGED LOCATION 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS plazavea_dev.gold
    MANAGED LOCATION 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/gold';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use Catalog plazavea_dev

-- COMMAND ----------

USE CATALOG plazavea_dev;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS plazavea_dev.landing.files
LOCATION 'abfss://plazavea@<ADLS_ACCOUNT>.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/plazavea_dev/landing/files

-- COMMAND ----------

