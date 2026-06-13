# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup — Plaza Vea Pricing
# MAGIC # 
# MAGIC Configuracion centralizada del pipeline.  
# MAGIC Todos los notebooks hacen %run a este archivo al inicio.  

# COMMAND ----------

# Catalog y schemas
CATALOG_NAME = "plazavea_dev"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LANDING_SCHEMA = "landing"
CONTROL_SCHEMA = "control"

# COMMAND ----------

# Tablas por capa
BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_productos"
SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_productos"
GOLD_TABLE_PREFIX = f"{CATALOG_NAME}.{GOLD_SCHEMA}"

# Tabla de control
CONTROL_TABLE = f"{CATALOG_NAME}.{CONTROL_SCHEMA}.batch_control"

# COMMAND ----------

# Rutas de Volumes
LANDING_VOLUME = "/Volumes/plazavea_dev/landing/files"
VOLUME_BASE = LANDING_VOLUME
LANDING_BASE = LANDING_VOLUME

# COMMAND ----------

# Funciones de acceso

def get_landing_path(batch_id):
    """Ruta para leer JSONs del landing"""
    return f"{LANDING_BASE}/batch_id={batch_id}/"

def get_norm_path(batch_id):
    """Ruta para guardar JSONs normalizados temporalmente"""
    return f"{LANDING_BASE}/normalizados/batch_id={batch_id}/"

def get_bronze_table():
    return BRONZE_TABLE

def get_silver_table():
    return SILVER_TABLE

def get_gold_schema():
    return GOLD_TABLE_PREFIX

def get_control_table():
    return CONTROL_TABLE

def get_control_schema():
    return f"{CATALOG_NAME}.{CONTROL_SCHEMA}"

# COMMAND ----------

print("=" * 50)
print("ENVIRONMENT CONFIGURATION")
print("=" * 50)
print(f"Catalog:           {CATALOG_NAME}")
print(f"Bronze schema:     {BRONZE_SCHEMA}")
print(f"Silver schema:     {SILVER_SCHEMA}")
print(f"Gold schema:       {GOLD_SCHEMA}")
print(f"Landing schema:    {LANDING_SCHEMA}")
print(f"Control schema:    {CONTROL_SCHEMA}")
print(f"Bronze table:      {BRONZE_TABLE}")
print(f"Silver table:      {SILVER_TABLE}")
print(f"Control table:     {CONTROL_TABLE}")
print(f"Landing volume:    {LANDING_VOLUME}")
print("=" * 50)