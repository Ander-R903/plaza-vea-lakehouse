# Databricks notebook source
import json
import re
import unicodedata

# COMMAND ----------

def normalizar_key(nombre):
    # Elimina acentos y caracteres especiales
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = nombre.encode("ascii", "ignore").decode("ascii")
    # Solo letras, numeros y guion bajo
    nombre = re.sub(r"[^a-zA-Z0-9]", "_", nombre)
    nombre = re.sub(r"_+", "_", nombre)
    return nombre.lower().strip("_")

# COMMAND ----------

COLUMN_MAPPING = {
    "Tipo De Producto": "tipo_especifico_producto",
    "Tipo de Producto": "canal_venta",
}

def aplicar_mapping(key):
    return COLUMN_MAPPING.get(key, normalizar_key(key))

# COMMAND ----------

def normalizar_producto(producto):
    nuevo = {}
    for k, v in producto.items():
        nuevo[aplicar_mapping(k)] = v
    return nuevo

# COMMAND ----------

def leer_y_normalizar(ruta_archivo):
    # Extraer nombre como seccion
    seccion = ruta_archivo.split("/")[-1].replace(".json", "")
    
    # Leer y parsear
    contenido = dbutils.fs.head(ruta_archivo, 100_000_000)
    productos = json.loads(contenido)
    
    # Normalizar cada producto
    resultado = []
    for p in productos:
        p_norm = normalizar_producto(p)
        p_norm["seccion"] = seccion
        resultado.append(p_norm)
    
    return resultado

# COMMAND ----------

