# Databricks notebook source
import requests
import pytz
from datetime import datetime
import os

LIMA_TZ = pytz.timezone("America/Lima")
fecha_lima = datetime.now(LIMA_TZ).strftime("%Y-%m-%d")
landing_path = f"/Volumes/plazavea_dev/landing/files/batch_id={fecha_lima}"

try:
    archivos = dbutils.fs.ls(landing_path)
    if len(archivos) >= 16:
        print(f"Landing ya existe para {fecha_lima} con {len(archivos)} archivos. Saltando scraping.")
        dbutils.notebook.exit("already_scraped")
    else:
        print(f"Solo {len(archivos)} archivos. Re-scraping.")
except Exception as e:
    if "java.io.FileNotFoundException" in str(e):
        print(f"Carpeta no existe. Iniciando scraping.")
    else:
        raise e

url = os.environ.get("AZURE_FUNCTION_URL", "https://<YOUR_FUNCTION_APP>.azurewebsites.net/api/httptrigger")
response = requests.get(url, timeout=120)

if response.status_code != 200:
    raise Exception(f"Azure Function fallo con status {response.status_code}")

data = response.json()

if data.get("status") != "success":
    raise Exception(f"Azure Function retorno status inesperado: {data.get('status')}")

batch_id = data["fecha"]
total_categorias = data["total_categorias"]

if total_categorias != 16:
    raise Exception(f"Se esperaban 16 categorias pero llegaron {total_categorias}")

print(f"Landing listo: {total_categorias} categorias para batch_id={batch_id}")