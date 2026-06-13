import json
from datetime import datetime
import pytz
from scraper import PlazaVeaScraper
from config import ScraperConfig
import logging
import os
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

LIMA_TZ = pytz.timezone('America/Lima')

def get_adls_client():
    """Obtener cliente de ADLS Gen2"""
    conn_string = os.environ.get("ADLS_CONNECTION_STRING")
    if not conn_string:
        raise Exception("ADLS_CONNECTION_STRING no configurada")
    return DataLakeServiceClient.from_connection_string(conn_string)

def upload_to_adls(json_content, file_path, container_name):
    """Subir archivo a ADLS Gen2"""
    service_client = get_adls_client()
    file_system_client = service_client.get_file_system_client(container_name)
    
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(json_content, overwrite=True)
    
    return f"{container_name}/{file_path}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        categoria_nombre = req.params.get('categoria')
        capa = req.params.get('capa', 'landing')
        
        config = ScraperConfig()
        scraper = PlazaVeaScraper(config)
        
        # Usar zona horaria de Lima
        ahora_lima = datetime.now(LIMA_TZ)
        fecha = ahora_lima.strftime("%Y-%m-%d")
        
        container_name = os.environ.get("ADLS_CONTAINER", "plazavea")
        carpeta = f"{capa}/batch_id={fecha}"
        
        if categoria_nombre and categoria_nombre in scraper.categorias:
            cat_id = scraper.categorias[categoria_nombre]
            productos = scraper.scrapear_categoria(categoria_nombre, cat_id)
            
            json_content = json.dumps(productos, ensure_ascii=False, indent=2).encode('utf-8')
            ruta_adls = f"{carpeta}/{categoria_nombre}.json"
            archivo_subido = upload_to_adls(json_content, ruta_adls, container_name)
            
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "fecha": fecha,
                    "zona_horaria": "America/Lima",
                    "capa": capa,
                    "categoria": categoria_nombre,
                    "total_productos": len(productos),
                    "ruta_adls": archivo_subido
                }, ensure_ascii=False, indent=2),
                mimetype="application/json",
                status_code=200
            )
        
        resultados = scraper.scrapear_todo()
        archivos_subidos = []
        
        for nombre, productos in resultados.items():
            json_content = json.dumps(productos, ensure_ascii=False, indent=2).encode('utf-8')
            ruta_adls = f"{carpeta}/{nombre}.json"
            archivo_subido = upload_to_adls(json_content, ruta_adls, container_name)
            
            archivos_subidos.append({
                "categoria": nombre,
                "total": len(productos),
                "ruta_adls": archivo_subido
            })
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "fecha": fecha,
                "zona_horaria": "America/Lima",
                "capa": capa,
                "carpeta": carpeta,
                "total_categorias": len(archivos_subidos),
                "archivos": archivos_subidos
            }, ensure_ascii=False, indent=2),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json",
            status_code=500
        )