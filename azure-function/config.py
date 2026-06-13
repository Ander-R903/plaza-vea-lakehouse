from dataclasses import dataclass


@dataclass
class ScraperConfig:
    base_url:    str = "https://www.plazavea.com.pe/api/catalog_system/pub/products/search"
    page_size:   int = 50
    max_workers: int = 10
    timeout:     int = 15
    max_pages:   int = 51
    user_agent:  str = "insomnia/11.6.1"

# =====================================================
# INSTRUCCIONES PARA OBTENER LOS IDs DE CATEGORÍA:
# 1. Abre herramientas de desarrollador (F12) → pestaña Network
# 2. Navega por las categorías de Plaza Vea
# 3. Busca peticiones a "/api/catalog_system/pub/products/search"
# 4. Extrae el ID del parámetro "fq=C:/{id}/"
# 5. Completa el diccionario con: "nombre-categoria": "id"
# =====================================================


CATEGORIAS: dict[str, str] = {
    "frutas-y-verduras":                    "77",
    "carnes-aves-y-pescados":               "####",
}
