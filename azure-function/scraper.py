import logging
import math
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from config import CATEGORIAS, ScraperConfig

logger = logging.getLogger(__name__)


class PlazaVeaScraper:

    def __init__(self, config: ScraperConfig = None):
        self.config  = config or ScraperConfig()
        self.headers = {"User-Agent": self.config.user_agent}
        self.categorias = CATEGORIAS

    def _total_paginas(self, categoria_id: str) -> int:
        params = {"fq": f"C:/{categoria_id}/", "_from": 0, "_to": 0}
        try:
            r = requests.get(self.config.base_url, headers=self.headers, params=params, timeout=self.config.timeout)
            resources = r.headers.get("resources")
            if resources:
                total = math.ceil(int(resources.split("/")[-1]) / self.config.page_size)
                return min(total, self.config.max_pages)
        except Exception as e:
            logger.warning(f"No se pudo obtener total de páginas para {categoria_id}: {e}")
        return self.config.max_pages

    def _obtener_pagina(self, categoria_id: str, pagina: int) -> List[Dict]:
        start = pagina * self.config.page_size
        params = {
            "fq": f"C:/{categoria_id}/",
            "_from": start,
            "_to": start + self.config.page_size - 1,
            "O": "OrderByScoreDESC",
        }
        try:
            r = requests.get(self.config.base_url, headers=self.headers, params=params, timeout=self.config.timeout)
            if r.status_code in (200, 206):
                return r.json()
        except Exception as e:
            logger.warning(f"Error en página {pagina} de {categoria_id}: {e}")
        return []

    def scrapear_categoria(self, nombre: str, categoria_id: str) -> List[Dict]:
        total = self._total_paginas(categoria_id)
        logger.info(f"{nombre.upper()} — {total} páginas")

        resultados = [None] * total
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self._obtener_pagina, categoria_id, i): i for i in range(total)}
            for future in as_completed(futures):
                resultados[futures[future]] = future.result()

        productos = [p for pagina in resultados if pagina for p in pagina]
        logger.info(f"{nombre.upper()} — {len(productos)} productos obtenidos")
        return productos

    def scrapear_todo(self) -> Dict[str, List[Dict]]:
        """Scraping paralelo de todas las categorías."""
        resultados = {}

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.scrapear_categoria, nombre, cat_id): nombre
                for nombre, cat_id in self.categorias.items()
            }
            for future in as_completed(futures):
                nombre = futures[future]
                try:
                    prods = future.result()
                    if prods:
                        resultados[nombre] = prods
                        logger.info(f"{nombre.upper()} — completado: {len(prods)} productos")
                except Exception as e:
                    logger.error(f"{nombre.upper()} — error: {e}")

        return resultados