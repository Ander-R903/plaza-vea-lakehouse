# Hallazgos de calidad de datos y resoluciones técnicas

## 1. Discrepancia en el cálculo de precios para productos variables

### Hallazgo

Durante la validación de precios contra la web de Plaza Vea, se detectaron diferencias significativas en productos cuyo precio no es por unidad estándar. Los casos más representativos fueron:

| Producto | unit_multiplier | Cálculo directo | Web | Diferencia |
|----------|-----------------|-----------------|-----|------------|
| Sandía | 3.0 kg | 0.69 x kg | 1.09 x kg | -0.40 |
| Pan Ciabatta | 0.09 kg | 7.70 x kg | 5.68 x kg | +2.02 |
| Colita de Cuadril | 1.1 kg | 32.34 x kg | 33.30 x kg | -0.96 |

### Análisis de causa raíz

Se probaron tres métodos de cálculo para identificar el correcto:

```python
# Método 1: Cálculo directo (asume descuento sobre precio por unidad)
precio_tarjeta_directo = precio_regular - mejor_descuento

# Método 2: Cálculo con ajuste por unit_multiplier (desc. sobre precio total)
precio_regular_total = precio_regular * unit_multiplier
precio_tarjeta_total = precio_regular_total - mejor_descuento
precio_tarjeta_ajustado = precio_tarjeta_total / unit_multiplier

# Método 3: Cálculo con porcentaje (alternativa)
descuento_pct = (mejor_descuento / precio_regular_total) * 100
precio_tarjeta_porcentaje = precio_regular * (1 - descuento_pct / 100)
```

La validación contra la web arrojó los siguientes resultados:

| Método | Sandía | Pan | Carne | Conclusión |
|--------|--------|-----|-------|------------|
| Directo | 0.69 | 7.70 | 32.34 | Incorrecto |
| Ajustado | 1.09 | 5.68 | 33.30 | Correcto |
| Porcentaje | 0.90 | 6.21 | 32.80 | Incorrecto |

### Solución implementada

Se adoptó la fórmula universal basada en precio total del paquete, validada con 9 productos de prueba:

```python
precio_tarjeta_sip = round(
    (precio_regular * unit_multiplier - mejor_descuento) / unit_multiplier, 2
)
```

### Código de validación

```python
import polars as pl

def validar_precios(productos_df, web_prices):
    resultados = []
    for row in productos_df.iter_rows():
        metodo_directo = row.precio_regular - row.mejor_descuento
        metodo_ajustado = (row.precio_regular * row.unit_multiplier - row.mejor_descuento) / row.unit_multiplier

        resultados.append({
            "producto": row.productname,
            "web": web_prices[row.productid],
            "directo": metodo_directo,
            "ajustado": metodo_ajustado,
            "coincide_ajustado": abs(metodo_ajustado - web_prices[row.productid]) < 0.01
        })
    return pl.DataFrame(resultados)
```

---

## 2. Duplicación de registros por múltiples secciones de búsqueda

### Hallazgo

La tabla de hechos presentaba una explosión de registros. Por ejemplo, el producto con `productid 23320` aparecía 633 veces en lugar de una sola. El análisis mostró que el mismo `productId` aparecía en diferentes archivos JSON.

### Diagnóstico inicial

```python
print("Productos duplicados en Fact (top 10):")
duplicados_fact = spark.sql("""
    SELECT p.productid, COUNT(*) as veces
    FROM gold_fact_precio_snapshot f
    JOIN gold_dim_producto p ON f.producto_key = p.producto_key
    GROUP BY p.productid
    HAVING COUNT(*) > 1
    ORDER BY veces DESC
    LIMIT 10
""")
duplicados_fact.show()
```

### Resultado del diagnóstico

| productid | veces | Causa |
|-----------|-------|-------|
| 100876460 | 633 | Mismo producto en bebidas y vinos |
| 101492909 | 2 | Mismo producto en limpieza y mascotas |

### Análisis de causa raíz

La documentación de VTEX aclara:
- El `categoryId` es la clave primaria de la categoría
- La jerarquía de categorías tiene tres niveles: departamento, categoría y subcategoría
- Las secciones de búsqueda son contextos de navegación, no atributos del producto

### Validación de la fuente de verdad

```python
df_categoria_test = silver_df.groupBy("categoryid").agg(
    F.countDistinct("categoria_principal").alias("variaciones")
)
df_categoria_test.filter(F.col("variaciones") > 1).show()
```

### Solución implementada

Se implementó deduplicación en Silver conservando la categoría más específica según la jerarquía de VTEX (`length(categoria_raw).desc()`), y registrando todas las secciones donde apareció el producto:

```python
window_spec = Window.partitionBy("productid", "ingesta_date").orderBy(
    F.length(F.col("categoria_raw")).desc()
)

df_silver = df_silver \
    .withColumn("secciones_origen",
        F.collect_set("seccion").over(
            Window.partitionBy("productid", "ingesta_date")
        )
    ) \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")
```

---

## 3. Corrección manual versus solución genérica

### Hallazgo

Inicialmente se implementó un parche manual:

```python
# Parche manual (descartado)
df_silver = df_silver.withColumn(
    "seccion",
    F.when(F.col("productid") == "23320", "vinos-licores-y-cervezas")
     .otherwise(F.col("seccion"))
)
```

Este enfoque resolvía el síntoma pero no la causa. Al escalar, se identificaron 159 productos adicionales con el mismo patrón.

### Solución implementada

Se reemplazó por una regla genérica basada en jerarquía (ver Hallazgo 2). El campo `secciones_origen` registra todas las secciones donde apareció el producto para trazabilidad completa.

---

## 4. Conteo de octógonos nutricionales con duplicados

### Hallazgo

Un producto presentaba `num_octogonos = 4`, un valor imposible según la Ley 30021 (máximo 3 octógonos).

### Diagnóstico

```python
spark.sql("""
    SELECT productid, productname, tipodeoctogono, size(tipodeoctogono) as num
    FROM silver_productos
    WHERE size(tipodeoctogono) > 3
""").show(truncate=False)
```

### Resultado

| productid | productname | tipodeoctogono | num |
|-----------|-------------|----------------|-----|
| 100050675 | Pan Hamburguesa | ["altoenazucar","altoenazucar","altoensodio","altoensodio"] | 4 |

### Solución implementada

```python
from pyspark.sql.functions import array_distinct

df_silver = df_silver \
    .withColumn("alto_sodio", array_contains(col("tipodeoctogono"), "altoensodio")) \
    .withColumn("alto_azucar", array_contains(col("tipodeoctogono"), "altoenazucar")) \
    .withColumn("alto_saturadas", array_contains(col("tipodeoctogono"), "altoengrasassaturadas")) \
    .withColumn("num_octogonos",
        when(col("tipodeoctogono").isNotNull(),
             size(array_distinct(col("tipodeoctogono"))))
        .otherwise(0)
    )
```

### Validación posterior

```sql
SELECT num_octogonos, COUNT(*) as cantidad
FROM silver_productos
GROUP BY num_octogonos
ORDER BY num_octogonos
```

| num_octogonos | cantidad |
|---------------|----------|
| 0 | 15,400 |
| 1 | 1,171 |
| 2 | 1,194 |
| 3 | 47 |

---

## 5. Inconsistencia en nombres de columnas entre archivos JSON

### Hallazgo

Spark lanzaba error `COLUMN_ALREADY_EXISTS` al leer múltiples JSONs. La causa era la existencia de columnas con nombres similares pero diferente capitalización o espacios:

- `Tipo de Producto`
- `Tipo De Producto`

### Diagnóstico con Polars

```python
import polars as pl

def inspeccionar_columnas_duplicadas(ruta_json):
    df = pl.read_json(ruta_json)
    columnas = df.columns
    normalizadas = [c.lower().replace(" ", "_") for c in columnas]
    duplicados = [c for c in set(normalizadas) if normalizadas.count(c) > 1]
    return duplicados

duplicados = inspeccionar_columnas_duplicadas("archivos/abarrotes.json")
print(f"Columnas conflictivas: {duplicados}")
```

### Solución implementada

```python
COLUMN_MAPPING = {
    "Tipo De Producto": "tipo_especifico_producto",
    "Tipo de Producto": "canal_venta",
}

def normalizar_key(nombre):
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = nombre.encode("ascii", "ignore").decode("ascii")
    nombre = re.sub(r"[^a-zA-Z0-9]", "_", nombre)
    return nombre.lower().strip("_")
```

---

## 6. Productos con disponibilidad inconsistente

### Hallazgo

360 productos tenían `precio_regular = 0` o nulo. Su análisis reveló que pertenecían a canales como SELLERCENTER (marketplace) o estaban descontinuados.

### Distribución final

```sql
SELECT
    CASE
        WHEN precio_regular IS NULL OR precio_regular <= 0 THEN 'Precio invalido'
        WHEN disponible = false THEN 'Producto no disponible'
        WHEN disponible = true  THEN 'Producto disponible'
    END as estado,
    COUNT(*) as cantidad
FROM silver_productos
GROUP BY estado
```

| Estado | Cantidad |
|--------|----------|
| Producto disponible | 15,672 |
| Producto no disponible | 1,780 |
| Precio invalido | 360 |

---

## 7. Filtro de productos promocionales (packs y combos)

### Hallazgo

Los productos tipo "pack" o "combo" estaban distorsionando los análisis de precios por categoría y generaban duplicación en los joins con `DIM_CATEGORIA`.

### Diagnóstico

```python
df_silver.filter(
    (F.col("categoria_principal") == "Packs") |
    (F.col("productname").rlike("(?i)pack|combo|promo"))
).count()
# Resultado: ~2,990 productos
```

### Causa raíz

Estos productos combinan elementos de diferentes categorías. Su precio no es comparable con productos individuales y son promociones temporales que no representan el precio regular del mercado.

### Solución implementada

```python
df_silver = df_silver.filter(
    ~F.col("categoria_principal").isin(["Packs"]) &
    ~F.col("productname").rlike("(?i)pack|combo|promo|llévate|lleva")
)
```

### Resultado

| | Registros |
|---|---|
| Antes del filtro | ~20,052 |
| Después del filtro | ~17,062 |
| Eliminados | -2,990 |

---

## 8. Manejo de arrays vacíos (get() vs [0])

### Hallazgo

Spark lanzaba error `INVALID_ARRAY_INDEX` al intentar acceder al índice 0 de arrays que podían estar vacíos (`tag_stock_tienda`, `tag_stock_cd`, `canal_venta`, `tipo_envio`).

### Diagnóstico

```python
# Código que fallaba
df_silver = df_silver.withColumn(
    "retira_hoy",
    when(col("tag_stock_tienda")[0] == "Sí", True).otherwise(False)
)
# Error: INVALID_ARRAY_INDEX — array vacío
```

### Causa raíz

La API de Plaza Vea no siempre devuelve arrays con elementos. Acceder con `[0]` a un array vacío causa excepción en Spark.

### Solución implementada

Se reemplazó el acceso directo con `get(col, 0)`, que devuelve `NULL` en lugar de lanzar error:

```python
from pyspark.sql.functions import get

df_silver = df_silver \
    .withColumn("retira_hoy",
        when(get(col("tag_stock_tienda"), 0) == "Sí", True).otherwise(False)) \
    .withColumn("llega_manana",
        when(get(col("tag_stock_cd"), 0) == "Sí", True).otherwise(False)) \
    .withColumn("canal_venta",
        upper(trim(get(col("canal_venta"), 0)))) \
    .withColumn("tipo_especifico_producto",
        upper(trim(get(col("tipo_especifico_producto"), 0))))
```

### Resultado

| | Comportamiento |
|---|---|
| Antes | Error en arrays vacíos |
| Después | `NULL` silencioso · manejado con `coalesce` |

---

## Nota metodológica

Cada hallazgo documenta: la discrepancia observada, el código de diagnóstico, el análisis de causa raíz y la solución implementada con su validación posterior.