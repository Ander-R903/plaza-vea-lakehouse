# Databricks notebook source
# MAGIC %md
# MAGIC # Pricing Analytics — Plaza Vea
# MAGIC
# MAGIC Análisis de precios sobre la capa Gold del Lakehouse.
# MAGIC Fuente principal: `plazavea_dev.gold.gold_fact_precio_snapshot` con joins a dimensiones.
# MAGIC Silver se usa únicamente donde Gold no tiene la columna requerida.
# MAGIC
# MAGIC Modelo: Star Schema — Periodic Snapshot — 7 batches diarios, ~17k productos/día.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. KPIs del último batch
# MAGIC
# MAGIC Solo el 3.1% del catálogo tiene descuento tarjeta SIP activo y el precio promedio
# MAGIC general es S/ 66.58. El 11.8% de productos no tiene stock disponible en el último batch.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ultimo_batch AS (
# MAGIC     SELECT MAX(df.fecha) AS fecha, MAX(df.fecha_key) AS fecha_key
# MAGIC     FROM plazavea_dev.gold.gold_dim_fecha df
# MAGIC ),
# MAGIC base AS (
# MAGIC     SELECT f.*
# MAGIC     FROM plazavea_dev.gold.gold_fact_precio_snapshot f
# MAGIC     INNER JOIN ultimo_batch u 
# MAGIC         ON f.fecha_key = u.fecha_key
# MAGIC )
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_productos,
# MAGIC     SUM(CASE WHEN disponible = true  THEN 1 ELSE 0 END) AS disponibles,
# MAGIC     SUM(CASE WHEN disponible = false THEN 1 ELSE 0 END) AS sin_stock,
# MAGIC     SUM(CASE WHEN en_promocion = true THEN 1 ELSE 0 END) AS con_sip,
# MAGIC     ROUND(AVG(CASE WHEN precio_valido = true
# MAGIC               THEN precio_regular END), 2) AS precio_promedio,
# MAGIC     ROUND(AVG(CASE WHEN en_promocion = true
# MAGIC               THEN descuento_tarjeta_sip_pct END), 1) AS descuento_sip_promedio
# MAGIC FROM base

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Evolución de precio promedio por categoría (Top 5)
# MAGIC
# MAGIC Bebé e Infantil es la categoría más cara con S/ 207 a S/ 230 en el período,
# MAGIC mostrando un pico notable el 11 de junio. Vinos, Mascotas y Limpieza se mantienen
# MAGIC estables en el rango S/ 85 a S/ 92. Cuidado Personal es la más estable del grupo.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         df.fecha,
# MAGIC         dc.categoria_principal,
# MAGIC         f.precio_regular
# MAGIC     FROM plazavea_dev.gold.gold_fact_precio_snapshot f
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_fecha df  
# MAGIC             ON f.fecha_key = df.fecha_key
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_categoria dc  
# MAGIC             ON f.categoria_key = dc.categoria_key
# MAGIC     WHERE f.precio_valido = true
# MAGIC       AND dc.categoria_principal IS NOT NULL
# MAGIC ),
# MAGIC top5 AS (
# MAGIC     SELECT categoria_principal
# MAGIC     FROM base
# MAGIC     GROUP BY categoria_principal
# MAGIC     ORDER BY AVG(precio_regular) DESC
# MAGIC     LIMIT 5
# MAGIC )
# MAGIC SELECT
# MAGIC     b.fecha,
# MAGIC     b.categoria_principal,
# MAGIC     ROUND(AVG(b.precio_regular), 2) AS precio_promedio
# MAGIC FROM base b
# MAGIC INNER JOIN top5 t ON b.categoria_principal = t.categoria_principal
# MAGIC GROUP BY b.fecha, b.categoria_principal
# MAGIC ORDER BY b.fecha, precio_promedio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Mayores movimientos de precio entre batches
# MAGIC
# MAGIC El Wafer NIK registró una variación de +731% en un batch, consistente con un error
# MAGIC temporal de precio en la API de Plaza Vea que el pipeline capturó fielmente.
# MAGIC Las bebidas SILK bajaron ~70%, correspondiente a una promoción real verificada en web.
# MAGIC La Papa Blanca Yungay subió 45%, alza estacional confirmada.
# MAGIC
# MAGIC Nota: esta query usa Silver porque Gold no conserva productname en la fact.
# MAGIC El join a gold_dim_producto provee el nombre del producto.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH precios_con_lag AS (
# MAGIC     SELECT
# MAGIC         f.precio_regular,
# MAGIC         f.precio_valido,
# MAGIC         df.fecha,
# MAGIC         dp.productid,
# MAGIC         dp.productname,
# MAGIC         dc.categoria_principal,
# MAGIC         LAG(f.precio_regular) OVER (
# MAGIC             PARTITION BY f.producto_key ORDER BY df.fecha
# MAGIC         ) AS precio_anterior
# MAGIC     FROM plazavea_dev.gold.gold_fact_precio_snapshot f
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_fecha df  
# MAGIC         ON f.fecha_key     = df.fecha_key
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_producto dp  
# MAGIC         ON f.producto_key  = dp.producto_key
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_categoria dc  
# MAGIC         ON f.categoria_key = dc.categoria_key
# MAGIC     WHERE f.precio_valido = true
# MAGIC ),
# MAGIC cambios AS (
# MAGIC     SELECT
# MAGIC         productid,
# MAGIC         productname,
# MAGIC         categoria_principal,
# MAGIC         fecha,
# MAGIC         precio_anterior,
# MAGIC         precio_regular AS precio_actual,
# MAGIC         ROUND(precio_regular - precio_anterior, 2) AS variacion,
# MAGIC         ROUND((precio_regular - precio_anterior)
# MAGIC               / precio_anterior * 100, 1) AS variacion_pct
# MAGIC     FROM precios_con_lag
# MAGIC     WHERE precio_anterior IS NOT NULL
# MAGIC       AND precio_regular != precio_anterior
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM cambios
# MAGIC ORDER BY ABS(variacion_pct) DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Uso de descuento tarjeta SIP por categoría
# MAGIC
# MAGIC Vinos, licores y cervezas concentra el mayor uso de la tarjeta SIP con 19.7%
# MAGIC de productos en promoción y un descuento promedio de 26.3%. Mascotas, Mercado
# MAGIC Saludable, Desayunos y Pollo Rostizado no registran ningún producto con SIP activo.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ultimo_batch AS (
# MAGIC     SELECT MAX(fecha_key) AS fecha_key
# MAGIC     FROM plazavea_dev.gold.gold_dim_fecha
# MAGIC ),
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         dc.categoria_principal,
# MAGIC         f.en_promocion,
# MAGIC         f.descuento_tarjeta_sip_pct
# MAGIC     FROM plazavea_dev.gold.gold_fact_precio_snapshot f
# MAGIC     INNER JOIN ultimo_batch u 
# MAGIC         ON f.fecha_key     = u.fecha_key
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_categoria dc 
# MAGIC         ON f.categoria_key = dc.categoria_key
# MAGIC     WHERE dc.categoria_principal IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     categoria_principal,
# MAGIC     COUNT(*) AS total_productos,
# MAGIC     SUM(CASE WHEN en_promocion = true THEN 1 ELSE 0 END) AS con_sip,
# MAGIC     ROUND(SUM(CASE WHEN en_promocion = true THEN 1 ELSE 0 END)
# MAGIC           * 100.0 / COUNT(*), 1) AS pct_con_sip,
# MAGIC     ROUND(AVG(CASE WHEN en_promocion = true
# MAGIC               THEN descuento_tarjeta_sip_pct END), 1) AS descuento_sip_promedio
# MAGIC FROM base
# MAGIC GROUP BY categoria_principal
# MAGIC ORDER BY pct_con_sip DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Disponibilidad de stock por categoría
# MAGIC
# MAGIC Pollo Rostizado y Comidas Preparadas tiene el mayor quiebre de stock con 49.5%
# MAGIC de productos no disponibles, seguido de Carnes con 44.8%. Limpieza, Mascotas
# MAGIC y Cuidado Personal mantienen disponibilidad superior al 99%.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ultimo_batch AS (
# MAGIC     SELECT MAX(fecha_key) AS fecha_key
# MAGIC     FROM plazavea_dev.gold.gold_dim_fecha
# MAGIC ),
# MAGIC base AS (
# MAGIC     SELECT
# MAGIC         dc.categoria_principal,
# MAGIC         f.disponible
# MAGIC     FROM plazavea_dev.gold.gold_fact_precio_snapshot f
# MAGIC     INNER JOIN ultimo_batch u 
# MAGIC         ON f.fecha_key     = u.fecha_key
# MAGIC     INNER JOIN plazavea_dev.gold.gold_dim_categoria dc 
# MAGIC         ON f.categoria_key = dc.categoria_key
# MAGIC     WHERE dc.categoria_principal IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     categoria_principal,
# MAGIC     COUNT(*) AS total,
# MAGIC     SUM(CASE WHEN disponible = true  THEN 1 ELSE 0 END) AS disponibles,
# MAGIC     SUM(CASE WHEN disponible = false THEN 1 ELSE 0 END)AS sin_stock,
# MAGIC     ROUND(SUM(CASE WHEN disponible = false THEN 1 ELSE 0 END)
# MAGIC           * 100.0 / COUNT(*), 1) AS pct_sin_stock
# MAGIC FROM base
# MAGIC GROUP BY categoria_principal
# MAGIC ORDER BY pct_sin_stock DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Rotación de catálogo entre batches
# MAGIC
# MAGIC Plaza Vea rota aproximadamente 2.2% de su catálogo diariamente. En el último batch
# MAGIC ingresaron 376 productos nuevos y salieron 387, afectando 11 categorías en ambos casos.
# MAGIC La salida masiva de chicles TRIDENT en un solo batch sugiere un retiro de línea,
# MAGIC no una descontinuación gradual.
# MAGIC
# MAGIC Nota: esta query usa Silver porque la fact no permite detectar ausencias entre batches
# MAGIC al no tener un registro explícito de productos que desaparecen.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ultimo_batch AS (
# MAGIC     SELECT MAX(ingesta_date) AS fecha
# MAGIC     FROM plazavea_dev.silver.silver_productos
# MAGIC ),
# MAGIC batch_anterior AS (
# MAGIC     SELECT MAX(ingesta_date) AS fecha
# MAGIC     FROM plazavea_dev.silver.silver_productos
# MAGIC     WHERE ingesta_date < (
# MAGIC         SELECT MAX(ingesta_date)
# MAGIC         FROM plazavea_dev.silver.silver_productos
# MAGIC     )
# MAGIC ),
# MAGIC nuevos AS (
# MAGIC     SELECT 'Nuevo' AS tipo, s.productid, s.productname, s.categoria_principal, s.precio_regular
# MAGIC     FROM plazavea_dev.silver.silver_productos s
# MAGIC     INNER JOIN ultimo_batch u 
# MAGIC         ON s.ingesta_date = u.fecha
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM plazavea_dev.silver.silver_productos s2
# MAGIC         INNER JOIN batch_anterior b 
# MAGIC             ON s2.ingesta_date = b.fecha
# MAGIC         WHERE s2.productid = s.productid
# MAGIC     )
# MAGIC ),
# MAGIC descontinuados AS (
# MAGIC     SELECT 'Descontinuado' AS tipo, s.productid, s.productname, s.categoria_principal, s.precio_regular
# MAGIC     FROM plazavea_dev.silver.silver_productos s
# MAGIC     INNER JOIN batch_anterior b 
# MAGIC         ON s.ingesta_date = b.fecha
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM plazavea_dev.silver.silver_productos s2
# MAGIC         INNER JOIN ultimo_batch u 
# MAGIC             ON s2.ingesta_date = u.fecha
# MAGIC         WHERE s2.productid = s.productid
# MAGIC     )
# MAGIC )
# MAGIC SELECT tipo, COUNT(*) AS total, COUNT(DISTINCT categoria_principal) AS categorias_afectadas
# MAGIC FROM (SELECT * FROM nuevos UNION ALL SELECT * FROM descontinuados)
# MAGIC GROUP BY tipo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusiones
# MAGIC
# MAGIC El catálogo de Plaza Vea muestra una dinámica de precios moderadamente estable
# MAGIC con excepciones puntuales. El uso de la tarjeta SIP está concentrado en bebidas
# MAGIC alcohólicas y perecibles, mientras que categorías de cuidado personal y mascotas
# MAGIC no acceden a ese beneficio. Los quiebres de stock más altos se concentran en
# MAGIC perecibles frescos, lo que es consistente con la naturaleza del producto.
# MAGIC La rotación diaria del catálogo (~2.2%) indica una gestión activa de surtido
# MAGIC por parte de Plaza Vea, con entradas y salidas que superan los 350 productos por día.