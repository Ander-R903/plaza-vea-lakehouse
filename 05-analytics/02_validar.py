# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM plazavea_dev.control.batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ps.batch_id,
# MAGIC        COUNT(*)
# MAGIC FROM plazavea_dev.silver.silver_productos ps
# MAGIC GROUP BY ps.batch_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Borrar tablas Gold
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_fact_precio_snapshot;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_producto;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_marca;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_categoria;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_canal;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_etiquetado;
# MAGIC DROP TABLE IF EXISTS plazavea_dev.gold.gold_dim_fecha;
# MAGIC
# MAGIC -- Borrar tabla Silver
# MAGIC DROP TABLE IF EXISTS plazavea_dev.silver.silver_productos;
# MAGIC
# MAGIC -- Borrar tabla Bronze
# MAGIC DROP TABLE IF EXISTS plazavea_dev.bronze.bronze_productos;
# MAGIC
# MAGIC -- Limpiar tabla de control (solo datos, no la tabla)
# MAGIC TRUNCATE TABLE plazavea_dev.control.batch_control;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver schemas
# MAGIC SHOW TABLES IN plazavea_dev.bronze;
# MAGIC SHOW TABLES IN plazavea_dev.silver;
# MAGIC SHOW TABLES IN plazavea_dev.gold;
# MAGIC
# MAGIC -- Ver control table vacia
# MAGIC SELECT * FROM plazavea_dev.control.batch_control;

# COMMAND ----------

from pyspark.sql import functions as F

print("=" * 60)
print("DEBUG SILVER - PLAZA VEA")
print("=" * 60)

# 1. Ver batch_ids en Silver
print("\n1. BATCHES EN SILVER")
print("-" * 40)
silver_batches = spark.sql("""
    SELECT batch_id, COUNT(*) as total, 
           MIN(created_timestamp) as min_created,
           MAX(updated_timestamp) as max_updated
    FROM plazavea_dev.silver.silver_productos
    GROUP BY batch_id
    ORDER BY batch_id
""")
silver_batches.show(truncate=False)

# 2. Verificar duplicados
print("\n2. VERIFICAR DUPLICADOS (productid + batch_id)")
print("-" * 40)
duplicados = spark.sql("""
    SELECT batch_id, productid, COUNT(*) as veces
    FROM plazavea_dev.silver.silver_productos
    GROUP BY batch_id, productid
    HAVING COUNT(*) > 1
""")
if duplicados.count() == 0:
    print("✅ No hay duplicados en Silver")
else:
    print("❌ Duplicados encontrados:")
    duplicados.show()

# 3. Verificar nulos en campos criticos
print("\n3. VERIFICAR NULOS EN CAMPOS CRITICOS")
print("-" * 40)
critical_cols = ["productid", "batch_id", "precio_regular", "precio_online"]
for col in critical_cols:
    null_count = spark.sql(f"""
        SELECT COUNT(*) as nulls
        FROM plazavea_dev.silver.silver_productos
        WHERE {col} IS NULL
    """).collect()[0][0]
    print(f"  {col}: {null_count} nulos")

# 4. Ver rangos de precios por batch
print("\n4. PRECIOS POR BATCH")
print("-" * 40)
spark.sql("""
    SELECT 
        batch_id,
        COUNT(*) as total,
        ROUND(MIN(precio_regular), 2) as min_precio,
        ROUND(MAX(precio_regular), 2) as max_precio,
        ROUND(AVG(precio_regular), 2) as avg_precio
    FROM plazavea_dev.silver.silver_productos
    WHERE precio_valido = true
    GROUP BY batch_id
    ORDER BY batch_id
""").show()

# 5. Ver un producto especifico en ambos batches (Alimentador)
print("\n5. PRODUCTO: ALIMENTADOR MASCOTAS (SKU: 12136386)")
print("-" * 40)
spark.sql("""
    SELECT 
        batch_id,
        productname,
        precio_online,
        disponible,
        created_timestamp,
        updated_timestamp
    FROM plazavea_dev.silver.silver_productos
    WHERE sku = '12136386'
    ORDER BY batch_id
""").show(truncate=False)

# 6. Resumen final
print("\n" + "=" * 60)
print("RESUMEN FINAL")
print("=" * 60)
total_silver = spark.table("plazavea_dev.silver.silver_productos").count()
print(f"Total registros en Silver: {total_silver}")

print("\n✅ DEBUG COMPLETADO")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total
# MAGIC FROM plazavea_dev.silver.silver_productos
# MAGIC WHERE batch_id = '2026-05-29';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM plazavea_dev.control.batch_control 
# MAGIC WHERE batch_id = '2026-05-29';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ps.batch_id,
# MAGIC        COUNT(*)
# MAGIC FROM  plazavea_dev.silver.silver_productos ps
# MAGIC GROUP BY ps.batch_id

# COMMAND ----------

from pyspark.sql import functions as F

print("=" * 60)
print("DEBUG - POR QUE 2026-05-29 NO ESTA EN SILVER")
print("=" * 60)

# 1. Verificar si Bronze tiene los datos
print("\n1. BRONZE - Datos para 2026-05-29")
bronze_count = spark.sql("""
    SELECT COUNT(*) as total
    FROM plazavea_dev.bronze.bronze_productos
    WHERE batch_id = '2026-05-29'
""").collect()[0][0]
print(f"Registros en Bronze para 2026-05-29: {bronze_count}")

# 2. Verificar si el job hijo se ejecuto correctamente
print("\n2. VERIFICAR EJECUCION DEL JOB HIJO")
print("-" * 40)
# Ver logs del job (esto lo ves en la UI de Jobs)

# 3. Verificar si hay error en la tabla de control
print("\n3. TABLA DE CONTROL")
spark.sql("""
    SELECT batch_id, status, start_time, end_time
    FROM plazavea_dev.control.batch_control
    WHERE batch_id = '2026-05-29'
""").show(truncate=False)

# 4. Verificar si Silver tiene datos para 2026-05-29 (incluyendo nulls)
print("\n4. SILVER - Buscar cualquier registro con batch_id = 2026-05-29")
silver_check = spark.sql("""
    SELECT COUNT(*) as total
    FROM plazavea_dev.silver.silver_productos
    WHERE batch_id = '2026-05-29'
""").collect()[0][0]
print(f"Registros en Silver para 2026-05-29: {silver_check}")

# 5. Verificar si el MERGE en Silver fallo silenciosamente
print("\n5. VERIFICAR POSIBLES ERRORES EN SILVER")
print("-" * 40)
# Ver si hay productos que deberian estar pero no
spark.sql("""
    SELECT b.productid, b.productname
    FROM plazavea_dev.bronze.bronze_productos b
    LEFT JOIN plazavea_dev.silver.silver_productos s 
        ON b.productid = s.productid AND s.batch_id = '2026-05-29'
    WHERE b.batch_id = '2026-05-29'
      AND s.productid IS NULL
    LIMIT 5
""").show(truncate=False)

# 6. Ver el estado actual de Silver
print("\n6. SILVER - Todos los batches actuales")
spark.sql("""
    SELECT batch_id, COUNT(*) as total
    FROM plazavea_dev.silver.silver_productos
    GROUP BY batch_id
    ORDER BY batch_id
""").show()

print("\n" + "=" * 60)
print("RECOMENDACION:")
print("-" * 40)
if bronze_count > 0 and silver_check == 0:
    print("❌ Bronze tiene datos pero Silver no. El job hijo fallo para 2026-05-29.")
    print("   Revisa los logs del job hijo en la UI de Databricks Jobs.")
elif bronze_count == 0:
    print("❌ Bronze no tiene datos para 2026-05-29.")
    print("   La particion no se creo correctamente.")
else:
    print("✅ Todo parece correcto. Verifica la consulta SQL.")
print("=" * 60)

# COMMAND ----------

# MAGIC %fs ls /Volumes/plazavea_dev/landing/files/batch_id=2026-05-29/

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM plazavea_dev.control.batch_control 
# MAGIC WHERE batch_id = '2026-05-29';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PS.batch_id,
# MAGIC     COUNT(ps.batch_id)
# MAGIC FROM plazavea_dev.silver.silver_productos ps
# MAGIC GROUP BY ps.batch_id

# COMMAND ----------

from pyspark.sql import functions as F

print("=" * 70)
print("DEBUG COMPLETO - PRODUCTOS CON CAMBIO DE PRECIO")
print("=" * 70)

# 1. Comparar ambos batches
print("\n1. PRODUCTOS QUE CAMBIARON DE PRECIO (29/05 vs 08/06)")
print("-" * 50)

cambios = spark.sql("""
    SELECT 
        s29.productid,
        s29.productname,
        s29.sku,
        s29.brand,
        ROUND(s29.precio_online, 2) as precio_29_mayo,
        ROUND(s08.precio_online, 2) as precio_08_junio,
        ROUND(s08.precio_online - s29.precio_online, 2) as diferencia,
        ROUND((s08.precio_online - s29.precio_online) * 100.0 / s29.precio_online, 2) as variacion_pct,
        CASE 
            WHEN s08.precio_online > s29.precio_online THEN 'SUBIO'
            WHEN s08.precio_online < s29.precio_online THEN 'BAJO'
            ELSE 'IGUAL'
        END as tendencia,
        s29.disponible as disponible_29,
        s08.disponible as disponible_08
    FROM plazavea_dev.silver.silver_productos s29
    INNER JOIN plazavea_dev.silver.silver_productos s08
        ON s29.productid = s08.productid
    WHERE s29.batch_id = '2026-05-29'
      AND s08.batch_id = '2026-06-08'
      AND s29.precio_valido = true
      AND s08.precio_valido = true
      AND s29.precio_online != s08.precio_online
    ORDER BY ABS(s08.precio_online - s29.precio_online) DESC
    LIMIT 5
""")

cambios.show(truncate=False)

# 2. Mostrar cambios mas significativos
print("\n2. TOP 5 MAYORES SUBIDAS DE PRECIO")
print("-" * 50)
spark.sql("""
    SELECT 
        productname,
        sku,
        ROUND(precio_29_mayo, 2) as precio_29,
        ROUND(precio_08_junio, 2) as precio_08,
        ROUND(variacion_pct, 2) as subida_pct
    FROM (
        SELECT 
            s29.productname,
            s29.sku,
            s29.precio_online as precio_29_mayo,
            s08.precio_online as precio_08_junio,
            (s08.precio_online - s29.precio_online) * 100.0 / s29.precio_online as variacion_pct
        FROM plazavea_dev.silver.silver_productos s29
        INNER JOIN plazavea_dev.silver.silver_productos s08
            ON s29.productid = s08.productid
        WHERE s29.batch_id = '2026-05-29'
          AND s08.batch_id = '2026-06-08'
          AND s29.precio_valido = true
          AND s08.precio_valido = true
          AND s08.precio_online > s29.precio_online
    )
    ORDER BY subida_pct DESC
    LIMIT 5
""").show(truncate=False)

# 3. Top 5 mayores bajadas
print("\n3. TOP 5 MAYORES BAJADAS DE PRECIO")
print("-" * 50)
spark.sql("""
    SELECT 
        productname,
        sku,
        ROUND(precio_29_mayo, 2) as precio_29,
        ROUND(precio_08_junio, 2) as precio_08,
        ROUND(variacion_pct, 2) as bajada_pct
    FROM (
        SELECT 
            s29.productname,
            s29.sku,
            s29.precio_online as precio_29_mayo,
            s08.precio_online as precio_08_junio,
            (s08.precio_online - s29.precio_online) * 100.0 / s29.precio_online as variacion_pct
        FROM plazavea_dev.silver.silver_productos s29
        INNER JOIN plazavea_dev.silver.silver_productos s08
            ON s29.productid = s08.productid
        WHERE s29.batch_id = '2026-05-29'
          AND s08.batch_id = '2026-06-08'
          AND s29.precio_valido = true
          AND s08.precio_valido = true
          AND s08.precio_online < s29.precio_online
    )
    ORDER BY bajada_pct ASC
    LIMIT 5
""").show(truncate=False)

# 4. Productos que volvieron a stock
print("\n4. PRODUCTOS QUE VOLVIERON A STOCK (agotados 29/05, disponibles 08/06)")
print("-" * 50)
spark.sql("""
    SELECT 
        s29.productname,
        s29.sku,
        s29.brand,
        ROUND(s29.precio_online, 2) as precio,
        s29.disponible as disponible_29,
        s08.disponible as disponible_08
    FROM plazavea_dev.silver.silver_productos s29
    INNER JOIN plazavea_dev.silver.silver_productos s08
        ON s29.productid = s08.productid
    WHERE s29.batch_id = '2026-05-29'
      AND s08.batch_id = '2026-06-08'
      AND s29.disponible = false
      AND s08.disponible = true
      AND s29.precio_valido = true
    LIMIT 5
""").show(truncate=False)

# 5. Resumen estadistico
print("\n5. RESUMEN ESTADISTICO DE CAMBIOS")
print("-" * 50)

stats = spark.sql("""
    SELECT 
        COUNT(*) as total_productos_comunes,
        SUM(CASE WHEN s29.precio_online != s08.precio_online THEN 1 ELSE 0 END) as productos_con_cambio,
        SUM(CASE WHEN s08.precio_online > s29.precio_online THEN 1 ELSE 0 END) as productos_subieron,
        SUM(CASE WHEN s08.precio_online < s29.precio_online THEN 1 ELSE 0 END) as productos_bajaron,
        ROUND(AVG(s08.precio_online - s29.precio_online), 2) as diferencia_promedio
    FROM plazavea_dev.silver.silver_productos s29
    INNER JOIN plazavea_dev.silver.silver_productos s08
        ON s29.productid = s08.productid
    WHERE s29.batch_id = '2026-05-29'
      AND s08.batch_id = '2026-06-08'
      AND s29.precio_valido = true
      AND s08.precio_valido = true
""")

stats.show()

print("\n" + "=" * 70)
print("✅ DEBUG COMPLETADO")
print("=" * 70)

# COMMAND ----------

