# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Plaza Vea Pricing
# MAGIC
# MAGIC **Procesamiento incremental con MERGE**
# MAGIC
# MAGIC - Fuente:  plazavea_dev.bronze.bronze_productos
# MAGIC - Destino: plazavea_dev.silver.silver_productos
# MAGIC - Filtra por batch_id para procesar solo datos nuevos
# MAGIC - MERGE por productid + ingesta_date
# MAGIC - created_timestamp NO se actualiza en UPDATE
# MAGIC - updated_timestamp SI se actualiza en UPDATE
# MAGIC - Productos sin precio se conservan para analisis de stock y disponibilidad

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration

# COMMAND ----------

# MAGIC %run "../00-common/01-environment-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Receive batch_id parameter

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")

# COMMAND ----------


v_batch_id = dbutils.widgets.get("p_batch_id")

if not v_batch_id:
    import datetime
    v_batch_id = datetime.datetime.now().strftime("%Y-%m-%d")

print(f"Procesando batch_id: {v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Read from Bronze (only current batch)

# COMMAND ----------

TABLA_BRONZE = get_bronze_table()
TABLA_SILVER = get_silver_table()

df_bronze = spark.table(TABLA_BRONZE) \
    .filter(F.col("batch_id") == v_batch_id)

print(f"Filas en Bronze para batch_id={v_batch_id}: {df_bronze.count()}")

if df_bronze.count() == 0:
    raise Exception(f"No hay datos en Bronze para batch_id={v_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Select needed columns

# COMMAND ----------

cols = [
    "productid", "productreferencecode", "productname",
    "brand", "brandid", "categoryid", "categories", "items",
    "tipodeoctogono", "canal_venta", "tipo_especifico_producto",
    "tag_stock_tienda", "tag_stock_cd", "tipo_de_envio",
    "vendido_por", "releasedate", "seccion", "ingesta_date", "batch_id"
]

df_silver = df_bronze.select(cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Extract nested fields

# COMMAND ----------

df_silver = df_silver \
    .withColumn("item",   F.get(col("items"), 0)) \
    .withColumn("seller", F.get(col("item.sellers"), 0)) \
    .withColumn("offer",  col("seller.commertialoffer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Delivery flags

# COMMAND ----------

df_silver = df_silver \
    .withColumn("retira_hoy",
        F.when(F.get(col("tag_stock_tienda"), 0) == "Si", True).otherwise(False)) \
    .withColumn("llega_manana",
        F.when(F.get(col("tag_stock_cd"), 0) == "Si", True).otherwise(False)) \
    .withColumn("envio_domicilio",
        F.array_contains(col("tipo_de_envio"), "Despacho a domicilio")) \
    .withColumn("recojo_tienda",
        F.array_contains(col("tipo_de_envio"), "Recojo en tienda"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Calculate prices
# MAGIC
# MAGIC El descuento siempre se aplica sobre el precio total del paquete.
# MAGIC Formula universal: (precio_regular * unit_multiplier - mejor_descuento) / unit_multiplier
# MAGIC Validada con productos de distintos unit_multiplier.
# MAGIC Los porcentajes se truncan con floor para coincidir con la web de Plaza Vea.

# COMMAND ----------

df_silver = df_silver.withColumns({
    "precio_regular":      col("offer.listprice").cast("double"),
    "precio_online":       col("offer.price").cast("double"),
    "unit_multiplier":     col("item.unitmultiplier").cast("double"),
    "disponible":          col("offer.isavailable").cast("boolean"),
    "cantidad_disponible": col("offer.availablequantity").cast("integer"),
})

df_silver = df_silver.withColumns({
    "full_price": col("precio_regular") * col("unit_multiplier"),

    "descuentos_array": F.when(
        F.size(col("offer.promotionteasers")) > 0,
        F.transform(
            col("offer.promotionteasers"),
            lambda t: F.get(
                F.filter(
                    t["effects"]["parameters"],
                    lambda p: p["name"] == "PromotionalPriceTableItemsDiscount"
                ), 0
            )["value"].cast("double")
        )
    ),
})

df_silver = df_silver.withColumns({
    "mejor_descuento": F.array_max(col("descuentos_array")),
})

df_silver = df_silver.withColumns({
    "precio_tarjeta_sip": F.when(
        col("mejor_descuento").isNotNull(),
        F.round(
            (col("precio_regular") * col("unit_multiplier") - col("mejor_descuento"))
            / col("unit_multiplier"), 2
        )
    ),
    "descuento_base_pct": F.when(
        col("precio_regular") > 0,
        F.floor((col("precio_regular") - col("precio_online")) / col("precio_regular") * 100)
    ),
})

df_silver = df_silver.withColumns({
    "descuento_tarjeta_sip_pct": F.when(
        col("mejor_descuento").isNotNull(),
        F.floor(col("mejor_descuento") / (col("precio_regular") * col("unit_multiplier")) * 100)
    ),
    "en_promocion":        col("precio_tarjeta_sip").isNotNull(),
    "precio_promo_exacto": col("unit_multiplier") == 1.0,
    "precio_valido":       F.when(
        col("precio_regular").isNotNull() & (col("precio_regular") > 0), True
    ).otherwise(False),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Extract nutritional octagons
# MAGIC
# MAGIC Plaza Vea duplica valores en tipodeoctogono.
# MAGIC array_distinct elimina duplicados antes de contar.

# COMMAND ----------

df_silver = df_silver \
    .withColumn("alto_sodio",
        F.array_contains(col("tipodeoctogono"), "altoensodio")) \
    .withColumn("alto_azucar",
        F.array_contains(col("tipodeoctogono"), "altoenazucar")) \
    .withColumn("alto_saturadas",
        F.array_contains(col("tipodeoctogono"), "altoengrasassaturadas")) \
    .withColumn("num_octogonos",
        F.when(col("tipodeoctogono").isNotNull(),
               F.size(F.array_distinct(col("tipodeoctogono"))))
        .otherwise(0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Extract seller and SKU

# COMMAND ----------

df_silver = df_silver \
    .withColumn("vendedor",
        F.coalesce(
            F.when(~F.trim(F.get(col("vendido_por"), 0)).isin("marcas aliadas", "", "none"),
                   F.trim(F.get(col("vendido_por"), 0))),
            col("seller.sellername")
        )
    ) \
    .withColumn("sku",
        F.coalesce(
            F.when(col("productreferencecode").isNotNull() &
                   ~col("productreferencecode").isin("", "-"),
                   col("productreferencecode")),
            col("item.itemid")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Split categories

# COMMAND ----------

df_silver = df_silver \
    .withColumn("categoria_raw",       F.get(col("categories"), 0)) \
    .withColumn("niveles",             F.split(col("categoria_raw"), "/")) \
    .withColumn("categoria_principal", F.get(col("niveles"), 1)) \
    .withColumn("subcategoria",        F.get(col("niveles"), 2)) \
    .withColumn("grupo_producto",      F.get(col("niveles"), 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Exclude promotional products

# COMMAND ----------

df_silver = df_silver.filter(
    ~col("categoria_principal").isin(["Packs"]) &
    ~col("productname").rlike("(?i)pack|combo|promo|llévate|lleva")
)
print(f"Despues de filtro packs: {df_silver.count()} productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Normalize text fields

# COMMAND ----------

df_silver = df_silver \
    .withColumn("canal_venta",
        F.coalesce(F.upper(F.trim(F.get(col("canal_venta"), 0))), F.lit("SUPERMERCADO"))) \
    .withColumn("tipo_especifico_producto",
        F.upper(F.trim(F.get(col("tipo_especifico_producto"), 0)))) \
    .withColumn("productname", F.trim(col("productname"))) \
    .withColumn("brand",       F.trim(col("brand")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Filter valid channels

# COMMAND ----------

df_silver = df_silver.filter(
    col("canal_venta").isin(["SUPERMERCADO", "SELLERCENTER"]) |
    col("canal_venta").isNull()
)
print(f"Despues de filtro canal: {df_silver.count()} productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. Consolidate sections and deduplicate
# MAGIC
# MAGIC Un producto puede aparecer en multiples secciones porque Plaza Vea
# MAGIC lo indexa en varias busquedas. El categoryId de VTEX es la fuente
# MAGIC de verdad. Se conserva la aparicion con la categoria mas especifica.
# MAGIC secciones_origen registra todas las secciones donde aparecio.

# COMMAND ----------

w_secciones = Window.partitionBy("productid", "ingesta_date")
w_dedup     = Window.partitionBy("productid", "ingesta_date") \
                    .orderBy(F.length(col("categoria_raw")).desc())

df_silver = df_silver \
    .withColumn("secciones_origen",
        F.collect_set("seccion").over(w_secciones)) \
    .withColumn("rn", F.row_number().over(w_dedup)) \
    .filter(col("rn") == 1) \
    .drop("rn", "categoria_raw", "niveles")

print(f"Despues de deduplicar: {df_silver.count()} productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16. Normalize release date

# COMMAND ----------

df_silver = df_silver \
    .withColumn("fecha_lanzamiento", F.to_date(col("releasedate")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 17. Select final columns

# COMMAND ----------

columnas_finales = [
    "productid", "sku", "productname", "brand", "brandid", "categoryid",
    "categoria_principal", "subcategoria", "grupo_producto",
    "canal_venta", "tipo_especifico_producto",
    "precio_regular", "precio_online", "precio_tarjeta_sip", "full_price",
    "descuento_base_pct", "descuento_tarjeta_sip_pct",
    "en_promocion", "precio_promo_exacto", "precio_valido",
    "disponible", "cantidad_disponible", "unit_multiplier",
    "vendedor", "retira_hoy", "llega_manana", "envio_domicilio", "recojo_tienda",
    "alto_sodio", "alto_azucar", "alto_saturadas", "num_octogonos",
    "fecha_lanzamiento", "secciones_origen", "ingesta_date", "batch_id"
]

df_silver = df_silver.select(columnas_finales)
print(f"Columnas finales: {len(df_silver.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 18. Add audit columns

# COMMAND ----------

df_silver_final = df_silver \
    .withColumn("created_timestamp", F.current_timestamp()) \
    .withColumn("updated_timestamp", F.current_timestamp())

total = df_silver_final.count()
print(f"Registros listos para MERGE: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19. Write to Silver with MERGE
# MAGIC
# MAGIC MERGE por productid + ingesta_date.
# MAGIC created_timestamp no se actualiza en UPDATE.
# MAGIC updated_timestamp si se actualiza en UPDATE.
# MAGIC La guardia batch_id >= t.batch_id evita que un reprocesamiento
# MAGIC antiguo sobreescriba datos mas recientes.

# COMMAND ----------

def write_to_silver(input_df, target_table, columns_to_update):
    if not spark.catalog.tableExists(target_table):
        input_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        print(f"Tabla {target_table} creada con {input_df.count()} registros")
        return

    delta_table = DeltaTable.forName(spark, target_table)

    update_map = {c: f"s.{c}" for c in columns_to_update}
    update_map["updated_timestamp"] = "s.updated_timestamp"

    delta_table.alias("t") \
        .merge(
            input_df.alias("s"),
            "t.productid = s.productid AND t.ingesta_date = s.ingesta_date"
        ) \
        .whenMatchedUpdate(
            condition="s.batch_id >= t.batch_id",
            set=update_map
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

    print(f"MERGE completado en {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20. Execute MERGE

# COMMAND ----------

columnas_actualizables = [
    "sku", "productname", "brand", "brandid", "categoryid",
    "categoria_principal", "subcategoria", "grupo_producto",
    "canal_venta", "tipo_especifico_producto",
    "precio_regular", "precio_online", "precio_tarjeta_sip", "full_price",
    "descuento_base_pct", "descuento_tarjeta_sip_pct",
    "en_promocion", "precio_promo_exacto", "precio_valido",
    "disponible", "cantidad_disponible", "unit_multiplier",
    "vendedor", "retira_hoy", "llega_manana", "envio_domicilio", "recojo_tienda",
    "alto_sodio", "alto_azucar", "alto_saturadas", "num_octogonos",
    "fecha_lanzamiento", "secciones_origen", "ingesta_date", "batch_id"
]

write_to_silver(
    input_df=df_silver_final,
    target_table=TABLA_SILVER,
    columns_to_update=columnas_actualizables
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21. Verify result

# COMMAND ----------

registros_silver = spark.table(TABLA_SILVER).count()
print(f"Total registros en Silver: {registros_silver}")

registros_batch = spark.table(TABLA_SILVER) \
    .filter(F.col("batch_id") == v_batch_id).count()
print(f"Registros del batch actual: {registros_batch}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 22. Preview data

# COMMAND ----------

display(spark.table(TABLA_SILVER).select(
    "productid", "productname", "precio_regular", "precio_online",
    "precio_tarjeta_sip", "precio_valido", "disponible",
    "created_timestamp", "updated_timestamp", "batch_id"
).filter(F.col("batch_id") == v_batch_id).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 23. Execution Summary

# COMMAND ----------

print("=" * 50)
print("RESUMEN EJECUCION SILVER")
print("=" * 50)
print(f"Batch ID:              {v_batch_id}")
print(f"Registros procesados:  {total}")
print(f"Total en Silver:       {registros_silver}")
print(f"Tabla destino:         {TABLA_SILVER}")
print(f"MERGE key:             productid + ingesta_date")
print("=" * 50)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     productid,
# MAGIC     productname,
# MAGIC     seccion,
# MAGIC     ingesta_date,
# MAGIC     batch_id
# MAGIC FROM plazavea_dev.bronze.bronze_productos
# MAGIC WHERE batch_id = '2026-06-08'
# MAGIC LIMIT 10

# COMMAND ----------

