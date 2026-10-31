
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = "airline_mantenimiento"
SILVER = "silver"
GOLD = "gold"

nombres = ["vuelos", "aeronaves", "aeropuertos", "mantenimientos"]
tablas = {}

for nombre in nombres:
    ruta = f"{CATALOG}.{SILVER}.{nombre}"

    if not spark.catalog.tableExists(ruta):
        raise Exception(f"No existe {ruta}. Ejecuta primero Silver.")

    tablas[nombre] = spark.table(ruta)

print("Tablas Silver verificadas correctamente")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")

vuelos = tablas["vuelos"]
aeronaves = tablas["aeronaves"]
aeropuertos = tablas["aeropuertos"]
mantenimientos = tablas["mantenimientos"]

origen = aeropuertos.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("nombre").alias("aeropuerto_origen"),
    F.col("ciudad").alias("ciudad_origen"),
    F.col("pais").alias("pais_origen")
)

destino = aeropuertos.select(
    F.col("aeropuerto_id").alias("destino_id"),
    F.col("nombre").alias("aeropuerto_destino"),
    F.col("ciudad").alias("ciudad_destino"),
    F.col("pais").alias("pais_destino")
)

fact_vuelos = (
    vuelos
    .join(aeronaves, "aeronave_id", "left")
    .join(origen, "origen_id", "left")
    .join(destino, "destino_id", "left")
    .withColumn("anio_vuelo", F.year("fecha"))
    .withColumn("mes_vuelo", F.month("fecha"))
    .withColumn("duracion_hr", F.round(F.col("duracion_min") / 60, 2))
    .select(
        "vuelo_id", "fecha", "anio_vuelo", "mes_vuelo",
        "origen_id", "aeropuerto_origen", "ciudad_origen", "pais_origen",
        "destino_id", "aeropuerto_destino", "ciudad_destino", "pais_destino",
        "aeronave_id", "modelo", "fabricante", "anio_fabricacion",
        "estado", "duracion_min", "duracion_hr"
    )
)

fact_mantenimientos = (
    mantenimientos
    .join(aeronaves, "aeronave_id", "left")
    .withColumn("anio_mantenimiento", F.year("fecha"))
    .withColumn("mes_mantenimiento", F.month("fecha"))
    .withColumn(
        "costo_por_hora_usd",
        F.when(
            F.col("duracion_hr") > 0,
            F.round(F.col("costo_usd") / F.col("duracion_hr"), 2)
        )
    )
    .select(
        "mantenimiento_id", "aeronave_id", "modelo", "fabricante",
        "anio_fabricacion", "fecha", "anio_mantenimiento",
        "mes_mantenimiento", "tipo", "costo_usd",
        "duracion_hr", "costo_por_hora_usd"
    )
)

resumen = (
    fact_mantenimientos
    .groupBy("aeronave_id", "modelo", "fabricante", "anio_fabricacion")
    .agg(
        F.countDistinct("mantenimiento_id").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.sum("duracion_hr").alias("horas_totales_mantenimiento"),
        F.avg("costo_usd").alias("costo_promedio_usd"),
        F.max("fecha").alias("fecha_ultimo_mantenimiento")
    )
)

for columna in [
    "costo_total_usd",
    "costo_promedio_usd",
    "horas_totales_mantenimiento"
]:
    resumen = resumen.withColumn(columna, F.round(F.col(columna), 2))

ventana = Window.orderBy(F.col("costo_total_usd").desc_nulls_last())
resumen = resumen.withColumn("ranking_costo", F.dense_rank().over(ventana))

tablas_gold = {
    "fact_vuelos": fact_vuelos,
    "fact_mantenimientos": fact_mantenimientos,
    "resumen_mantenimiento_aeronave": resumen
}

for nombre, df in tablas_gold.items():
    ruta = f"{CATALOG}.{GOLD}.{nombre}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ruta)
    )

    tabla = spark.table(ruta)
    total = tabla.count()

    print(f"\nTabla Gold: {nombre} - Registros: {total}")

    if total == 0:
        raise ValueError(f"La tabla {nombre} no contiene registros")

    tabla.show(5, truncate=False)

vuelos_iguales = fact_vuelos.filter(
    F.col("origen_id") == F.col("destino_id")
)

print("Vuelos con origen y destino iguales:", vuelos_iguales.count())

(
    vuelos_iguales
    .select("vuelo_id", "fecha", "origen_id", "destino_id", "estado")
    .orderBy("vuelo_id")
    .show(100, truncate=False)
)

for nombre, df, columna in [
    ("vuelos", fact_vuelos, "vuelo_id"),
    ("mantenimientos", fact_mantenimientos, "mantenimiento_id")
]:
    nulos = df.filter(F.col(columna).isNull()).count()
    print(f"Identificadores nulos en {nombre}: {nulos}")

    if nulos > 0:
        raise ValueError("Se encontraron identificadores nulos en las tablas Gold")

print("Proceso Gold finalizado correctamente")
