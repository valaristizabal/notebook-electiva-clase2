
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "airline_mantenimiento"
BRONZE = "bronze"
SILVER = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")

columnas = {
    "vuelos": [
        "vuelo_id", "fecha", "origen_id", "destino_id",
        "aeronave_id", "estado", "duracion_min"
    ],
    "aeronaves": [
        "aeronave_id", "modelo", "fabricante", "anio_fabricacion"
    ],
    "aeropuertos": [
        "aeropuerto_id", "nombre", "ciudad", "pais", "lat", "lon"
    ],
    "mantenimientos": [
        "mantenimiento_id", "aeronave_id", "fecha",
        "tipo", "costo_usd", "duracion_hr"
    ]
}

tipos = {
    "vuelos": {"fecha": "date", "duracion_min": "int"},
    "aeronaves": {"anio_fabricacion": "int"},
    "aeropuertos": {"lat": "double", "lon": "double"},
    "mantenimientos": {
        "fecha": "date",
        "costo_usd": "double",
        "duracion_hr": "double"
    }
}

identificadores = {
    "vuelos": "vuelo_id",
    "aeronaves": "aeronave_id",
    "aeropuertos": "aeropuerto_id",
    "mantenimientos": "mantenimiento_id"
}

for nombre in columnas:
    tabla = f"{CATALOG}.{BRONZE}.{nombre}"

    if not spark.catalog.tableExists(tabla):
        raise Exception(
            f"No existe la tabla {tabla}. Ejecuta primero Bronze."
        )

print("Tablas Bronze verificadas correctamente")

tablas_silver = {}

for nombre, campos in columnas.items():
    df = spark.table(f"{CATALOG}.{BRONZE}.{nombre}").select(*campos)

    for columna, tipo in tipos[nombre].items():
        if tipo == "date":
            df = df.withColumn(columna, F.to_date(F.col(columna)))
        else:
            df = df.withColumn(columna, F.col(columna).cast(tipo))

    df = df.dropDuplicates([identificadores[nombre]])
    tablas_silver[nombre] = df

vuelos = tablas_silver["vuelos"]

nulos = vuelos.filter(F.col("vuelo_id").isNull()).count()

duplicados = (
    vuelos.groupBy("vuelo_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Nulos vuelo_id:", nulos)
print("Duplicados vuelos:", duplicados)

if nulos > 0:
    raise ValueError("Se encontraron vuelos con identificadores nulos")

if duplicados > 0:
    raise ValueError("Se encontraron identificadores de vuelos duplicados")

print("Validaciones de calidad completadas")

for nombre, df in tablas_silver.items():
    tabla = f"{CATALOG}.{SILVER}.{nombre}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla)
    )

    print(f"Tabla Silver guardada: {tabla}")

print("Vuelos con origen y destino iguales:")

(
    spark.table(f"{CATALOG}.{SILVER}.vuelos")
    .filter(F.col("origen_id") == F.col("destino_id"))
    .select("vuelo_id", "origen_id", "destino_id", "estado")
    .orderBy("vuelo_id")
    .show(100, truncate=False)
)

print("Proceso Silver finalizado correctamente")
