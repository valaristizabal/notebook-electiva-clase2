
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "airline_mantenimiento"
SCHEMA = "bronze"
VOLUME = "landing"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

archivos = {
    "vuelos": "vuelos_diarios.csv",
    "aeronaves": "aeronaves.csv",
    "aeropuertos": "aeropuertos.csv",
    "mantenimientos": "mantenimientos_rds.csv"
}

tipos = {
    "vuelos": {
        "fecha": "date",
        "vuelo_id": "string",
        "origen_id": "string",
        "destino_id": "string",
        "aeronave_id": "string",
        "estado": "string",
        "duracion_min": "int"
    },
    "aeronaves": {
        "aeronave_id": "string",
        "modelo": "string",
        "fabricante": "string",
        "anio_fabricacion": "int"
    },
    "aeropuertos": {
        "aeropuerto_id": "string",
        "nombre": "string",
        "ciudad": "string",
        "pais": "string",
        "lat": "double",
        "lon": "double"
    },
    "mantenimientos": {
        "mantenimiento_id": "string",
        "aeronave_id": "string",
        "fecha": "date",
        "tipo": "string",
        "costo_usd": "double",
        "duracion_hr": "double"
    }
}

for nombre, archivo in archivos.items():
    ruta = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{archivo}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ruta)
    )

    for columna, tipo in tipos[nombre].items():
        if tipo == "date":
            df = df.withColumn(columna, F.to_date(F.col(columna)))
        else:
            df = df.withColumn(columna, F.col(columna).cast(tipo))

    df = df.withColumn("ingestion_time", F.current_timestamp())

    tabla = f"{CATALOG}.{SCHEMA}.{nombre}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla)
    )

    print(f"Tabla creada: {tabla}")

print("Proceso Bronze completado correctamente")
