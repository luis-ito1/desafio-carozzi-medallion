import os
from pyspark.sql.functions import input_file_name, current_timestamp
from src.spark_utils import get_spark_session

RAW_PATH = "data/raw"           # Carpeta donde están los CSV originales
BRONZE_PATH = "data/bronze"     # Carpeta donde se guardará la capa Bronze (parquet)

def ingest_bronze():

    """
    Lee los archivos CSV de la capa raw, les agrega metadata de ingesta
    y los escribe en formato parquet en la capa Bronze, una carpeta por entidad.
    """

    spark = get_spark_session("bronze_ingest")

    # Mapeo lógico de tablas a patrones de archivos en raw.
    files = {
        "orders_header": "orders_header*.csv",
        "orders_detail": "orders_detail*.csv",
        "customers": "customers*.csv",
        "products": "products*.csv",
    }

    for name, pattern in files.items():
        # Construimos el path completo al/los CSV de cada tabla
        file_path = os.path.join(RAW_PATH, pattern)

        # Leemos los CSV con header y agregamos columnas de metadata (ingestion_timestamp, source_file)
        df = (
            spark.read.option("header", True)
            .csv(file_path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )

        # Ruta de salida en Bronze (una carpeta por nombre lógico de tabla)
        output_path = os.path.join(BRONZE_PATH, name)

        # Sobreescribimos siempre Bronze con la nueva ingesta
        df.write.mode("overwrite").parquet(output_path)

    print("Bronze layer ingestion complete!")
