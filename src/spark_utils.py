from pyspark.sql import SparkSession

def get_spark_session(app_name: str):
    
    """
    Crea y devuelve una SparkSession con una configuración básica
    para este proyecto.

    Parámetros:
        app_name (str): Nombre de la aplicación para identificarla en Spark UI.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)                              # Nombre de la app en Spark
        .config("spark.sql.shuffle.partitions", "2")    # Menos particiones para entornos pequeños
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
