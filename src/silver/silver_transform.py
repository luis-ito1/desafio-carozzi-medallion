import os
from pyspark.sql import functions as F
from src.spark_utils import get_spark_session

BRONZE_PATH = "data/bronze"     # Origen: parquet de Bronze
SILVER_PATH = "data/silver"     # Destino: parquet de Silver

def run_silver():

    """
    Toma los datos de la capa Bronze, corrige tipos, filtra registros inválidos,
    elimina duplicados y construye:
      - fact_order_line
      - dim_customer
      - dim_product
      - dim_date
    """

    spark = get_spark_session("silver_layer")

    # ============================
    # LECTURA DESDE BRONZE
    # ============================
    # Leer Bronze
    orders_header = spark.read.parquet(os.path.join(BRONZE_PATH, "orders_header"))
    orders_detail = spark.read.parquet(os.path.join(BRONZE_PATH, "orders_detail"))
    customers     = spark.read.parquet(os.path.join(BRONZE_PATH, "customers"))
    products      = spark.read.parquet(os.path.join(BRONZE_PATH, "products"))

    # ============================
    # CAST: ORDERS HEADER (TRANSFORMACION DE TIPO DE DATO)
    # ============================
    orders_header = (
        orders_header
        # Aseguramos que order_date sea un tipo date
        .withColumn("order_date", F.to_date("order_date"))
        # Flag de promo a entero (0/1)
        .withColumn("flag_promo_pedido", F.col("flag_promo_pedido").cast("int"))
        # Montos a numéricos (double)
        .withColumn("order_net_amount", F.col("order_net_amount").cast("double"))
        .withColumn("order_gross_amount", F.col("order_gross_amount").cast("double"))
    )

    # ============================
    # CAST: ORDERS DETAIL (TRANSFORMACION DE TIPO DE DATO)
    # ============================
    orders_detail = (
    orders_detail
    .withColumn("qty_units", F.col("qty_units").cast("double"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount_amount", F.col("discount_amount").cast("double"))
    .withColumn("promo_flag", F.col("promo_flag").cast("int"))
    .withColumn("line_net_amount", F.col("line_net_amount").cast("double"))
    .withColumn("order_net_amount_replicated", F.col("order_net_amount_replicated").cast("double"))
)
    #Eliminamos los campos que no necesitamos para evitar columnas duplicadas
    orders_detail = orders_detail.drop("ingestion_timestamp", "source_file")

    # ============================
    # FILTRAR NO VÁLIDOS
    # ============================
    # Pedidos con monto neto <= 0 no tienen sentido de negocio
    orders_header = orders_header.filter(F.col("order_net_amount") > 0)
    # Líneas con cantidad <= 0 se excluyen
    orders_detail = orders_detail.filter(F.col("qty_units") > 0)

    # ============================
    # ELIMINAMOS DUPLICADOS
    # ============================
    orders_header = orders_header.dropDuplicates(["order_id"])
    orders_detail = orders_detail.dropDuplicates(["order_id", "line_id"])
    customers     = customers.dropDuplicates(["customer_id"])
    products      = products.dropDuplicates(["product_id"])

    # ============================
    # FACT TABLE: fact_order_line  (TABLA DE HECHOS)
    # ============================
    # Join de cabecera y detalle a nivel de línea de pedido
    fact_order_line = (
        orders_header.alias("h")
        .join(orders_detail.alias("d"), on="order_id", how="left")
    )

    # ============================
    # DIMENSIONES
    # ============================
    dim_customer = customers
    dim_product  = products

    # ============================
    # DIM FECHA
    # ============================
    date_col = "order_date"

    # Obtenemos la fecha mínima y máxima de la fact para construir el rango de fechas
    bounds = fact_order_line.agg(
        F.min(date_col).alias("min_date"),
        F.max(date_col).alias("max_date")
    )

    # sequence(min_date, max_date) genera un array de fechas día a día.
    # explode lo convierte en filas (una fila por día).
    dim_date = (
        bounds
        .select(F.explode(F.sequence("min_date", "max_date")).alias("date"))
    )

    # ============================
    # ESCRIBIMOS SILVER
    # ============================
    fact_order_line.write.mode("overwrite").parquet(os.path.join(SILVER_PATH, "fact_order_line"))
    dim_customer.write.mode("overwrite").parquet(os.path.join(SILVER_PATH, "dim_customer"))
    dim_product.write.mode("overwrite").parquet(os.path.join(SILVER_PATH, "dim_product"))
    dim_date.write.mode("overwrite").parquet(os.path.join(SILVER_PATH, "dim_date"))

    print("Silver layer generation complete!")
