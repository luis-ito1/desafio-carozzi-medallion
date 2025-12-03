import os
from pyspark.sql import functions as F
from pyspark.sql import Window
from src.spark_utils import get_spark_session

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

def run_gold():

    """
    Construye la tabla dim_features a partir de la fact table de Silver,
    calculando 7 métricas de comportamiento del cliente en el canal tradicional
    y en los últimos 3 meses.
    """

    spark = get_spark_session("gold_dim_features")

    # Leemos la fact table de Silver
    fact = spark.read.parquet(os.path.join(SILVER_PATH, "fact_order_line"))

    # Asegurarnos de que order_date sea date
    fact = fact.withColumn("order_date", F.to_date("order_date"))

    # ============================
    # 1) FECHA DE REFERENCIA
    # ============================
    # Usamos la fecha máxima de pedido como "hoy" (fecha de referencia)
    ref_row = fact.agg(F.max("order_date").alias("ref_date")).first()
    ref_date = ref_row["ref_date"]

    # Ventana de 3 meses hacia atrás desde la fecha de referencia
    three_months_ago = F.add_months(F.lit(ref_date), -3)

    # ============================
    # 2) DATA A NIVEL PEDIDO
    # ============================
    # Nos quedamos con un registro por pedido (evitar replicar por línea)
    orders = (
        fact
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_net_amount_replicated",
            "flag_promo_pedido",
            "canal"
        )
        .dropDuplicates(["order_id"])
    )

    # Solo canal tradicional
    orders = orders.filter(F.col("canal") == "tradicional")

    # Pedidos de los últimos 3 meses
    orders_3m = orders.filter(
        (F.col("order_date") > three_months_ago) & (F.col("order_date") <= F.lit(ref_date))
    )

    # ============================
    # 3) MÉTRICAS DE PEDIDOS (3M)
    # ============================
    # Agregamos a nivel cliente en los últimos 3 meses
    agg_3m = (
        orders_3m
        .groupBy("customer_id")
        .agg(
            # 1) Ventas totales 3m
            F.sum("order_net_amount_replicated").alias("ventas_total_3m"),
            # 2) Frecuencia de pedidos 3m
            F.countDistinct("order_id").alias("frecuencia_pedidos_3m"),
            # Conteo de pedidos con promo para luego calcular %
            F.sum(F.when(F.col("flag_promo_pedido") > 0, 1).otherwise(0)).alias("pedidos_promo_3m")
        )
    )

    # Calculamos ticket promedio y porcentaje de pedidos en promo
    agg_3m = (
        agg_3m
        .withColumn(
            "ticket_promedio_3m",
            F.when(F.col("frecuencia_pedidos_3m") > 0,
                   F.col("ventas_total_3m") / F.col("frecuencia_pedidos_3m")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "porcentaje_pedidos_promo_3m",
            F.when(F.col("frecuencia_pedidos_3m") > 0,
                   F.col("pedidos_promo_3m") / F.col("frecuencia_pedidos_3m")
            ).otherwise(F.lit(0.0))
        )
        .drop("pedidos_promo_3m") # ya no necesitamos el conteo intermedio
    )

    # ============================
    # 4) RECENCIA (EN DÍAS)
    # ============================
    # Última fecha de compra por cliente (en canal tradicional)
    last_order = (
        orders
        .groupBy("customer_id")
        .agg(F.max("order_date").alias("last_order_date"))
        .withColumn(
            "recencia_dias",
            F.datediff(F.lit(ref_date), F.col("last_order_date"))
        )
    )

    # ============================
    # 5) DÍAS PROMEDIO ENTRE PEDIDOS
    # ============================
    # Ventana para ordenar pedidos por fecha por cliente
    w = Window.partitionBy("customer_id").orderBy("order_date")

    # LAG para comparar fecha actual con fecha anterior del mismo cliente
    orders_with_lag = (
        orders
        .withColumn("prev_order_date", F.lag("order_date").over(w))
        .withColumn(
            "diff_dias",
            F.when(F.col("prev_order_date").isNotNull(),
                   F.datediff(F.col("order_date"), F.col("prev_order_date"))
            )
        )
    )

    # Promedio de días entre pedidos por cliente
    diff_avg = (
        orders_with_lag
        .groupBy("customer_id")
        .agg(
            F.avg("diff_dias").alias("dias_promedio_entre_pedidos")
        )
    )

    # ============================
    # 6) VARIEDAD DE CATEGORÍAS (3M)
    # ============================
    # Nos quedamos con líneas de los últimos 3 meses y canal tradicional
    lines_3m = fact.filter(
        (F.col("canal") == "tradicional") &
        (F.col("order_date") > three_months_ago) &
        (F.col("order_date") <= F.lit(ref_date))
    )

    # Cantidad de categorías distintas compradas en ese periodo
    categorias_3m = (
        lines_3m
        .groupBy("customer_id")
        .agg(
            F.countDistinct("product_category").alias("variedad_categorias_3m")
        )
    )

    # ============================
    # 7) UNIR TODAS LAS MÉTRICAS
    # ============================
    dim_features = (
        agg_3m
        .join(last_order, on="customer_id", how="outer")
        .join(diff_avg, on="customer_id", how="outer")
        .join(categorias_3m, on="customer_id", how="outer")
        # Manejamos posibles nulos con valores aceptables
        .fillna({
            "ventas_total_3m": 0.0,
            "frecuencia_pedidos_3m": 0,
            "ticket_promedio_3m": 0.0,
            "porcentaje_pedidos_promo_3m": 0.0,
            "dias_promedio_entre_pedidos": 0.0,
            "variedad_categorias_3m": 0
        })
    )

    # ============================
    # 8) GRABAR EN GOLD
    # ============================
    output_path = os.path.join(GOLD_PATH, "dim_features")
    dim_features.write.mode("overwrite").parquet(output_path)

    print("Gold layer (dim_features) generation complete!")
