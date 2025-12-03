from src.bronze.bronze_ingest import ingest_bronze
from src.silver.silver_transform import run_silver
from src.gold.gold_features import run_gold

if __name__ == "__main__":
    """
    Orquestador principal del pipeline:
      1. Genera capa Bronze desde raw
      2. Genera capa Silver desde Bronze
      3. Genera capa Gold (dim_features) desde Silver
    """
    ingest_bronze()
    run_silver()
    run_gold()
