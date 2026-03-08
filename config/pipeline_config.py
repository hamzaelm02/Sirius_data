# -*- coding: utf-8 -*-
"""
Configuration centralisée du pipeline SCMD.
"""

# HDFS
HDFS_MASTER      = "hdfs://master:9000"
HDFS_BRONZE_PATH = f"{HDFS_MASTER}/bronze/scmd/raw"
HDFS_SILVER_PATH = f"{HDFS_MASTER}/silver/scmd_cleaned"

# Chemins locaux
CSV_SOURCE_PATH   = "file:///home/spark/scmd_data/*.csv"
POSTGRES_JAR_PATH = "file:///home/spark/postgresql.jar"
SPARK_SCRIPTS_DIR = "/home/spark/scripts"

# Vault
VAULT_ADDR_DEFAULT = "http://172.31.250.180:8200"
VAULT_SECRET_PATH  = "secret/data/postgresql"

# PostgreSQL
POSTGRES_JDBC_TEMPLATE = "jdbc:postgresql://{host}:{port}/{database}"

# Mois à conserver dans le pipeline (données 2025)
MONTHS_TO_KEEP = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-12"]

# Plages de valeurs acceptables
COST_MIN      = 0.01
COST_MAX      = 10_000_000.0
QTY_MAX       = 500_000.0
UNIT_COST_MIN = 0.01
UNIT_COST_MAX = 5_000.0

# Seuil z-score pour la suppression des outliers de coût
ZSCORE_THRESHOLD = 3.0

# Mapping mois réels -> mois d'affichage pour la présentation
# Les données 2025 sont décalées de 6 mois pour anonymisation
MONTH_DISPLAY_MAPPING = {
    "2025-05": "2025-12",
    "2025-04": "2025-11",
    "2025-03": "2025-10",
    "2025-02": "2025-09",
    "2025-01": "2025-08",
    "2025-12": "2025-07",
}

# Seuils qualité Silver
QUALITY_MIN_ROW_COUNT       = 1_000
QUALITY_MAX_NULL_RATE_TRUST = 0.01   # 1% max
QUALITY_MAX_NULL_RATE_COST  = 0.30   # 30% acceptable, certains trusts ne remontent pas les coûts
QUALITY_MIN_DISTINCT_MONTHS = 5
QUALITY_MIN_DISTINCT_TRUSTS = 10

# KPI Gold
TOP_N_COST       = 20
TOP_N_QTY        = 20
TOP_N_VOLATILITY = 50

MOVING_AVG_3M_WINDOW = (-2, 0)
MOVING_AVG_6M_WINDOW = (-5, 0)

# Grafana
GRAFANA_TIMEOUT    = 10
GRAFANA_SLEEP_AFTER = 10
