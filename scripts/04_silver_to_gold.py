# -*- coding: utf-8 -*-
"""Etape 4/4 - Calcul des KPI Gold depuis Silver et écriture dans PostgreSQL."""

import os
import sys
import logging

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, stddev, desc, countDistinct, lag, when, lit,
)
from pyspark.sql.window import Window

sys.path.insert(0, "/home/spark/scripts")
from config.pipeline_config import (
    HDFS_MASTER,
    HDFS_SILVER_PATH,
    POSTGRES_JAR_PATH,
    POSTGRES_JDBC_TEMPLATE,
    VAULT_ADDR_DEFAULT,
    VAULT_SECRET_PATH,
    TOP_N_COST,
    TOP_N_QTY,
    TOP_N_VOLATILITY,
    MOVING_AVG_3M_WINDOW,
    MOVING_AVG_6M_WINDOW,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver_to_gold")


def get_postgres_credentials() -> dict:
    """Récupère les credentials PostgreSQL depuis Vault. VAULT_TOKEN doit être défini en env."""
    vault_addr  = os.getenv("VAULT_ADDR", VAULT_ADDR_DEFAULT)
    vault_token = os.getenv("VAULT_TOKEN")

    if not vault_token:
        raise EnvironmentError("VAULT_TOKEN non defini - export VAULT_TOKEN='<token>'")

    log.info("Recuperation credentials depuis Vault : %s", vault_addr)
    response = requests.get(
        f"{vault_addr}/v1/{VAULT_SECRET_PATH}",
        headers={"X-Vault-Token": vault_token},
        timeout=10,
    )

    if response.status_code != 200:
        raise ConnectionError(f"Vault HTTP {response.status_code} : {response.text}")

    log.info("Credentials PostgreSQL recuperes")
    return response.json()["data"]["data"]


def write_pg(df: DataFrame, table: str, url: str, props: dict) -> None:
    log.info("Ecriture -> %s", table)
    df.write.mode("overwrite").jdbc(url, table, properties=props)


# KPI Financiers

def compute_f1(df: DataFrame) -> DataFrame:
    """Coût total par mois."""
    return (
        df.filter(col("has_cost"))
        .groupBy("month_display")
        .agg(spark_sum("indicative_cost").alias("total_cost"))
        .orderBy("month_display")
    )


def compute_f2(df: DataFrame) -> DataFrame:
    """Coût par trust et par mois."""
    return (
        df.filter(col("has_cost"))
        .groupBy("trust_code", "month_display")
        .agg(spark_sum("indicative_cost").alias("total_cost"))
        .orderBy("month_display", desc("total_cost"))
    )


def compute_f3(df: DataFrame) -> DataFrame:
    """Top N médicaments par coût total."""
    return (
        df.filter(col("has_cost"))
        .groupBy("vmp_name")
        .agg(spark_sum("indicative_cost").alias("total_cost"))
        .orderBy(desc("total_cost"))
        .limit(TOP_N_COST)
    )


def compute_f4(df: DataFrame) -> DataFrame:
    """Coût unitaire moyen par médicament (coût total / quantité totale)."""
    return (
        df.filter(col("has_cost"))
        .groupBy("vmp_name")
        .agg(
            (spark_sum("indicative_cost") / spark_sum("total_quantity")).alias("avg_unit_cost")
        )
        .orderBy(desc("avg_unit_cost"))
    )


def compute_f5(kpi_f1: DataFrame) -> DataFrame:
    """Variation mois-sur-mois du coût total, calculée depuis F1."""
    w = Window.orderBy("month_display")
    return (
        kpi_f1
        .withColumn("prev_cost", lag("total_cost", 1).over(w))
        .withColumn(
            "mom_variation_pct",
            when(
                col("prev_cost").isNotNull(),
                ((col("total_cost") - col("prev_cost")) / col("prev_cost")) * 100,
            ).otherwise(lit(None)),
        )
    )


def compute_f6(df: DataFrame) -> DataFrame:
    """Contribution de chaque médicament à la variation MoM."""
    base = (
        df.filter(col("has_cost"))
        .groupBy("vmp_name", "month_display")
        .agg(spark_sum("indicative_cost").alias("cost"))
    )
    w = Window.partitionBy("vmp_name").orderBy("month_display")
    return (
        base
        .withColumn("prev_cost",  lag("cost", 1).over(w))
        .withColumn("delta_cost", col("cost") - col("prev_cost"))
        .filter(col("delta_cost").isNotNull())
    )


# KPI Prescriptions

def compute_p1(df: DataFrame) -> DataFrame:
    """Quantité totale dispensée par mois."""
    return (
        df.groupBy("month_display")
        .agg(spark_sum("total_quantity").alias("total_qty"))
        .orderBy("month_display")
    )


def compute_p2(df: DataFrame) -> DataFrame:
    """Quantité par trust et par mois."""
    return (
        df.groupBy("trust_code", "month_display")
        .agg(spark_sum("total_quantity").alias("total_qty"))
        .orderBy("month_display", desc("total_qty"))
    )


def compute_p3(df: DataFrame) -> DataFrame:
    """Top N médicaments par quantité totale dispensée."""
    return (
        df.groupBy("vmp_name")
        .agg(spark_sum("total_quantity").alias("total_qty"))
        .orderBy(desc("total_qty"))
        .limit(TOP_N_QTY)
    )


def compute_p4(df: DataFrame) -> DataFrame:
    """Mix de prescription par unité de mesure (UOM)."""
    return (
        df.groupBy("uom_name")
        .agg(
            spark_sum("total_quantity").alias("total_qty"),
            countDistinct("vmp_name").alias("nb_medicaments"),
        )
        .orderBy(desc("total_qty"))
    )


def compute_p5(df: DataFrame) -> DataFrame:
    """Volatilité des prescriptions par médicament (coefficient de variation)."""
    return (
        df.groupBy("vmp_name")
        .agg(
            avg("total_quantity").alias("avg_qty"),
            stddev("total_quantity").alias("stddev_qty"),
        )
        .withColumn("cv", col("stddev_qty") / col("avg_qty"))
        .filter(col("cv").isNotNull())
        .orderBy(desc("cv"))
        .limit(TOP_N_VOLATILITY)
    )


def compute_p6(df: DataFrame) -> DataFrame:
    """Moyennes mobiles 3M et 6M des quantités par médicament."""
    w3 = (
        Window.partitionBy("vmp_name")
        .orderBy("month_display")
        .rowsBetween(*MOVING_AVG_3M_WINDOW)
    )
    w6 = (
        Window.partitionBy("vmp_name")
        .orderBy("month_display")
        .rowsBetween(*MOVING_AVG_6M_WINDOW)
    )
    return (
        df.groupBy("vmp_name", "month_display")
        .agg(spark_sum("total_quantity").alias("monthly_qty"))
        .withColumn("moving_avg_3m", avg("monthly_qty").over(w3))
        .withColumn("moving_avg_6m", avg("monthly_qty").over(w6))
        .orderBy("vmp_name", "month_display")
    )


def main() -> None:
    log.info("=" * 80)
    log.info("ETAPE 04/04 - SILVER -> GOLD")
    log.info("=" * 80)

    creds = get_postgres_credentials()

    spark = (
        SparkSession.builder
        .appName("04_Silver_to_Gold")
        .config("spark.hadoop.fs.defaultFS", HDFS_MASTER)
        .config("spark.jars", POSTGRES_JAR_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        log.info("Lecture Silver depuis : %s", HDFS_SILVER_PATH)
        df = spark.read.parquet(HDFS_SILVER_PATH)
        silver_count = df.count()
        log.info("Lignes Silver : %d", silver_count)

        jdbc_url = POSTGRES_JDBC_TEMPLATE.format(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
        )
        jdbc_props = {
            "user":     creds["user"],
            "password": creds["password"],
            "driver":   "org.postgresql.Driver",
        }

        # F5 dépend de F1, on le calcule séparément
        kpi_f1 = compute_f1(df)

        kpi_map = {
            "gold_cost_per_month":      kpi_f1,
            "gold_cost_per_trust":      compute_f2(df),
            "gold_top20_cost":          compute_f3(df),
            "gold_avg_unit_cost":       compute_f4(df),
            "gold_cost_mom_variation":  compute_f5(kpi_f1),
            "gold_cost_contribution":   compute_f6(df),
            "gold_qty_per_month":       compute_p1(df),
            "gold_qty_per_trust":       compute_p2(df),
            "gold_top20_qty":           compute_p3(df),
            "gold_uom_mix":             compute_p4(df),
            "gold_volatility":          compute_p5(df),
            "gold_moving_averages":     compute_p6(df),
        }

        for table, kpi_df in kpi_map.items():
            write_pg(kpi_df, table, jdbc_url, jdbc_props)

        log.info("Lignes Silver traitees : %d | Tables Gold creees : %d",
                 silver_count, len(kpi_map))

    except Exception:
        log.exception("Echec Silver -> Gold")
        sys.exit(1)
    finally:
        spark.stop()

    log.info("Etape 04 terminee avec succes")


if __name__ == "__main__":
    main()
