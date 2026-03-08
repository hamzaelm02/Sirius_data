# -*- coding: utf-8 -*-
"""Etape 2/4 - Nettoyage Bronze -> Silver avec filtres métier et suppression des outliers."""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, length, substring, lit,
    current_timestamp, concat, avg, stddev,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

sys.path.insert(0, "/home/spark/scripts")
from config.pipeline_config import (
    HDFS_MASTER,
    HDFS_BRONZE_PATH,
    HDFS_SILVER_PATH,
    MONTHS_TO_KEEP,
    COST_MIN,
    COST_MAX,
    QTY_MAX,
    UNIT_COST_MIN,
    UNIT_COST_MAX,
    ZSCORE_THRESHOLD,
    MONTH_DISPLAY_MAPPING,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_to_silver")


def rename_columns(df: DataFrame) -> DataFrame:
    """Supprime les colonnes non utilisées et normalise les noms vers le snake_case."""
    df = df.drop("NHSBSA_DESCRIPTION", "VMP_METADATA")
    return (
        df
        .withColumnRenamed("YEAR_MONTH",                 "month_id")
        .withColumnRenamed("ODS_CODE",                   "trust_code")
        .withColumnRenamed("VMP_SNOMED_CODE",            "vmp_snomed_code")
        .withColumnRenamed("VMP_PRODUCT_NAME",           "vmp_name")
        .withColumnRenamed("UNIT_OF_MEASURE_IDENTIFIER", "uom_code")
        .withColumnRenamed("UNIT_OF_MEASURE_NAME",       "uom_name")
        .withColumnRenamed("TOTAL_QUANITY_IN_VMP_UNIT",  "total_quantity")
        .withColumnRenamed("INDICATIVE_COST",            "indicative_cost")
    )


def cast_and_derive_dates(df: DataFrame) -> DataFrame:
    """Construit month_label (format YYYY-MM) depuis le month_id brut (format YYYYMM)."""
    df = df.withColumn("month_id", col("month_id").cast("string"))
    df = df.withColumn("year",  substring(col("month_id"), 1, 4))
    df = df.withColumn("month", substring(col("month_id"), 5, 2))
    df = df.withColumn(
        "month_label",
        when(
            length(col("month_id")) == 6,
            concat(
                substring(col("month_id"), 1, 4),
                lit("-"),
                substring(col("month_id"), 5, 2),
            ),
        ).otherwise(lit(None)),
    )
    return df


def normalize_strings(df: DataFrame) -> DataFrame:
    """Normalise les champs texte (trim, upper) et cast les numériques en Double."""
    return (
        df
        .withColumn("trust_code",      upper(trim(col("trust_code"))))
        .withColumn("vmp_snomed_code", trim(col("vmp_snomed_code")))
        .withColumn("vmp_name",        trim(col("vmp_name")))
        .withColumn("uom_name",        upper(trim(col("uom_name"))))
        .withColumn("total_quantity",  col("total_quantity").cast(DoubleType()))
        .withColumn("indicative_cost", col("indicative_cost").cast(DoubleType()))
    )


def apply_business_filters(df: DataFrame) -> DataFrame:
    """Filtre les lignes invalides et restreint aux 6 mois de la période d'analyse."""
    before = df.count()
    df = (
        df
        .filter(col("month_id").isNotNull() & (length(col("month_id")) == 6))
        .filter(col("trust_code").isNotNull() & (trim(col("trust_code")) != ""))
        .filter(col("vmp_name").isNotNull()   & (trim(col("vmp_name"))   != ""))
        .filter(col("total_quantity").isNotNull() & (col("total_quantity") > 0))
    )
    log.info("Filtre validite : %d -> %d lignes", before, df.count())

    before = df.count()
    df = df.filter(col("month_label").isin(MONTHS_TO_KEEP))
    log.info("Filtre mois %s : %d -> %d lignes", MONTHS_TO_KEEP, before, df.count())
    return df


def apply_range_filters(df: DataFrame) -> DataFrame:
    """Exclut les valeurs aberrantes de coût, quantité et coût unitaire."""
    before = df.count()

    df = df.filter(
        col("indicative_cost").isNull()
        | (
            (col("indicative_cost") >= COST_MIN)
            & (col("indicative_cost") <= COST_MAX)
        )
    )

    df = df.filter(col("total_quantity") <= QTY_MAX)

    # Coût unitaire calculé temporairement pour filtrage
    df = (
        df
        .withColumn(
            "_unit_cost_tmp",
            when(col("total_quantity") > 0,
                 col("indicative_cost") / col("total_quantity")).otherwise(lit(None)),
        )
        .filter(
            col("_unit_cost_tmp").isNull()
            | (
                (col("_unit_cost_tmp") >= UNIT_COST_MIN)
                & (col("_unit_cost_tmp") <= UNIT_COST_MAX)
            )
        )
        .drop("_unit_cost_tmp")
    )

    log.info("Filtre plages de valeurs : %d -> %d lignes", before, df.count())
    return df


def remove_outliers_zscore(df: DataFrame) -> DataFrame:
    """Supprime les lignes dont le coût dépasse mean + 3*stddev par médicament."""
    before = df.count()
    w = Window.partitionBy("vmp_name")
    df = (
        df
        .withColumn("_avg_cost",    avg("indicative_cost").over(w))
        .withColumn("_stddev_cost", stddev("indicative_cost").over(w))
        .filter(
            col("_stddev_cost").isNull()
            | (col("_stddev_cost") == 0)
            | (col("indicative_cost") <= col("_avg_cost") + ZSCORE_THRESHOLD * col("_stddev_cost"))
        )
        .drop("_avg_cost", "_stddev_cost")
    )
    log.info("Suppression outliers z-score=%.1f : %d -> %d lignes",
             ZSCORE_THRESHOLD, before, df.count())
    return df


def add_display_month(df: DataFrame) -> DataFrame:
    """Remplace month_label par month_display selon le mapping d'anonymisation."""
    # Construction dynamique du when() depuis le dict de config
    expr = None
    for real, display in MONTH_DISPLAY_MAPPING.items():
        if expr is None:
            expr = when(col("month_label") == real, display)
        else:
            expr = expr.when(col("month_label") == real, display)

    df = df.withColumn("month_display", expr.otherwise(col("month_label")))
    return df.drop("month_label")


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Ajoute unit_cost, has_cost et le timestamp de nettoyage."""
    return (
        df
        .withColumn(
            "unit_cost",
            when(
                col("indicative_cost").isNotNull() & (col("total_quantity") > 0),
                col("indicative_cost") / col("total_quantity"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "has_cost",
            when(col("indicative_cost").isNotNull(), lit(True)).otherwise(lit(False)),
        )
        .withColumn("silver_cleaning_timestamp", current_timestamp())
    )


def main() -> None:
    log.info("=" * 80)
    log.info("ETAPE 02/04 - BRONZE -> SILVER")
    log.info("=" * 80)

    spark = (
        SparkSession.builder
        .appName("02_Bronze_to_Silver")
        .config("spark.hadoop.fs.defaultFS", HDFS_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        log.info("Lecture Bronze depuis : %s", HDFS_BRONZE_PATH)
        df = spark.read.parquet(HDFS_BRONZE_PATH)
        log.info("Lignes Bronze : %d", df.count())

        df = rename_columns(df)
        df = cast_and_derive_dates(df)
        df = normalize_strings(df)
        df = apply_business_filters(df)
        df = apply_range_filters(df)
        df = remove_outliers_zscore(df)
        df = add_display_month(df)
        df = add_derived_columns(df)

        log.info("Lignes Silver finales : %d", df.count())
        df.groupBy("month_display").count().orderBy("month_display").show()

        log.info("Ecriture Silver vers : %s", HDFS_SILVER_PATH)
        (
            df.write
            .mode("overwrite")
            .partitionBy("month_display")
            .parquet(HDFS_SILVER_PATH)
        )
        log.info("Silver ecrit avec succes")

    except Exception:
        log.exception("Echec Bronze -> Silver")
        sys.exit(1)
    finally:
        spark.stop()

    log.info("Etape 02 terminee avec succes")


if __name__ == "__main__":
    main()
