# -*- coding: utf-8 -*-
"""Etape 3/4 - Contrôle qualité de la couche Silver avant le calcul des KPI."""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame

sys.path.insert(0, "/home/spark/scripts")
from config.pipeline_config import (
    HDFS_MASTER,
    HDFS_SILVER_PATH,
    QUALITY_MIN_ROW_COUNT,
    QUALITY_MAX_NULL_RATE_TRUST,
    QUALITY_MAX_NULL_RATE_COST,
    QUALITY_MIN_DISTINCT_MONTHS,
    QUALITY_MIN_DISTINCT_TRUSTS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("check_silver_quality")


def check_min_rows(total: int) -> bool:
    ok = total >= QUALITY_MIN_ROW_COUNT
    log.info("[%s] Lignes : %d (min=%d)", "OK" if ok else "ECHEC", total, QUALITY_MIN_ROW_COUNT)
    return ok


def check_null_rate(df: DataFrame, total: int, column: str, max_rate: float) -> bool:
    null_count = df.filter(df[column].isNull()).count()
    rate = null_count / total if total > 0 else 1.0
    ok = rate <= max_rate
    log.info(
        "[%s] Nulls '%s' : %.2f%% (max=%.2f%%)",
        "OK" if ok else "ECHEC", column, rate * 100, max_rate * 100,
    )
    return ok


def check_distinct_months(df: DataFrame) -> bool:
    n = df.select("month_display").distinct().count()
    ok = n >= QUALITY_MIN_DISTINCT_MONTHS
    log.info("[%s] Mois distincts : %d (min=%d)", "OK" if ok else "ECHEC", n, QUALITY_MIN_DISTINCT_MONTHS)
    return ok


def check_distinct_trusts(df: DataFrame) -> bool:
    n = df.select("trust_code").distinct().count()
    ok = n >= QUALITY_MIN_DISTINCT_TRUSTS
    log.info("[%s] Trusts distincts : %d (min=%d)", "OK" if ok else "ECHEC", n, QUALITY_MIN_DISTINCT_TRUSTS)
    return ok


def check_duplicates(df: DataFrame, total: int) -> None:
    """Vérifie les doublons sur la clé (trust, medicament, mois) - avertissement seulement."""
    key_cols = ["trust_code", "vmp_snomed_code", "month_display"]
    duplicates = total - df.dropDuplicates(key_cols).count()
    if duplicates > 0:
        log.warning("[AVERTISSEMENT] %d doublon(s) detecte(s) sur la cle metier", duplicates)
    else:
        log.info("[OK] Aucun doublon sur la cle metier")


def run_all_checks(df: DataFrame) -> bool:
    total = df.count()
    log.info("Total lignes Silver : %d", total)

    passed = all([
        check_min_rows(total),
        check_null_rate(df, total, "trust_code",      QUALITY_MAX_NULL_RATE_TRUST),
        check_null_rate(df, total, "indicative_cost", QUALITY_MAX_NULL_RATE_COST),
        check_distinct_months(df),
        check_distinct_trusts(df),
    ])

    # Contrôle non bloquant
    check_duplicates(df, total)

    return passed


def main() -> None:
    log.info("=" * 70)
    log.info("ETAPE 03/04 - CONTROLE QUALITE SILVER")
    log.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName("03_Check_Silver_Quality")
        .config("spark.hadoop.fs.defaultFS", HDFS_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(HDFS_SILVER_PATH)
        passed = run_all_checks(df)
    except Exception:
        log.exception("Erreur controle qualite Silver")
        sys.exit(1)
    finally:
        spark.stop()

    if not passed:
        log.error("Controle qualite ECHOUE - arret du pipeline")
        sys.exit(1)

    log.info("Tous les controles qualite sont OK")
    log.info("Etape 03 terminee avec succes")


if __name__ == "__main__":
    main()
