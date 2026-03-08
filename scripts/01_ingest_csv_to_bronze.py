# -*- coding: utf-8 -*-
"""Etape 1/4 - Ingestion des CSV SCMD vers la couche Bronze HDFS (Parquet)."""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

sys.path.insert(0, "/home/spark/scripts")
from config.pipeline_config import HDFS_MASTER, HDFS_BRONZE_PATH, CSV_SOURCE_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_csv_to_bronze")


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    log.info("Lecture CSV depuis : %s", path)
    df = spark.read.csv(path, header=True, inferSchema=True)
    log.info("Lignes lues : %d", df.count())
    return df


def write_bronze(df: DataFrame, output_path: str) -> None:
    log.info("Ecriture Bronze vers : %s", output_path)
    (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .write
        .mode("overwrite")
        .partitionBy("YEAR_MONTH")
        .parquet(output_path)
    )
    log.info("Bronze ecrit avec succes")


def main() -> None:
    log.info("=" * 70)
    log.info("ETAPE 01/04 - CSV -> BRONZE HDFS")
    log.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName("01_Ingest_CSV_to_Bronze")
        .config("spark.hadoop.fs.defaultFS", HDFS_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = read_csv(spark, CSV_SOURCE_PATH)
        df.printSchema()
        df.show(5, truncate=True)
        write_bronze(df, HDFS_BRONZE_PATH)
    except Exception:
        log.exception("Echec ingestion CSV -> Bronze")
        sys.exit(1)
    finally:
        spark.stop()

    log.info("Etape 01 terminee avec succes")


if __name__ == "__main__":
    main()
