# -*- coding: utf-8 -*-
"""Health check HDFS - vérifie l'accessibilité depuis Spark avant de lancer le pipeline."""

import sys
import logging

from pyspark.sql import SparkSession

sys.path.insert(0, "/home/spark/scripts")
from config.pipeline_config import HDFS_MASTER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("check_hdfs")


def check_hdfs(spark: SparkSession) -> bool:
    """Liste la racine HDFS via l'API Hadoop JVM. Retourne False si inaccessible."""
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        jvm = spark.sparkContext._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(HDFS_MASTER),
            hadoop_conf,
        )
        statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path("/"))
        dirs = [s.getPath().getName() for s in statuses]
        log.info("Contenu HDFS (/) : %s", dirs)
        return True
    except Exception:
        log.exception("Impossible d'acceder a HDFS")
        return False


def main() -> None:
    log.info("=" * 60)
    log.info("HEALTH CHECK - CONNEXION HDFS")
    log.info("=" * 60)

    spark = (
        SparkSession.builder
        .appName("00_Check_HDFS_Connection")
        .config("spark.hadoop.fs.defaultFS", HDFS_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        ok = check_hdfs(spark)
    finally:
        spark.stop()

    if not ok:
        log.error("Health check HDFS ECHOUE - arret du pipeline")
        sys.exit(1)

    log.info("Health check HDFS OK")


if __name__ == "__main__":
    main()
