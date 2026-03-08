# -*- coding: utf-8 -*-
"""
Pipeline SCMD complet - Bronze -> Silver -> Gold -> Grafana.

Prérequis Airflow :
  - Connexion SSH  : spark_ssh
  - Variables      : GRAFANA_URL, GRAFANA_API_KEY
  - Env Spark      : VAULT_TOKEN défini sur le nœud Spark
"""

import os
import glob
import time
import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

log = logging.getLogger(__name__)

SCRIPTS_DIR  = "/home/spark/scripts"
SPARK_BIN    = "/opt/spark/bin/spark-submit"
POSTGRES_JAR = "file:///home/spark/postgresql.jar"
SCMD_BASE_URL = "https://opendata.nhsbsa.net/api/3/action/datastore_search"
SCMD_DATA_DIR = "/home/spark/scmd_data"

# IDs des ressources SCMD sur le portail NHSBSA Open Data
SCMD_RESOURCES = {
    "scmd-2025-01": "scmd_2025_01.csv",
    "scmd-2025-02": "scmd_2025_02.csv",
    "scmd-2025-03": "scmd_2025_03.csv",
    "scmd-2025-04": "scmd_2025_04.csv",
    "scmd-2025-05": "scmd_2025_05.csv",
    "scmd-2025-12": "scmd_2025_12.csv",
}

default_args = {
    "owner":            "Hamza El Moukadem",
    "depends_on_past":  False,
    "email":            ["elmoukadem62@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "start_date":       datetime(2026, 2, 27),
}


def check_vault_accessible(**context) -> str:
    """Vérifie que Vault répond sur /sys/health avant de démarrer le pipeline."""
    vault_addr = os.getenv("VAULT_ADDR", "http://172.31.250.180:8200")
    log.info("Verification Vault : %s", vault_addr)

    try:
        resp = requests.get(f"{vault_addr}/v1/sys/health", timeout=5)
        # 200 = unsealed, 429 = standby - les deux sont OK
        if resp.status_code not in (200, 429):
            raise ConnectionError(f"Vault repond HTTP {resp.status_code} : {resp.text}")
    except requests.exceptions.ConnectionError as exc:
        raise ConnectionError(f"Impossible de joindre Vault ({vault_addr})") from exc

    log.info("Vault accessible (HTTP %d)", resp.status_code)
    return "vault_ok"


def download_scmd_csv(**context) -> str:
    """
    Télécharge les fichiers CSV SCMD depuis le portail NHS Open Data.
    Si les fichiers sont déjà présents dans SCMD_DATA_DIR, le téléchargement est ignoré.
    """
    os.makedirs(SCMD_DATA_DIR, exist_ok=True)

    existing = glob.glob(os.path.join(SCMD_DATA_DIR, "*.csv"))
    if existing:
        log.info(
            "%d fichier(s) deja presents, telechargement ignore : %s",
            len(existing), [os.path.basename(f) for f in existing],
        )
        return f"{len(existing)} fichiers deja presents"

    downloaded = []
    total_size  = 0.0

    for resource_id, filename in SCMD_RESOURCES.items():
        dest = os.path.join(SCMD_DATA_DIR, filename)
        url  = f"{SCMD_BASE_URL}?resource_id={resource_id}&limit=9999999"
        log.info("[%s] Telechargement -> %s", resource_id, dest)

        try:
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()

            bytes_written = 0
            with open(dest, "wb") as fh:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
                        bytes_written += len(chunk)

            if bytes_written == 0:
                raise ValueError(f"Fichier vide apres telechargement : {filename}")

            size_mb = bytes_written / (1024 * 1024)
            total_size += size_mb
            log.info("[OK] '%s' - %.2f Mo", filename, size_mb)
            downloaded.append(filename)

        except requests.exceptions.HTTPError as exc:
            log.error("Erreur HTTP '%s' : %s", filename, exc)
            raise
        except Exception as exc:
            log.error("Echec telechargement '%s' : %s", filename, exc)
            raise

    log.info("Telechargement termine : %d fichier(s), %.2f Mo", len(downloaded), total_size)
    return f"{len(downloaded)} fichiers telecharges ({total_size:.1f} Mo)"


def notify_grafana(**context) -> str:
    """Recharge les dashboards Grafana via l'API Admin. Credentials lus depuis les Variables Airflow."""
    grafana_url = Variable.get("GRAFANA_URL")
    api_key     = Variable.get("GRAFANA_API_KEY")

    try:
        resp = requests.post(
            f"{grafana_url}/api/admin/provisioning/dashboards/reload",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            log.info("Dashboards Grafana recharges")
        else:
            # Non bloquant : Grafana peut être temporairement indisponible
            log.warning("Grafana HTTP %d : %s", resp.status_code, resp.text)

    except requests.exceptions.RequestException as exc:
        log.error("Erreur notification Grafana : %s", exc)

    time.sleep(10)
    return "grafana_notifie"


with DAG(
    dag_id="scmd_pipeline",
    default_args=default_args,
    description="Pipeline SCMD : CSV -> Bronze -> Silver -> Gold -> Grafana",
    schedule="0 3 * * *",
    catchup=False,
    tags=["scmd", "bigdata", "grafana"],
    doc_md=__doc__,
) as dag:

    # Pré-checks en parallèle avant de toucher aux données
    t_check_vault = PythonOperator(
        task_id="check_vault",
        python_callable=check_vault_accessible,
    )

    t_check_hdfs = SSHOperator(
        task_id="check_hdfs",
        ssh_conn_id="spark_ssh",
        command=f"cd {SCRIPTS_DIR} && {SPARK_BIN} 00_check_hdfs_connection.py",
        cmd_timeout=300,
    )

    t_download = PythonOperator(
        task_id="download_csv_files",
        python_callable=download_scmd_csv,
    )

    t_ingest = SSHOperator(
        task_id="ingest_csv_to_bronze",
        ssh_conn_id="spark_ssh",
        command=f"cd {SCRIPTS_DIR} && {SPARK_BIN} 01_ingest_csv_to_bronze.py",
        cmd_timeout=3600,
    )

    t_clean = SSHOperator(
        task_id="bronze_to_silver",
        ssh_conn_id="spark_ssh",
        command=f"cd {SCRIPTS_DIR} && {SPARK_BIN} 02_bronze_to_silver.py",
        cmd_timeout=3600,
    )

    t_quality = SSHOperator(
        task_id="check_silver_quality",
        ssh_conn_id="spark_ssh",
        command=f"cd {SCRIPTS_DIR} && {SPARK_BIN} 03_check_silver_quality.py",
        cmd_timeout=600,
    )

    t_gold = SSHOperator(
        task_id="silver_to_gold",
        ssh_conn_id="spark_ssh",
        command=f"cd {SCRIPTS_DIR} && {SPARK_BIN} --jars {POSTGRES_JAR} 04_silver_to_gold.py",
        cmd_timeout=3600,
    )

    t_grafana = PythonOperator(
        task_id="notify_grafana",
        python_callable=notify_grafana,
    )

    t_done = EmptyOperator(task_id="pipeline_complete")

    # check_vault et check_hdfs s'exécutent en parallèle, le download attend les deux
    [t_check_vault, t_check_hdfs] >> t_download
    t_download >> t_ingest >> t_clean >> t_quality >> t_gold >> t_grafana >> t_done
