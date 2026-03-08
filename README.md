@"
# Pipeline SCMD - Secondary Care Medicines Data

Pipeline Big Data de traitement des données de prescription hospitalière du NHS (UK).
Architecture Medallion Bronze -> Silver -> Gold orchestrée par Apache Airflow.

## Architecture
```
CSV (NHS Open Data)
       |
       v
[Bronze HDFS]  - Parquet brut partitionné par YEAR_MONTH
       |
       v
[Silver HDFS]  - Parquet nettoyé partitionné par month_display
       |
       v
[Gold PostgreSQL] - 12 tables KPI
       |
       v
[Grafana Dashboards]
```

## DAG Airflow
```
check_vault  \
              --> download --> ingest --> silver --> quality --> gold --> grafana --> done
check_hdfs   /
```

## Scripts

| Script | Role |
|--------|------|
| 00_check_hdfs_connection.py | Health check HDFS avant demarrage |
| 01_ingest_csv_to_bronze.py  | Ingestion CSV -> Bronze HDFS |
| 02_bronze_to_silver.py      | Nettoyage et filtres metier |
| 03_check_silver_quality.py  | Controle qualite Silver |
| 04_silver_to_gold.py        | Calcul des 12 KPI -> PostgreSQL |

## KPI Gold

**Financiers** : cout total/mois, cout/trust, top 20 medicaments, cout unitaire, variation MoM, contribution MoM

**Prescriptions** : quantite/mois, quantite/trust, top 20 medicaments, mix UOM, volatilite, moyennes mobiles 3M/6M

## Stack technique

- Apache Spark 3.x
- Apache Airflow 2.x
- HDFS
- PostgreSQL + driver JDBC
- HashiCorp Vault (gestion des credentials)
- Grafana (dashboards)

## Prerequis

Connexion SSH Airflow : spark_ssh

Variables Airflow : GRAFANA_URL, GRAFANA_API_KEY

Variable environnement sur le noeud Spark :
export VAULT_TOKEN='votre-token'
export VAULT_ADDR='http://172.31.250.180:8200'

## Configuration

Toutes les constantes sont centralisees dans config/pipeline_config.py.
Aucun credential dans le code - tout passe par Vault ou Variables Airflow.
"@ | Set-Content C:\Desktop\scmd_pipeline\README.md