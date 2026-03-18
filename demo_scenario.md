# Scénario de Démonstration : Architecture SCMD & Intégration Sécurisée

Ce document présente les étapes de validation permettant d'éprouver l'architecture mise en place, allant de l'ingestion des données à leur restitution sécurisée via les Microservices.

## Objectif de la Démonstration
Prouver le bon fonctionnement de la chaîne de bout en bout :
1. Déploiement automatisé et sécurisé de l'infrastructure via Docker Compose.
2. Injection statique des secrets de la base de données via HashiCorp Vault.
3. Authentification des Microservices auprès de Vault pour initialiser les connexions PostgreSQL en mémoire.
4. Traitement Big Data (Spark) et restitution temps réel par les APIs.

---

## Étape 1 : Initialisation de l'Infrastructure Docker
L'infrastructure applicative (Microservices, Base de Données Gold, et Coffre-fort Vault) est définie pour être redéployable à la demande.

**Action :**
Démarrage des services à partir du fichier `docker-compose.yml`.

```bash
docker-compose up -d --build
```

**Résultat attendu :**
- Le conteneur `scmd_postgres` (Base de Données) démarre.
- Le conteneur `scmd_vault` démarre en mode développement.
- Le conteneur éphémère `scmd_vault_init` s'exécute, inscrit les identifiants PostgreSQL de manière sécurisée dans Vault, puis s'arrête (`Exited 0`).
- Les conteneurs `scmd_finance_api` et `scmd_prescription_api` démarrent après validation des Healthchecks de dépendance.

---

## Étape 2 : Vérification de l'Intégration Vault (Zero-Trust)
Les Microservices ne contiennent aucun mot de passe en dur. À leur lancement, ils effectuent un appel API interne vers Vault.

**Action :**
Test du endpoint de santé (`/health`) sur l'API Finance.

```bash
curl -s http://localhost:8001/health
```

**Résultat attendu :**
```json
{"status": "ok", "service": "finance_api", "db_initialized": true}
```
*Le flag `db_initialized: true` démontre que l'application a réussi à s'authentifier auprès de Vault via son Token, à lire le secret, et à instancier le moteur SQLAlchemy connecté à PostgreSQL. En cas d'échec d'authentification ou si le secret est absent, le serveur refuse de démarrer.*

---

## Étape 3 : Traitement de la Donnée (Airflow/Spark)
Une fois l'infrastructure prête, les tables métiers dans PostgreSQL sont initialement vides ou non créées, attendant l'exécution complète du pipeline Data.

**Action :**
Simulation de l'orchestration Airflow en exécutant la dernière étape du pipeline (Script PySpark de traitement Silver vers Gold) sur le noeud Master HDFS.

```bash
spark-submit --jars /home/spark/postgresql.jar /home/spark/scripts/04_silver_to_gold.py
```

**Résultat attendu :**
- Spark lit les données nettoyées depuis la zone Silver (HDFS).
- Spark calcule les agrégations complexes (ex: Variations MoM, Coûts Unitaire).
- Spark crée et peuple les 12 tables KPIs dans `scmd_gold` via une connexion JDBC.

---

## Étape 4 : Validation de la Restitution (APIs Microservices)
Dès que le script Spark termine, les données doivent être immédiatement disponibles de manière sécurisée pour les applications frontales ou les outils BI (Grafana / Next.js).

**Action :**
Interrogation d'un endpoint de la Data Finance (ex: KPI F1 - Coût total par mois).

```bash
curl -s http://localhost:8001/kpis/f1
```

**Résultat attendu :**
L'API retourne un tableau JSON contenant les données fraîchement calculées par Spark.

```json
[
  {"month_display": "2025-08", "total_cost": 142580.50},
  {"month_display": "2025-09", "total_cost": 150230.10}
]
```

## Conclusion
Ces quatres étapes valident que le couplage entre l'ingestion massive (Spark/HDFS) et l'exposition sécurisée (Vault/FastAPI) est opérationnel, résilient, et conforme aux standards DevSecOps de découplage des secrets industriels.
