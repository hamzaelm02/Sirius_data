# 🎯 Scénario de Démonstration Live (SCMD)

Ce document décrit étape par étape les manipulations techniques à réaliser devant le professeur pour prouver que l'ensemble de votre architecture (Pipeline Data + Sécurité Vault + API Microservices) fonctionne correctement.

---

## ⏳ Pré-requis avant l'arrivée du professeur
* **Connecté** en SSH sur votre cluster VMs.
* Ayez **deux terminaux ouverts** :
    1. Un terminal dans le dossier contenant le `docker-compose.yml` (votre VM *Docker Host / Mircroservices*).
    2. Un terminal sur la VM *Spark/HDFS* configuré pour lancer des jobs.
* **Optionnel :** Le navigateur web ouvert prêt à afficher le Swagger (Swagger UI).

---

## 🎬 Action 1 : Le "Reset de Production" (2 min)
*Le but est de montrer que tout votre environnement peut se monter en quelques secondes depuis le code source.*

1. **Dans le Terminal Docker :**
   Exécutez la commande pour tout éteindre et nettoyer.
   ```bash
   docker-compose down -v
   ```
   > *"Monsieur, je commence par une page blanche pour vous montrer le déploiement de zéro."*

2. **Toujours dans le Terminal Docker :**
   Démarrez l'infrastructure (PostgreSQL, Vault, et les APIs).
   ```bash
   docker-compose up -d --build
   ```
   > *"Docker compose vient de démarrer ma base de données PostgreSQL, mon serveur de coffre-fort HashiCorp Vault, mon script de configuration éphémère vault-init, et mes deux APIs FastAPI (Finance et Prescriptions)."*

---

## 🔐 Action 2 : La preuve de l'authentification Vault (2 min)
*Le but est de montrer que vos APIs n'ont pas de mots de passe codés en dur, mais qu'elles ont réussi à se connecter.*

1. **Dans le Terminal Docker :**
   Exécutez cette requête (ou utilisez votre navigateur) :
   ```bash
   curl -s http://localhost:8001/health | jq
   ```
   *Ce qui va s'afficher : `{"status":"ok","service":"finance_api","db_initialized":true}`*
   
   > *"Ici, on voit que l'API Finance est passée au statut 'true' pour la base de données. Cela prouve qu'elle a pu contacter le Vault, s'authentifier, lire le secret sécurisé, et construire dynamiquement la connexion SQLAlchemy ("`engine`") vers PostgreSQL."*

---

## 💥 Action 3 : La Base de Données est Vide (1 min)
*Le but est de montrer que vos APIs pointent bien sur la bonne base, mais que les KPIs n'ont pas encore été calculés par Spark.*

1. **Sur le Navigateur :**
   Ouvrez le Swagger de FastAPI : `http://<IP_DE_VOTRE_VM>:8001/docs`
2. **Cliquez sur l'endpoint `/kpis/f1`**, puis **"Try it out"** et **"Execute"**.
3. **Pointez l'écran :**
   L'API retourne une Erreur 500 : `relation "gold_cost_per_month" does not exist`.

   > *"C'est tout à fait normal. L'infrastructure web est prête, mais le pipeline de données n'a pas encore tourné aujourd'hui. Spark n'a pas encore publié ses statistiques dans PostgreSQL."*

---

## ⚡ Action 4 : Exécution du Pipeline Big Data (3 min)
*Le but est de prouver votre chaîne Hadoop/Spark devant le jury.*

1. **Changez de Terminal (allez sur la VM Spark) :**
   Lancez à la main le script PySpark de la dernière étape du pipeline (Bronze/Silver vers Gold).
   ```bash
   spark-submit --jars /home/spark/postgresql.jar /home/spark/scripts/04_silver_to_gold.py
   ```
   *(Pendant que Spark calcule et affiche ses logs `INFO`, commentez l'action)* : 
   > *"Habituellement, c'est Airflow qui déclenche ce script toutes les nuits. Ici, je lance le job Spark qui prend les données parquet de la zone Silver sur le cluster HDFS, calcule les indicateurs complexes, et les pousse en BDD."*

---

## 🏆 Action 5 : Résultat final et Microservices (2 min)
*Le but est de fermer la boucle et de montrer que l'API distribue instantanément la nouvelle donnée.*

1. **Retournez sur le Navigateur (Swagger) :**
   Cliquez à nouveau sur **"Execute"** pour l'endpoint `/kpis/f1`.
2. **La magie opère :**
   Le JSON s'affiche instantanément avec le top des coûts des médicaments par mois calculés !

   > *"Spark a terminé, les données Gold sont en base. Notre API n'a subi aucune interruption (Zéro Downtime), et la nouvelle information est maintenant disponible de manière traçable et sécurisée pour le Frontend (Next.js) ou Grafana de mon collègue."*

---
## ✨ Fin de la Démontration
*"Avez-vous des questions sur un point technique précis de ce processus ?"*
