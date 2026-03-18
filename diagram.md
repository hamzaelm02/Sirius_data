# SCMD Pipeline & Web Application Architecture

Ce diagramme illustre l'architecture complète du projet, regroupant deux systèmes distincts mais complémentaires : 
1. **La partie Data/Analytics (Pipeline Big Data & APIs BI)** : Traitement des données du NHS via Spark, stockage final sécurisé et distribution via API FastAPI/Grafana.
2. **La partie Web Application (Gestion & Authentification)** : Application utilisateur sécurisée avec Keycloak via l'approche "Gatekeeper", microservices Node.js, et CI/CD automatisée.

```mermaid
flowchart TB
    %% ==========================================
    %% PARTIE 1 : BIG DATA & ANALYTICS (Votre Partie)
    %% ==========================================
    subgraph Data_Analytics ["📊 PARTIE 1: Data Pipeline & Analytics APIs"]
        direction TB
        
        subgraph VM_Airflow ["VM: 10.0.1.10 (Orchestration)"]
            Airflow["Apache Airflow 2.x<br>(scmd_pipeline_dag)"]
        end

        subgraph VM_DataLake ["VM: 10.0.1.20 (HDFS Master)"]
            Spark_Jobs["Apache Spark 3.x<br>(Scripts 00-04)"]
            HDFS_Bronze[("HDFS Bronze<br>Raw CSV")]
            HDFS_Silver[("HDFS Silver<br>Clean Parquet")]
        end

        subgraph VM_Vault ["VM: 172.31.250.180 (Securité)"]
            Vault{"HashiCorp Vault<br>(Gestion des Secrets)"}
        end

        subgraph VM_Database_1 ["VM: 10.0.1.30 (BDD Analytics)"]
            Postgres_Data[("PostgreSQL Analytics<br>Gold Zone (KPIs)")]
        end

        subgraph VM_Microservices_API ["VM: 10.0.1.40 (Docker APIs)"]
            FinanceAPI["Finance API<br>[FastAPI - Python]"]
            PrescriptionAPI["Prescription API<br>[FastAPI - Python]"]
        end

        subgraph VM_BI ["VM: 10.0.1.50 (Business Intelligence)"]
            Grafana["Grafana Dashboards"]
        end

        NHS_Open_Data["NHS Open Data API (Source)"]
        
        %% Flux Data
        NHS_Open_Data -->|Ingestion Brute| HDFS_Bronze
        HDFS_Bronze -.->|Nettoyage (Spark)| HDFS_Silver
        HDFS_Silver -.->|Calcul KPIs (Spark)| Postgres_Data
        
        FinanceAPI -->|"Query Finance KPIs"| Postgres_Data
        PrescriptionAPI -->|"Query Prescription KPIs"| Postgres_Data
        Grafana -->|"Visualize Data"| Postgres_Data

        %% Flux Control & Auth
        Airflow -.->|Orchestration| Spark_Jobs
        Airflow -.->|Reload Dashboards| Grafana
        Spark_Jobs -.->|Lecture Mots de Passe| Vault
        FinanceAPI -.->|Lecture Mots de Passe| Vault
        PrescriptionAPI -.->|Lecture Mots de Passe| Vault
    end

    %% ==========================================
    %% PARTIE 2 : WEB APPLICATION & DEVOPS (Collègue)
    %% ==========================================
    subgraph Web_Application ["💻 PARTIE 2: Web Application & DevOps"]
        direction TB

        subgraph VM_CICD ["VM 1: CI/CD"]
            GitlabRunner["GitLab Runner<br>(Build & Deploy)"]
        end

        subgraph VM_RProxy ["VM 6: Edge Proxy"]
            Nginx["Nginx Reverse Proxy<br>('Approche Gatekeeper')"]
        end

        subgraph VM_IAM ["VM 5: Identity & Access"]
            Keycloak{"Keycloak<br>(Authentification IAM)"}
        end

        subgraph VM_Frontend ["VM 4: Frontend Host"]
            NextJS["IHM UI<br>[Next.js]"]
        end

        subgraph VM_Backend ["VM 3: Backend Host (Docker)"]
            GardService["GardService<br>[Express.js]"]
            ViewService["ViewService<br>[Express.js]"]
        end

        subgraph VM_Database_2 ["VM 2: BDD Application"]
            Postgres_App[("PostgreSQL App<br>(Données Métiers)")]
        end

        %% Utilisateur Externe
        UserExtern((Utilisateur Externe))

        %% Flux Gatekeeper (Auth Proxy)
        UserExtern -->|"Requête (Non-Auth / ou Cookie)"| Nginx
        Nginx -.->|"1. Redirection Auth (Si Non-Auth)"| Keycloak
        Keycloak -.->|"2. Retour Token d'Accès"| Nginx
        Nginx -->|"3. Header Injecté (X-Forwarded-User)"| NextJS
        Nginx -->|"4. Routage Sécurisé API"| GardService
        Nginx -->|"5. Routage Sécurisé API"| ViewService
        
        NextJS -->|Appels Backends| Nginx

        %% Flux Backend vers DB
        GardService -->|"Lectures/Écritures"| Postgres_App
        ViewService -->|"Lectures/Écritures"| Postgres_App

        %% Flux CI/CD
        GitlabRunner -.->|"Deploiements Conteneurs"| NextJS
        GitlabRunner -.->|"Deploiements Conteneurs"| GardService
        GitlabRunner -.->|"Deploiements Conteneurs"| ViewService
        GitlabRunner -.->|"Deploiement IAM"| Keycloak
    end

    %% Style de base
    classDef dataBox fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
    classDef webBox fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef extUser fill:#eeeeee,stroke:#333,stroke-width:3px;

    class VM_Airflow,VM_DataLake,VM_Vault,VM_Database_1,VM_Microservices_API,VM_BI dataBox;
    class VM_CICD,VM_RProxy,VM_IAM,VM_Frontend,VM_Backend,VM_Database_2 webBox;
    class UserExtern extUser;

```
