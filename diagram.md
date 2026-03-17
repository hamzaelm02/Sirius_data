# SCMD Pipeline & Microservices Architecture

This diagram illustrates the infrastructure and application components making up the SCMD (Secondary Care Medicines Data) pipeline and its serving layer. The infrastructure is represented by boxes identified by their target IP addresses.

```mermaid
flowchart TB
    %% Definitions of infrastructure nodes as subgraphs with IP addresses
    
    subgraph External ["External Data Provider"]
        NHS_Open_Data["NHS Open Data API (CSV)"]
    end

    subgraph VM_Airflow ["VM: 10.0.1.10 (Orchestration)"]
        Airflow["Apache Airflow 2.x<br>(scmd_pipeline_dag)"]
    end

    subgraph VM_DataLake ["VM: 10.0.1.20 (spark_ssh / HDFS master)"]
        Spark_Jobs["Apache Spark 3.x<br>(PySpark Scripts 00-04)"]
        HDFS_Bronze[("HDFS: Bronze Zone<br>Raw Parquet<br>Partitioned by YEAR_MONTH")]
        HDFS_Silver[("HDFS: Silver Zone<br>Clean Parquet<br>Partitioned by month_display")]
    end

    subgraph VM_Vault ["VM: 172.31.250.180 (Security)"]
        Vault{"HashiCorp Vault<br>(Secret Management)"}
    end

    subgraph VM_Database ["VM: 10.0.1.30 (Relational Database)"]
        Postgres[("PostgreSQL<br>Gold Zone<br>(12 KPI Tables)")]
    end

    subgraph VM_Microservices ["VM: 10.0.1.40 (Docker Compose Host)"]
        FinanceAPI["Finance API<br>[FastAPI - Port 8001]"]
        PrescriptionAPI["Prescription API<br>[FastAPI - Port 8002]"]
    end

    subgraph VM_BI ["VM: 10.0.1.50 (Business Intelligence)"]
        Grafana["Grafana"]
    end

    %% Control Flow
    Airflow -.->|SSH Execution| Spark_Jobs
    Airflow -.->|API Reload Request| Grafana

    %% Main Data Flow
    Airflow -->|HTTP Download| NHS_Open_Data
    NHS_Open_Data -->|Ingest Raw CSV| HDFS_Bronze
    HDFS_Bronze -->|Clean & Filter| HDFS_Silver
    HDFS_Silver -->|Compute 12 KPIs| Postgres

    %% Consumer Data Flow
    FinanceAPI -->|"Query Finance KPIs (F1-F6)"| Postgres
    PrescriptionAPI -->|"Query Prescription KPIs (P1-P6)"| Postgres
    Grafana -->|Visualize Dashboards| Postgres

    %% Credentials / Security Flow
    Spark_Jobs -.->|Fetch JDBC Credentials| Vault
    FinanceAPI -.->|Fetch DB Credentials| Vault
    PrescriptionAPI -.->|Fetch DB Credentials| Vault

    classDef vmBox fill:#f9f9f9,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5;
    class VM_Airflow,VM_DataLake,VM_Vault,VM_Database,VM_Microservices,VM_BI vmBox;
```
