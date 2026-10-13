# E-Commerce Multi-Source Data Pipeline

## Einführung

In modernen E-Commerce-Unternehmen fallen täglich große Mengen an Transaktionsdaten aus verschiedenen Quellen an: Webshop-Bestellungen, Kundenstammdaten aus ERP-Systemen, Klickstreams aus Marketing-Plattformen und mehr. Die Herausforderung besteht darin, diese heterogenen Datenquellen in ein zentrales Data Warehouse zu integrieren, um fundierte Geschäftsentscheidungen zu ermöglichen.

Dieses Projekt implementiert eine **produktionsreife, Cloud-native Data Pipeline**, die Kundendaten aus einer On-Premise PostgreSQL-Datenbank und Bestelldaten aus AWS S3 in ein AWS Redshift Data Warehouse lädt. Die Pipeline wird durch Apache Airflow orchestriert und nutzt AWS Lambda für die serverlose Datenextraktion sowie AWS Glue (PySpark) für die Transformation. Ein besonderer Fokus liegt auf der Implementierung von **intelligentem Branching** (Full vs. Incremental Load), um Ressourcen effizient zu nutzen und Cloud-Kosten zu optimieren – eine Fähigkeit, die in der Praxis entscheidend ist.

Das Projekt demonstriert nicht nur technische Implementierung, sondern auch **Best Practices im Data Engineering**: Idempotente Skripte, Data Quality Constraints an der Quelle, Least-Privilege-Zugriffskonzepte und saubere Trennung von Metadaten (Airflow) und Geschäftsdaten (PostgreSQL).


## Tech Stack

- **Orchestrierung:** Apache Airflow (DAG mit 8 Tasks & dynamischem Branching)
- **ETL / Verarbeitung:** AWS Glue (PySpark)
- **Data Sources:** 
  - AWS S3 (JSON - Webshop Events/Orders)
  - PostgreSQL (Kundenstammdaten)
- **Micro-ETL / Trigger:** AWS Lambda (Extraktion Postgres -> S3)
- **Data Sink:** AWS Redshift (Data Warehouse)
- **Infrastructure:** Hostinger VPS (Airflow), AWS Cloud



##  Architektur 

```mermaid
flowchart TB
    subgraph "On-Premise (Hostinger VPS)"
        PG[(PostgreSQL<br/>ecom_db<br/>Port: 5433)]
        AIRFLOW[Apache Airflow<br/>DAG Orchestrierung]
    end
    
    subgraph "AWS Cloud"
        LAMBDA[AWS Lambda<br/>extract_postgres_lambda<br/>Python 3.12 + pg8000]
        
        subgraph "S3 Data Lake"
            S3_CUSTOMERS[S3: postgres_customers/<br/>NDJSON Format]
            S3_ORDERS[S3: orders/<br/>NDJSON Format]
        end
        
        GLUE[AWS Glue<br/>PySpark ETL Job<br/>Join & Transform]
        REDSHIFT[(AWS Redshift<br/>Data Warehouse<br/>ecommerce.fact_customer_orders)]
    end
    
    PG -->|"1. Extract<br/>(pg8000, SSL disabled)"| LAMBDA
    LAMBDA -->|"2. Write NDJSON"| S3_CUSTOMERS
    S3_ORDERS -->|"3. Raw Data"| GLUE
    S3_CUSTOMERS -->|"3. Raw Data"| GLUE
    GLUE -->|"4. Load<br/>(JDBC + IAM Role)"| REDSHIFT
    
    AIRFLOW -->|"Trigger Lambda<br/>(BranchPythonOperator)"| LAMBDA
    AIRFLOW -->|"Trigger Glue Job<br/>(GlueJobOperator)"| GLUE
    
    style PG fill:#e1f5ff
    style AIRFLOW fill:#fff4e1
    style LAMBDA fill:#ffe1e1
    style GLUE fill:#e1ffe1
    style REDSHIFT fill:#f0e1ff
    style S3_CUSTOMERS fill:#fff9e1
    style S3_ORDERS fill:#fff9e1

```
##  Der Airflow DAG (8 Tasks, 2 Branches)

Der DAG `ecommerce_data_pipeline` implementiert ein intelligentes Routing basierend auf dem Datenvolumen:

1. `start_pipeline` (Dummy)
2. `check_data_volume` (PythonOperator - Prüft erwartetes Datenaufkommen)
3. `branch_decision` (BranchPythonOperator - Entscheidet über Routing)
   - **Branch A:** `branch_full_load` (Lambda Trigger für kompletten Postgres Export)
   - **Branch B:** `branch_incremental_load` (Lambda Trigger für Daily Delta)
4. `join_branches` (Dummy - Synchronisiert die Pfade)
5. `run_glue_etl` (GlueJobOperator - Führt PySpark Join & Transformation aus)
6. `validate_redshift_data` (PythonOperator - Data Quality Check)



