# 🛒 E-Commerce Multi-Source Data Pipeline

## 📋 Einführung

In modernen E-Commerce-Unternehmen fallen täglich große Mengen an Transaktionsdaten aus verschiedenen Quellen an: Webshop-Bestellungen, Kundenstammdaten aus ERP-Systemen, Klickstreams aus Marketing-Plattformen und mehr. Die Herausforderung besteht darin, diese heterogenen Datenquellen in ein zentrales Data Warehouse zu integrieren, um fundierte Geschäftsentscheidungen zu ermöglichen.

Dieses Projekt implementiert eine **produktionsreife, Cloud-native Data Pipeline**, die Kundendaten aus einer On-Premise PostgreSQL-Datenbank und Bestelldaten aus AWS S3 in ein AWS Redshift Data Warehouse lädt. Die Pipeline wird durch Apache Airflow orchestriert und nutzt AWS Lambda für die serverlose Datenextraktion sowie AWS Glue (PySpark) für die Transformation. Ein besonderer Fokus liegt auf der Implementierung von **intelligentem Branching** (Full vs. Incremental Load), um Ressourcen effizient zu nutzen und Cloud-Kosten zu optimieren – eine Fähigkeit, die in der Praxis entscheidend ist.

Das Projekt demonstriert nicht nur technische Implementierung, sondern auch **Best Practices im Data Engineering**: Idempotente Skripte, Data Quality Constraints an der Quelle, Least-Privilege-Zugriffskonzepte und saubere Trennung von Metadaten (Airflow) und Geschäftsdaten (PostgreSQL).

## 🏗️ Architektur & Tech Stack

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
┌─────────────────

│ start_pipeline  │
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│ check_data_volume   │  ← PythonOperator: Prüft erwartetes Volumen
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ branch_decision     │  ← BranchPythonOperator: Entscheidet Routing
└────────┬────────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌──────────────┐
│ full   │ │ incremental  │  ← Lambda Trigger (Payload steuert Load-Typ)
│ load   │ │ load         │
└────┬───┘ └──────┬───────┘
     │            │
     └─────┬──────┘
           │
           ▼
   ┌───────────────┐
   │ join_branches │  ← DummyOperator (trigger_rule='none_failed')
   └───────┬───────┘
           │
           ▼
   ┌───────────────┐
   │ run_glue_etl  │  ← GlueJobOperator (PySpark ETL)
   └───────┬───────┘
           │
           ▼
   ┌───────────────┐
   │ validate_data │  ← PythonOperator (Data Quality Check)
   └───────────────┘
