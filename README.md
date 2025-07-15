# 🚀 Azure Data Engineering Project: End-to-End Lakehouse Pipeline

🔧 Overview
This project demonstrates a modern Data Lakehouse architecture using various Azure services to ingest, transform, store, and visualize data efficiently.

## 🔧 Technologies Used

📍 Source: On-Prem SQL Server
📍 Destination: Azure Synapse Analytics + Power BI
📍 Transformation: Azure Databricks using PySpark
📍 Storage: Azure Data Lake Gen2 with Bronze, Silver, and Gold layers
📍 Security: Azure Key Vault for credential management


## 📊 Architecture Diagram

<img width="1107" height="577" alt="image" src="https://github.com/user-attachments/assets/2d67d06c-701f-438b-b3c4-91c705e92a49" />

## 📊 Project Flow (ETL Steps) 

1.Data Ingestion
   
  -  Used Azure Data Factory to pull data from On-Prem SQL Server.

  -  Credentials stored securely in Azure Key Vault

2.Data Storage

  -  Raw data stored in Bronze layer (unprocessed).

  -  Cleaned and deduplicated data moved to Silver layer.

  -  Aggregated and business-ready data pushed to Gold layer.

3.Data Transformation

  -  PySpark jobs in Azure Databricks for:

      -  Cleaning and deduplication

      -  Joins and aggregations

      -  Delta Lake operations

4.Data Warehouse

  -  Gold layer data loaded into Azure Synapse Analytics using ADF for analytics queries.

5.Visualization

      -  Connected Power BI to Synapse for building dashboards and insights. 

## 📌 Key Features

✅ End-to-End Azure Data Pipeline (Ingestion to Dashboard)

✅ Follows Medallion Architecture (Bronze, Silver, Gold)

✅ Delta Lake for versioned, ACID-compliant data

✅ Parameterized ADF pipelines

✅ Visual reports in Power BI


##  👨‍💻 Author

Pushkar Kulkarni
Data Engineer Enthusiast | Azure | PySpark | SQL | Power BI

    




