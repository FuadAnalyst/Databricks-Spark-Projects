# Project 1 – Databricks & Spark (Medallion Architecture)

## 📌 **Project Overview**  
This project demonstrates a **Data Engineering pipeline** built using **Azure Databricks** and **Apache Spark**, following the **Medallion Architecture** (**Bronze → Silver → Gold**).  
The pipeline ingests data from multiple sources, performs transformations using **PySpark** and **SparkSQL**, and publishes processed data to **Power BI** for reporting.

---

## 🔄 **Architecture**  

### **Medallion Layers**  
- **Bronze Layer** – Raw data ingestion from multiple sources  
- **Silver Layer** – Data cleaning, standardization  
- **Gold Layer** – Aggregated, business-ready tables optimized for analytics  

### **Data Sources**  
| **Source Type** | **File Format** | **Description** |
|------------------|-----------------|-----------------|
| CSV              | customer.csv   | Ingested and transformed using PySpark |
| JSON             | products.json    | Transformed using SparkSQL |
| Parquet          | transaction.snappy.parquet | Processed using both SparkSQL & PySpark |

---

## ⚙️ **Tech Stack**  
- **Azure Databricks**
- **Apache Spark** (**PySpark** + **SparkSQL**)  
- **Delta Lake** (Merge/Upsert operations)  
- **Power BI** (connected to Databricks for dashboarding)

---
