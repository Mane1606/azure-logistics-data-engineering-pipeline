# 🚚 Logistics Shipment Analytics Platform  
### Azure Databricks | PySpark | Delta Lake | Medallion Architecture

A production-grade **Data Engineering project** that simulates a real-world **Logistics Shipment Analytics Platform** built using Azure Databricks and Delta Lake.  

The platform incrementally ingests shipment data using **Databricks Auto Loader** (parameterized for multiple files), applies data cleaning and transformation logic with **PySpark**, models the data using a **Star Schema**, and serves **Gold analytics tables** for dashboards and reporting. This project demonstrates modern **cloud data engineering best practices** used in enterprise environments.


---

## 📌 Architecture Overview

```
Source CSV Files
      ↓
Bronze (Auto Loader – Streaming Ingestion)
      ↓
Silver (Cleaning + Validation + Dedup + SCD Type 1 + MERGE)
      ↓
Gold (Aggregations + KPIs)
      ↓
Power BI / Databricks Dashboard
```


### Pipeline Flow
![Pipeline](PipelineSS.png)

### Dashboard Preview
![Dashboard](Dashboard.png)

---

## ⚙ Tech Stack

- Azure Databricks  
- PySpark  
- Delta Lake  
- Azure Data Lake Storage Gen2  
- Databricks Auto Loader (cloudFiles)  
- SQL  
- Power BI / Databricks SQL Dashboard  

---

## 🏗 Medallion Architecture

### 🥉 Bronze Layer – Raw Ingestion
**Purpose:** Reliable, scalable ingestion of raw data  

- Streaming ingestion using Databricks Auto Loader  
- **Parameterized ingestion**: supports multiple CSV files in a for-each loop  
- Incremental file detection  
- Schema inference & evolution  
- Exactly-once processing  
- Stored in Delta format  
- Raw, unmodified data  

**Benefits:**  
- No full reloads  
- Handles large-scale files efficiently  
- Production-ready streaming ingestion  
- **Dynamic table creation per file source for traceability**  

---

### 🥈 Silver Layer – Cleaned & Modeled
**Purpose:** Data quality and business transformations  

#### Fact Processing
- Trim and uppercase standardization  
- Multi-format date parsing  
- Null handling  
- Duplicate removal (latest record kept)  
- Incremental loads using Delta MERGE  

#### Dimension Processing
- Type-1 Slowly Changing Dimensions  
- Latest record overwrite logic  
- Standardization and cleansing  

**Benefits:**  
- Trusted, consistent datasets  
- Business-ready structure  

---

### 🥇 Gold Layer – Analytics
**Purpose:** BI and reporting optimized tables  

Pre-built aggregated tables:  
- Daily shipments  
- Revenue metrics  
- Customer summary  
- Product summary  
- Carrier performance  

**Benefits:**  
- Fast queries  
- Dashboard ready  
- Reduced compute costs  

---

## 🔄 Parameterized Pipeline Workflow

### Step 1 – Bronze (Parameterized Auto Loader)
- A **Databricks Job** orchestrates the pipeline.  
- Bronze ingestion is **parameterized for multiple file sources**:  
  - A list of files or directories is passed as parameters.  
  - A **for-each loop** iterates through each file.  
  - Each file is processed individually into a **dedicated Bronze Delta table**.  
  - Schema evolution, incremental processing, and error handling are automatic.  

### Step 2 – Silver
- Cleaning, deduplication, transformations, Delta MERGE  

### Step 3 – Gold
- Aggregations and KPI generation  

### Step 4 – Dashboard
- Power BI or Databricks SQL connects to Gold tables  

**Benefits of parameterization:**  
- Dynamic ingestion without manual intervention  
- Traceable tables per file source  
- Fully automated end-to-end ETL pipeline  

---

## 📊 KPIs Implemented

- Total Shipments  
- Total Revenue  
- Active Customers  
- Delivery Completion %  
- Revenue by Country  
- Revenue by Category  
- Shipment by category

---

## ✅ Engineering Concepts Demonstrated

- Medallion Architecture  
- Streaming ingestion with Auto Loader  
- **Parameterized pipeline & dynamic table creation**  
- Delta Lake MERGE (upserts)  
- Incremental processing  
- Deduplication strategies  
- Type-1 SCD handling  
- Star Schema modeling  
- Data quality checks  
- Scalable cloud storage design  
- BI serving layer  

---

## 🎯 Skills Showcased

- End-to-end data pipeline design  
- Cloud data lake implementation  
- Production ETL/ELT engineering  
- Performance optimization with Delta  
- Analytical data modeling  
- Dashboard-ready serving layer  

---

## 👨‍💻 Author

Yash Mane  
Data Engineer

