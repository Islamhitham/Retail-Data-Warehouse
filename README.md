# Retail Data Warehouse: End-to-End Analytics Pipeline

A production-ready data engineering project implementing a **Medallion Architecture** (Bronze, Silver, Gold) using **dbt**, **Airflow**, and **SQL Server**.

This repository showcases a complete Data Engineering lifecycle: from raw CSV extraction via Python, to dbt transformations, and finally to automated orchestration using Airflow within a Docker container executing on a Windows host via SSH.

---

##  Project Overview
This project automates the extraction, transformation, and loading (ETL) of retail data from disparate sources (CRM and ERP) into a clean, business-ready **Star Schema**. It ensures data quality through rigorous testing and centralizes orchestration via Airflow.

###  Tech Stack
- **Data Transformation**: [dbt](https://www.getdbt.com/) (Core of the transformation logic)
- **Orchestration**: [Apache Airflow](https://airflow.apache.org/) (Running in Docker)
- **Database**: SQL Server (SQLExpress)
- **Ingestion**: Python (Pandas & SQLAlchemy)

---

## Data Architecture (Medallion Layers)

### 1. Data Ingestion & Bronze Layer (Raw)
Raw data is provided as 6 individual flat CSV files representing two different business units:
- **CRM Sources**: `cust_info`, `prd_info`, `sales_details`
- **ERP Sources**: `CUST_AZ12`, `LOC_A101`, `PX_CAT_G1V2`

**Ingestion Process (`load_data.py`)**:
A custom Python script utilizes `pandas` and `sqlalchemy` to extract these CSVs and load them directly into SQL Server. 
- Creates isolated schemas (`source_crm`, `source_erp`).
- Uses `fast_executemany=True` for optimized bulk inserts via ODBC.
- Reads everything as strings to prevent premature Pandas type coercion, leaving the strict casting entirely to dbt.

###  2. Silver Layer (Cleanse & Standardize)
The Silver layer is responsible for creating a Single Source of Truth by cleansing, deduplicating, and standardizing the raw data from both systems.

**Key Transformations**:
- **Deduplication**: CRM customer info frequently contained duplicate keys due to system updates. We implemented a `ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC)` window function to filter and keep only the absolute latest (`flag_last = 1`) record for each customer.
- **Data Cleaning**: Applied `TRIM()` across all string columns to eliminate trailing whitespaces often found in legacy ERP extracts.
- **Data Type Casting**: Strict casting of dates and numerics in the dbt models.
- **Categorical Standardization**: Realigned mismatched business codes:
    - Gender: `M/F` ➔ `Male/Female`
    - Marital Status: `S/M` ➔ `Single/Married`
- **Null Handling**: COALESCE functions implemented to handle missing data gracefully.

###  3. Gold Layer (Business / Star Schema)
The Gold layer integrates the cleansed CRM and ERP data into a highly optimized dimensional **Star Schema** designed for BI reporting.

- **Dimension Tables**: 
    - `dim_customers`: A unified view of customers prioritizing CRM data, but enriching missing attributes (like Birthdate or Country) by joining ERP data (`cust_az12`, `loc_a101`) using the common `cst_key` / `cid`. It generates a contiguous `customer_key` surrogate key.
    - `dim_products`: Enriched product catalog joining CRM product info with ERP category mappings.
- **Fact Table**: 
    - `fct_sales`: Transactional data optimized for analytical queries. It maps raw business keys directly to their respective surrogate keys (`customer_key`, `product_key`) from the Dimension tables to ensure referential integrity.

---

##  Orchestration (Airflow & Docker)
The pipeline is fully automated via an Airflow DAG (`dbt_daily_full_refresh`).

### The Docker-to-Host Bridge Architecture
Because Airflow is running inside a Linux Docker container while the Data Warehouse and dbt environment live on the Windows Host, a specialized bridge was built.

**Airflow `SSHOperator`**: Airflow uses SSH to connect directly to the `host.docker.internal` Windows machine.

### DAG Workflow
1. **`dbt_deps`**: Ensures all dbt packages (e.g., `dbt-sqlserver`) are up to date.
2. **`dbt_run_bronze` ➔ `silver` ➔ `gold`**: Sequential, dependent tasks that execute a `--full-refresh` build.
3. **`dbt_test`**: Triggers the comprehensive data quality testing suite suite.

---

##  Data Quality & Testing
Data integrity is maintained using dbt's testing framework to guarantee the Star Schema is robust.

- **Generic YAML Tests**: Deployed `unique`, `not_null`, `relationships` (foreign keys ensuring facts map to dimensions), and `accepted_values` across all layers.
- **Singular SQL Tests**:
    - `assert_sales_positive`: Ensures `sales_amount` >= 0.
    - `assert_ship_after_order`: Validates that `shipping_date` never precedes `order_date`.
    - `assert_no_orphan_orders`: Validates that every record in the sales fact table successfully joined to a dimension key.
- **Results**: **66/66 Data Quality Tests Passing** 

---

##  Project Structure
```text
├── dataset/             # Raw CSV data files
├── dags/                # Airflow orchestration logic (DAGs)
├── models/
│   ├── bronze/          # Source definitions & Staging
│   ├── silver/          # Cleaning, Deduplication & Standardization
│   └── gold/            # Final Star Schema (Dim & Fact)
├── tests/               # Singular custom SQL assertions
├── load_data.py         # Python raw ingestion script
└── dbt_project.yml      # dbt configuration
```

---