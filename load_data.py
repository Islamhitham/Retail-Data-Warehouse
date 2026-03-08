"""
load_raw_data.py
────────────────
Loads the 6 CSV files from `datasets/` into SQL Server as raw tables.
These tables are defined as dbt sources in models/bronze/sources.yml.

Run with:
    python load_raw_data.py

Requires:
    pip install pandas pyodbc sqlalchemy
"""

import pandas as pd
from sqlalchemy import create_engine, text
import urllib

# ── Connection ──────────────────────────────────────────────────────────────
SERVER   = r"LAPTOP-L54MSQG1\SQLEXPRESS" 
DATABASE = "dwh"
DRIVER   = "ODBC Driver 17 for SQL Server"

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

# ── Schema setup ─────────────────────────────────────────────────────────────
SCHEMAS = ["source_crm", "source_erp"]

with engine.connect() as conn:
    for schema in SCHEMAS:
        conn.execute(text(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]')"))
    conn.commit()

print("e:\first project\dataset Schemas created: source_crm, source_erp")

# ── File → table mapping ──────────────────────────────────────────────────────
DATASETS_DIR = r"C:\Users\islam hitham\dbt_project\dataset"
files = [
    # (csv_path,                           schema,       table_name)
    (f"{DATASETS_DIR}/source_crm/cust_info.csv",       "source_crm", "cust_info"),
    (f"{DATASETS_DIR}/source_crm/prd_info.csv",        "source_crm", "prd_info"),
    (f"{DATASETS_DIR}/source_crm/sales_details.csv",   "source_crm", "sales_details"),
    (f"{DATASETS_DIR}/source_erp/CUST_AZ12.csv",       "source_erp", "cust_az12"),
    (f"{DATASETS_DIR}/source_erp/LOC_A101.csv",        "source_erp", "loc_a101"),
    (f"{DATASETS_DIR}/source_erp/PX_CAT_G1V2.csv",     "source_erp", "px_cat_g1v2"),
]

# ── Load ─────────────────────────────────────────────────────────────────────
for csv_path, schema, table in files:
    print(f" Loading {csv_path}  →  [{schema}].[{table}] ...", end=" ", flush=True)
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)  # everything as string to avoid type coercion issues
    df.columns = [c.strip().lower() for c in df.columns]           # lower-case column names

    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="replace",   # drop & recreate each run
        index=False,
        chunksize=1000,
    )
    print(f" {len(df):,} rows loaded.")

print("\n All raw tables loaded successfully!")
