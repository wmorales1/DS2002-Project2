"""
Exports dim_product and dim_supplier from Aiven MySQL back to CSVs.
Run this on your laptop. The output CSVs get uploaded to DBFS for the
Databricks notebook to read (workaround for Databricks Serverless not
being able to reach Aiven over JDBC).

Data still originates from Aiven MySQL — this is just a staging step.

Requirements: pip install pymysql sqlalchemy pandas

Before running: paste your Aiven password into AIVEN_PASSWORD below.
"""
import pandas as pd
from sqlalchemy import create_engine

# ============ CONFIG ============
AIVEN_HOST     = "ds2002-mysql-ds-2002.c.aivencloud.com"
AIVEN_PORT     = 24006
AIVEN_USER     = "avnadmin"
AIVEN_PASSWORD = "my password"
AIVEN_DATABASE = "defaultdb"
# ================================

conn_str = f"mysql+pymysql://{AIVEN_USER}:{AIVEN_PASSWORD}@{AIVEN_HOST}:{AIVEN_PORT}/{AIVEN_DATABASE}"
engine = create_engine(
    conn_str,
    pool_recycle=3600,
    connect_args={"ssl": {"ssl_disabled": False}},
)

# Pull dim_product
df_product = pd.read_sql("SELECT * FROM dim_product", engine)
df_product.to_csv("dim_product_from_mysql.csv", index=False)
print(f"dim_product_from_mysql.csv → {len(df_product):,} rows exported")

# Pull dim_supplier
df_supplier = pd.read_sql("SELECT * FROM dim_supplier", engine)
df_supplier.to_csv("dim_supplier_from_mysql.csv", index=False)
print(f"dim_supplier_from_mysql.csv → {len(df_supplier):,} rows exported")

print("\nDone. Now upload these to DBFS:")
print("  /FileStore/ds2002-capstone/source_data/batch/dim_product_from_mysql.csv")
print("  /FileStore/ds2002-capstone/source_data/batch/dim_supplier_from_mysql.csv")
