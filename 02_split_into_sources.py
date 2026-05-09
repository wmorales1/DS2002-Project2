"""
Splits the REAL Montgomery County retail_sales.csv into:
  - dim_product.csv     → load to Azure SQL Database
  - dim_supplier.csv    → load to Azure SQL Database
  - dim_item_type.json  → load to MongoDB Atlas
  - dim_date.csv        → upload to DBFS
  - fact_retail_sales_01.json, _02.json, _03.json → upload to DBFS for AutoLoader

Schema matches the midterm exactly:
  - dim_date: date_key, full_date  (one row per month, like the midterm)
  - dim_item_type: item_type_key, item_type
  - dim_product: product_key, item_code, item_description, item_type
  - dim_supplier: supplier_key, supplier_name
  - fact: date_key, product_key, supplier_key, item_type_key,
          retail_sales, retail_transfers, warehouse_sales

Run from the same folder as retail_sales.csv:
    python 02_split_into_sources.py
"""
import json
import pandas as pd

INPUT = "retail_sales.csv"

df = pd.read_csv(INPUT)
print(f"Loaded {len(df):,} source rows from {INPUT}")
print(f"Columns: {list(df.columns)}\n")

# ---------- DIM_PRODUCT (→ Azure SQL) ----------
df_product = (
    df[["ITEM CODE", "ITEM DESCRIPTION", "ITEM TYPE"]]
    .drop_duplicates(subset=["ITEM CODE"])
    .rename(columns={
        "ITEM CODE": "item_code",
        "ITEM DESCRIPTION": "item_description",
        "ITEM TYPE": "item_type",
    })
    .reset_index(drop=True)
)
df_product.insert(0, "product_key", range(1, len(df_product) + 1))
df_product.to_csv("dim_product.csv", index=False)
print(f"dim_product.csv  → {len(df_product):,} rows  (load to Azure SQL)")

# ---------- DIM_SUPPLIER (→ Azure SQL) ----------
df_supplier = (
    df[["SUPPLIER"]]
    .drop_duplicates()
    .rename(columns={"SUPPLIER": "supplier_name"})
    .reset_index(drop=True)
)
df_supplier.insert(0, "supplier_key", range(1, len(df_supplier) + 1))
df_supplier.to_csv("dim_supplier.csv", index=False)
print(f"dim_supplier.csv → {len(df_supplier):,} rows  (load to Azure SQL)")

# ---------- DIM_ITEM_TYPE (→ MongoDB Atlas) ----------
df_item_type = (
    df[["ITEM TYPE"]]
    .drop_duplicates()
    .rename(columns={"ITEM TYPE": "item_type"})
    .reset_index(drop=True)
)
df_item_type.insert(0, "item_type_key", range(1, len(df_item_type) + 1))
docs = df_item_type.to_dict(orient="records")
with open("dim_item_type.json", "w") as f:
    json.dump(docs, f, indent=2)
print(f"dim_item_type.json → {len(docs):,} rows  (load to MongoDB Atlas)")

# ---------- DIM_DATE (→ DBFS) — matches midterm exactly ----------
df["full_date"] = pd.to_datetime(
    df["YEAR"].astype(str) + "-" + df["MONTH"].astype(str) + "-01"
).dt.date

df_date = (
    df[["full_date"]]
    .drop_duplicates()
    .sort_values("full_date")
    .reset_index(drop=True)
)
df_date["date_key"] = pd.to_datetime(df_date["full_date"]).dt.strftime("%Y%m%d").astype(int)
df_date = df_date[["date_key", "full_date"]]
df_date.to_csv("dim_date.csv", index=False)
print(f"dim_date.csv     → {len(df_date):,} rows  (upload to DBFS)")

# ---------- FACT_RETAIL_SALES (→ 3 streaming JSON files) ----------
df_fact = df[[
    "ITEM CODE", "SUPPLIER", "ITEM TYPE", "full_date",
    "RETAIL SALES", "RETAIL TRANSFERS", "WAREHOUSE SALES",
]].rename(columns={
    "ITEM CODE": "item_code",
    "SUPPLIER": "supplier_name",
    "ITEM TYPE": "item_type",
    "RETAIL SALES": "retail_sales",
    "RETAIL TRANSFERS": "retail_transfers",
    "WAREHOUSE SALES": "warehouse_sales",
})

# Resolve to surrogate keys
df_fact = df_fact.merge(df_product[["product_key", "item_code"]], on="item_code", how="left")
df_fact = df_fact.merge(df_supplier, on="supplier_name", how="left")
df_fact = df_fact.merge(df_item_type, on="item_type", how="left")
df_fact = df_fact.merge(df_date, on="full_date", how="left")

df_fact = df_fact[[
    "date_key", "product_key", "supplier_key", "item_type_key",
    "retail_sales", "retail_transfers", "warehouse_sales",
]].copy()

# Drop any rows with null surrogate keys (defensive)
before = len(df_fact)
df_fact = df_fact.dropna(subset=["date_key", "product_key", "supplier_key", "item_type_key"])
if before != len(df_fact):
    print(f"  Dropped {before - len(df_fact)} fact rows with null FKs")

# Cast keys to int (merges can produce floats from NaN handling)
for col in ["date_key", "product_key", "supplier_key", "item_type_key"]:
    df_fact[col] = df_fact[col].astype(int)

df_fact = df_fact.sort_values("date_key").reset_index(drop=True)
print(f"\nFact rows: {len(df_fact):,}")

# Split into 3 chronological thirds by unique date_key
unique_dates = sorted(df_fact["date_key"].unique())
n = len(unique_dates)
cutoff_1 = unique_dates[n // 3]
cutoff_2 = unique_dates[(2 * n) // 3]
print(f"Splitting at date_keys {cutoff_1} and {cutoff_2}")

batch_1 = df_fact[df_fact["date_key"] < cutoff_1]
batch_2 = df_fact[(df_fact["date_key"] >= cutoff_1) & (df_fact["date_key"] < cutoff_2)]
batch_3 = df_fact[df_fact["date_key"] >= cutoff_2]

# Newline-delimited JSON — AutoLoader's expected format
def write_ndjson(path, frame):
    with open(path, "w") as f:
        for rec in frame.to_dict(orient="records"):
            f.write(json.dumps(rec) + "\n")

write_ndjson("fact_retail_sales_01.json", batch_1)
write_ndjson("fact_retail_sales_02.json", batch_2)
write_ndjson("fact_retail_sales_03.json", batch_3)

print(f"\nfact_retail_sales_01.json → {len(batch_1):,} rows")
print(f"fact_retail_sales_02.json → {len(batch_2):,} rows")
print(f"fact_retail_sales_03.json → {len(batch_3):,} rows")
print(f"                            Total: {len(batch_1) + len(batch_2) + len(batch_3):,}")
