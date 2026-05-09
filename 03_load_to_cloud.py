"""
One-time setup: loads dimension data into Aiven MySQL and MongoDB Atlas.

Aiven specifics:
- Custom port (NOT 3306)
- SSL is REQUIRED — sqlalchemy/pymysql handle this with ssl_disabled=False

Requirements:
    pip install pymysql sqlalchemy pymongo cryptography
    (cryptography is needed for MySQL 8 caching_sha2_password auth)

Before running:
    1. Aiven MySQL service is "Running" (not "Building") in the console
    2. Have your Aiven password ready (reveal it in the Aiven console)
    3. MongoDB Atlas cluster is running with IP allowlisted
    4. Fill in the CONFIG block below
"""
import json
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient

# ============ CONFIG — fill in your password + Mongo details ============
AIVEN_HOST     = "ds2002-mysql-ds-2002.c.aivencloud.com"
AIVEN_PORT     = 24006
AIVEN_USER     = "avnadmin"
AIVEN_PASSWORD = "my password"
AIVEN_DATABASE = "defaultdb"  # default DB; we'll create our tables here

MONGO_USER       = "pvv5ag_db_user"
MONGO_PASSWORD   = "my password"
MONGO_CLUSTER    = "s2002.wl5gd0p"  # the part between mongodb+srv:// and .mongodb.net
MONGO_DATABASE   = "retail_sales_dlh"
MONGO_COLLECTION = "item_type"
# =========================================================================


def load_aiven_mysql():
    """Create dim_product and dim_supplier in Aiven MySQL and load them."""
    # Aiven requires SSL. Pass it via connect_args so pymysql gets a proper dict
    # (the ?ssl=true URL param breaks newer pymysql versions).
    db_conn_str = (
        f"mysql+pymysql://{AIVEN_USER}:{AIVEN_PASSWORD}@{AIVEN_HOST}:{AIVEN_PORT}/{AIVEN_DATABASE}"
    )
    db_engine = create_engine(
        db_conn_str,
        pool_recycle=3600,
        connect_args={"ssl": {"ssl_disabled": False}},
    )

    # Test connection
    with db_engine.connect() as conn:
        result = conn.execute(text("SELECT VERSION()"))
        version = result.fetchone()[0]
        print(f"Connected to Aiven MySQL — server version: {version}\n")

    # ---- dim_product ----
    # Aiven enforces sql_require_primary_key=ON, so we must CREATE the table with a PK
    # BEFORE inserting any rows. Drop-then-create-then-append.
    df_product = pd.read_csv("dim_product.csv")
    with db_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_product"))
        conn.execute(text("""
            CREATE TABLE dim_product (
                product_key      INT          PRIMARY KEY,
                item_code        VARCHAR(50)  NOT NULL,
                item_description VARCHAR(255),
                item_type        VARCHAR(50)
            )
        """))
        conn.commit()
    df_product.to_sql("dim_product", con=db_engine, index=False, if_exists="append")
    print(f"dim_product → {len(df_product):,} rows loaded to Aiven MySQL")

    # ---- dim_supplier ----
    df_supplier = pd.read_csv("dim_supplier.csv")
    # Drop any rows with null supplier_name (real-world data has some blanks)
    before = len(df_supplier)
    df_supplier = df_supplier.dropna(subset=["supplier_name"])
    dropped = before - len(df_supplier)
    if dropped:
        print(f"  Dropped {dropped} supplier row(s) with null name")
    with db_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_supplier"))
        conn.execute(text("""
            CREATE TABLE dim_supplier (
                supplier_key  INT          PRIMARY KEY,
                supplier_name VARCHAR(255)
            )
        """))
        conn.commit()
    df_supplier.to_sql("dim_supplier", con=db_engine, index=False, if_exists="append")
    print(f"dim_supplier → {len(df_supplier):,} rows loaded to Aiven MySQL")


def load_mongodb():
    """Load dim_item_type into MongoDB Atlas."""
    mongo_uri = (
        f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@{MONGO_CLUSTER}.mongodb.net/?retryWrites=true&w=majority"
    )
    client = MongoClient(mongo_uri)
    db = client[MONGO_DATABASE]
    coll = db[MONGO_COLLECTION]
    coll.drop()

    with open("dim_item_type.json", "r") as f:
        docs = json.load(f)
    coll.insert_many(docs)
    print(f"\ndim_item_type → {len(docs)} documents loaded to MongoDB Atlas")
    client.close()


if __name__ == "__main__":
    print("Loading dimensions to cloud sources...\n")
    load_aiven_mysql()
    load_mongodb()
    print("\nDone. Next steps:")
    print("  1. Upload to DBFS via the Databricks UI:")
    print("     /FileStore/ds2002-capstone/source_data/batch/dim_date.csv")
    print("     /FileStore/ds2002-capstone/source_data/batch/dim_item_type.json  (optional)")
    print("     /FileStore/ds2002-capstone/source_data/stream/fact_retail_sales_01.json")
    print("  2. Run project2_lakehouse.ipynb")
    print("  3. For incremental loads: upload _02.json then _03.json, re-run streaming cells")
