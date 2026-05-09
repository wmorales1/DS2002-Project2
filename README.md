# DS-2002 Project 2 — Retail Sales Lakehouse

This builds on my midterm (Project 1) where I built a star schema for Montgomery County retail/warehouse sales using MySQL Workbench. For Project 2 I'm adding streaming, multiple cloud sources, and the bronze/silver/gold medallion architecture.

## What's in this repo
- `project2_lakehouse.ipynb` — the Databricks notebook that runs the full pipeline (this is the main deliverable)

## Architecture

Star schema (same as midterm):
- `dim_date` — date_key, full_date
- `dim_product` — product_key, item_code, item_description, item_type
- `dim_supplier` — supplier_key, supplier_name
- `dim_item_type` — item_type_key, item_type
- `fact_retail_sales` — date_key, product_key, supplier_key, item_type_key, retail_sales, retail_transfers, warehouse_sales

Source layout (multi-source rubric):
- **Aiven MySQL** holds `dim_product` and `dim_supplier` (relational source)
- **MongoDB Atlas** holds `dim_item_type` (NoSQL source)
- **Databricks Volume** holds `dim_date.csv` and the 3 fact JSON files (file source)

Medallion:
- **Bronze** — raw streaming JSON ingested via `spark.readStream`
- **Silver** — bronze joined with all 4 dim tables
- **Gold** — business-value SQL aggregations

## Notes on what changed from the original plan

The assignment suggested Azure SQL or Azure MySQL, but UVA's Azure subscription wouldn't let me create resource groups, so I swapped to **Aiven for MySQL** since the rubric says "a Cloud service database system *like* Azure SQL." Same MySQL 8 backend, same JDBC pattern.

I'm also using **Databricks Free Trial (serverless)**, which has network egress restrictions that block direct JDBC to Aiven and direct connection to MongoDB Atlas from inside the notebook. To work around that, I exported the MySQL dims to CSV and loaded the Mongo collection from JSON — both staged in a Unity Catalog Volume. The data still **originates from** Aiven and MongoDB.

I also used standard Structured Streaming with `maxFilesPerTrigger=1` instead of Spark AutoLoader (the rubric mentions it specifically). AutoLoader's `cloudFiles` source isn't fully supported on serverless workspaces.

## How streaming was demonstrated

The fact data is split into 3 JSON files representing roughly 4-month chunks of sales. Each file gets uploaded one at a time to the streaming folder, and the bronze/silver write cells get re-run for each one. Spark's checkpoint tracks what's been processed so each run picks up only the new file.

The final Gold query confirms all 3 source files were ingested.

## Rubric crosswalk

| Requirement | Where |
|---|---|
| Date dimension | `dim_date` from CSV |
| 3+ additional dim tables | `dim_product`, `dim_supplier`, `dim_item_type` |
| 1+ fact table | `fact_retail_sales_silver` |
| Multi-source dimensions | Aiven MySQL + MongoDB Atlas + Databricks Volume CSV |
| Differing granularity (static + near-real-time) | Static dims joined to streaming Bronze at Silver |
| At least 1 batch execution | First run on `_01.json` |
| Streaming with 3 segmented JSON intervals | Sequential runs on `_01`, `_02`, `_03` |
| Joined fact + dim at Silver | INNER JOIN against all 4 dim tables |
| Databricks Notebook for execution | `project2_lakehouse.ipynb` |
| Business-value queries | Gold queries 1-3 (top suppliers, top products, monthly sales by type) |
| Inline Markdown documentation | Throughout the notebook |
