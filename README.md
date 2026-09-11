# FMCG Data Consolidation Platform

A Databricks lakehouse project that integrates an acquired company's data into its parent company's analytical model using AWS S3, PySpark, Spark SQL, and Delta Lake. The implementation covers historical loading, incremental order processing, data standardization, monthly consolidation, and sales analytics.

## Business Problem

A parent company has acquired a child company, Sports Bar. The parent already has an established reporting model containing customers, products, prices, and monthly sales. The acquired company supplies separate CSV datasets with different field names, inconsistent values, and orders recorded at a more detailed, daily level.

The business needs to bring the child's data into the parent's model and keep that consolidated model current as new child data arrives. New business keys must be inserted, while records matching existing merge keys must be updated. Reporting should then cover the combined business, including the acquisition channel.

This requires more than copying files. The pipeline must clean source data, map child attributes to the parent's schema, resolve product identifiers, and aggregate child orders to the same monthly grain used by the parent.

## Architecture

![Data platform architecture](docs/images/data_platform_architecture.png)

The platform follows a Bronze, Silver, and Gold architecture:

| Component | Responsibility |
| --- | --- |
| Source datasets | CSV extracts for customers, products, gross prices, and orders |
| AWS S3 | Stores incoming child files and archives ingested order files |
| Bronze | Retains raw records with ingestion timestamps and source-file metadata |
| Silver | Applies cleaning, deduplication, date parsing, and product mapping |
| Child Gold | Exposes standardized child dimensions and detailed child orders |
| Consolidated Gold | Merges child data into parent dimensions and monthly order facts |
| Serving layer | Provides an enriched SQL view, a Power BI model, sales dashboards, and Genie analysis |

The notebooks use the `fmcg` catalog with `bronze`, `silver`, and `gold` schemas. Some screenshots and an alternative SQL script use `arc`; those names refer to a different workspace configuration. The table references below follow the notebooks.

The repository implements batch ingestion from CSV files. The source-system extraction shown in the diagram is the upstream context; a live OLTP extraction connector is not included.

## AWS S3: Landing and Persistent File Storage

![AWS S3 order landing and processed folders](docs/images/aws_s3_data_landing.png)

The order pipeline separates incoming files from files already ingested:

```text
s3://<your-bucket>/
    customers/
        customers.csv
    products/
        products.csv
    gross_price/
        gross_price.csv
    orders/
        landing/
            orders_YYYY_MM_DD.csv
        processed/
            orders_YYYY_MM_DD.csv
```

`landing/` is the entry point for new order files. The notebooks read its CSV files, add metadata, and write the raw records into Bronze. They then move the files into `processed/`, which serves as the persistent file archive. There is no folder literally named `persistent/` in the implementation.

For a full load, files move after the Bronze append succeeds. For an incremental load, they move after both the Bronze history and Bronze staging writes succeed. This happens before Silver and Gold processing, so an archived file indicates successful ingestion, not necessarily successful completion of the entire pipeline.

The archive keeps previously ingested files out of the next landing-folder read. Bronze separately retains their raw records for downstream processing and investigation. Customer, product, and price notebooks read their own source folders directly; they do not implement the order-file move sequence.

The notebooks currently reference `sportsbar-final`, while the S3 screenshot shows `sport-bar-try`. Replace the notebook bucket paths with the bucket configured for your environment.

## Full Load

The full load establishes the historical baseline and consolidates the child's existing data with the parent's reporting tables.

### 1. Initialize the Catalog and Parent Tables

Run `setup_catalog.ipynb` to create the `fmcg` catalog and its three schemas. The shared `utilities.ipynb` defines the schema names used by the processing notebooks.

Load the parent full-load CSV files into these Delta tables before running the child merges:

| Parent source file | Target table |
| --- | --- |
| `dim_customers.csv` | `fmcg.gold.dim_customers` |
| `dim_products.csv` | `fmcg.gold.dim_products` |
| `dim_gross_price.csv` | `fmcg.gold.dim_gross_price` |
| `fact_orders.csv` | `fmcg.gold.fact_orders` |

The catalog setup notebook creates the namespaces; it does not import these parent datasets. The child notebooks expect the parent target tables to exist.

Run `dim_date_table_creation.ipynb` to create `fmcg.gold.dim_date`. It contains one row per month from January 2024 through December 2025, with year, month, and quarter attributes.

### 2. Process Child Dimensions

Each dimension notebook reads CSV data into Bronze, transforms it in Silver, publishes a child Gold table, and merges standardized records into the matching parent dimension. Child dimension tables are overwritten during these runs; the final parent writes use Delta merges.

| Dataset | Implemented transformations | Parent merge key |
| --- | --- | --- |
| Customers | Deduplicate by customer ID; trim and title-case names; correct known city typos; apply explicit city corrections; cast IDs to strings; construct the parent customer label | `customer_code` |
| Products | Deduplicate by product ID; normalize category casing; correct `Protien` spelling; assign divisions; extract variants; generate SHA-256 product codes from cleaned product names | `product_code` |
| Gross prices | Parse multiple date formats; convert numeric prices to doubles; make negative prices positive; replace nonnumeric prices with zero; join to product codes | `product_code` in the current merge |

Child customers receive `market = India`, `platform = Sports Bar`, and `channel = Acquisition`. These fields make acquisition activity identifiable in consolidated reporting.

The price notebook selects one price per product and year, prioritizing nonzero prices and then the latest month. It renames the selected value to `price_inr` for the parent model. Its final merge currently matches only on `product_code`; extending this to multiple price years requires reviewing that condition against the product-and-year reporting grain.
