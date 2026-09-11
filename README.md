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

Product processing must precede pricing and order processing because both resolve child `product_id` values through `fmcg.silver.products`.

### 3. Ingest Historical Orders into Bronze

`1_full_load_fact.ipynb` reads the historical order CSV files from `orders/landing/` and adds:

- `read_timestamp`
- `file_name`
- `file_size`

The records are appended to `fmcg.bronze.orders`, and the input files are moved to `orders/processed/`. Bronze preserves the raw history without deduplication.

### 4. Clean and Merge Orders into Silver

The full-load notebook reads the accumulated Bronze order table and applies these rules:

1. Remove rows with a missing `order_qty`.
2. Replace nonnumeric customer IDs with the string `999999`.
3. Remove weekday prefixes from textual dates.
4. Parse the supported date formats into `order_placement_date`.
5. Drop duplicates using order ID, date, customer ID, product ID, and quantity.
6. Cast product IDs to strings and join to the Silver product table to obtain `product_code`.

The product lookup is an inner join, so orders without a matching Silver product are excluded from the joined result.

The result is written to `fmcg.silver.orders`. If the table already exists, a merge updates matching rows and inserts new ones using:

```text
order_placement_date + order_id + product_code + customer_id
```

### 5. Publish Detailed Child Gold Orders

The notebook creates or merges into `fmcg.gold.sb_fact_orders`, aligning the field names with the parent model:

| Silver field | Child Gold field |
| --- | --- |
| `order_placement_date` | `date` |
| `customer_id` | `customer_code` |
| `order_qty` | `sold_quantity` |

Child Gold retains `order_id`, `product_id`, and `product_code`. Its merge key is `date + order_id + product_code + customer_code`, preserving detailed order records before monthly consolidation.

### 6. Consolidate Orders at the Parent's Monthly Grain

The child records are grouped by month start, product code, and customer code. Their quantities are summed, producing one row per:

```text
month + product_code + customer_code
```

These monthly rows are merged into `fmcg.gold.fact_orders` using `date + product_code + customer_code`. Matching rows receive the calculated values; new keys are inserted. Parent records outside the incoming keys remain unchanged.

This is an upsert, not an addition to an existing matched quantity. Consolidation therefore assumes that the child keys identify the intended child records in the parent model.

## Incremental Load

`2_incremental_load_fact.ipynb` processes newly delivered child order files while retaining the historical Bronze, Silver, and child Gold tables.

### 1. Land the New Batch

Place the next order CSV files in `orders/landing/`. The provided child incremental dataset contains daily files for December 2025.

### 2. Preserve Raw History and Isolate the Batch

The notebook reads the landing files with ingestion metadata, appends them to `fmcg.bronze.orders`, and overwrites `fmcg.bronze.staging_orders` with only the current batch. It then moves the files to `processed/`.

The permanent Bronze table accumulates history. The staging table limits the next transformation step to the newly arrived records.

### 3. Transform and Merge the Current Batch

The notebook reads Bronze staging, applies the same order-cleaning and product-mapping rules used by the full load, and merges the result into `fmcg.silver.orders`.

It also overwrites `fmcg.silver.staging_orders` with the cleaned current batch. This staging table supplies the records used to update child Gold.

### 4. Update Child Gold

The Silver staging records are renamed to the child Gold schema and merged into `fmcg.gold.sb_fact_orders`. Existing matching order records are updated, and previously unseen keys are inserted.

### 5. Identify Affected Months

The notebook extracts distinct month-start dates from Silver staging and registers them as the temporary view `incremental_months`.

### 6. Recalculate Complete Monthly Totals

For those affected months, the pipeline reads all corresponding records from the updated child Gold fact table, including previously loaded orders. It recalculates monthly quantities by product and customer, then merges those complete totals into the parent fact table.

For example, if a month's existing child orders total 100 units and a new batch adds 20, the parent receives the recalculated total of 120. It does not receive only the new batch's 20 units as a replacement for the month's total.

This is the central consolidation step: the detailed child tables receive batch updates, while the parent receives refreshed monthly totals for the affected periods.

### 7. Remove Staging Tables

After the parent merge, the notebook drops the Bronze and Silver staging tables. Historical records remain in the permanent tables, and the input files remain in the S3 archive.

Although Change Data Feed is enabled on several Delta writes, the implemented incremental path uses landing files and staging tables. It does not consume Delta Change Data Feed or use a streaming checkpoint.

## Databricks Workflow

![Databricks pipeline workflow](docs/images/databricks_pipeline_workflow.png)

The captured workflow shows customer, pricing, and product tasks feeding the incremental orders task. All four tasks completed successfully in the displayed run.

The dimension tasks use their existing full-refresh notebooks even when included in the incremental orders workflow. Their presence in that job does not make dimension processing incremental.

For an initial deployment, run products before pricing because pricing reads `fmcg.silver.products`. The screenshot shows those tasks in parallel, so reproducing that arrangement assumes the product table is already available. Configure an explicit product-to-pricing dependency when the price task must use the products refreshed in the same run.

The repository includes notebooks and workflow evidence, but no exported job definition; configure task dependencies in the target Databricks workspace.

## Analytical Model

![Power BI data model](docs/images/power_bi_data_model.png)

The consolidated reporting model centers on `fact_orders` and uses four dimensions:

| Table | Reporting purpose |
| --- | --- |
| `fact_orders` | Monthly sold quantity by product and customer |
| `dim_date` | Month, quarter, and year filtering |
| `dim_customers` | Customer, market, platform, and channel analysis |
| `dim_products` | Product, category, division, and variant analysis |
| `dim_gross_price` | Product pricing by year |

The Power BI screenshot shows a `ProductYear` relationship for pricing. The enriched SQL view expresses the same product-and-year lookup using `product_code` and `YEAR(date)`.

The serving query in [denormalise_table_query_fmcg.txt](project-de-fmcg-atlikon/2_dashboarding/denormalise_table_query_fmcg.txt) creates `fmcg.gold.vw_fact_orders_enriched`. It left-joins the monthly fact to the dimensions and calculates:

```sql
fo.sold_quantity * gp.price_inr AS total_amount_inr
```

This produces a reporting surface with date attributes, customer segments, product attributes, quantities, and gross-price-based sales amounts. Missing price matches result in null calculated amounts, so pricing coverage matters when interpreting revenue.

## Sales Dashboard

![Sales analytics dashboard](docs/images/sales_analytics_dashboard.png)

The Sales Insights dashboard presents the consolidated business through:

- Total revenue, total quantity, unique products, and average selling price cards.
- Revenue by channel, including the acquisition channel.
- The top five products by revenue.
- Year, quarter, month, and channel filters.

The captured dashboard displays approximately INR 119.93 billion in revenue, 39.05 million units, and 133 unique products. These are values from the saved screenshot, not live results or a fresh execution of the repository.

The screenshot also includes an average selling price card. Its measure definition is not included in the supplied notebooks or SQL, so its exact aggregation should be checked in the original dashboard before reuse.

A separate dashboard export is available in [fmcg_dashboard.pdf](project-de-fmcg-atlikon/2_dashboarding/fmcg_dashboard.pdf).

## Databricks Genie Analysis

![Databricks Genie KPI analysis](docs/images/databricks_genie_kpi_analysis.png)

The Genie screenshot demonstrates natural-language exploration of the project's reporting data, including revenue and year-over-year comparisons. This complements the dashboard by letting users ask business questions about the consolidated data.

The screenshot references an `arc` view from the captured environment. When reproducing the project, connect Genie to the reporting view created in your own catalog and verify generated calculations against that view.

The saved analysis is available in [genie_kpi_summary.pdf](docs/genie_kpi_summary.pdf).

## Repository Structure

```text
.
|-- README.md
|-- docs/
|   |-- images/
|   |   |-- data_platform_architecture.png
|   |   |-- aws_s3_data_landing.png
|   |   |-- databricks_pipeline_workflow.png
|   |   |-- power_bi_data_model.png
|   |   |-- sales_analytics_dashboard.png
|   |   `-- databricks_genie_kpi_analysis.png
|   `-- genie_kpi_summary.pdf
`-- project-de-fmcg-atlikon/
    |-- 0_data/
    |   |-- 1_parent_company/
    |   |   |-- full_load/
    |   |   `-- incremental_load/
    |   `-- 2_child_company/
    |       |-- full_load/
    |       `-- incremental_load/
    |-- 1_codes/
    |   |-- 1_setup/
    |   |-- 2_dimension_data_processing/
    |   `-- 3_fact_data_processing/
    |-- 2_dashboarding/
    `-- resources/
```

## Running the Project

Use a Databricks environment with PySpark, Delta Lake, catalog permissions, and access to the configured S3 locations. S3 access must support reading incoming files and moving them to the archive. The notebooks use Databricks-specific APIs such as `dbutils`, notebook widgets, and `%run`.

1. Import `1_codes/` into your Databricks workspace. Update the `%run /Workspace/consolidated_pipeline/1_setup/utilities` references if your import location differs.
2. Replace the hardcoded S3 bucket paths. Use `fmcg` consistently, or update all catalog references, including hardcoded SQL and staging cleanup statements; changing the widget alone is insufficient.
3. Run `setup_catalog.ipynb` and import the four parent full-load CSVs into the Gold Delta tables listed above, using date and numeric types appropriate to their columns.
4. Run `dim_date_table_creation.ipynb`. Extend its date range if your reporting periods go beyond the supplied dataset.
5. Upload the child customer, product, and gross-price files into their respective S3 folders, and historical order files into `orders/landing/`.
6. Run the customer and product notebooks, followed by the pricing notebook, then `1_full_load_fact.ipynb`.
7. Upload the next child order batch into `orders/landing/` and run `2_incremental_load_fact.ipynb`.
8. Run the `fmcg` enriched-view SQL, configure the reporting connections, and refresh the dashboard after successful consolidation.

When validating a run, compare affected monthly totals in child Gold with their matching parent fact rows, inspect unmatched product and pricing keys, and confirm staging cleanup. The notebooks contain interactive row counts and previews; an automated end-to-end test suite is not included.

Because files are archived before downstream completion, a failed Silver or Gold step requires deliberate recovery from retained tables or archived files. Rerunning with an empty landing folder is not a complete recovery procedure. The implementation also does not propagate source deletions.

## Scope Notes

1. **Incremental processing focuses on `fact_orders`.** This project's incremental implementation targets the child's order facts. Customers, products, and prices use full-refresh processing followed by parent merges. Incremental loading for those datasets would be a separate case requiring its own change-detection rules and update handling.

2. **Parent incremental loading is simplified for this demonstration.** The supplied parent SQL uses a direct `COPY INTO` query to load additional parent order data. This is a shortcut, not a complete production parent incremental pipeline. The project's main case is integrating and incrementally processing the acquired child's data; building the parent's own incremental ingestion pipeline was outside that scope. The sample query also requires replacing its volume-path placeholder and aligning its `arc` catalog reference with the chosen environment.
