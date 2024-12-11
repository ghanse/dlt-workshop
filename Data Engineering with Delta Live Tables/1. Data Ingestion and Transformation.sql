-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Ingestion and Transformation
-- MAGIC In this notebook, we'll show how [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html) (DLT) simplifies data ingestion and transformation.
-- MAGIC
-- MAGIC <br><img src="https://www.databricks.com/sites/default/files/2023-11/delta-live-tables-flow-01-2x.png?v=1699554148" width=900><br><br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingesting Data from Source Systems
-- MAGIC The first step in any data pipeline is to bring raw data from source systems into an analytics environment where it can be processed and queried. With DLT, we can write production-grade ingestion code with simple, declarative statements. These often resemble `CREATE TABLE ... AS SELECT ...` statements from SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Batch Ingestion from Files
-- MAGIC When we run the following cell, we'll define a **materialized view** from CSV files in a Databricks volume. Materialized views are Delta tables managed by a DLT pipeline. Because materialized views are fully recomputed whenever we trigger the pipeline, they're useful for batch loads.
-- MAGIC
-- MAGIC Note that data will not be immediately ingested when defining a materialized view in the notebook. Running each cell will syntactically validate and define the datasets. To trigger data processing, we need to [create a DLT pipeline](https://docs.databricks.com/en/delta-live-tables/configure-pipeline.html) using the UI.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** When defining tables, we can add a descriptive comment that can be used by large language models when analysts want to discover data.*

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW b_items
COMMENT 
  "Raw inventory item data from csv files; Fully recomputed with each pipeline execution"
AS SELECT 
  *
FROM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/items',
  format => 'csv',
  header => true
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use `read_files(...)`, a generic file reader with [options for various file formats](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html#options-1), to ingest from many different file formats. In the next cell, we'll use the `sep` option to read pipe-delimited data into another materialized view.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW b_customers
COMMENT 
  "Raw customer data from | delimited files; Fully recomputed with each pipeline execution"
AS SELECT 
  *
FROM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/customers',
  format => 'csv',
  header => true,
  sep => '|'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Streaming Ingestion from Files
-- MAGIC `read_files(...)` also allows us to incrementally ingest files as they're added to a directory. In DLT, we use **streaming tables** for datasets which should be updated incrementally. Common scenarios for streaming tables include ingesting data from Kafka, incrementally reading from very large tables, or processing continuously-arriving files. When processing incremental updates, DLT manages streaming checkpoints and state for you!
-- MAGIC
-- MAGIC Many data sources used in DLT (including `read_files`) can be incrementalized by prepending the `STREAM` keyword. We'll demonstrate this by defining a streaming table from an incrementalized set of CSV files.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Streaming tables don't require continuous execution! We can trigger incremental updates on any interval and process any records that have arrived since our last execution. This can be useful when ingesting from large historical datasets which are expensive to fully recompute.*

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE b_orders
COMMENT 
  "Raw order data from csv files; Processed incrementally"
AS SELECT 
  *
FROM STREAM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/orders',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Processing and Transformation
-- MAGIC To prepare data for analysts and machine learning engineers, we often want to join and aggregate datasets. DLT lets us express these transformations with simple `CREATE TABLE AS SELECT ...` statements. We can select and join tables defined in our pipeline using the `LIVE.table_name` syntax. DLT will resolve any dependencies and create an execution plan when we run our pipeline!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Joining Datasets
-- MAGIC We can join datasets in DLT using familiar SQL expressions. This includes static joins between 2 or more materialized views, stream-static joins between streaming tables and materialized views, and stream-stream joins between multiple streaming tables. In the following cell, we'll create a new streaming table by joining our `b_orders`, `b_items`, and `b_customers` tables.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** When joining multiple streaming tables, use [watermarks](https://docs.databricks.com/en/delta-live-tables/stateful-processing.html#use-watermarks-with-stream-stream-joins) to control streaming state and limit memory consumption.*

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE s_orders
COMMENT 
  "Sales orders with customer and item information"
AS SELECT
  bo.id AS order_id,
  bo.order_date,
  bo.customer_id,
  bc.customer_name,
  bc.billing_address,
  bc.mailing_address,
  bc.phone_number,
  bc.email,
  bc.payment_terms,
  bo.description,
  bo.item_id,
  bi.description AS item_description,
  bi.unit_price,
  bo.qty_ordered,
  bo.qty_ordered * bi.unit_price AS total_price
FROM 
  STREAM(LIVE.b_orders) bo
JOIN 
  LIVE.b_customers bc ON bc.id = bo.customer_id
JOIN 
  LIVE.b_items bi ON bi.id = bo.item_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggregating Data
-- MAGIC Once we've joined data, we often want to roll-up values and compute results. This could include calculating the total sales per customer or per hour. Databricks SQL has a number of [aggregate functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html#aggregate-functions) covering a range of scenarios. Let's use these functions to create a few aggregate tables from our `s_orders` table.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Aggregates over ranges of timestamps in our data, often referred to as windowed aggregates, should also use watermarks to avoid unbounded state and out-of-memory errors.*

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE g_sales_summary_by_customer
COMMENT 
  "Sales order summary data grouped by customer"
AS SELECT
  customer_id,
  customer_name,
  count(order_id) AS total_orders,
  sum(total_price) AS total_sales,
  avg(total_price) AS avg_sales,
  min(total_price) AS min_order_amount,
  max(total_price) AS max_order_amount
FROM 
  STREAM(LIVE.s_orders)
GROUP BY
  customer_id,
  customer_name;

CREATE OR REFRESH STREAMING TABLE g_sales_summary_by_day
COMMENT
  "Sales order summary data grouped by day"
AS SELECT
  window(order_date, '1 day') AS order_date,
  count(order_id) AS total_orders,
  sum(total_price) AS total_sales,
  avg(total_price) AS avg_sales,
  min(total_price) AS min_order_amount,
  max(total_price) AS max_order_amount
FROM
  STREAM(LIVE.s_orders)
WATERMARK 
  order_date DELAY OF INTERVAL 1 HOUR
GROUP BY
  window(order_date, '1 day')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Pipeline
-- MAGIC We've now defined a DLT pipeline that ingests data from files, applies data quality rules, and transforms the data into analyst-ready Delta tables. Next, we'll create and run our pipeline. In any of the notebook cells, click the **Create Pipeline** button to open the [Create Pipeline UI](https://docs.databricks.com/en/delta-live-tables/configure-pipeline.html#configure-a-new-delta-live-tables-pipeline). From there, enter the pipeline name, specify a target catalog and schema, and configure compute. In this example, we'll use the current notebook and schema and choose serverless compute.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/YKnKuSI.png" width=1000><br><br>
-- MAGIC
-- MAGIC Pipelines can be [triggered or continuous](https://docs.databricks.com/en/delta-live-tables/pipeline-mode.html). A **triggered** refresh will process data until completion and then stop. Any streaming tables will ingest and process data up to the time the refresh was started. A **continuous** refresh will continue to execute until the pipeline is canceled or fails. Any materialized views will be processed once.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Running a Pipeline Update
-- MAGIC Once you've created your pipeline, you can run an update [through the DLT UI](https://docs.databricks.com/en/delta-live-tables/updates.html) or by [connecting a pipeline to the notebook](https://docs.databricks.com/en/delta-live-tables/dlt-notebook-devex.html#connect-a-notebook-to-a-delta-live-tables-pipeline). Running a **validate** will syntactically check the table definitions and dependencies between tables. Starting a **refresh** will execute the end-to-end pipeline.
-- MAGIC
-- MAGIC To completely recompute data for both materialized views and streaming tables, we can run a **full refresh**.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/MiycgtA.png" width=1200>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC Once you've run the update, check out the next notebook covering <a href="$./2. Data Quality with Expectations">***Data Quality with Expectations***.
