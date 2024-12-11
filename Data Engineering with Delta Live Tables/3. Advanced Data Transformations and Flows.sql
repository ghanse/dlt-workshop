-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Advanced Data Transformations and Flows
-- MAGIC Production data pipelines often require more complex dependencies. Data engineers must often backfill historical data, upsert batches of records, and track historical changes to source data. In this notebook, we'll demonstrate patterns for handling these advanced data flows with DLT.
-- MAGIC
-- MAGIC Add this notebook to your DLT pipeline via the pipeline UI and we'll get started.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/isDXm6P.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Combining Datasets with Append Flows
-- MAGIC Many scenarios may require data engineers to union data from multiple sources into a single dataset. We could have several Kafka topics processing data from various regions. We may want to backfill historical records into a target table. It can be challenging to append streaming datasets with traditional ETL tools. DLT simplifies these scenarios with **flows**.
-- MAGIC
-- MAGIC Flows are incrementalized datasets used during pipeline execution (e.g. to materialize streaming tables). With `CREATE FLOW ...`, we can create a single dataset from several incremental flows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding New Sources to an Existing Streaming Table
-- MAGIC We've already defined a streaming table to ingest order data. Suppose we want to add orders from a new business unit with a separate ERP system. We can define a flow to add this data to our existing `b_orders` table. You can think of each flow as its own non-materialized streaming dataset.
-- MAGIC
-- MAGIC We can append a flow into an existing table with the `INSERT INTO ...` syntax. You might use this pattern wherever you need the `UNION` of several datasets.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Flows are suitable when we want to add some streaming data into a persisted table. For validations or other scenarios where materializing data is not required, use views.*

-- COMMAND ----------

CREATE FLOW b_orders_new_bu
COMMENT
 "Raw order data from a new business unit; Parsed from csv files; Processed incrementally"
AS INSERT INTO 
  b_orders BY NAME
SELECT
  *
FROM STREAM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/orders_new',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Backfilling Historical Data
-- MAGIC Analysts and data scientists are often interested in datasets with several years of historical data. When building new data pipelines, data engineers must consider how to backfill data with minimal impact to the incremental ingestion. In the next cell, we'll ingest some historical order data and insert it into our `b_orders` table using a one-time flow.

-- COMMAND ----------

CREATE FLOW b_orders_history
COMMENT 
  "Historical order data from csv files; Processed incrementally"
AS INSERT INTO 
  b_orders BY NAME
SELECT
  *
FROM STREAM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/orders_backlog',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When we validate or run our pipeline, we can see incremental flows defined in the **event log**.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/RCbsUMh.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data with Apply Changes
-- MAGIC Upserting and deduplicating are common tasks when processing data. We may want to maintain a table with the most recent orders for each customer. It can be complex to reason about upserts and deduplication with streaming data. When incrementally upserting data into a target table, we must order the new set of records, choose a record for each upsert key, then finally merge data into the target table. With DLT, we can use `APPLY CHANGES` to streamline upserts from an incremental data source.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Because apply changes performs updates, truncates, and deletes, the target table cannot be directly used as a streaming source. While we can use the `skipChangeCommits` option to work around this limitation, we must carefully consider how to propagate updates and deletes into downstream tables.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Change Data Capture
-- MAGIC Many databases have mechanisms for capturing and streaming changes to source data. We can process these updates using DLT's apply changes APIs. We can specify the `KEYS`, `SEQUENCE BY` columns, change behavior, and the SCD type. DLT abstracts the complicated logic and handles these updates for us!
-- MAGIC
-- MAGIC In this section, we'll ingest some change data to maintain a history of our suppliers using SCD type 2.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE b_suppliers_cdc
COMMENT
  "Change data for suppliers; processed incrementally from CSV files"
AS SELECT
  *
FROM STREAM read_files(
  '/Volumes/dlt_workshop_gregory_hansen_databricks_com/finance/_files/suppliers_cdc',
  format => 'csv',
  header => true,
  mode => 'PERMISSIVE'
);

CREATE OR REFRESH STREAMING TABLE s_suppliers
COMMENT "Supplier data processed incrementally from a change data feed";

APPLY CHANGES INTO
  LIVE.s_suppliers
FROM
  STREAM(LIVE.b_suppliers_cdc)
KEYS
  (id)
APPLY AS DELETE WHEN
  update_type = "DELETE"
SEQUENCE BY 
  update_date
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Other Incremental Upserts
-- MAGIC Apply changes has numerous practical applications outside of change data capture. Suppose we want a table with each customer's most recent order. We need some way of grouping orders by customer, ordering by date, and merging new orders. With the apply changes APIs, we need only specify the `KEYS` and `SEQUENCE BY` columns. DLT will abstract the complicated logic for us!

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE g_recent_customer_orders
COMMENT "Most recent orders by customer; Incrementally updated using apply changes";

APPLY CHANGES INTO
  LIVE.g_recent_customer_orders
FROM
  STREAM(LIVE.s_orders)
KEYS
  (customer_id)
SEQUENCE BY
  order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC Run a pipeline update and check the new table graph. Note how flows are tracked in the details of each table and how apply changes tracks different operations.
-- MAGIC
-- MAGIC Our next notebook covers <a href="$./4. Pipeline and Table Properties">***Pipeline and Table Properties***.
