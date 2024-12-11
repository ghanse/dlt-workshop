-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pipeline and Table Properties
-- MAGIC When running workloads in production, we must often specify how we want our data to be persisted and how we want our workloads to execute. These might dictate how we connect to data, how results are persisted in Delta tables, or how frequently our pipelines process continuous data.
-- MAGIC
-- MAGIC Once you've added this notebook to your DLT pipeline, we'll demonstrate how to control table and pipeline behavior with configuration properties.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/WC0AxNr.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Table-Scoped Properties
-- MAGIC Table-scoped properties are specified when defining a materialized view or streaming table. These include table layout decisions like [liquid clustering](https://docs.databricks.com/en/delta/clustering.html#language-sql) or Z-ordering, Delta table properties like the `dataSkippingStatsColumns`, or execution settings like the optimized writes and auto compaction.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Databricks recommends table-scoped properties for most workloads.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Liquid Clustering with DLT
-- MAGIC Liquid clustering optimizes queries and simplifies data layout decisions when working with Delta tables. With liquid clustering, data is co-located based on the values of **cluster keys**, allowing queries to skip files when reading data. Columns used in `MERGE`, `JOIN`, or `WHERE` clauses are often good candidate cluster keys. Unlike partitioning, liquid clustering works with high cardinality columns. Cluster keys can be safely redefined over time without recomputing the entire dataset.
-- MAGIC
-- MAGIC In DLT, we specify cluster keys using `CLUSTER BY (...)` in any table definition statement. We can safely update these keys over time if query patterns change. We simply modify our table definition statement. DLT handles changes to the cluster columns without any explict `ALTER TABLE` statement.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW c_orders_liquid_clustered
COMMENT
  "Customer names, addresses, and contact information; clustered by order_date, customer_id, and item_id"
AS SELECT
  *
FROM 
  LIVE.s_orders
CLUSTER BY (
  order_date,
  customer_id,
  item_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Indexing Columns for Statistics
-- MAGIC Queries on non-cluster columns can still benefit from file skipping! By default, the first 32 columns of a Delta table will be **indexed**. Min and max values will be recorded in each file for these columns. When filtering results based on these columns, Delta readers will inspect the file-level statistics and skip files accordingly.
-- MAGIC
-- MAGIC We can control column indexing with a few table properties. The `delta.dataSkippingNumIndexedCols` can be tuned to include more columns. With very wide tables, we can specify a list of indexed columns via the `delta.dataSkippingStatsColumns` property. To specify Delta properties in DLT, we include the `TBLPROPERTIES(...)` clause when defining the table.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE c_orders_fixed_indices
COMMENT
  "Sales orders with customer and item information; indexes only for order_date and customer_id"
TBLPROPERTIES (
  "delta.dataSkippingStatsColumns" = "order_date, customer_id"
)
AS SELECT
  *
FROM 
  STREAM(LIVE.s_orders)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Partitioning and Z-ordering
-- MAGIC In situations where we can't use liquid clustering, partitioning and Z-ordering can be used. The next cell shows how to partition and Z-order a streaming table with DLT.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE c_orders_partitioned_zordered
COMMENT
  "Sales orders with customer and item information; partitioned by customer_id, Z-ordered by item_id and order_date"
TBLPROPERTIES (
  "pipelines.autoOptimize.zOrderCols" = "item_id, order_date"
)
PARTITIONED BY (
  customer_id
)
AS SELECT
  *
FROM 
  STREAM(LIVE.s_orders)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Preventing Full Reprocessing
-- MAGIC By default, DLT will fully recompute all tables whenever a full refresh is triggered. We may want to explicitly prevent full recomputation for specific tables. Imagine, for example, a table that is updated by some external user or workflow. External updates would be lost when the data is reprocessed. We can prevent this scenario with the `pipelines.reset.allowed` property. Full recomputation of the table will be avoided. A full refresh of the pipeline will trigger incremental processing of any new records.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Use `pipelines.reset.allowed = false` when ingesting data from Kafka to prevent data loss.*

-- COMMAND ----------

/*
CREATE OR REFRESH STREAMING TABLE c_orders_no_reset
COMMENT
  "Sales orders with customer and item information; will not be fully recomputed when a full refresh is run"
TBLPROPERTIES (
  "pipelines.reset.allowed" = "false"
)
AS SELECT
  *
FROM 
  STREAM(LIVE.s_orders)
*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Trigger Intervals for Continuous Processing
-- MAGIC When processing data continuously, **trigger intervals** specify how often new data is requested from the source system. Using triggers can help save costs for data that arrives slowly but must be processed continuously. Setting the `pipelines.trigger.interval` controls how often we process data. This can be included in each table's properties to specifically control how often each table is updated.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE c_orders_30s_trigger
COMMENT
  "Sales orders with customer and item information; when processed continuously, will update every 30 seconds"
TBLPROPERTIES (
  "pipelines.trigger.interval" = "30 seconds"
)
AS SELECT
  *
FROM 
  STREAM(LIVE.s_orders)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline-Scoped Properties
-- MAGIC In limited scenarios, we may want to apply the same property configurations to an entire DLT pipeline. Retry policies are one example. By default, DLT will retry a pipeline after certain failures. We can use the `pipelines.numUpdateRetryAttempts` setting to limit these retries. To apply settings at a pipeline level, navigate to the **Advanced** section of the pipeline settings UI, add a configuration, and save the pipeline.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/HDoHuQd.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parameterizing Table Definitions
-- MAGIC Some aspects of a table definition can be parameterized. We may, for example, want to parameterize a `WHERE` clause. Parameterized pipelines adhere to a lot of software engineering best-practices like modularization, reusability, etc. We can pass parameters to our table definitions by defining arbitrary pipeline-scoped configurations. To filter all orders before a specific date, we could add a `test_pipeline.orders.start_date` configuration to our pipeline, then reference that value in our table definition.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Use uniquely-identifying prefixes to avoid overlap/collision with existing DLT and Spark properties when adding custom parameters.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://i.imgur.com/2NgphMt.png" width=800>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE c_orders_parameterized
COMMENT
  "Sales orders with customer and item information; uses the test_pipeline.orders.start_date parameter to filter orders by date"
AS SELECT
  *
FROM 
  STREAM(LIVE.s_orders)
WHERE
  order_date > "${test_pipeline.orders.start_date}"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC For detailed information on configuration, refer to Databricks' documentation for common [Delta table properties](https://docs.databricks.com/en/delta/table-properties.html) and [DLT configurations settings](https://docs.databricks.com/en/delta-live-tables/configure-pipeline.html).
-- MAGIC
-- MAGIC The last notebook in our workshop covers <a href="$./5. Scheduling and Monitoring Pipelines">***Scheduling and Monitoring Pipelines***.
