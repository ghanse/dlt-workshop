-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Quality with Expectations
-- MAGIC High-quality data is the backbone of trustworthy reporting and high-value predictive models. Producing reliable, accurate data should be a primary goal of any data engineering effort.
-- MAGIC
-- MAGIC In this notebook, we'll show how Delta Live Tables promotes data quality with [expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html#language-sql). To start, open the DLT pipeline UI you created in Part 1. Open the pipeline settings page, add this notebook, and save the pipeline.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/AdEr02f.png" width=800>
-- MAGIC
-- MAGIC Adding notebooks to an existing pipeline allows us to reference datasets across multiple notebooks. We'll re-use many of the datasets we created in Part 1 to demonstrate expectations. Let's get started.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Applying Data Quality Rules
-- MAGIC [Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html#language-sql) are logical rules we can apply when defining tables in DLT. When we trigger an update, the DLT UI shows metrics about our data's adherence with each expectation. 
-- MAGIC
-- MAGIC To apply expectations, we use `CONSTRAINT ... EXPECT ...` statements in the table definition.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW s_items (
  CONSTRAINT valid_unit_price EXPECT (unit_price IS NOT NULL AND unit_price > 0)
  -- Tracks missing or negative unit prices in the DLT UI
)
COMMENT 
  "Iventory item data cleand up by applying business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setting the Response Actions
-- MAGIC We can set different responses for expectations with different severity levels. Each expectation can warn, drop, or fail.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Warning on Violation
-- MAGIC By default, DLT will **warn** whenever data violates an expectation. Violations will be tracked as violations in the DLT UI. Any violations will still be written into the target table. We might use this to track metrics and report missing values to data stewards.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** Checking data against expectations has a performance cost. Limit usage of expectations that warn on violation to ensure good performance.*

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW validated_customer_names_by_warning (
  CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL AND customer_name <> '')
  -- Tracks records without a customer_name in the DLT UI
)
COMMENT 
  "Customer data after applying business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_customers;

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ### Dropping on Violation
-- MAGIC With **drop**, rows violating the expectation will be dropped and will not be written into the target table. Dropping records is common when missing or invalid values might create bad reporting or hurt the training of a machine learning model.
-- MAGIC
-- MAGIC When dropping rows with expectations, we often use a **quarantine table** to hold the invalid records. These can be monitored for patterns and re-processed after some manual clean-up.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW validated_customer_names_by_dropping (
  CONSTRAINT valid_customer_name EXPECT (customer_name IS NOT NULL AND customer_name <> '') ON VIOLATION DROP ROW
  -- Drops any records with a missing or empty customer_name from the target table
)
COMMENT 
  "Customer data after applying business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_customers;

CREATE OR REFRESH MATERIALIZED VIEW quarantined_customer_names (
  CONSTRAINT invalid_customer_name EXPECT (customer_name IS NULL OR customer_name = '') ON VIOLATION DROP ROW
)
COMMENT "Customer data which failed business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Failing on Violation
-- MAGIC If an expectation is set to **fail** any violations will cause the update to immediately fail. We use this mode whenever bad data quality requires manual intervention or reprocessing an update.

-- COMMAND ----------

/* NOTE:  We won't define this failing expectation to ensure our pipeline completes successfully.


CREATE OR REFRESH MATERIALIZED VIEW validate_customer_names_by_failing (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 
  "Customer data after applying business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_customers;

CREATE OR REFRESH MATERIALIZED VIEW q_customers (
  CONSTRAINT invalid_customer_name EXPECT (customer_name IS NULL OR customer_name = '') ON VIOLATION DROP ROW
)
COMMENT "Customer data which failed business and data quality rules"
AS SELECT 
  *
FROM 
  LIVE.b_customers
-- */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Advanced Validation Rules
-- MAGIC Expectations apply to any columns defined in our target table. We can check validity across tables to do things like validate primary keys and ensure referential integrity of joins.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validating Primary Keys
-- MAGIC Data from relational databases often contains primary key columns used by analysts to uniquely identify records and join datasets. We often want to check for duplicate primary keys when processing data. Any duplicates can cause row explosion or even failure when querying the data. In the next cell, we'll use the `count(...)` aggregate function to ensure all primary keys are unique.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW quarantined_order_pks (
  CONSTRAINT valid_pk_order_id EXPECT (num_orders <> 1)
)
COMMENT
  "Quarantined primary keys for sales order data"
AS SELECT
  id,
  count(id) AS num_orders
FROM
  LIVE.b_orders
GROUP BY
  id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validating Foreign Keys
-- MAGIC Foreign keys are useful for joining data to other tables. When processing data, we may want to check that each record's foreign key value is a valid primary key in another table.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW quarantined_order_fks (
  CONSTRAINT valid_fk_item_id EXPECT (fk_item_id IS NOT NULL)
)
COMMENT
  "Quarantined foreign keys for sales order items"
AS SELECT
  ord.*,
  itm.id as fk_item_id
FROM
  LIVE.b_orders ord
LEFT JOIN
  LIVE.b_items itm ON itm.id = ord.item_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC Connect to the DLT pipeline UI and run a **validate** step. Once the validation completes, you'll see the new tables we defined in this notebook in the DLT graph.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/4WmWwEK.png" width=700>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When you've finished validating the pipeline, move on to <a href="$./3. Advanced Data Transformations and Flows">***Advanced Data Transformations and Flows***.
