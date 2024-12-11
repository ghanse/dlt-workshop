-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Deploying and Monitoring Pipelines
-- MAGIC In this notebook, we'll cover several best-practices for deploying DLT pipelines to production.
-- MAGIC
-- MAGIC Unlike previous sections, this notebook won't add any new table definitions to our pipeline and shouldn't be added to the pipeline's source code. We will, however, query the pipeline event logs and properties using Databricks SQL functions. Attach the notebook to an available cluster to run the cells and view the output.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Orchestrating Pipelines
-- MAGIC When running pipelines in triggered execution mode, we often want to schedule regular updates or run dependent tasks once an update finishes. We can use **pipeline tasks** in Databricks workflows to run these updates and any other functionality. We could, for example, run our DLT pipeline to process new data, then run some data analysis in a Databricks notebook. Let's navigate to the workflows UI and create a new job. 
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/1D2fVnm.png" width=1000>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Within the create job UI, give the job a name, change the task type to **Pipeline**, choose your pipeline from the dropdown list, and create the task.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/DdlWm8G.png" width=800>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once we've created our workflow, we can attach downstream tasks and schedule recurring runs. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Monitoring Pipelines
-- MAGIC Once a pipeline is scheduled and running in production, we want to ensure updates complete reliably. DLT offers several tools for monitoring and auditing pipelines.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Viewing Updates in the UI
-- MAGIC Each update is tracked in the DLT UI with various execution metrics (e.g. number of rows processed, data quality violations, and processing time for each table).
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/DkBbFqh.png" width=1400>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Querying the Event Log
-- MAGIC Details about pipeline exeuction can be queried from a notebook or SQL editor using Databricks SQL commands. In Unity Catalog-enabled workspaces, we use the `event_log(...)` function to return logs by pipeline ID. We can also return the event log by passing a materialized view or streaming table managed by a DLT pipeline. Run the commands in the next few cells to query the event log for the pipeline you created in **Part 1**.

-- COMMAND ----------

SELECT * FROM event_log("4eb1c4a2-6eff-456a-9ff7-2b14914feee3") -- Query the DLT event log using the pipeline ID

-- COMMAND ----------

SELECT * FROM event_log(
  TABLE(dlt_workshop_gregory_hansen_databricks_com.finance.g_sales_summary_by_customer)
) -- Query the same event log for a table managed by DLT

-- COMMAND ----------

SELECT 
  timestamp, 
  details:user_action:action, 
  details:user_action:user_name 
FROM 
  event_log("4eb1c4a2-6eff-456a-9ff7-2b14914feee3")
WHERE 
  event_type = 'user_action' -- Query the user actions performed on a pipeline (e.g. CREATE pipeline, EDIT pipeline, or START an update)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Querying the event log has many potential applications. For more example queries and a full documentation of the event log schema, see the [Databricks documentation](https://docs.databricks.com/en/delta-live-tables/observability.html#what-is-the-delta-live-tables-event-log).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Notifications
-- MAGIC When rapid response is required, we can configure email notifications for various progress events. To set-up notifications, navigate to the pipeline settings, click **Add notification**, and enter the desired settings.
-- MAGIC
-- MAGIC <img src="https://i.imgur.com/ggmPsjg.png" width=1000>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Governing Pipeline Access
-- MAGIC We may want to limit access to our pipeline to specific workspace users, groups, or service principals. DLT pipelines have several levels of access:
-- MAGIC
-- MAGIC - `IS OWNER` has full management privileges. This can be assigned to a workspace user or service principal. Pipeline updates will be run with the data access levels of the `IS OWNER` user.
-- MAGIC - `CAN MANAGE` grants privileges to edit pipeline settings, run the pipeline, and view updates. This can be assigned to any workspace user, service principal, or group.
-- MAGIC - `CAN RUN` grants privileges to run the pipeline and view updatess.
-- MAGIC - `CAN VIEW` grants privileges to view updates.
-- MAGIC
-- MAGIC ***BEST PRACTICE!** In workspaces governed with Unity Catalog, we can grant users access to view the Spark driver logs by setting the `spark.databricks.acl.needAdminPermissionToViewLogs` to `false`.*
-- MAGIC
-- MAGIC <br><img src="https://i.imgur.com/umLJaEq.png" width=600>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC DLT has numerous applications for data ingestion and transformation. In this series, we showed:
-- MAGIC
-- MAGIC - How DLT simplifies pipeline development
-- MAGIC - Data quality expectations and monitoring
-- MAGIC - Patterns for merging and appending datasets
-- MAGIC - Configuring ipeline and table properties
-- MAGIC - Productionizing DLT workloads
-- MAGIC
-- MAGIC Feel free to explore the demos, modify pipeline and table definitions, run updates, and check results. Please note that any code is *intended for demonstration* and *should be modified* for your production use-cases.
