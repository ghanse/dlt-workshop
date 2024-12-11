# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering with Delta Live Tables
# MAGIC This series of notebooks will demonstrate many capabilities of Databricks' Delta Live Tables. Used by customers across a range of industries, Delta Live Tables resolves common data engineering challenges and allows data engineers to focus on data through its declarative syntax.
# MAGIC
# MAGIC Please note that all notebooks are intended for demonstration. While the code can serve as a reference, it should always be modified when building production use-cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Let's start by running a setup script. This will:
# MAGIC
# MAGIC - Create a Unity Catalog volume to hold your files
# MAGIC - Create a Unity Catalog schema for any tables you create
# MAGIC - Generate some simulated financial data

# COMMAND ----------

# MAGIC %run ./Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sections
# MAGIC We'll divide our hands-on content into several sections:
# MAGIC
# MAGIC 1. <a href="$./1. Data Ingestion and Transformation">Data Ingestion and Transformation</a> introduces Delta Live Tables and shows how to implement a basic medallion architecture
# MAGIC 1. <a href="$./2. Data Quality with Expectations">Data Quality with Expectations</a> demonstrates DLT's rules-based approach to promoting data quality
# MAGIC 1. <a href="$./3. Advanced Data Transformations and Flows">Advanced Data Transformations and Flows</a> discusses appending and merging datasets
# MAGIC 1. <a href="$./4. Pipeline and Table Properties">Pipeline and Table Properties</a> shows how we can dictate table layout, customize pipeline settings, and add custom parameters
# MAGIC 1. <a href="$./5. Deploying and Monitoring Pipelines">Deploying and Monitoring Pipelines</a> overviews DLT's observability mechanisms
# MAGIC
# MAGIC Once you've run the setup script, open a notebook to get started!
