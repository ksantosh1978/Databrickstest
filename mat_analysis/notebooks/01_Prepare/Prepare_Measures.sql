-- Databricks notebook source
-- DBTITLE 1,Global Widget Values
-- MAGIC %python
-- MAGIC
-- MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "Target database")
-- MAGIC #dbutils.widgets.text("RPStartDate", "2021-12-01", "Reporting Period Start Date")
-- MAGIC params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
-- MAGIC           ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
-- MAGIC           ,"outSchema" : dbutils.widgets.get("outSchema")
-- MAGIC           ,"dbSchema" : dbutils.widgets.get("dbSchema") 
-- MAGIC           ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
-- MAGIC           ,"month_id" : dbutils.widgets.get("month_id")
-- MAGIC          }
-- MAGIC print(params)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Clear out Measures Tables
-- MAGIC %sql
-- MAGIC
-- MAGIC TRUNCATE TABLE $outSchema.Measures_raw;
-- MAGIC
-- MAGIC TRUNCATE TABLE $outSchema.Measures_Aggregated;
-- MAGIC
-- MAGIC DELETE
-- MAGIC FROM $outSchema.Measures_CSV
-- MAGIC WHERE RPStartDate = '$RPStartdate';
-- MAGIC
-- MAGIC VACUUM $outSchema.Measures_CSV RETAIN 8 HOURS;

-- COMMAND ----------

-- DBTITLE 1,Create Geographic view over the raw table
CREATE OR REPLACE VIEW $outSchema.Measures_Geographies AS
SELECT * 
FROM $outSchema.Measures_raw AS nr
      LEFT JOIN $outSchema.geogtlrr AS geo
ON nr.OrgCodeProvider = geo.Trust_ORG

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit("Notebook: Prepare_Measures ran successfully")