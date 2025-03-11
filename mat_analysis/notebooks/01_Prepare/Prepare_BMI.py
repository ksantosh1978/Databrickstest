# Databricks notebook source
# DBTITLE 1,Widget values
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $outSchema.Measures_Raw WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartdate';
# MAGIC DELETE FROM $outSchema.Measures_Aggregated WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartdate';
# MAGIC DELETE FROM $outSchema.Measures_CSV WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartdate';

# COMMAND ----------

dbutils.notebook.exit("Notebook: Prepare_BMI ran successfully")