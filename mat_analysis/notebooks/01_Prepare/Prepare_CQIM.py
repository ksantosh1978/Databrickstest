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
# MAGIC TRUNCATE TABLE $outSchema.cqim_measures_and_rates;
# MAGIC TRUNCATE TABLE $outSchema.cqim_output_rates;
# MAGIC
# MAGIC DELETE FROM $outSchema.cqim_dq_csv
# MAGIC WHERE RPStartDate = '$RPStartdate';
# MAGIC VACUUM $outSchema.cqim_dq_csv RETAIN 8 HOURS;

# COMMAND ----------

dbutils.notebook.exit("Notebook: Prepare_CQIM ran successfully")