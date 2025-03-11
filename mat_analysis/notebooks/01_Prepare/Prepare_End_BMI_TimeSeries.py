# Databricks notebook source
# DBTITLE 1,Widget values
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert outSchema
RPStartDate = dbutils.widgets.get("RPStartDate")
print(RPStartDate)
assert RPStartDate

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from $outSchema.Measures_Raw WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartDate';
# MAGIC delete from $outSchema.Measures_Aggregated WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartDate';
# MAGIC