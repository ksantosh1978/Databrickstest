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
# MAGIC DELETE FROM  $outSchema.Measures_Raw WHERE IndicatorFamily like 'BMI%';
# MAGIC DELETE FROM $outSchema.Measures_Aggregated WHERE IndicatorFamily like 'BMI%';
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DELETE FROM $outSchema.Measures_CSV WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartDate';
# MAGIC