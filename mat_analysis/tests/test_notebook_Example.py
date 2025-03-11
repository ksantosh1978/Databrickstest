# Databricks notebook source
dbutils.widgets.text("db","", "target db")
db = dbutils.widgets.get("db")
assert db

# COMMAND ----------

total_count = spark.sql(
  "select * FROM {}.CaP_monthly_Unformatted WHERE REPORTING_PERIOD = 'TEST_REPORTING_PERIOD' AND BREAKDOWN = 'TEST_BREAKDOWN'".format(db)).count()

assert total_count == 1

# COMMAND ----------

