-- Databricks notebook source
-- MAGIC %python
-- MAGIC # dbutils.widgets.text("outSchema", "mat_analysis")
-- MAGIC # dbutils.widgets.text("RPEnddate", "2019-04-01")
-- MAGIC # dbutils.widgets.text("runTimestamp", "2019")

-- COMMAND ----------

delete from  $outSchema.dq_store
where ReportingPeriodEndDate = '$RPEnddate';

VACUUM $outSchema.dq_store RETAIN 8 HOURS;

-- COMMAND ----------

insert into $outSchema.dq_store
select ReportingPeriodStartDate, ReportingPeriodEndDate, Org_Code, DataTable, UID, DataItem, Valid, Default, Invalid, Missing, Denominator, '$runTimestamp' as DateTimeRun
from $outSchema.dq_csv

-- COMMAND ----------

delete from  $outSchema.maternity_monthly_measures_store
where ReportingPeriodEndDate = '$RPEnddate';

VACUUM $outSchema.maternity_monthly_measures_store RETAIN 8 HOURS;

-- COMMAND ----------

insert into $outSchema.maternity_monthly_measures_store
select ReportingPeriodStartDate, ReportingPeriodEndDate, Dimension, Org_Level, Org_Code, Org_Name, Measure, Count_Of, Value_Unsuppressed, '$runTimestamp' as DateTimeRun
from $outSchema.maternity_monthly_csv_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Notebook:persist_data ran successfully")