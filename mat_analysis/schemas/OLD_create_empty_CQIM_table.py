# Databricks notebook source
# dbutils.widgets.text("outSchema", "mat_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists $outSchema.cqim
# MAGIC (
# MAGIC OrgCodeProvider string,
# MAGIC ReportingPeriodStartDate string, --this relates to the period of data used to calculate the measure
# MAGIC ReportingPeriodEndDate string, 
# MAGIC CQIM_ID string,
# MAGIC Numerator float,
# MAGIC Denominator float
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC --#TODO: change the percentage to be a value column and adjust for rate per thousand items? see Giles.
# MAGIC
# MAGIC create or replace view $outSchema.cqim_with_percentage
# MAGIC
# MAGIC as 
# MAGIC
# MAGIC select OrgCodeProvider,
# MAGIC ReportingPeriodStartDate, 
# MAGIC ReportingPeriodEndDate, 
# MAGIC CQIM_ID,
# MAGIC Numerator,
# MAGIC Denominator,
# MAGIC round((Numerator*100.0)/Denominator,0) as Percentage
# MAGIC
# MAGIC from $outSchema.cqim;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.cqim_rates
# MAGIC (Org_Code	string,
# MAGIC Org_Name	string,
# MAGIC Org_Level	string,
# MAGIC ReportingPeriodStartDate	string,
# MAGIC ReportingPeriodEndDate	string,
# MAGIC CQIM_ID	string,
# MAGIC Numerator	float,
# MAGIC Denominator	float,
# MAGIC Rate	float)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);