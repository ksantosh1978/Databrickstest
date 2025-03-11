# Databricks notebook source
# dbutils.widgets.text("outSchema", "mat_analysis")

# COMMAND ----------

# DBTITLE 1,Create Maternity_Monthly_CSV_SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.maternity_monthly_csv_sql(
# MAGIC        Count_Of STRING,
# MAGIC        ReportingPeriodStartDate STRING,
# MAGIC        ReportingPeriodEndDate STRING,
# MAGIC        Dimension STRING,
# MAGIC        Org_Code STRING,
# MAGIC        Org_Name STRING,
# MAGIC        Org_Level STRING,
# MAGIC        Measure STRING,
# MAGIC        Measure_Desc STRING,
# MAGIC        Value_Unsuppressed DECIMAL
# MAGIC        )
# MAGIC        USING DELTA
# MAGIC        PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# DBTITLE 1,Create DQ_CSV
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.dq_csv(
# MAGIC        ReportingPeriodStartDate STRING,
# MAGIC        ReportingPeriodEndDate STRING,
# MAGIC        Org_Code STRING,
# MAGIC        DataTable STRING,
# MAGIC        UID STRING,
# MAGIC        DataItem STRING,
# MAGIC        Valid INT,
# MAGIC        Default INT,
# MAGIC        Invalid INT,
# MAGIC        Missing INT,
# MAGIC        Denominator INT      
# MAGIC        )
# MAGIC        USING DELTA
# MAGIC        PARTITIONED BY (ReportingPeriodEndDate);
# MAGIC        

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists $outSchema.dq_store
# MAGIC (
# MAGIC ReportingPeriodStartDate STRING, 
# MAGIC ReportingPeriodEndDate STRING, 
# MAGIC Org_Code STRING, 
# MAGIC DataTable STRING, 
# MAGIC UID STRING, 
# MAGIC DataItem STRING, 
# MAGIC Valid INT, 
# MAGIC Default INT, 
# MAGIC Invalid INT, 
# MAGIC Missing INT,
# MAGIC DateTimeRun STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

outputSchema = dbutils.widgets.get("outSchema")

# COMMAND ----------

# fixes bug DMO-567

sql = "DESCRIBE {outputSchema}.dq_store Denominator".format(outputSchema=outputSchema)
try:
  spark.sql(sql)
except:
  sql = "ALTER TABLE {outputSchema}.dq_store ADD COLUMNS (Denominator INT AFTER Missing)".format(outputSchema=outputSchema)
  print(sql)
  spark.sql(sql)
else:
  pass

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists $outSchema.maternity_monthly_measures_store
# MAGIC (
# MAGIC ReportingPeriodStartDate STRING, 
# MAGIC ReportingPeriodEndDate STRING, 
# MAGIC Dimension STRING, 
# MAGIC Org_Level STRING, 
# MAGIC Org_Code STRING, 
# MAGIC Org_Name STRING, 
# MAGIC Measure STRING, 
# MAGIC Count_Of STRING, 
# MAGIC Value_Unsuppressed DECIMAL,
# MAGIC DateTimeRun STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists $outSchema.cqim_store
# MAGIC (
# MAGIC ReportingPeriodStartDate STRING, 
# MAGIC ReportingPeriodEndDate STRING, 
# MAGIC OrgCodeProvider STRING, 
# MAGIC CQIM_ID STRING, 
# MAGIC Numerator FLOAT, 
# MAGIC Denominator FLOAT, 
# MAGIC DateTimeRun STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.cqim_rates_store
# MAGIC (Org_Code	string,
# MAGIC Org_Name	string,
# MAGIC Org_Level	string,
# MAGIC ReportingPeriodStartDate	string,
# MAGIC ReportingPeriodEndDate	string,
# MAGIC CQIM_ID	string,
# MAGIC Numerator	float,
# MAGIC Denominator	float,
# MAGIC Rate	float,
# MAGIC DateTimeRun STRING)
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.audit_mat_analysis
# MAGIC (
# MAGIC     MONTH_ID int,   
# MAGIC     STATUS string,   
# MAGIC     REPORTING_PERIOD_START date,  
# MAGIC     REPORTING_PERIOD_END date,   
# MAGIC     SOURCE_DB string,   
# MAGIC     RUN_START timestamp,   
# MAGIC     RUN_END timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (MONTH_ID, STATUS)