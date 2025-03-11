# Databricks notebook source
# dbutils.widgets.text("outSchema", "mat_analysis")
outputSchema = dbutils.widgets.get("outSchema")
assert outputSchema

print(outputSchema)

# COMMAND ----------

# DBTITLE 1,1. Checking whether any non-blank columns not populated
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check as
# MAGIC (select count(*) as check1
# MAGIC from $outSchema.maternity_monthly_csv_sql
# MAGIC where Dimension is null or Org_Level is null or Org_Code is null or Org_Name is null or Value_Unsuppressed is null)
# MAGIC

# COMMAND ----------

# DBTITLE 1,2. Check all 8 geography breakdown is populated for all the dimensions
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check2 as
# MAGIC select count(*) as check2 from
# MAGIC (
# MAGIC select Dimension, count (distinct Org_Level) as count_orglevel
# MAGIC from $outSchema.maternity_monthly_csv_sql
# MAGIC group by Dimension
# MAGIC having count (distinct Org_Level) <> 8
# MAGIC )
# MAGIC
# MAGIC --#TODO: drive this number from the geographies dictionary? passing in value in Widget?

# COMMAND ----------

# DBTITLE 1,3. National figure should be <= sum of each org_level breakdown figure (except for the Average dimensions)
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check3 as
# MAGIC select count(*) as check3 
# MAGIC from(
# MAGIC select Dimension, sum(National) as National, sum(Region) as Region, sum(LocalOffice) as LocalOffice, sum(Provider) as Provider
# MAGIC from (
# MAGIC select Dimension
# MAGIC       ,case when Org_Level = 'National' then sum(Value_Unsuppressed) end as National
# MAGIC       ,case when Org_Level = 'NHS England (Region)' then sum(Value_Unsuppressed) end as Region
# MAGIC       ,case when Org_Level = 'NHS England (Region, Local Office)' then sum(Value_Unsuppressed) end as LocalOffice
# MAGIC       ,case when Org_Level = 'Provider' then sum(Value_Unsuppressed) end as Provider
# MAGIC  from $outSchema.maternity_monthly_csv_sql
# MAGIC  where Dimension not like '%Average'
# MAGIC group by Dimension, Org_Level
# MAGIC )
# MAGIC group by Dimension
# MAGIC )
# MAGIC where National > Region or National > LocalOffice or National > Provider

# COMMAND ----------

# DBTITLE 1,4. Difference btwn National figure & sum of each geography breakdown figure is not greater than 10 (except for the Average dimensions)
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check4 as
# MAGIC select count(*)
# MAGIC from(
# MAGIC select Dimension, sum(National) as National, sum(Region) as Region, sum(LocalOffice) as LocalOffice, sum(Provider) as Provider
# MAGIC from (
# MAGIC select Dimension
# MAGIC       ,case when Org_Level = 'National' then sum(Value_Unsuppressed) end as National
# MAGIC       ,case when Org_Level = 'NHS England (Region)' then sum(Value_Unsuppressed) end as Region
# MAGIC       ,case when Org_Level = 'NHS England (Region, Local Office)' then sum(Value_Unsuppressed) end as LocalOffice
# MAGIC       ,case when Org_Level = 'Provider' then sum(Value_Unsuppressed) end as Provider
# MAGIC  from $outSchema.maternity_monthly_csv_sql
# MAGIC  where Dimension not like '%Avg'
# MAGIC group by Dimension, Org_Level
# MAGIC )
# MAGIC group by Dimension
# MAGIC )
# MAGIC where (Region- National) > 10  
# MAGIC or (LocalOffice - National) > 10 
# MAGIC or (Provider - National) > 10
# MAGIC
# MAGIC -- #TODO: Why a difference of 10? Count distinct could give bigger variation. This is why this is a warning and we need to tweak the threshold in the future maybe.
# MAGIC

# COMMAND ----------

# DBTITLE 1,5. For Dimensions which are broken down by various measures, check whether the number measures are consistent at different geography level  
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check5 as
# MAGIC select count(*) from
# MAGIC (
# MAGIC   select Dimension
# MAGIC         ,case when Org_Level = 'National' then count (distinct Measure) end as National_measure_count
# MAGIC         ,case when Org_Level = 'NHS England (Region)' then count (distinct Measure) end as Region_measure_count
# MAGIC         ,case when Org_Level = 'NHS England (Region, Local Office)' then count (distinct Measure) end as LRegion_measure_count
# MAGIC         ,case when Org_Level = 'Provider' then count (distinct Measure) end as Provider_measure_count
# MAGIC   from $outSchema.maternity_monthly_csv_sql
# MAGIC   group by Dimension, Org_Level
# MAGIC )
# MAGIC where (National_measure_count <> Region_measure_count) or 
# MAGIC       (National_measure_count <> LRegion_measure_count) or
# MAGIC       (National_measure_count <> Provider_measure_count) 

# COMMAND ----------

# DBTITLE 1,6. Check whether total submissions for each measure is same across all geography breakdown
# MAGIC %sql
# MAGIC CREATE or replace TEMPORARY View  csv_check6 as
# MAGIC select count(*) from
# MAGIC (
# MAGIC   select Dimension,Measure, sum(National) as National, sum(Region) as Region, sum(LRegion) as LRegion, sum(Provider) as Provider
# MAGIC   from
# MAGIC   (
# MAGIC     select Dimension
# MAGIC           ,Measure
# MAGIC           ,case when Org_Level = 'National' then sum(Value_Unsuppressed) end as National
# MAGIC           ,case when Org_Level = 'NHS England (Region)' then sum(Value_Unsuppressed) end as Region
# MAGIC           ,case when Org_Level = 'NHS England (Region, Local Office)' then sum(Value_Unsuppressed) end as LRegion
# MAGIC           ,case when Org_Level = 'Provider' then sum(Value_Unsuppressed) end as Provider
# MAGIC     from $outSchema.maternity_monthly_csv_sql
# MAGIC     where Dimension = 'Submission_Orgs'
# MAGIC     group by Dimension, Measure, Org_Level
# MAGIC   )
# MAGIC   group by Dimension, Measure
# MAGIC )
# MAGIC where National = Region or
# MAGIC       National <> Lregion or
# MAGIC       National <> Provider
# MAGIC       
# MAGIC

# COMMAND ----------

# DBTITLE 1,Pass all the Error/Warning message to the main caller notebook
import pandas as pd
import numpy as np
from pyspark import SparkContext

pdCheck1 = spark.sql("select * from csv_check").toPandas()
pdCheck2 = spark.sql("select * from csv_check2").toPandas()
pdCheck3 = spark.sql("select * from csv_check3").toPandas()
pdCheck4 = spark.sql("select * from csv_check4").toPandas()
pdCheck5 = spark.sql("select * from csv_check5").toPandas()
pdCheck6 = spark.sql("select * from csv_check6").toPandas()

check1 = int(pdCheck1.loc[0])
check2 = int(pdCheck2.loc[0])
check3 = int(pdCheck3.loc[0])
check4 = int(pdCheck4.loc[0])
check5 = int(pdCheck5.loc[0])
check6 = int(pdCheck6.loc[0])

check1
A = []
if check1 > 0 : 
  Error1 = "Error: Mandatory columns Dimension/Org_Level/Org_Code/Org_Name/Value_Unsuppressed not populated for one or more records" 
  A = [Error1]
if check2 > 0 :
  Error2 = "Error: if after April 2020, check this. Some Dimensions geography breakdowns missing or over-generated. Warning: before April 2020. May be extra local region"
  A= A + [Error2]
if check3 > 0 :
  Error3 = "Error: National figure should be <= sum of each org_level breakdown figure for all Dimensions"
  A= A + [Error3]
if check5 > 0 :
  Error5 = "Error: Each geography (Org_Level) breakdown should have the same set of Measures across all Dimensions"
  A= A + [Error5]
if check6 > 0 :
  Error6 = "Error: For Dimension 'Submission_Orgs', the total number of submitters should be same for each geography (Org_Level) breakdown"
  A= A + [Error6]
if check4 > 0 :
  Warning1 = "Warning: Difference btwn National figure & sum of each geography (Org_Level) breakdown figure is than greater than 10 for one or more Dimension"
  A= A + [Warning1]

if any(A): 
    mystring = A
    dbutils.notebook.exit(mystring)
else: 
  dbutils.notebook.exit("All basic checks successfully completed on the csv output!")