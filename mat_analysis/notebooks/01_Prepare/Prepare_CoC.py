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

# DBTITLE 1,CoC_Numerator_Geographies and CoC_Denominator_Geographies are used to determine provider name...
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW $outSchema.CoC_Numerator_Geographies AS
# MAGIC SELECT * from $outSchema.CoC_Numerator_Raw as nr
# MAGIC LEFT JOIN $outSchema.geogtlrr as geo
# MAGIC ON nr.OrgCodeProvider = geo.Trust_ORG

# COMMAND ----------

# DBTITLE 1,...and are also used to group by when determining geographic aggregations (Region, STP etc.)
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW $outSchema.CoC_Denominator_Geographies AS
# MAGIC SELECT * from $outSchema.CoC_Denominator_Raw as dr
# MAGIC LEFT JOIN $outSchema.geogtlrr as geo
# MAGIC ON dr.OrgCodeProvider = geo.Trust_ORG

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table $outSchema.CoC_Numerator_Raw;
# MAGIC truncate table $outSchema.CoC_Denominator_Raw;
# MAGIC truncate table $outSchema.CoC_Provider_Aggregated;
# MAGIC truncate table $outSchema.CoC_Geography_Aggregated;
# MAGIC
# MAGIC -- DELETE FROM $outSchema.CoC_Numerator_Raw WHERE RPStartDate = '$RPStartdate' ;
# MAGIC -- VACUUM $outSchema.CoC_Numerator_Raw RETAIN 8 HOURS;
# MAGIC
# MAGIC -- DELETE FROM $outSchema.CoC_Denominator_Raw WHERE RPStartDate = '$RPStartdate' ;
# MAGIC -- VACUUM $outSchema.CoC_Denominator_Raw RETAIN 8 HOURS;
# MAGIC
# MAGIC -- DELETE FROM $outSchema.CoC_Provider_Aggregated WHERE RPStartDate = '$RPStartdate' ;
# MAGIC -- VACUUM $outSchema.CoC_Provider_Aggregated RETAIN 8 HOURS;
# MAGIC
# MAGIC -- DELETE FROM $outSchema.CoC_Geography_Aggregated WHERE RPStartDate = '$RPStartdate' ;
# MAGIC -- VACUUM $outSchema.CoC_Geography_Aggregated RETAIN 8 HOURS;
# MAGIC
# MAGIC DELETE FROM $outSchema.CoC_CSV WHERE RPStartDate = '$RPStartdate';
# MAGIC VACUUM $outSchema.CoC_CSV RETAIN 8 HOURS;

# COMMAND ----------

dbutils.notebook.exit("Notebook: Prepare_CoC ran successfully")