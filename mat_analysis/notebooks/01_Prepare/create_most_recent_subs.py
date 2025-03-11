# Databricks notebook source
# DBTITLE 1,Widgets
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# DBTITLE 1,Limiting data to most recent submission and including orgcode where orgs merge
# MAGIC %sql
# MAGIC -- #TODO: parametrize output database name
# MAGIC
# MAGIC -- """
# MAGIC -- This code allows for use of the _cur_ schemas for testing purposes. Also, eliminates issue from bug which allows multiple header rows for one organisation. 
# MAGIC -- """
# MAGIC
# MAGIC create or replace view $outSchema.MostRecentSubmissions
# MAGIC
# MAGIC as
# MAGIC
# MAGIC select latest.RPStartDate, latest.RPEndDate, coalesce(h.OrgCodeProviderMerge, latest.OrgCodeProvider) as OrgCodeProvider, latest.UniqSubmissionID
# MAGIC from (
# MAGIC   select RPStartDate, RPEndDate, OrgCodeProvider,UniqSubmissionID,
# MAGIC   RANK () OVER ( PARTITION BY OrgCodeProvider ORDER BY cast(UniqSubmissionID as int) desc) as R_USB
# MAGIC   from $dbSchema.msd000header
# MAGIC   where RPStartDate = '$RPStartdate'
# MAGIC ) as latest
# MAGIC INNER JOIN $outSchema.msd000header as h on R_USB = 1 and h.UniqSubmissionID = latest.UniqSubmissionID 

# COMMAND ----------

dbutils.notebook.exit("Notebook: create_most_recent_subs ran successfully") # This will return a value with text specified