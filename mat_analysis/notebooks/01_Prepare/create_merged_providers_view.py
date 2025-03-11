# Databricks notebook source

params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# DBTITLE 1,Translate organisation codes where mergers have taken place
# MAGIC %sql
# MAGIC create or replace view $outSchema.msd000header
# MAGIC
# MAGIC as 
# MAGIC with cte as 
# MAGIC (
# MAGIC select distinct a.REL_TO_ORG_CODE as OldCode, a.REL_FROM_ORG_CODE as NewCode 
# MAGIC from $dss_corporate.ORG_RELATIONSHIP_DAILY a
# MAGIC inner join $dbSchema.msd000header b on a.REL_TO_ORG_CODE = b.OrgCodeProvider and RPEndDate <= '$RPEnddate'
# MAGIC where REL_TYPE_CODE = 'P' and REL_OPEN_DATE < '$RPEnddate' and (REL_CLOSE_DATE is null or REL_CLOSE_DATE >= '$RPEnddate') 
# MAGIC )
# MAGIC select OrgCodeProvider, coalesce(NewCode, OrgCodeProvider) as OrgCodeProviderMerge, UniqSubmissionID, RPStartDate, RPEndDate
# MAGIC from  $dbSchema.msd000header h
# MAGIC left join cte m on m.OldCode = h.OrgCodeProvider

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view $outSchema.HESAnnualBirths
# MAGIC
# MAGIC as 
# MAGIC with cte as 
# MAGIC (
# MAGIC select distinct a.REL_TO_ORG_CODE as OldCode, a.REL_FROM_ORG_CODE as NewCode 
# MAGIC from $dss_corporate.ORG_RELATIONSHIP_DAILY a
# MAGIC inner join $outSchema.annual_hes_input2223 b on a.REL_TO_ORG_CODE = b.org_code
# MAGIC where REL_TYPE_CODE = 'P' and REL_OPEN_DATE < '$RPEnddate' and (REL_CLOSE_DATE is null or REL_CLOSE_DATE >= '$RPEnddate') 
# MAGIC )
# MAGIC select coalesce(NewCode, org_code) as OrgCodeProviderMerge, h.*
# MAGIC from  $outSchema.annual_hes_input2223 h
# MAGIC left join cte m on m.OldCode = h.org_code

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit("Notebook: create_merged_providers_view ran successfully")