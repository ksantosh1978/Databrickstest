# Databricks notebook source
# DBTITLE 1,Create widgets
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT RPBegindate DEFAULT "2019-04-01";
# MAGIC CREATE WIDGET TEXT dbSchema DEFAULT "mat_cur_clear";
# MAGIC CREATE WIDGET TEXT outSchema DEFAULT "mat_analysis"

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view $outSchema.DQInventoryTemp as
# MAGIC select * 
# MAGIC from $outSchema.dq_store
# MAGIC where ReportingPeriodStartDate between add_months("$RPBegindate", -6) and "$RPBegindate" and UID in ("M001070", "M002010", "M001040", "M001050", "M001020", "M001060", "M101040")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view $outSchema.StageOne as
# MAGIC select
# MAGIC   a.*,
# MAGIC   b.Data_Item_Name__Data_Dict_Element
# MAGIC from $outSchema.DQInventoryTemp a
# MAGIC inner join $outSchema.tosasrefdata b on a.UID=b.UID
# MAGIC order by a.ReportingPeriodStartDate, a.Org_Code, a.UID

# COMMAND ----------

# DBTITLE 1,Production of OUTPUT (Coverage) 
# MAGIC %sql
# MAGIC create or replace view $outSchema.Coverage1 as
# MAGIC select 
# MAGIC   distinct x.Org_Code as DataProviderCode,
# MAGIC   d.NAME as DataProviderName,
# MAGIC   1 as ExpectedToSubmit,
# MAGIC   case when y.Submitted = 1 then y.Submitted else 0 end as Submitted
# MAGIC from $outSchema.StageOne x 
# MAGIC left outer join (select distinct Org_Code, 1 as Submitted from $outSchema.StageOne where ReportingPeriodStartDate = "$RPBegindate") y on x.Org_Code = y.Org_Code 
# MAGIC inner join dss_corporate.ORG_DAILY d on x.Org_Code = d.ORG_CODE
# MAGIC where d.BUSINESS_END_DATE is null and x.ReportingPeriodStartDate between add_months("$RPBegindate", -6) and "$RPBegindate"
# MAGIC order by DataProviderCode

# COMMAND ----------

# DBTITLE 1,Production of OUTPUT (Coverage) - Select into permanent table
# MAGIC %sql
# MAGIC truncate table $outSchema.DQMI_Coverage;
# MAGIC
# MAGIC insert into $outSchema.DQMI_Coverage
# MAGIC select
# MAGIC   concat(date_format("$RPBegindate", "MMM"), " ", date_format("$RPBegindate", "yyyy")) as PeriodFrom,
# MAGIC   concat(date_format("$RPBegindate", "MMM"), " ", date_format("$RPBegindate", "yyyy")) as PeriodTo,
# MAGIC   "Maternity" as Dataset,
# MAGIC   *
# MAGIC from $outSchema.Coverage1

# COMMAND ----------

# DBTITLE 1,Production of OUTPUT (VODIM by provider)
# MAGIC %sql
# MAGIC create or replace view $outSchema.VODIM as
# MAGIC select 
# MAGIC   c.ReportingPeriodStartDate,
# MAGIC   "Maternity" as Dataset,
# MAGIC   c.Org_Code as DataProviderCode,
# MAGIC   d.NAME as DataProviderName,
# MAGIC   c.Data_Item_Name__Data_Dict_Element as DataItem, 
# MAGIC   (c.Valid + c.Default + c.Invalid) as CompleteNumerator, 
# MAGIC   c.Denominator as CompleteDenominator, 
# MAGIC   c.Valid + c.Default as ValidNumerator, 
# MAGIC   c.Default as DefaultNumerator, 
# MAGIC   (c.Valid + c.Default + c.Invalid) as ValidDenominator
# MAGIC from $outSchema.StageOne c 
# MAGIC inner join dss_corporate.ORG_DAILY d on c.Org_Code = d.ORG_CODE
# MAGIC where c.ReportingPeriodStartDate = "$RPBegindate" and d.BUSINESS_END_DATE is null

# COMMAND ----------

# DBTITLE 1,Production of OUTPUT (VODIM by provider) - Select into permanent table
# MAGIC %sql
# MAGIC truncate table $outSchema.DQMI_Data;
# MAGIC
# MAGIC insert into table $outSchema.DQMI_Data
# MAGIC select
# MAGIC   concat(date_format("$RPBegindate", "MMM"), " ", date_format("$RPBegindate", "yyyy")) as PeriodFrom,
# MAGIC   concat(date_format("$RPBegindate", "MMM"), " ", date_format("$RPBegindate", "yyyy")) as PeriodTo,
# MAGIC   Dataset,
# MAGIC   DataProviderCode,
# MAGIC   DataProviderName,
# MAGIC   DataItem, 
# MAGIC   sum(CompleteNumerator) as CompleteNumerator,
# MAGIC   sum(CompleteDenominator) as CompleteDenominator,
# MAGIC   sum(ValidNumerator) as ValidNumerator, 
# MAGIC   sum(DefaultNumerator) as DefaultNumerator,
# MAGIC   sum(ValidDenominator) as ValidDenominator
# MAGIC from $outSchema.VODIM 
# MAGIC group by Dataset, DataProviderCode, DataProviderName, DataItem
# MAGIC order by DataProviderCode, DataItem

# COMMAND ----------

# %sql
# select *
# from $outSchema.DQMI_Coverage

# COMMAND ----------

# %sql
# select *
# from $outSchema.DQMI_Data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists $outSchema.dqinventorytemp;
# MAGIC drop view if exists $outSchema.stageone;
# MAGIC drop view if exists $outSchema.coverage1;
# MAGIC drop view if exists $outSchema.vodim

# COMMAND ----------

dbutils.notebook.exit("Notebook: DQMI ran successfully")