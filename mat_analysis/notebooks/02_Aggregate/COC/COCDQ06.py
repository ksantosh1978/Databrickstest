# Databricks notebook source
# %py
#  dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,WIDGETS
RPBegindate = dbutils.widgets.get("RPBegindate")
print(RPBegindate)
assert(RPBegindate)
RPEnddate = dbutils.widgets.get("RPEnddate")
print(RPEnddate)
assert(RPEnddate)
dbSchema = dbutils.widgets.get("dbSchema")
print(dbSchema)
assert(dbSchema)
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)
status = dbutils.widgets.get("status")
print(status)
assert(status)

# COMMAND ----------

# DBTITLE 1,DenomDQ06_base
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ06_base as
# MAGIC with cte as
# MAGIC (
# MAGIC select 
# MAGIC    msd101.MSD101_ID as KeyValue
# MAGIC   ,msd101.UniqPregId
# MAGIC   ,msd101.Person_ID_Mother
# MAGIC   ,msd101.PregnancyID
# MAGIC   ,msd101.AntenatalAppDate
# MAGIC   ,msd101.RecordNumber
# MAGIC   ,msd101.GestAgeBooking
# MAGIC   ,msd101.DischargeDateMatService
# MAGIC   ,msd101.OrgCodeProvider
# MAGIC   ,rank() OVER(PARTITION BY msd101.Person_ID_Mother, msd101.UniqPregId ORDER BY msd101.AntenatalAppDate desc, msd101.RecordNumber desc)as rank
# MAGIC   --  from $dbSchema.MSD101PregnancyBooking msd101
# MAGIC from global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC where 
# MAGIC   msd101.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC   AND 
# MAGIC     msd101.GestAgeBooking > 0
# MAGIC     )
# MAGIC
# MAGIC select * from cte where rank = 1
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(add_months('$RPBegindate',-4), AntenatalAppDate)) <= 203
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(last_day(add_months('$RPEnddate',-4)), AntenatalAppDate)) >= 203
# MAGIC   AND 
# MAGIC     (DischargeDateMatService is null or (GestAgeBooking + datediff(DischargeDateMatService, AntenatalAppDate)) > 203)

# COMMAND ----------

# DBTITLE 1,Denominator - Raw data
# MAGIC %sql
# MAGIC create or replace temp view COC_DQ06_Denominator_Raw as 
# MAGIC with cte as (
# MAGIC select 
# MAGIC   d_base.KeyValue
# MAGIC  ,'$RPBegindate' AS RPStartDate
# MAGIC  ,'$RPEnddate' AS RPEndDate
# MAGIC  ,'$status' AS Status
# MAGIC  ,'COC_DQ06' AS Indicator
# MAGIC  ,'COC_DQ_Measure' AS IndicatorFamily
# MAGIC  ,d_base.Person_ID_Mother
# MAGIC  ,d_base.UniqPregID              
# MAGIC  ,d_base.Person_ID_Mother                
# MAGIC  ,d_base.AntenatalAppDate                
# MAGIC  ,d_base.GestAgeBooking                
# MAGIC  ,d_base.OrgCodeProvider    
# MAGIC  ,MSD102.CarePlanDate                
# MAGIC  ,MSD102.RecordNumber 
# MAGIC  ,msd102.ContCarePathInd
# MAGIC  ,msd102.CareProfLID
# MAGIC  ,msd102.TeamLocalID
# MAGIC  ,current_timestamp() as CreatedAt
# MAGIC  ,rank() OVER(PARTITION BY msd102.Person_ID_Mother,  msd102.UniqPregId ORDER BY msd102.CarePlanDate desc, msd102.RecordNumber desc) as rank
# MAGIC from DenomDQ06_base d_base
# MAGIC inner join $dbSchema.MSD102MatCarePlan msd102
# MAGIC on d_base.Person_ID_Mother = msd102.Person_ID_Mother and d_base.UniqPregID = msd102.UniqPregID
# MAGIC  
# MAGIC Where MSD102.RPStartDate <= add_months('$RPBegindate',-4)            
# MAGIC             and msd102.CarePlanType = "05"
# MAGIC             and msd102.ContCarePathInd is not null
# MAGIC             and (d_base.GestAgeBooking + datediff(MSD102.CarePlanDate,d_base.AntenatalAppDate  )) <= 203 --THIS ONE NEEDS SANITY CHECKING
# MAGIC ) 
# MAGIC               
# MAGIC select  KeyValue
# MAGIC        ,RPStartDate
# MAGIC        ,RPEndDate
# MAGIC        ,Status
# MAGIC        ,Indicator
# MAGIC        ,IndicatorFamily
# MAGIC        ,Person_ID_Mother
# MAGIC        ,UniqPregID
# MAGIC        ,CarePlanDate
# MAGIC        ,OrgCodeProvider 
# MAGIC        ,CreatedAt
# MAGIC from cte where rank = 1
# MAGIC and  ContCarePathInd = 'Y'
# MAGIC and CareProfLID is not null
# MAGIC and TeamLocalID is not null

# COMMAND ----------

# DBTITLE 1,Denominator - Raw into generic table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.CoC_Denominator_Raw 
# MAGIC SELECT d.KeyValue
# MAGIC        , d.RPStartDate
# MAGIC        , d.RPEndDate
# MAGIC        , d.Status
# MAGIC        , d.Indicator
# MAGIC        , d.IndicatorFamily
# MAGIC        , d.Person_ID_Mother as Person_ID
# MAGIC        , d.UniqPregID as PregnancyID
# MAGIC        , null as CareConID
# MAGIC        , d.OrgCodeProvider
# MAGIC        , 1 as Rank
# MAGIC        , 0 as OverDQThreshold
# MAGIC        , CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ06_Denominator_Raw d
# MAGIC

# COMMAND ----------

# DBTITLE 1,COC_DQ06_Numerator_Raw
# MAGIC %sql
# MAGIC  
# MAGIC create or replace temp view COC_DQ06_Numerator_Raw as
# MAGIC with cte as (
# MAGIC select
# MAGIC  DenomDQ06.RPStartDate
# MAGIC ,DenomDQ06.RPEndDate
# MAGIC ,DenomDQ06.KeyValue
# MAGIC ,DenomDQ06.UniqPregID                
# MAGIC ,DenomDQ06.Person_ID_Mother                
# MAGIC ,DenomDQ06.OrgCodeProvider    
# MAGIC ,DenomDQ06.Status
# MAGIC ,DenomDQ06.Indicator
# MAGIC ,DenomDQ06.IndicatorFamily
# MAGIC ,0 as OverDQThreshold
# MAGIC ,DenomDQ06.CreatedAt
# MAGIC ,rank() OVER(PARTITION BY msd101.Person_ID_Mother,  msd101.UniqPregId order by MSD101.DischargeDateMatService desc, msd101.RecordNumber desc) as rank
# MAGIC FROM 
# MAGIC   COC_DQ06_Denominator_Raw DenomDQ06
# MAGIC INNER JOIN
# MAGIC --   $dbSchema.MSD101PregnancyBooking msd101
# MAGIC    global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC   
# MAGIC on MSD101.Person_ID_Mother = DenomDQ06.Person_ID_Mother and MSD101.UniqPregID = DenomDQ06.UniqPregID
# MAGIC Where MSD101.RPStartDate <= '$RPBegindate'            
# MAGIC and (MSD101.DischargeDateMatService is not null and MSD101.DischargeDateMatService >= DenomDQ06.CarePlanDate)
# MAGIC )
# MAGIC select  KeyValue
# MAGIC        ,RPStartDate
# MAGIC        ,RPEndDate
# MAGIC        ,Status
# MAGIC        ,Indicator
# MAGIC        ,IndicatorFamily
# MAGIC        ,Person_ID_Mother
# MAGIC        ,UniqPregID
# MAGIC        ,OrgCodeProvider
# MAGIC        ,OverDQThreshold
# MAGIC        ,CreatedAt
# MAGIC from cte 
# MAGIC where rank = 1

# COMMAND ----------

# DBTITLE 1,Numerator - Raw into generic table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.CoC_Numerator_Raw 
# MAGIC SELECT d.KeyValue
# MAGIC        , d.RPStartDate
# MAGIC        , d.RPEndDate
# MAGIC        , d.Status
# MAGIC        , d.Indicator
# MAGIC        , d.IndicatorFamily
# MAGIC        , d.Person_ID_Mother as Person_ID
# MAGIC        , d.UniqPregID as PregnancyID
# MAGIC        , null as CareConID
# MAGIC        , d.OrgCodeProvider
# MAGIC        , 1 as Rank
# MAGIC        , d.OverDQThreshold
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ06_Numerator_Raw d
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateProvider
# MAGIC %sql
# MAGIC
# MAGIC with cteclosed as (
# MAGIC                             select ORG_CODE, NAME 
# MAGIC                             from $dss_corporate.ORG_DAILY 
# MAGIC                             where BUSINESS_END_DATE is NULL 
# MAGIC                             and org_type_code='TR'
# MAGIC                           ) 
# MAGIC INSERT INTO $outSchema.CoC_Provider_Aggregated 
# MAGIC SELECT 
# MAGIC den.RPStartDate
# MAGIC        , den.RPEndDate
# MAGIC        , den.Status
# MAGIC        , den.Indicator
# MAGIC        , den.IndicatorFamily
# MAGIC        , den.OrgCodeProvider
# MAGIC       , COALESCE(den.Trust, c.NAME) AS OrgName
# MAGIC       , 'Provider' AS OrgLevel
# MAGIC       , COALESCE(num.Total, 0) as Unrounded_Numerator
# MAGIC       , den.Total as Unrounded_Denominator
# MAGIC       , case when COALESCE(num.Total, 0) > 7 then round((COALESCE(num.Total, 0)/den.Total)*100, 1) else 0 end as Unrounded_Rate
# MAGIC       , case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 end as Rounded_Numerator
# MAGIC       , case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator
# MAGIC       , round((case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 END / case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end)*100, 1) as Rounded_Rate
# MAGIC       , 0 AS OverDQThreshold
# MAGIC       , current_timestamp() as CreatedAt
# MAGIC FROM (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, count(*) as Total 
# MAGIC         FROM $outSchema.CoC_Numerator_Geographies
# MAGIC         WHERE Indicator = 'COC_DQ06'
# MAGIC         GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC               FROM $outSchema.CoC_Denominator_Geographies
# MAGIC               WHERE Indicator = 'COC_DQ06'
# MAGIC               GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) AS den
# MAGIC   ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_DQ06'
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumeratorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Numerator_Raw as nr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Numerator_Raw as nr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ06' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg 
# MAGIC --       ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = 'COC_DQ06' 
# MAGIC --      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DenominatorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Denominator_Raw as dr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Denominator_Raw as dr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ06' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg
# MAGIC --       ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = 'COC_DQ06'
# MAGIC --       WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

dbutils.notebook.exit("Notebook: COCDQ06 ran successfully")