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

# DBTITLE 1,DenomDQ09_base
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ09_base as
# MAGIC with cte as
# MAGIC (select 
# MAGIC   msd101.msd101_ID as KeyValue
# MAGIC   ,msd101.UniqPregId
# MAGIC   ,msd101.Person_ID_Mother
# MAGIC   ,msd101.AntenatalAppDate
# MAGIC   ,msd101.RPStartDate
# MAGIC   ,msd101.RPEnddate
# MAGIC   ,msd101.RecordNumber
# MAGIC   ,msd101.GestAgeBooking
# MAGIC   ,msd101.DischargeDateMatService
# MAGIC   ,COALESCE(ord.REL_FROM_ORG_CODE, msd101.OrgCodeProvider) AS OrgCodeProvider -- this code handles merged orgCodeProviders
# MAGIC   ,rank() OVER(PARTITION BY msd101.Person_ID_Mother,  msd101.UniqPregId ORDER BY msd101.AntenatalAppDate desc, msd101.RecordNumber desc)as rank
# MAGIC from $dbSchema.MSD101PregnancyBooking msd101
# MAGIC LEFT JOIN $dss_corporate.org_relationship_daily as ord 
# MAGIC       ON msd101.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' 
# MAGIC       and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate')
# MAGIC where 
# MAGIC   msd101.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC   AND 
# MAGIC     msd101.GestAgeBooking > 0)
# MAGIC  
# MAGIC  
# MAGIC select * from cte where rank = 1
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(add_months('$RPBegindate',-4), AntenatalAppDate)) <= 203
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(last_day(add_months('$RPEnddate',-4)), AntenatalAppDate)) >= 203
# MAGIC   AND 
# MAGIC     (DischargeDateMatService is null or 
# MAGIC     (GestAgeBooking + datediff(DischargeDateMatService, AntenatalAppDate)) > 203)
# MAGIC  

# COMMAND ----------

# DBTITLE 1,DenomDQ09_mid
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ09_mid as
# MAGIC with cte as
# MAGIC (
# MAGIC select 
# MAGIC   DenomDQ09_base.KeyValue
# MAGIC   ,DenomDQ09_base.UniqPregId
# MAGIC   ,DenomDQ09_base.Person_ID_Mother
# MAGIC   ,DenomDQ09_base.AntenatalAppDate
# MAGIC   ,DenomDQ09_base.RPStartDate
# MAGIC   ,DenomDQ09_base.RPEnddate
# MAGIC   ,DenomDQ09_base.GestAgeBooking
# MAGIC   ,DenomDQ09_base.OrgCodeProvider
# MAGIC   ,msd102.CarePlanDate
# MAGIC   ,msd102.RecordNumber
# MAGIC   ,msd102.ContCarePathInd
# MAGIC   ,msd102.CareProfLID
# MAGIC   ,msd102.TeamLocalID
# MAGIC   ,current_timestamp() as CreatedAt
# MAGIC   ,rank() OVER(PARTITION BY msd102.Person_ID_Mother,  msd102.UniqPregId ORDER BY msd102.CarePlanDate desc, msd102.RecordNumber desc) as rank
# MAGIC from
# MAGIC   DenomDQ09_base
# MAGIC inner join
# MAGIC   $dbSchema.MSD102MatCarePlan msd102
# MAGIC on
# MAGIC   DenomDQ09_base.Person_ID_Mother = msd102.Person_ID_Mother and DenomDQ09_base.UniqPregID = msd102.UniqPregID
# MAGIC where 
# MAGIC   msd102.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC AND 
# MAGIC   msd102.CarePlanType = '05'
# MAGIC AND 
# MAGIC   msd102.ContCarePathInd is not null
# MAGIC AND 
# MAGIC  (GestAgeBooking + datediff(CarePlanDate, DenomDQ09_base.AntenatalAppDate)) <= 203
# MAGIC )
# MAGIC select * from cte 
# MAGIC where 
# MAGIC   rank = 1
# MAGIC AND 
# MAGIC   ContCarePathInd = 'Y'
# MAGIC AND 
# MAGIC   CareProfLID is not null
# MAGIC AND 
# MAGIC   TeamLocalID is not null

# COMMAND ----------

# DBTITLE 1,COC_DQ09_Denominator_Raw (DenomDQ09)
# MAGIC %sql
# MAGIC create or replace temp view COC_DQ09_Denominator_Raw  as
# MAGIC select distinct
# MAGIC   KeyValue
# MAGIC   ,'$RPBegindate' AS RPStartDate
# MAGIC   ,'$RPEnddate' AS RPEndDate
# MAGIC   ,'$status' AS Status
# MAGIC   ,'COC_DQ09' AS Indicator
# MAGIC   ,'COC_DQ_Measure' AS IndicatorFamily
# MAGIC   ,DenomDQ09_mid.UniqPregId
# MAGIC   ,DenomDQ09_mid.Person_ID_Mother
# MAGIC   ,DenomDQ09_mid.AntenatalAppDate
# MAGIC   ,DenomDQ09_mid.GestAgeBooking
# MAGIC   ,DenomDQ09_mid.OrgCodeProvider
# MAGIC   ,DenomDQ09_mid.CarePlanDate
# MAGIC   ,DenomDQ09_mid.RecordNumber
# MAGIC   ,DenomDQ09_mid.CreatedAt
# MAGIC   
# MAGIC from
# MAGIC   DenomDQ09_mid
# MAGIC inner join
# MAGIC   $dbSchema.MSD301LabourDelivery msd301
# MAGIC on
# MAGIC   DenomDQ09_mid.Person_ID_Mother = msd301.Person_ID_Mother and DenomDQ09_mid.UniqPregID = msd301.UniqPregID
# MAGIC where 
# MAGIC   msd301.RPStartDate <= '$RPBegindate'

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
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ09_Denominator_Raw d

# COMMAND ----------

# DBTITLE 1,COC_DQ09_Numerator_Raw  (NumerDQ09)
# MAGIC %sql
# MAGIC  
# MAGIC create or replace temp view COC_DQ09_Numerator_Raw as 
# MAGIC select distinct
# MAGIC    DenomDQ09.KeyValue
# MAGIC    , DenomDQ09.RPStartDate
# MAGIC    , DenomDQ09.RPEndDate
# MAGIC    , DenomDQ09.Status
# MAGIC    , DenomDQ09.Indicator
# MAGIC    , DenomDQ09.IndicatorFamily
# MAGIC    , DenomDQ09.Person_ID_Mother
# MAGIC    , DenomDQ09.UniqPregID
# MAGIC    , null as CareconID
# MAGIC    , DenomDQ09.OrgCodeProvider
# MAGIC    , null as Rank
# MAGIC    , 0 as OverDQThreshold
# MAGIC    , CreatedAt
# MAGIC    , null as rank_imd_decile
# MAGIC    , null as ethniccategory
# MAGIC    , null as ethnicgroup
# MAGIC from 
# MAGIC   COC_DQ09_Denominator_Raw DenomDQ09 
# MAGIC inner join 
# MAGIC   $dbSchema.MSD301LabourDelivery msd301
# MAGIC on
# MAGIC   DenomDQ09.UniqPregID = msd301.UniqPregID and DenomDQ09.Person_ID_Mother = msd301.Person_ID_Mother
# MAGIC inner join
# MAGIC   $dbSchema.MSD302CareActivityLabDel msd302
# MAGIC on 
# MAGIC   msd301.UniqPregID = msd302.UniqPregID and msd301.Person_ID_Mother = msd302.Person_ID_Mother and msd301.LabourDeliveryID = msd302.LabourDeliveryID and msd301.RecordNumber = msd302.RecordNumber
# MAGIC where
# MAGIC   msd302.RPStartDate <= '$RPBegindate'
# MAGIC AND
# MAGIC   msd302.CareProfLID is not null

# COMMAND ----------

# DBTITLE 1,Numerator - Raw into generic table
# MAGIC %sql
# MAGIC  
# MAGIC INSERT INTO $outSchema.CoC_Numerator_Raw 
# MAGIC SELECT distinct d.KeyValue
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
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ09_Numerator_Raw d

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
# MAGIC         WHERE Indicator = 'COC_DQ09'
# MAGIC         GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC               FROM $outSchema.CoC_Denominator_Geographies
# MAGIC               WHERE Indicator = 'COC_DQ09'
# MAGIC               GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) AS den
# MAGIC   ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_DQ09'
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumeratorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Numerator_Raw as nr
# MAGIC -- MERGE INTO CoC_Measure_3_CoC_Numerator_Raw as nr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ09' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg 
# MAGIC --       ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = 'COC_DQ09' 
# MAGIC --      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DenominatorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Denominator_Raw as dr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Denominator_Raw as dr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ09' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg
# MAGIC --       ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = 'COC_DQ09'
# MAGIC --       WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

dbutils.notebook.exit("Notebook: COCDQ09 ran successfully")