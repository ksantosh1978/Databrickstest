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

# DBTITLE 1,DenomDQ07_base
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ07_base as
# MAGIC  
# MAGIC with cte as
# MAGIC (
# MAGIC select 
# MAGIC   msd101.MSD101_ID as KeyValue
# MAGIC   ,msd101.UniqPregId
# MAGIC   ,msd101.Person_ID_Mother
# MAGIC   ,msd101.AntenatalAppDate
# MAGIC   ,msd101.RPStartDate
# MAGIC   ,msd101.RPEnddate
# MAGIC   ,msd101.RecordNumber
# MAGIC   ,msd101.GestAgeBooking
# MAGIC   ,msd101.DischargeDateMatService
# MAGIC   ,COALESCE(ord.REL_FROM_ORG_CODE, msd101.OrgCodeProvider) AS OrgCodeProvider --This code handles when Orgs have merged
# MAGIC   ,DATE_ADD(DATE_SUB(msd101.AntenatalAppDate, int(msd101.GestAgeBooking)),203) as Gest_29Weeks
# MAGIC   ,current_timestamp() as CreatedAt
# MAGIC   ,rank() OVER(PARTITION BY msd101.Person_ID_Mother,  msd101.UniqPregId ORDER BY msd101.AntenatalAppDate desc, msd101.RecordNumber desc)as rank
# MAGIC  from $dbSchema.MSD101PregnancyBooking msd101
# MAGIC  LEFT JOIN $dss_corporate.org_relationship_daily as ord 
# MAGIC       ON msd101.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' 
# MAGIC       and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate')
# MAGIC where 
# MAGIC   msd101.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC   AND 
# MAGIC     msd101.GestAgeBooking > 0
# MAGIC )
# MAGIC     
# MAGIC select * from cte where rank = 1
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(add_months('$RPBegindate',-4), AntenatalAppDate)) <= 203
# MAGIC   AND 
# MAGIC     (GestAgeBooking + datediff(last_day(add_months('$RPEnddate',-4)), AntenatalAppDate)) >= 203
# MAGIC   AND 
# MAGIC     (DischargeDateMatService is null or 
# MAGIC     (GestAgeBooking + datediff(DischargeDateMatService, AntenatalAppDate)) > 203)

# COMMAND ----------

# DBTITLE 1,COC_DQ07_Denominator_Raw (DenomDQ07)
# MAGIC %sql
# MAGIC create or replace temp view COC_DQ07_Denominator_Raw as 
# MAGIC
# MAGIC with cte as
# MAGIC (
# MAGIC select  
# MAGIC   DenomDQ07_base.KeyValue
# MAGIC  ,'$RPBegindate' AS RPStartDate
# MAGIC  ,'$RPEnddate' AS RPEndDate
# MAGIC  ,'$status' AS Status
# MAGIC  ,'COC_DQ07' AS Indicator
# MAGIC  ,'COC_DQ_Measure' AS IndicatorFamily
# MAGIC  , DenomDQ07_base.UniqPregID
# MAGIC  ,DenomDQ07_base.Person_ID_Mother
# MAGIC  ,DenomDQ07_base.AntenatalAppDate
# MAGIC  ,DenomDQ07_base.GestAgeBooking
# MAGIC  ,DenomDQ07_base.OrgCodeProvider
# MAGIC  ,msd102.CarePlanDate
# MAGIC  ,msd102.ContCarePathInd
# MAGIC  ,msd102.CareProfLID
# MAGIC  ,msd102.TeamLocalID
# MAGIC  ,msd102.RecordNumber
# MAGIC  ,DenomDQ07_base.CreatedAt
# MAGIC  ,rank() OVER(PARTITION BY msd102.Person_ID_Mother,  msd102.UniqPregId ORDER BY msd102.CarePlanDate desc, msd102.RecordNumber desc)as rank
# MAGIC from 
# MAGIC DenomDQ07_base
# MAGIC inner join $dbSchema.MSD102MatCarePlan msd102
# MAGIC on DenomDQ07_base.Person_ID_Mother = msd102.Person_ID_Mother and DenomDQ07_base.UniqPregID = msd102.UniqPregID
# MAGIC where msd102.RPStartDate <= add_months('$RPBegindate',-4)
# MAGIC and msd102.CarePlanType = "05"
# MAGIC and msd102.ContCarePathInd is not null
# MAGIC and (DenomDQ07_base.GestAgeBooking + datediff(msd102.CarePlanDate, DenomDQ07_base.AntenatalAppDate)) <= 203
# MAGIC )
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
# MAGIC        ,AntenatalAppDate
# MAGIC        ,GestAgeBooking
# MAGIC from cte 
# MAGIC where rank = 1
# MAGIC and ContCarePathInd = 'Y'
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
# MAGIC FROM COC_DQ07_Denominator_Raw d
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumerDQ07_disch
# MAGIC %sql
# MAGIC create or replace temp view NumerDQ07_disch  as
# MAGIC with cte as (
# MAGIC select 
# MAGIC   DenomDQ07.UniqPregID                
# MAGIC   ,DenomDQ07.Person_ID_Mother                
# MAGIC   ,DenomDQ07.AntenatalAppDate                
# MAGIC   ,DenomDQ07.GestAgeBooking                
# MAGIC   ,DenomDQ07.OrgCodeProvider                
# MAGIC   ,MSD101.RecordNumber                
# MAGIC   ,MSD101.DischargeDateMatService
# MAGIC   ,rank() OVER(PARTITION BY DenomDQ07.Person_ID_Mother,  DenomDQ07.UniqPregId  ORDER BY msd101.DischargeDateMatService desc, msd101.RecordNumber desc)as rank
# MAGIC FROM 
# MAGIC   COC_DQ07_Denominator_Raw DenomDQ07
# MAGIC INNER JOIN
# MAGIC   $dbSchema.MSD101PregnancyBooking msd101
# MAGIC on 
# MAGIC   msd101.Person_ID_Mother = DenomDQ07.Person_ID_Mother and msd101.UniqPregID = DenomDQ07.UniqPregID
# MAGIC Where
# MAGIC   msd101.RPStartDate <= '$RPBegindate'            
# MAGIC and
# MAGIC   msd101.DischargeDateMatService is not null
# MAGIC and
# MAGIC   msd101.DischargeDateMatService >= DenomDQ07.CarePlanDate
# MAGIC )
# MAGIC  
# MAGIC select Person_ID_Mother,UniqPregID, DischargeDateMatService from cte where rank = 1

# COMMAND ----------

# DBTITLE 1,NumerDQ07_base
# MAGIC %sql
# MAGIC create or replace temp view NumerDQ07_base  as
# MAGIC with cte as (
# MAGIC select 
# MAGIC DenomDQ07.KeyValue
# MAGIC ,DenomDQ07.UniqPregID                
# MAGIC ,DenomDQ07.Person_ID_Mother                
# MAGIC ,DenomDQ07.AntenatalAppDate                
# MAGIC ,DenomDQ07.GestAgeBooking                
# MAGIC ,DenomDQ07.OrgCodeProvider                
# MAGIC ,MSD201.CareConID                
# MAGIC ,MSD201.CContactDate
# MAGIC ,rank() OVER(PARTITION BY DenomDQ07.Person_ID_Mother,  DenomDQ07.UniqPregId, MSD201.CareConID ORDER BY MSD201.CContactDate desc, MSD201.RecordNumber desc)as rank
# MAGIC FROM 
# MAGIC   COC_DQ07_Denominator_Raw DenomDQ07
# MAGIC INNER JOIN
# MAGIC   $dbSchema.MSD201CareContactPreg  MSD201
# MAGIC ON 
# MAGIC   MSD201.Person_ID_Mother = DenomDQ07.Person_ID_Mother and MSD201.UniqPregID = DenomDQ07.UniqPregID
# MAGIC LEFT JOIN
# MAGIC   NumerDQ07_disch 
# MAGIC ON 
# MAGIC   DenomDQ07.Person_ID_Mother = NumerDQ07_disch.Person_ID_Mother and  DenomDQ07.UniqPregID = NumerDQ07_disch.UniqPregID 
# MAGIC Where MSD201.RPStartDate <= '$RPBegindate'            
# MAGIC and MSD201.CContactDate >= DenomDQ07.CarePlanDate
# MAGIC and (MSD201.CContactDate <= NumerDQ07_disch.DischargeDateMatService or NumerDQ07_disch.DischargeDateMatService is null)
# MAGIC  
# MAGIC )
# MAGIC select * from cte where rank = 1

# COMMAND ----------

# DBTITLE 1,NumerDQ07_Ref
# MAGIC %sql
# MAGIC create or replace temp view NumerDQ07_Ref  as
# MAGIC with cte as
# MAGIC (
# MAGIC select 
# MAGIC    UniqPregID
# MAGIC   ,Person_ID_mother
# MAGIC   ,count(distinct CareConID) as Contact_Total
# MAGIC FROM 
# MAGIC   NumerDQ07_base
# MAGIC group by NumerDQ07_base.UniqPregID, NumerDQ07_base.Person_ID_Mother
# MAGIC  
# MAGIC )
# MAGIC select * from cte where Contact_Total >= 3

# COMMAND ----------

# DBTITLE 1,COC_DQ07_Numerator_Raw (NumerDQ07)
# MAGIC %sql
# MAGIC create or replace temp view COC_DQ07_Numerator_Raw  as
# MAGIC with step20 as (
# MAGIC select 
# MAGIC   NumerDQ07_Base.KeyValue
# MAGIC   ,'$RPBegindate' AS RPStartDate
# MAGIC   ,'$RPEnddate' AS RPEndDate
# MAGIC   ,'$status' AS Status
# MAGIC   ,'COC_DQ07' AS Indicator
# MAGIC   ,'COC_DQ_Measure' AS IndicatorFamily
# MAGIC   ,NumerDQ07_Base.UniqPregID
# MAGIC   ,NumerDQ07_Base.Person_ID_Mother                
# MAGIC   ,NumerDQ07_Base.AntenatalAppDate                
# MAGIC   ,NumerDQ07_Base.GestAgeBooking                
# MAGIC   ,NumerDQ07_Base.OrgCodeProvider                
# MAGIC   ,NumerDQ07_Base.CareConID    
# MAGIC   ,NumerDQ07_Base.CContactDate
# MAGIC   ,0 as OverDQThreshold
# MAGIC   ,current_timestamp() as CreatedAt
# MAGIC FROM 
# MAGIC  NumerDQ07_Ref
# MAGIC INNER JOIN 
# MAGIC  NumerDQ07_base 
# MAGIC ON
# MAGIC  NumerDQ07_base.UniqPregID = NumerDQ07_Ref.UniqPregID and NumerDQ07_base.Person_ID_Mother = NumerDQ07_Ref.Person_id_Mother
# MAGIC  
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
# MAGIC from step20

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
# MAGIC        , d.OverDQThreshold
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ07_Numerator_Raw d
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
# MAGIC         WHERE Indicator = 'COC_DQ07'
# MAGIC         GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC               FROM $outSchema.CoC_Denominator_Geographies
# MAGIC               WHERE Indicator = 'COC_DQ07'
# MAGIC               GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) AS den
# MAGIC   ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_DQ07'
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumeratorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Numerator_Raw as nr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Numerator_Raw as nr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ07' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg 
# MAGIC --       ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = 'COC_DQ07' 
# MAGIC --      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DenominatorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Denominator_Raw as dr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Denominator_Raw as dr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ07' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg
# MAGIC --       ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = 'COC_DQ07'
# MAGIC --       WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

dbutils.notebook.exit("Notebook: COCDQ07 ran successfully")