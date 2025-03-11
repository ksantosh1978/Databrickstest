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

# DBTITLE 1,DenomDQ08_base
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ08_base as
# MAGIC with cte as
# MAGIC (select 
# MAGIC   msd101.MSD101_ID as KeyValue
# MAGIC   ,msd101.UniqPregId
# MAGIC   ,msd101.Person_ID_Mother
# MAGIC   ,msd101.AntenatalAppDate
# MAGIC   ,msd101.RPStartDate
# MAGIC   ,msd101.RPEnddate
# MAGIC   ,msd101.RecordNumber
# MAGIC   ,msd101.GestAgeBooking
# MAGIC   ,msd101.DischargeDateMatService
# MAGIC   ,msd101.OrgCodeProvider
# MAGIC   ,rank() OVER(PARTITION BY msd101.Person_ID_Mother,  msd101.UniqPregId ORDER BY msd101.AntenatalAppDate desc, msd101.RecordNumber desc)as rank
# MAGIC -- from $dbSchema.MSD101PregnancyBooking msd101
# MAGIC from global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC where 
# MAGIC   msd101.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC   AND 
# MAGIC     msd101.GestAgeBooking > 0
# MAGIC     )
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

# COMMAND ----------

# DBTITLE 1,DenomDQ08_mid
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ08_mid  as
# MAGIC with cte as
# MAGIC (select 
# MAGIC    DenomDQ08_base.KeyValue
# MAGIC   ,DenomDQ08_base.UniqPregId
# MAGIC   ,DenomDQ08_base.Person_ID_Mother
# MAGIC   ,DenomDQ08_base.AntenatalAppDate
# MAGIC   ,DenomDQ08_base.RPStartDate
# MAGIC   ,DenomDQ08_base.RPEnddate
# MAGIC   ,DenomDQ08_base.GestAgeBooking
# MAGIC   ,DenomDQ08_base.OrgCodeProvider
# MAGIC   ,msd102.CarePlanDate
# MAGIC   ,msd102.RecordNumber
# MAGIC   ,msd102.ContCarePathInd
# MAGIC   ,msd102.CareProfLID
# MAGIC   ,msd102.TeamLocalID
# MAGIC   ,rank() OVER(PARTITION BY msd102.Person_ID_Mother,  msd102.UniqPregId ORDER BY msd102.CarePlanDate desc, msd102.RecordNumber desc) as rank
# MAGIC from 
# MAGIC   DenomDQ08_base
# MAGIC inner join 
# MAGIC   $dbSchema.MSD102MatCarePlan msd102
# MAGIC on 
# MAGIC   msd102.Person_ID_Mother = DenomDQ08_base.Person_ID_Mother and msd102.UniqPregID = DenomDQ08_base.UniqPregID 
# MAGIC where 
# MAGIC   msd102.RPStartDate <= add_months('$RPBegindate',-4) 
# MAGIC and
# MAGIC   msd102.CarePlanType = '05'
# MAGIC and
# MAGIC   msd102.ContCarePathInd is not null
# MAGIC and
# MAGIC   (GestAgeBooking + datediff(CarePlanDate, AntenatalAppDate)) <= 203
# MAGIC )
# MAGIC select * from cte 
# MAGIC where rank = 1
# MAGIC and ContCarePathInd = 'Y'
# MAGIC and CareProfLID is not null
# MAGIC and TeamLocalID is not null

# COMMAND ----------

# DBTITLE 1,DenomDQ08_disch
# MAGIC %sql
# MAGIC create or replace temp view DenomDQ08_disch  as
# MAGIC with cte as
# MAGIC (
# MAGIC  SELECT        
# MAGIC    DenomDQ08_mid.*
# MAGIC    , msd101.DischargeDateMatService as DischargedDateLatest
# MAGIC    , ROW_NUMBER () OVER(PARTITION BY DenomDQ08_mid.Person_ID_Mother, DenomDQ08_mid.UniqPregID ORDER BY msd101.DischargeDateMatService DESC, msd101.RecordNumber DESC) AS rank_disch
# MAGIC FROM
# MAGIC   DenomDQ08_mid
# MAGIC INNER JOIN
# MAGIC --   $dbSchema.msd101pregnancybooking msd101 
# MAGIC global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC ON
# MAGIC   msd101.Person_ID_Mother = DenomDQ08_mid.Person_ID_Mother AND msd101.UniqPregID = DenomDQ08_mid.UniqPregID
# MAGIC WHERE
# MAGIC   msd101.RPStartDate <= '$RPBegindate' 
# MAGIC AND 
# MAGIC   msd101.DischargeDateMatService IS NOT NULL      
# MAGIC AND
# MAGIC   msd101.DischargeDateMatService >= DenomDQ08_mid.CarePlanDate      
# MAGIC )
# MAGIC  
# MAGIC select Person_ID_Mother, UniqPregID,DischargedDateLatest   from cte where rank_disch = 1

# COMMAND ----------

# DBTITLE 1,COC_DQ08_Denominator_Raw (DenomDQ08)
# MAGIC %sql
# MAGIC create or replace temp view COC_DQ08_Denominator_Raw   as
# MAGIC with cte as
# MAGIC (
# MAGIC  
# MAGIC SELECT     
# MAGIC   DenomDQ08_mid.KeyValue
# MAGIC   ,'$RPBegindate' AS RPStartDate
# MAGIC   ,'$RPEnddate' AS RPEndDate
# MAGIC   ,'$status' AS Status
# MAGIC   ,'COC_DQ08' AS Indicator
# MAGIC   ,'COC_DQ_Measure' AS IndicatorFamily
# MAGIC   ,DenomDQ08_mid.UniqPregId
# MAGIC   ,DenomDQ08_mid.Person_ID_Mother
# MAGIC   ,DenomDQ08_mid.AntenatalAppDate
# MAGIC   ,DenomDQ08_mid.GestAgeBooking
# MAGIC   ,DenomDQ08_mid.OrgCodeProvider
# MAGIC   ,msd201.CareConID
# MAGIC   ,msd201.CContactDate
# MAGIC   ,msd201.AttendCode
# MAGIC   ,rank() OVER(PARTITION BY msd201.Person_ID_Mother,  msd201.UniqPregId, msd201.careConID ORDER BY msd201.CContactDate desc, msd201.RecordNumber desc) as Rank_DenomDQ08
# MAGIC FROM
# MAGIC   DenomDQ08_mid                             
# MAGIC INNER JOIN    
# MAGIC   $dbSchema.MSD201CareContactPreg msd201  
# MAGIC ON
# MAGIC   DenomDQ08_mid.Person_ID_Mother = msd201.Person_ID_Mother AND DenomDQ08_mid.UniqPregID = msd201.UniqPregID       
# MAGIC LEFT JOIN
# MAGIC   DenomDQ08_disch DenomDQ08_disch  
# MAGIC ON
# MAGIC   DenomDQ08_mid.Person_ID_Mother = DenomDQ08_disch.Person_ID_Mother AND DenomDQ08_mid.UniqPregID = DenomDQ08_disch.UniqPregID
# MAGIC WHERE
# MAGIC   msd201.RPStartDate <= '$RPBegindate'
# MAGIC and
# MAGIC   msd201.CContactDate >= DenomDQ08_mid.CarePlanDate
# MAGIC and
# MAGIC (msd201.CContactDate <= DenomDQ08_disch.DischargedDateLatest OR DenomDQ08_disch.DischargedDateLatest IS NULL)              
# MAGIC  
# MAGIC )
# MAGIC select  *
# MAGIC from cte where Rank_DenomDQ08 = 1

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
# MAGIC        , d.CareConID
# MAGIC        , d.OrgCodeProvider
# MAGIC        , 1 as Rank
# MAGIC        , 0 as OverDQThreshold
# MAGIC        , current_timestamp() as CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ08_Denominator_Raw d
# MAGIC

# COMMAND ----------

# DBTITLE 1,COC_DQ08_Numerator_Raw  (NumerDQ08)
# MAGIC %sql
# MAGIC  
# MAGIC create or replace temp view COC_DQ08_Numerator_Raw as 
# MAGIC select 
# MAGIC     DenomDQ08.KeyValue
# MAGIC   ,'$RPBegindate' AS RPStartDate
# MAGIC   ,'$RPEnddate' AS RPEndDate
# MAGIC   ,'$status' AS Status
# MAGIC   ,'COC_DQ08' AS Indicator
# MAGIC   ,'COC_D8_Measure' AS IndicatorFamily
# MAGIC   ,DenomDQ08.UniqPregId
# MAGIC   ,DenomDQ08.Person_ID_Mother
# MAGIC   ,DenomDQ08.OrgCodeProvider
# MAGIC   ,DenomDQ08.CareConID
# MAGIC from 
# MAGIC   COC_DQ08_Denominator_Raw DenomDQ08 
# MAGIC inner join 
# MAGIC   $dbSchema.MSD202CareActivityPreg MSD202
# MAGIC on
# MAGIC   MSD202.UniqPregID = DenomDQ08.UniqPregID and MSD202.CareConID = DenomDQ08.CareConID
# MAGIC inner join
# MAGIC   $dbSchema.MSD901StaffDetails MSD901
# MAGIC on 
# MAGIC   MSD901.CareProfLID = MSD202.CareProfLID
# MAGIC where
# MAGIC   MSD901.RPStartDate <= '$RPBegindate'
# MAGIC and
# MAGIC   DenomDQ08.AttendCode in ('5','6')
# MAGIC and
# MAGIC   MSD901.StaffGroup in ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '98')

# COMMAND ----------

# DBTITLE 1,Numerator- Raw into generic table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.CoC_Numerator_Raw 
# MAGIC SELECT distinct n.KeyValue
# MAGIC        , n.RPStartDate
# MAGIC        , n.RPEndDate
# MAGIC        , n.Status
# MAGIC        , n.Indicator
# MAGIC        , n.IndicatorFamily
# MAGIC        , n.Person_ID_Mother as Person_ID
# MAGIC        , n.UniqPregID as PregnancyID
# MAGIC        , n.CareconID
# MAGIC        , n.OrgCodeProvider
# MAGIC        , 1 as Rank
# MAGIC        , 0 as OverDQThreshold
# MAGIC        , current_timestamp() as CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM COC_DQ08_Numerator_Raw n
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
# MAGIC         WHERE Indicator = 'COC_DQ08'
# MAGIC         GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC               FROM $outSchema.CoC_Denominator_Geographies
# MAGIC               WHERE Indicator = 'COC_DQ08'
# MAGIC               GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) AS den
# MAGIC   ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_DQ08'
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

dbutils.notebook.exit("Notebook: COCDQ08 ran successfully")