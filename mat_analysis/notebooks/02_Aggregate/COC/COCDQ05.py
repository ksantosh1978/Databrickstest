# Databricks notebook source
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

# COMMAND ----------

# %sql
# CREATE WIDGET TEXT RPBegindate DEFAULT "2021-06-01";
# CREATE WIDGET TEXT RPEnddate DEFAULT "2021-06-30";
# CREATE WIDGET TEXT status DEFAULT "Performance";
# CREATE WIDGET TEXT dbSchema DEFAULT "mat_pre_clear";
# CREATE WIDGET TEXT outSchema DEFAULT "mat_analysis";
# CREATE WIDGET TEXT dss_corporate DEFAULT "dss_corporate";


# COMMAND ----------

# %py
#  dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Denominator - Raw data
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_5_CoC_Denominator_Raw AS 
# MAGIC   SELECT tmp.KeyValue
# MAGIC        , '$RPBegindate' AS RPStartDate
# MAGIC        , '$RPEnddate' AS RPEndDate
# MAGIC        , '$status' AS Status
# MAGIC        
# MAGIC        , 'COC_DQ05' AS Indicator
# MAGIC        , 'COC_DQ_Measure' AS IndicatorFamily
# MAGIC        , tmp.Person_ID
# MAGIC        , tmp.PregnancyID
# MAGIC        , COALESCE(ord.REL_FROM_ORG_CODE, tmp.OrgCodeProvider) AS OrgCodeProvider -- this code handles merged orgCodeProviders
# MAGIC        , tmp.Rank 
# MAGIC        , tmp.GestAgeBooking 
# MAGIC        , tmp.AntenatalAppDate
# MAGIC        , 0 AS OverDQThreshold
# MAGIC        , current_timestamp() as CreatedAt
# MAGIC FROM (
# MAGIC       SELECT MSD101_ID AS KeyValue
# MAGIC              , Person_ID_Mother AS Person_ID
# MAGIC              , UniqPregID AS PregnancyID 
# MAGIC              , OrgCodeProvider 
# MAGIC              , GestAgeBooking
# MAGIC              , AntenatalAppDate
# MAGIC              , DischargeDateMatService
# MAGIC              , row_number() OVER (PARTITION BY Person_ID_Mother, UniqPregID ORDER BY AntenatalAppDate DESC, RecordNumber DESC) as Rank 
# MAGIC       FROM $dbSchema.msd101pregnancybooking
# MAGIC       WHERE RPStartDate <= '$RPBegindate'  AND GestAgeBooking > 0 ) AS tmp
# MAGIC LEFT JOIN $dss_corporate.org_relationship_daily as ord 
# MAGIC       ON tmp.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' 
# MAGIC       and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate') 
# MAGIC
# MAGIC WHERE    Rank = 1
# MAGIC         AND (GestAgeBooking + DATEDIFF('$RPBegindate', AntenatalAppDate)) <= 203 
# MAGIC         AND (GestAgeBooking + DATEDIFF('$RPEnddate', AntenatalAppDate)) >= 203 
# MAGIC         AND (DischargeDateMatService is null or (GestAgeBooking + DATEDIFF(DischargeDateMatService, AntenatalAppDate)) > 203)
# MAGIC         

# COMMAND ----------

# MAGIC %sql
# MAGIC          
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_5_CoC_Denominator_Raw_Final AS 
# MAGIC SELECT * 
# MAGIC FROM (SELECT d.KeyValue
# MAGIC       , d.PregnancyID				
# MAGIC       , d.Person_ID				
# MAGIC       , d.AntenatalAppDate				
# MAGIC       , d.GestAgeBooking				
# MAGIC       , d.OrgCodeProvider				
# MAGIC       , MSD102.CarePlanDate				
# MAGIC       , MSD102.RecordNumber	
# MAGIC       , MSD102.CareProfLID	
# MAGIC       , MSD102.TeamLocalID	
# MAGIC       , MSD102.ContCarePathInd
# MAGIC       , d.RPStartDate
# MAGIC       , d.RPEndDate
# MAGIC       , d.status
# MAGIC       , d.Indicator
# MAGIC       , d.IndicatorFamily
# MAGIC       , d.OverDQThreshold
# MAGIC       , d.CreatedAt
# MAGIC       , row_number() OVER (PARTITION BY d.Person_ID, d.PregnancyID ORDER BY MSD102.CarePlanDate DESC, MSD102.RecordNumber desc) as Rank1 
# MAGIC
# MAGIC FROM global_temp.CoC_Measure_5_CoC_Denominator_Raw d
# MAGIC   INNER JOIN $dbSchema.msd102matcareplan as MSD102 ON d.Person_ID = MSD102.Person_ID_Mother AND d.PregnancyID = MSD102.UniqPregID 
# MAGIC
# MAGIC WHERE MSD102.RPStartDate <= '$RPBegindate'
# MAGIC         AND MSD102.CarePlanType = '05'
# MAGIC         AND MSD102.ContCarePathInd IS NOT NULL
# MAGIC         AND (d.GestAgeBooking + DATEDIFF(MSD102.CarePlanDate, d.AntenatalAppDate)) <= 203                 
# MAGIC       ) AS deno
# MAGIC         
# MAGIC WHERE Rank1 = 1 AND deno.ContCarePathInd = 'Y'
# MAGIC

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
# MAGIC        , d.Person_ID
# MAGIC        , d.PregnancyID
# MAGIC        , null as CareConID
# MAGIC        , d.OrgCodeProvider
# MAGIC        , d.Rank1 AS Rank
# MAGIC        , d.OverDQThreshold
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM global_temp.CoC_Measure_5_CoC_Denominator_Raw_Final d
# MAGIC

# COMMAND ----------

# DBTITLE 1,Numerator - Raw data
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_5_CoC_Numerator_Raw AS 
# MAGIC SELECT * 
# MAGIC FROM global_temp.CoC_Measure_5_CoC_Denominator_Raw_Final d
# MAGIC WHERE CareProfLID IS NOT NULL AND TeamLocalID IS NOT NULL
# MAGIC
# MAGIC

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
# MAGIC        , d.Person_ID
# MAGIC        , d.PregnancyID
# MAGIC        , null as CareConID
# MAGIC        , d.OrgCodeProvider
# MAGIC        , d.Rank1 as Rank
# MAGIC        , d.OverDQThreshold
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM global_temp.CoC_Measure_5_CoC_Numerator_Raw d
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from $outSchema.CoC_Measure_5_CoC_Numerator_Raw where indicator = 'COC_DQ05'

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
# MAGIC         WHERE Indicator = 'COC_DQ05'
# MAGIC         GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC               FROM $outSchema.CoC_Denominator_Geographies
# MAGIC               WHERE Indicator = 'COC_DQ05'
# MAGIC               GROUP BY RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) AS den
# MAGIC   ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_DQ05'
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumeratorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Numerator_Raw as nr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Numerator_Raw as nr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ05' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg 
# MAGIC --       ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = 'COC_DQ05' 
# MAGIC --      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DenominatorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Denominator_Raw as dr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Denominator_Raw as dr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ05' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg
# MAGIC --       ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = 'COC_DQ05'
# MAGIC --       WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

dbutils.notebook.exit("Notebook: COCDQ05 ran successfully")