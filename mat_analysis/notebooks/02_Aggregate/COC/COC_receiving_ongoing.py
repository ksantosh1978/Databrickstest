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

# MAGIC %sql
# MAGIC -- CREATE WIDGET TEXT RPStart DEFAULT "2020-10-01";
# MAGIC -- CREATE WIDGET TEXT RPEnd DEFAULT "2020-10-31";
# MAGIC -- CREATE WIDGET TEXT status DEFAULT "Performance";
# MAGIC -- CREATE WIDGET TEXT DatabaseSchema DEFAULT "mat_pre_clear";
# MAGIC -- CREATE WIDGET TEXT outSchema DEFAULT "mat_analysis";
# MAGIC -- CREATE WIDGET TEXT dss_corporate DEFAULT "dss_corporate";
# MAGIC

# COMMAND ----------

# DBTITLE 1,Denominator - Final for reuse
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_CoC_Denominator_Raw AS 
# MAGIC SELECT tmp.KeyValue
# MAGIC        , '$RPBegindate' AS RPStartDate
# MAGIC        , '$RPEnddate' AS RPEndDate
# MAGIC        , '$status' AS Status
# MAGIC        , 'COC_receiving_ongoing' AS Indicator
# MAGIC        , 'COC_Rate' AS IndicatorFamily
# MAGIC        , tmp.Person_ID
# MAGIC        , tmp.PregnancyID
# MAGIC        --, tmp.OrgCodeProvider
# MAGIC        , COALESCE(ord.REL_FROM_ORG_CODE, tmp.OrgCodeProvider) AS OrgCodeProvider 
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
# MAGIC       WHERE RPStartDate <= add_months('$RPBegindate', -4)  AND GestAgeBooking > 0 ) AS tmp
# MAGIC  
# MAGIC  LEFT JOIN $dss_corporate.org_relationship_daily as ord 
# MAGIC       ON tmp.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' 
# MAGIC       and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate') 
# MAGIC       
# MAGIC WHERE Rank = 1
# MAGIC         AND (GestAgeBooking + DATEDIFF(add_months('$RPBegindate', -4) , AntenatalAppDate)) <= 203 
# MAGIC         AND (GestAgeBooking + DATEDIFF(last_day(add_months('$RPEnddate', -4)) , AntenatalAppDate)) >= 203 
# MAGIC         AND (DischargeDateMatService is null or (GestAgeBooking + DATEDIFF(DischargeDateMatService, AntenatalAppDate)) > 203)

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
# MAGIC        , d.Rank 
# MAGIC        , d.OverDQThreshold
# MAGIC        , d.CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC FROM global_temp.CoC_Measure_3_CoC_Denominator_Raw d
# MAGIC

# COMMAND ----------

# DBTITLE 1,Numerator - Step 2
# MAGIC %sql
# MAGIC  
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_CoC_Placed AS 
# MAGIC SELECT * 
# MAGIC FROM (SELECT d.KeyValue 
# MAGIC             , d.Person_ID, d.PregnancyID, d.OrgCodeProvider, d.AntenatalAppDate, CarePlanDate
# MAGIC             , CareProfLID as CareProfLID_102, TeamLocalID as TeamLocalID_102, ContCarePathInd            
# MAGIC             , row_number() OVER (PARTITION BY d.Person_ID, d.PregnancyID ORDER BY CarePlanDate DESC, RecordNumber desc) as Rank1 
# MAGIC             , d.RPStartDate, d.RPEndDate, d.Status
# MAGIC             , d.Indicator, d.IndicatorFamily, d.CreatedAt
# MAGIC             --, , d.GestAgeBooking, 102cp.
# MAGIC  
# MAGIC       FROM global_temp.CoC_Measure_3_CoC_Denominator_Raw d
# MAGIC         INNER JOIN $dbSchema.msd102matcareplan as 102cp ON d.Person_ID = 102cp.Person_ID_Mother AND d.PregnancyID = 102cp.UniqPregID 
# MAGIC       WHERE 102cp.RPStartDate <= add_months('$RPBegindate', -4) 
# MAGIC         AND (d.GestAgeBooking + DATEDIFF(CarePlanDate, d.AntenatalAppDate)) <= 203
# MAGIC         AND CarePlanType = '05'
# MAGIC         AND ContCarePathInd is not null
# MAGIC       ) AS careplan
# MAGIC         
# MAGIC WHERE Rank1 =1 AND ContCarePathInd = 'Y' AND CareProfLID_102 is not null AND TeamLocalID_102 is not null 
# MAGIC  
# MAGIC

# COMMAND ----------

# DBTITLE 1,Numerator - Step 3 & 4 merged
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_cohort_disch AS 
# MAGIC  
# MAGIC SELECT tmp.KeyValue, tmp.RPStartDate, tmp.RPEndDate, tmp.Status, tmp.Indicator, tmp.IndicatorFamily, tmp.Person_ID, tmp.PregnancyID, tmp.OrgCodeProvider
# MAGIC       , tmp.CreatedAt, tmp.DischargeDateMatService, tmp.Rank2
# MAGIC       , tmp.CarePlanDate , tmp.CareProfLID_102, tmp.TeamLocalID_102
# MAGIC  
# MAGIC FROM (
# MAGIC   SELECT d.KeyValue, d.RPStartDate, d.RPEndDate, d.Status, d.Indicator, d.IndicatorFamily, d.Person_ID, d.PregnancyID, d.OrgCodeProvider, d.CreatedAt
# MAGIC        , MSD101.DischargeDateMatService , d.CarePlanDate, d.CareProfLID_102, d.TeamLocalID_102
# MAGIC        , row_number() OVER (PARTITION BY MSD101.Person_ID_Mother, MSD101.UniqPregID ORDER BY MSD101.DischargeDateMatService  DESC, MSD101.RecordNumber DESC ) as Rank2
# MAGIC   FROM global_temp.CoC_Measure_3_CoC_Placed as d
# MAGIC     inner join $dbSchema.MSD101pregnancybooking as MSD101 on MSD101.Person_ID_Mother = d.Person_ID and MSD101.UniqPregID = d.PregnancyID
# MAGIC   where MSD101.RPStartDate <= '$RPBegindate'
# MAGIC     and MSD101.DischargeDateMatService is not null 
# MAGIC     and MSD101.DischargeDateMatService >= d.CareplanDate
# MAGIC   ) tmp
# MAGIC WHERE Rank2 = 1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Numerator - Step 5
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_MSD202_appnt AS 
# MAGIC  
# MAGIC   SELECT d.KeyValue, d.RPStartDate, d.RPEndDate, d.Status, d.Indicator, d.IndicatorFamily, d.Person_ID, d.PregnancyID, d.OrgCodeProvider, d.CreatedAt
# MAGIC        , d.DischargeDateMatService
# MAGIC        , d.CarePlanDate, d.CareProfLID_102, d.TeamLocalID_102       
# MAGIC        , MSD202.CareProfLID as CareProfLID_202
# MAGIC        , MSD202.TeamLocalID as TeamLocalID_202
# MAGIC        , MSD201.CContactDate
# MAGIC        , MSD201.CareConID
# MAGIC  
# MAGIC   FROM global_temp.CoC_Measure_3_cohort_disch as d
# MAGIC     inner join $dbSchema.MSD201CareContactPreg as MSD201 on MSD201.Person_ID_Mother = d.Person_ID and MSD201.UniqPregID = d.PregnancyID
# MAGIC     inner join $dbSchema.MSD202CareActivityPreg as MSD202 on MSD201.UniqPregID = MSD202.UniqPregID and MSD201.CareConID = MSD202.CareConID
# MAGIC     inner join $dbSchema.MSD901StaffDetails as MSD901 on MSD202.CareProfLID = MSD901.CareProfLID
# MAGIC   where MSD201.RPStartDate <= '$RPBegindate'
# MAGIC     and MSD202.RPStartDate <= '$RPBegindate'
# MAGIC     and MSD901.RPStartDate <= '$RPBegindate'
# MAGIC     and MSD201.CContactDate >= d.CareplanDate
# MAGIC     and MSD201.CContactDate <= d.DischargeDateMatService
# MAGIC     and MSD201.AttendCode in ('5', '6')
# MAGIC     and MSD901.StaffGroup in ('05', '06', '07')
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Numerator - Step 6a
# MAGIC %sql
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_tmp_Step6 AS 
# MAGIC  
# MAGIC SELECT DISTINCT OrgCodeProvider, Person_ID, PregnancyID, CareplanDate, CareProfLID_102, TeamLocalID_102, CContactDate, CareConID
# MAGIC                 , CareProfLID_202, TeamLocalID_202
# MAGIC                 , CASE WHEN (CareProfLID_102 = CareProfLID_202 or TeamLocalID_102 = TeamLocalID_202) THEN 'Y'
# MAGIC                        ELSE 'N'
# MAGIC                    END AS CoC_flag    
# MAGIC FROM global_temp.CoC_Measure_3_MSD202_appnt a
# MAGIC           

# COMMAND ----------

# DBTITLE 1,Numerator - Step 6b & 6c
# MAGIC %sql
# MAGIC
# MAGIC  
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW CoC_Measure_3_MSD202_appnt_Final AS 
# MAGIC  
# MAGIC SELECT DISTINCT a.KeyValue, a.OrgCodeProvider, a.Person_ID, a.PregnancyID, a.CareProfLID_102, a.TeamLocalID_102, b.Ratio
# MAGIC     , a.RPStartDate, a.RPEndDate, a.status, a.Indicator, a.IndicatorFamily
# MAGIC FROM global_temp.CoC_Measure_3_MSD202_appnt a
# MAGIC INNER JOIN (
# MAGIC       SELECT tmp_CountA.Person_ID
# MAGIC             , tmp_CountA.PregnancyID
# MAGIC             , (count_MSD201_contacts_CoC/count_MSD201_contacts) AS Ratio
# MAGIC  
# MAGIC       FROM (
# MAGIC             --SELECT  Person_ID, PregnancyID, count (*) as count_MSD201_contacts -- Changed as per ticket - DMS001/1041
# MAGIC             SELECT  Person_ID, PregnancyID, count (distinct CareConID, CContactDate) as count_MSD201_contacts
# MAGIC             FROM global_temp.CoC_Measure_3_tmp_Step6
# MAGIC             GROUP BY Person_ID, PregnancyID
# MAGIC           ) tmp_CountA
# MAGIC  
# MAGIC       INNER JOIN 
# MAGIC           (
# MAGIC             --SELECT  Person_ID, PregnancyID, count (*) as count_MSD201_contacts_CoC -- Changed as per ticket - DMS001/1041
# MAGIC             SELECT  Person_ID, PregnancyID, count (distinct CareConID, CContactDate) as count_MSD201_contacts_CoC
# MAGIC             FROM global_temp.CoC_Measure_3_tmp_Step6
# MAGIC             WHERE CoC_flag = 'Y'
# MAGIC             GROUP BY Person_ID, PregnancyID
# MAGIC           ) tmp_CountB
# MAGIC           ON tmp_CountA.Person_ID = tmp_CountB.Person_ID AND tmp_CountA.PregnancyID = tmp_CountB.PregnancyID
# MAGIC  
# MAGIC       WHERE count_MSD201_contacts_CoC/count_MSD201_contacts >= 0.7
# MAGIC     ) b
# MAGIC     ON a.Person_ID = b.Person_ID AND a.PregnancyID = b.PregnancyID
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from global_temp.CoC_Measure_3_MSD202_appnt_Final

# COMMAND ----------

# DBTITLE 1,Numerator - Step 7 & 8 (merged) - Final step
# MAGIC %sql
# MAGIC      
# MAGIC INSERT INTO $outSchema.CoC_Numerator_Raw
# MAGIC SELECT DISTINCT a.KeyValue, a.RPStartDate, a.RPEndDate, a.status, a.Indicator, a.IndicatorFamily, a.Person_ID, a.PregnancyID, null as CareConID, a.OrgCodeProvider, 1 as Rank, 0 as OverDQThreshold, current_timestamp() as CreatedAt
# MAGIC              , null as rank_imd_decile, null as ethniccategory, null as ethnicgroup
# MAGIC
# MAGIC FROM global_temp.CoC_Measure_3_MSD202_appnt_Final a
# MAGIC   LEFT JOIN $dbSchema.msd301labourdelivery MSD301 on a.Person_ID = MSD301.Person_ID_Mother and a.PregnancyID = MSD301.UniqPregId and MSD301.RPStartDate <= '$RPBegindate'
# MAGIC   LEFT JOIN $dbSchema.msd302careactivitylabdel MSD302 on a.Person_ID = MSD302.Person_ID_Mother and a.PregnancyID = MSD302.UniqPregId and MSD302.RPStartDate <= '$RPBegindate'
# MAGIC  
# MAGIC     
# MAGIC WHERE MSD301.LabourDeliveryID is null or CareProfLID_102 = MSD302.CareProfLID or TeamLocalID_102 = MSD302.TeamLocalID
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
# MAGIC FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, count(*) as Total from $outSchema.CoC_Numerator_Geographies
# MAGIC WHERE Indicator = 'COC_receiving_ongoing'
# MAGIC group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC from $outSchema.CoC_Denominator_Geographies
# MAGIC WHERE Indicator = 'COC_receiving_ongoing'
# MAGIC group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) as den
# MAGIC ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE
# MAGIC

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC -- The OverDQThreshold for COC_receiving_ongoing is based on the four underlying data quality measures, so as long as a provider passes COC_DQ06, 07, 08 and 09 then they are considered as a Pass for the main measure
# MAGIC -- Previous code: UPDATE $outSchema.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate > 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_receiving_ongoing'
# MAGIC  
# MAGIC with COC_receiving_ongoing as
# MAGIC (
# MAGIC select * from $outSchema.CoC_Provider_Aggregated where indicator = 'COC_receiving_ongoing'
# MAGIC ), DQ06 as
# MAGIC (
# MAGIC SELECT * FROM $outSchema.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ06'
# MAGIC ), DQ07 as
# MAGIC (
# MAGIC SELECT * FROM $outSchema.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ07'
# MAGIC ), DQ08 as
# MAGIC (
# MAGIC SELECT * FROM $outSchema.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ08'
# MAGIC ), DQ09 as
# MAGIC (
# MAGIC SELECT * FROM $outSchema.CoC_Provider_Aggregated WHERE Indicator = 'COC_DQ09'
# MAGIC ), OverDQThreshold as
# MAGIC (
# MAGIC select COC_receiving_ongoing.* from COC_receiving_ongoing 
# MAGIC
# MAGIC inner join DQ06 on COC_receiving_ongoing.OrgCodeProvider = DQ06.OrgCodeProvider and DQ06.OverDQThreshold = 1
# MAGIC
# MAGIC inner join DQ07 on COC_receiving_ongoing.OrgCodeProvider = DQ07.OrgCodeProvider and DQ07.OverDQThreshold = 1
# MAGIC
# MAGIC inner join DQ08 on COC_receiving_ongoing.OrgCodeProvider = DQ08.OrgCodeProvider and DQ08.OverDQThreshold = 1
# MAGIC
# MAGIC inner join DQ09 on COC_receiving_ongoing.OrgCodeProvider = DQ09.OrgCodeProvider and DQ09.OverDQThreshold = 1
# MAGIC )
# MAGIC MERGE INTO $outSchema.CoC_Provider_Aggregated cpa
# MAGIC USING OverDQThreshold
# MAGIC ON OverDQThreshold.orgcodeProvider = cpa.orgcodeProvider and cpa.indicator = 'COC_receiving_ongoing'
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET cpa.OverDQThreshold = 1
# MAGIC  
# MAGIC

# COMMAND ----------

# DBTITLE 1,NumeratorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Numerator_Raw as nr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Numerator_Raw as nr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_receiving_ongoing' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg 
# MAGIC --       ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = 'COC_receiving_ongoing' 
# MAGIC --      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

# DBTITLE 1,DenominatorDQThreshold
# MAGIC %sql
# MAGIC  
# MAGIC -- --MERGE INTO {outSchema}.CoC_Denominator_Raw as dr
# MAGIC -- MERGE INTO global_temp.CoC_Measure_3_CoC_Denominator_Raw as dr
# MAGIC --       USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.CoC_Provider_Aggregated WHERE Indicator = 'COC_receiving_ongoing' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg
# MAGIC --       ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = 'COC_receiving_ongoing'
# MAGIC --       WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold
# MAGIC       

# COMMAND ----------

dbutils.notebook.exit("Notebook: COC_receiving_ongoing ran successfully")