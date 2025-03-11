# Databricks notebook source
#dbutils.widgets.removeAll();

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
status = dbutils.widgets.get("status")
print(status)
assert(status)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)

rp_startdate_rolling_two_months = dbutils.widgets.get("rp_startdate_rolling_two_months")
print(rp_startdate_rolling_two_months)
assert(rp_startdate_rolling_two_months)
rp_startdate_rolling_six_months = dbutils.widgets.get("rp_startdate_rolling_six_months")
print(rp_startdate_rolling_six_months)
assert(rp_startdate_rolling_six_months)

RunTime = dbutils.widgets.get("RunTime")
print(RunTime)
assert(RunTime)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE WIDGET TEXT rp_startdate_rolling_two_months DEFAULT "2020-08-01";
# MAGIC -- CREATE WIDGET TEXT rp_startdate_rolling_six_months DEFAULT "2020-04-01";
# MAGIC -- CREATE WIDGET TEXT outSchema DEFAULT "mat_analysis";
# MAGIC -- CREATE WIDGET TEXT dbSchema DEFAULT "mat_pre_clear";
# MAGIC -- CREATE WIDGET TEXT status DEFAULT "Performance";
# MAGIC -- CREATE WIDGET TEXT RPStartdate DEFAULT "2020-09-01";
# MAGIC -- CREATE WIDGET TEXT RPEnddate DEFAULT "2020-09-30";
# MAGIC -- CREATE WIDGET TEXT dss_corporate DEFAULT "dss_corporate";

# COMMAND ----------

# DBTITLE 1,PCSP_Step1
# MAGIC %sql
# MAGIC -- msd101pregnancybooking_old_orgs_updated created in 01_Prepare/update_base_tables
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step1 AS
# MAGIC SELECT *,
# MAGIC        rank() OVER (PARTITION BY Person_ID_Mother,UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS step1_RANK
# MAGIC from global_temp.msd101pregnancybooking_old_orgs_updated
# MAGIC where RPstartdate <= '$RPEnddate'
# MAGIC and GestAgeBooking > 0;

# COMMAND ----------

# DBTITLE 1,PCSP_STEP2 - This is your 2_D Dataset
# MAGIC %sql
# MAGIC
# MAGIC -- BS - Temporarily data is being pushed to global temp as the numerator needs these data. Though some of these data is pushed into Denominatoir_Raw table, but Numerator might need other columns for performing any more join
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step2 AS
# MAGIC SELECT *
# MAGIC FROM global_temp.PCSP_Step1 step1
# MAGIC WHERE step1_RANK = 1 
# MAGIC AND  (DischargeDateMatService IS NULL OR DischargeDateMatService > date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT) ),259))
# MAGIC AND date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT)),259) BETWEEN '$RPBegindate' and '$RPEnddate'

# COMMAND ----------

# DBTITLE 1,PCSP - Final Denominator to RAW table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.PCSP_Denominator_Raw 
# MAGIC SELECT MSD101_ID AS KeyValue
# MAGIC        , '$RPBegindate' AS RPStartDate
# MAGIC        , '$RPEnddate' AS RPEndDate
# MAGIC        , '$status' AS Status
# MAGIC        , 'PCP_All_Pathways' AS Indicator
# MAGIC        , 'PCP' AS IndicatorFamily
# MAGIC        , Person_ID_Mother AS Person_ID
# MAGIC        , PregnancyID
# MAGIC        , OrgCodeProvider
# MAGIC        , step1_RANK AS Rank 
# MAGIC        , 0 AS OverDQThreshold
# MAGIC        , TO_TIMESTAMP('$RunTime','dd/MM/yyyy HH:mm:ss') as CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC               
# MAGIC FROM global_temp.PCSP_Step2
# MAGIC

# COMMAND ----------

# DBTITLE 1,PCSP_Step3 - Assemble first part (of three) for the numerator
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step3 AS
# MAGIC --PCSP_Step2 is D dataset
# MAGIC SELECT step2.*,
# MAGIC   msd102.MatPersCarePlanInd,
# MAGIC   rank() OVER (PARTITION BY step2.Person_ID_Mother,step2.UniqPregID ORDER BY msd102.CarePlanDate DESC, msd102.RecordNumber  DESC) AS step3_rank
# MAGIC FROM global_temp.PCSP_Step2 step2
# MAGIC LEFT JOIN global_temp.msd102matcareplan_old_orgs_updated msd102
# MAGIC ON step2.Person_ID_mother = msd102.Person_ID_mother AND step2.UniqPregID = msd102.UniqPregID
# MAGIC WHERE step2.RPStartDate <= '$RPEnddate'
# MAGIC AND msd102.CarePlanType = '07'
# MAGIC AND msd102.MatPersCarePlanInd  IS NOT NULL
# MAGIC AND msd102.CarePlanDate <= date_add(date_sub(step2.AntenatalAppDate, TRY_CAST(step2.GestAgeBooking AS INT) ),259)

# COMMAND ----------

# DBTITLE 1,PCSP_Step4 - Dataset N1
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step4 AS
# MAGIC SELECT * FROM global_temp.PCSP_Step3 step3
# MAGIC WHERE step3_rank = 1
# MAGIC AND step3.MatPersCarePlanInd = 'Y'
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,STEP5
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step5 AS
# MAGIC SELECT *,
# MAGIC        rank() OVER (PARTITION BY Person_ID_Mother,UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS RANK_msd101
# MAGIC FROM global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC WHERE RPStartDate <= '$RPEnddate'
# MAGIC

# COMMAND ----------

# DBTITLE 1,STEP6
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step6 AS
# MAGIC SELECT * FROM global_temp.PCSP_Step5 step5
# MAGIC WHERE RANK_msd101 = 1
# MAGIC AND (DischargeDateMatService IS NULL OR DischargeDateMatService > date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT) ),245))
# MAGIC AND date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT) ),245) BETWEEN '$rp_startdate_rolling_two_months' and '$RPEnddate'
# MAGIC

# COMMAND ----------

# DBTITLE 1,STEP7
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step7 AS
# MAGIC SELECT step6.*,
# MAGIC        msd102.MatPersCarePlanInd,
# MAGIC        rank() OVER (PARTITION BY msd102.Person_ID_Mother,msd102.UniqPregID ORDER BY step6.AntenatalAppDate  DESC, msd102.RecordNumber  DESC) AS RANK_msd102
# MAGIC FROM global_temp.PCSP_Step6 step6
# MAGIC LEFT JOIN global_temp.msd102matcareplan_old_orgs_updated msd102
# MAGIC ON step6.Person_ID_mother = msd102.Person_ID_mother AND step6.UniqPregID = msd102.UniqPregID
# MAGIC WHERE msd102.RPStartDate <= '$RPEnddate'
# MAGIC AND msd102.CarePlanType = '06'
# MAGIC AND msd102.MatPersCarePlanInd IS NOT NULL
# MAGIC AND msd102.CarePlanDate <= date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT) ),245)

# COMMAND ----------

# DBTITLE 1,PCSP_Step8 - Dataset N2
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step8 AS
# MAGIC SELECT * FROM global_temp.PCSP_Step7 step7
# MAGIC WHERE MatPersCarePlanInd  = 'Y'
# MAGIC AND RANK_msd102 = 1

# COMMAND ----------

# DBTITLE 1,STEP9
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step9 AS
# MAGIC SELECT *,
# MAGIC        rank() OVER (PARTITION BY Person_ID_Mother,UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS step9_RANK
# MAGIC FROM global_temp.msd101pregnancybooking_old_orgs_updated msd101
# MAGIC WHERE RPstartdate <= '$RPEnddate'

# COMMAND ----------

# DBTITLE 1,PCSP_Step10
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step10 AS
# MAGIC SELECT *
# MAGIC FROM global_temp.PCSP_Step9 step9
# MAGIC WHERE step9_RANK = 1
# MAGIC AND (DischargeDateMatService IS NULL OR DischargeDateMatService > date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT)),119))
# MAGIC AND date_add(date_sub(AntenatalAppDate, TRY_CAST(GestAgeBooking AS INT)),119) BETWEEN '$rp_startdate_rolling_six_months' and '$RPEnddate'

# COMMAND ----------

# DBTITLE 1,PCSP_Step11
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step11 AS
# MAGIC SELECT msd102.Person_ID_Mother,
# MAGIC        msd102.UniqPregID,
# MAGIC        msd102.MatPersCarePlanInd,
# MAGIC        rank() OVER (PARTITION BY step10.Person_ID_Mother,step10.UniqPregID ORDER BY msd102.CarePlanDate DESC, msd102.RecordNumber  DESC) AS step11_rank
# MAGIC FROM global_temp.PCSP_Step10 step10
# MAGIC LEFT JOIN global_temp.msd102matcareplan_old_orgs_updated msd102
# MAGIC ON step10.Person_ID_Mother = msd102.Person_ID_Mother and step10.UniqPregID = msd102.UniqPregID
# MAGIC WHERE step10.RPstartdate <= '$RPEnddate'
# MAGIC AND msd102.CarePlanType = '05'
# MAGIC AND msd102.MatPersCarePlanInd  IS NOT NULL
# MAGIC AND msd102.CarePlanDate <= date_add(date_sub(AntenatalAppDate, TRY_CAST (GestAgeBooking AS INT)),119) 

# COMMAND ----------

# DBTITLE 1,PCSP_Step12 - This is dataset N3 
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step12 AS
# MAGIC SELECT * FROM global_temp.PCSP_Step11 step11
# MAGIC WHERE step11.step11_rank = 1
# MAGIC AND MatPersCarePlanInd = 'Y'
# MAGIC

# COMMAND ----------

# DBTITLE 1,PCSP_Step13 - This is dataset N
# MAGIC %sql
# MAGIC
# MAGIC -- BS - stop here - push data to RAW_Numerator
# MAGIC
# MAGIC --PCSP_Step4 = N1
# MAGIC --PCSP_Step8 = N2
# MAGIC --PCSP_Step12 = N3
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Step13 AS
# MAGIC SELECT step4.*
# MAGIC FROM global_temp.PCSP_Step4 step4
# MAGIC INNER JOIN global_temp.PCSP_Step8 step8
# MAGIC on step4.Person_ID_Mother = step8.Person_ID_Mother AND step4.UniqPregID = step8.UniqPregID
# MAGIC INNER JOIN global_temp.PCSP_Step12 step12
# MAGIC on step4.Person_ID_Mother = step12.Person_ID_Mother AND step4.UniqPregID = step12.UniqPregID

# COMMAND ----------

# DBTITLE 1,PCSP - Final Numerator to RAW table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.PCSP_Numerator_Raw 
# MAGIC SELECT MSD101_ID AS KeyValue
# MAGIC        , '$RPBegindate' AS RPStartDate
# MAGIC        , '$RPEnddate' AS RPEndDate
# MAGIC        , '$status' AS Status
# MAGIC        , 'PCP_All_Pathways' AS Indicator
# MAGIC        , 'PCP' AS IndicatorFamily
# MAGIC        , Person_ID_Mother AS Person_ID
# MAGIC        , PregnancyID
# MAGIC        , OrgCodeProvider
# MAGIC        , step1_RANK AS Rank 
# MAGIC        , 0 AS OverDQThreshold
# MAGIC        , TO_TIMESTAMP('$RunTime','dd/MM/yyyy HH:mm:ss')  as CreatedAt
# MAGIC        , null as rank_imd_decile
# MAGIC        , null as ethniccategory
# MAGIC        , null as ethnicgroup
# MAGIC               
# MAGIC FROM global_temp.PCSP_Step13
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
# MAGIC INSERT INTO $outSchema.PCSP_Provider_Aggregated 
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
# MAGIC       , TO_TIMESTAMP('$RunTime','dd/MM/yyyy HH:mm:ss') as CreatedAt
# MAGIC FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, count(*) as Total from $outSchema.PCSP_Numerator_Geographies
# MAGIC WHERE Indicator = 'PCP_All_Pathways'
# MAGIC group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num
# MAGIC RIGHT JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total
# MAGIC from $outSchema.PCSP_Denominator_Geographies
# MAGIC WHERE Indicator = 'PCP_All_Pathways'
# MAGIC group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) as den
# MAGIC ON num.OrgCodeProvider = den.OrgCodeProvider
# MAGIC LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE

# COMMAND ----------

# DBTITLE 1,AggregateDQThreshold
# MAGIC %sql
# MAGIC UPDATE $outSchema.PCSP_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate >= 5 THEN 1 ELSE 0 END WHERE Indicator = 'PCP_All_Pathways'

# COMMAND ----------

dbutils.notebook.exit("Notebook: PCP_All_Pathways ran successfully")