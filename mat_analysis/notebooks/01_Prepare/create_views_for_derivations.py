# Databricks notebook source
# RPStartdate = "2023-01-01"
# RPEnddate = "2023-01-31"
# outSchema = "mat_analysis"
# dbSchema = "testdata_mat_analysis_mat_pre_clear"
# dss_corporate = "dss_corporate"
# month_id = ""

RPStartdate = dbutils.widgets.get("RPStartdate")
RPEnddate = dbutils.widgets.get("RPEnddate")
outSchema = dbutils.widgets.get("outSchema")
dbSchema = dbutils.widgets.get("dbSchema") 
dss_corporate = dbutils.widgets.get("dss_corporate")
month_id = dbutils.widgets.get("month_id")
         
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

SmokingFindingCode = "'697956009','56294008','30310000','110483000','ZV6D8','ZV4K0','ZRh4.','ZRh4.','ZRBm2','ZRBm2','ZRao.','ZRaM.','ZG233','Eu171','Eu17.','E251z','E2511','E2510','E251.','E023.','9OOZ.','9OOA.','9OO9.','9OO8.','9OO7.','9OO6.','9OO5.','9OO4.','9OO3.','9OO2.','9OO1.','9OO..','9N4M.','9N2k.','9ko..','9kc0.','9kc..','8I3M.','8I39.','8I2J.','8I2I.','8HTK.','8HkQ.','8HBM.','8H7i.','8CAL.','8CAg.','8BP3.','8B3Y.','8B3f.','8B2B.','745Hz','745Hy','745H4','745H3','745H2','745H1','745H0','745H.','67H6.','67A3.','38DH.','13p6.','13p5.','13p4.','13p3.','13p2.','13p1.','13p0.','13p..','137Z.','137Y.','137X.','137V.','137R.','137Q.','137Q.','137P.','137P.','137M.','137J.','137h.','137H.','137G.','137f.','137e.','137d.','137c.','137C.','137b.','137a.','1376.','1375.','1374.','1373.','1372.','Z720','Z716','F17','XE0or','XE0oq','XaWNE','XaLQh','XaJX2','XaItg','XaIIu','XagO3','Xaa26','Ub0pT','Ub0pJ','Eu17.','E251.','137R.','137M.','137G.','137C.','449345000','446172000','365982000','160616005','394871007','394873005','169940006','65568007','365981007','203191000000107','77176002','230065006','59978006','160605003','230063004','56771006','160603005','230060001','160604004','230062009','56578002','230059006','428041000124106','82302008','160619003','308438006','266929003','266920004','160606002','230064005'"


NonSmokingFindingCode = "'E2513','9km..','137T.','137S.','137O.','137N.','137K.','137j.','137F.','137B.','137A.','1379.','1378.','1377.','Xa1bv','Ub1na','137K.','9kn..','13WK.','137L.','8392000','XE0oh','137L.','1371.','908781000000104','722499006','266919005','449368009','8517006','405746006','360900008','360918006','160618006','160621008','281018007','266928006','266924008','1092071000000105','266922007','1092111000000104','266923002','1092091000000109','160620009','492191000000103','1092031000000108','735128000','48031000119106','266921000','1092131000000107','266925009','1092041000000104','360890004','360929005','105541001','105539002','105540000','53896009','87739003','786063001','785889008'"

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE WIDGET TEXT RPEnddate DEFAULT "2015-04-30";
# MAGIC --CREATE WIDGET TEXT dbSchema DEFAULT "testdata_mat_analysis_mat_pre_clear";

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nina Prosser Audited this field 12 Oct 2021
# MAGIC
# MAGIC The table MSD101PregnancyBooking is represented as 2 new temp views (MSD101PregnancyBooking_with_smokingstatusbooking_derived & msd101pregnancybooking_with_smokingstatusdelivery_derived) where the extra fields smokingstatusbooking_derived and smokingstatusdelivery_derived are created.  The original fields smokingstatusbooking and smokingstatusdelivery are still in place and will contain null values going forward.
# MAGIC

# COMMAND ----------

# DBTITLE 1, MSD101PregnancyBooking with the field smokingstatusbooking_derived added
sql = f"""
create or replace global temp view MSD101PregnancyBooking_with_smokingstatusbooking_derived as
 
with smokingstatus_derived as
(
select
--If MSD101.AntenatalAppDate = MSD201.CContactDate AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER
 case when msd101.AntenatalAppDate = msd201.CContactDate then
        (CASE WHEN msd202.findingcode in ({SmokingFindingCode})
                   OR CAST(CIGARETTESPERDAY AS FLOAT) > 0 
                   OR CAST(COMONREADING AS FLOAT) >= 4 then 'Smoker' 
              WHEN msd202.findingcode in ({NonSmokingFindingCode})
                   OR msd202.ObsCode in ('160625004') 
                   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0 
                   OR CAST(COMONREADING AS FLOAT) <4 then 'Non-Smoker / Ex-Smoker' 
          END) 
     --else if MSD101.AntenatalAppDate within +/- 3 days of MSD201.CContactDate AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER  
     when (msd101.AntenatalAppDate between date_sub(msd201.CContactDate,3) and date_add(msd201.CContactDate,3)) then
        (CASE WHEN msd202.findingcode in ({SmokingFindingCode}) 
                   OR CAST(CIGARETTESPERDAY AS FLOAT) > 0 
                   OR CAST(COMONREADING AS FLOAT) >= 4 then 'Smoker' 
              WHEN msd202.findingcode in ({NonSmokingFindingCode}) 
                   OR msd202.ObsCode in ('160625004') 
                   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0 
                   OR CAST(COMONREADING AS FLOAT) <4 then 'Non-Smoker / Ex-Smoker' 
         --   else if MSD101.AntenatalAppDate within +/- 3 days of MSD201.CContactDate AND MSD202.NewSmokingStatus is unknown
              WHEN msd202.findingcode in ('266927001') then 'Unknown'
          END) 
    else null end as smokingstatusbooking_derived
   ,msd101.*
   ,msd202.person_id_mother as msd202_person_id_mother
   ,msd202.RecordNumber as msd202_RecordNumber
   ,msd202.UniqPregID as msd202_UniqPregID
   ,msd202.MSD202_ID as msd202_MSD202_ID
   ,msd201.ccontactdate as msd201_ccontactdate
    from {dbSchema}.MSD101PregnancyBooking msd101
    left outer join {dbSchema}.MSD201CareContactPreg msd201
      on msd101.person_id_Mother = msd201.person_id_Mother and msd101.uniqpregid = msd201.uniqpregid
    left join {dbSchema}.msd202careactivitypreg msd202
      on msd201.person_id_Mother = msd202.person_id_Mother and msd201.uniqpregid = msd202.uniqpregid and msd201.careconid = msd202.careconid
    where msd101.rpenddate = '{RPEnddate}' 
      and msd201.RPStartDate in ('{RPStartdate}', add_months('{RPStartdate}',-1))
      and msd202.RPEndDate = msd201.RPEndDate
    order by CContactTime desc
)
, smokingstatus_derived_ranked as
(select *  
   , RANK () OVER (PARTITION BY msd202_person_id_Mother,msd202_uniqpregid,AntenatalAppDate 
                ORDER BY CASE WHEN (AntenatalAppDate = msd201_ccontactdate) AND smokingstatusbooking_derived LIKE 'Smoker%' THEN 1 
                              WHEN (AntenatalAppDate = msd201_ccontactdate) AND smokingstatusbooking_derived LIKE '%Smoker%' THEN 2 
                              WHEN smokingstatusbooking_derived LIKE 'Smoker%' THEN 3 
                              WHEN smokingstatusbooking_derived LIKE '%Smoker%' THEN 4 
                              ELSE 5 END ASC
                         ,msd202_MSD202_ID DESC) as rnk_202
--  , RANK () OVER(PARTITION BY msd202_person_id_Mother,msd202_uniqpregid,AntenatalAppDate ORDER BY cast(msd202_RecordNumber as bigint) desc, cast(msd202_MSD202_ID as bigint) desc) as rnk_202
   from smokingstatus_derived
   where smokingstatusbooking_derived is not null )
, msd101_with_smokingstatusbooking as
--25,523
(select distinct *
  from smokingstatus_derived_ranked
  where rnk_202 = 1)
, MSD101PregnancyBooking_derived as
(select * from msd101_with_smokingstatusbooking
union
select msd101_derived.smokingstatusbooking_derived
  , msd101.*
  , msd101_derived.msd202_person_id_mother
  , msd101_derived.RecordNumber
  , msd101_derived.msd202_UniqPregID
  , msd101_derived.msd202_MSD202_ID
  , msd101_derived.msd201_ccontactdate
  , msd101_derived.rnk_202
from {dbSchema}.MSD101PregnancyBooking msd101
left join  msd101_with_smokingstatusbooking msd101_derived
  on msd101.MSD101_ID = msd101_derived.MSD101_ID 
where msd101.rpenddate = '{RPEnddate}'
  and msd101_derived.MSD101_ID is null
)
select * from MSD101PregnancyBooking_derived"""
print(sql)
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There is requirement to include the MSD302CareActivityLabDel table records as providers are submitting to this table as well, so the global view MSD101PregnancyBooking_with_smokingstatusdelivery_derived which was dependent on table msd202careactivitypreg is made dependant on two views made below. MSD202PregnancyBooking_with_smokingstatusdelivery_derived and MSD302PregnancyBooking_with_smokingstatusdelivery_derived
# MAGIC
# MAGIC - Had to declare MSD202PregnancyBooking_with_smokingstatusdelivery_derived and MSD302PregnancyBooking_with_smokingstatusdelivery_derived as global temp views even though we are using in the current scope, as on spark 3 it is giving exceptions on table not found at CQIM Master notebook
# MAGIC
# MAGIC - Had to update add_months() to take RPStartDate as it behaves different with RPEndDate on spark2 and spark3 cluster. Mentioned in https://nhsd-confluence.digital.nhs.uk/display/DSP/Upgrade+of+Spark+to+version+3.1.2.+-++Issues+encountered 

# COMMAND ----------

sql = f"""

create or replace global temp view MSD202PregnancyBooking_with_smokingstatusdelivery_derived as

select
 case when (MSD301.CaesareanDate = MSD201.CContactDate OR MSD301.LabourOnsetDate= MSD201.CContactDate) then
   --If MSD101.AntenatalAppDate = MSD201.CContactDate AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER
   (CASE WHEN msd202.findingcode in ({SmokingFindingCode}) 
    --    OR CAST(CIGARETTESPERDAY AS FLOAT) > 0  
    --    OR CAST(COMONREADING AS FLOAT) >= 4 
          OR (msd202.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI') AND CAST(msd202.ObsValue AS FLOAT) > 0) --Cigarettes per day recorded
          OR (msd202.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
              AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
              AND CAST(msd202.ObsValue AS FLOAT) >= 4) --CO monitoring recorded
           then 'Smoker' 
         WHEN msd202.findingcode in ({NonSmokingFindingCode})
           OR msd202.ObsCode in ('160625004')
      --   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0  
      --   OR CAST(COMONREADING AS FLOAT) <4  
           OR (msd202.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
              AND CAST(msd202.ObsValue AS FLOAT) = 0
                ) --Cigarettes per day recorded
           OR (msd202.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
              AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
              AND CAST(msd202.ObsValue AS FLOAT) < 4
                ) --CO monitoring recorded
            then 'Non-Smoker / Ex-Smoker' 
          WHEN msd202.findingcode = '266927001'
            then 'Unknown'
          END) 
   
--   Where (MSD301.CaesareanDate OR MSD301.LabourOnsetDate within -/+3 days of MSD201.CContactDate) AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER  
  when (MSD301.CaesareanDate between date_sub(msd201.CContactDate,3) and date_add(msd201.CContactDate,3) or MSD301.LabourOnsetDate between date_sub(msd201.CContactDate,3) and date_add(msd201.CContactDate,3)) then
       ( CASE WHEN msd202.findingcode in ({SmokingFindingCode}) 
          --    OR CAST(CIGARETTESPERDAY AS FLOAT) > 0  
          --    OR CAST(COMONREADING AS FLOAT) >= 4 
                OR (msd202.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                      AND CAST(msd202.ObsValue AS FLOAT) > 0
                    ) --Cigarettes per day recorded
                OR (msd202.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                    AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                    AND CAST(msd202.ObsValue AS FLOAT) >= 4
                    ) --CO monitoring recorded
         then 'Smoker' 
             WHEN msd202.findingcode in ({NonSmokingFindingCode}) 
               OR msd202.ObsCode in ('160625004')
          --   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0  
          --   OR CAST(COMONREADING AS FLOAT) <4  
               OR (msd202.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                    AND CAST(msd202.ObsValue AS FLOAT) = 0
                  ) --Cigarettes per day recorded
               OR (msd202.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                    AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                    AND CAST(msd202.ObsValue AS FLOAT) < 4
                  ) --CO monitoring recorded
          then 'Non-Smoker / Ex-Smoker' 
              when msd202.findingcode in ('266927001') 
          then 'Unknown' else null
        END  ) 
  else null end as  smokingstatusdelivery_derived
  ,msd101.*
  ,msd202.person_id_mother as msd202_person_id_mother
  ,msd202.RecordNumber as msd202_RecordNumber
  ,msd202.UniqPregID as msd202_UniqPregID
  ,msd202.MSD202_ID as msd202_MSD202_ID
  ,MSD201.CContactDate as msd201_CContactDate
  ,MSD301.LabourOnsetDate as msd301_LabourOnsetDate
  ,MSD301.CaesareanDate as msd301_CaesareanDate
  from {dbSchema}.MSD101PregnancyBooking msd101
  left outer join {dbSchema}.MSD201CareContactPreg msd201
    on msd101.person_id_Mother = msd201.person_id_Mother and msd101.uniqpregid = msd201.uniqpregid
  left join {dbSchema}.msd202careactivitypreg msd202
    on msd201.person_id_Mother = msd202.person_id_Mother and msd201.uniqpregid = msd202.uniqpregid and msd201.careconid = msd202.careconid
  left join {dbSchema}.MSD301LabourDelivery msd301
    on msd101.person_id_Mother = msd301.person_id_Mother and msd101.uniqpregid = msd301.uniqpregid and msd101.OrgCodeProvider = msd301.OrgCodeProvider
  -- where msd101.rpenddate = '{RPEnddate}' 
  where msd201.RPStartDate in ('{RPStartdate}', add_months('{RPStartdate}',-1),add_months('{RPStartdate}',-2))
    and msd202.RPEndDate = msd201.RPEndDate"""
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = f"""

create or replace global temp view MSD302PregnancyBooking_with_smokingstatusdelivery_derived as
select
 --If MSD101.AntenatalAppDate = MSD201.CContactDate AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER
 case when (MSD301.CaesareanDate = msd302.ClinInterDateMother OR MSD301.LabourOnsetDate= msd302.ClinInterDateMother) then
     (CASE WHEN msd302.findingcode in ({SmokingFindingCode}) 
            OR (msd302.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                AND CAST(msd302.ObsValue AS FLOAT) > 0
                ) --Cigarettes per day recorded
            OR (msd302.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                AND CAST(msd302.ObsValue AS FLOAT) >= 4
                ) --CO monitoring recorded
      --    OR CAST(CIGARETTESPERDAY AS FLOAT) > 0  
      --    OR CAST(COMONREADING AS FLOAT) >= 4 
         then 'Smoker' 
           WHEN msd302.findingcode in ({NonSmokingFindingCode})
             OR msd302.ObsCode in ('160625004')
             OR (msd302.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                  AND CAST(msd302.ObsValue AS FLOAT) = 0
                ) --Cigarettes per day recorded
             OR (msd302.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                  AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                  AND CAST(msd302.ObsValue AS FLOAT) < 4
                ) --CO monitoring recorded
        --   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0  
        --   OR CAST(COMONREADING AS FLOAT) <4  
          then 'Non-Smoker / Ex-Smoker' 
            WHEN msd302.findingcode = '266927001'
          then 'Unknown'
      END ) 
   
  -- Where (MSD301.CaesareanDate OR MSD301.LabourOnsetDate within -/+3 days of MSD201.CContactDate) AND MSD202.NewSmokingStatus is SMOKER or NON/EX-SMOKER
  when (MSD301.CaesareanDate between date_sub(msd302.ClinInterDateMother,3) and date_add(msd302.ClinInterDateMother,3) or MSD301.LabourOnsetDate between date_sub(msd302.ClinInterDateMother,3) and date_add(msd302.ClinInterDateMother,3)) then

     (CASE WHEN msd302.findingcode in ({SmokingFindingCode}) 
             OR (msd302.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                  AND CAST(msd302.ObsValue AS FLOAT) > 0
                ) --Cigarettes per day recorded
             OR (msd302.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                  AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                  AND CAST(msd302.ObsValue AS FLOAT) >= 4
                ) --CO monitoring recorded
        --   OR CAST(CIGARETTESPERDAY AS FLOAT) > 0  
        --   OR CAST(COMONREADING AS FLOAT) >= 4 
       then 'Smoker' 
           WHEN msd302.findingcode in ({NonSmokingFindingCode}) 
             OR msd302.ObsCode in ('160625004')
             OR (msd302.ObsCode in ('230056004', '.137X', '137X.', 'Ub1tI')
                  AND CAST(msd302.ObsValue AS FLOAT) = 0
                ) --Cigarettes per day recorded
             OR (msd302.ObsCode in ('251900003', '.4I90', '4I90.', 'X77Qd')
                  AND UPPER(REPLACE(UCUMUNIT, ' ', '')) like '%PPM%'
                  AND CAST(msd302.ObsValue AS FLOAT) < 4
                ) --CO monitoring recorded
        --   OR CAST(CIGARETTESPERDAY AS FLOAT) = 0  
        --   OR CAST(COMONREADING AS FLOAT) <4  
        then 'Non-Smoker / Ex-Smoker' 
            WHEN msd302.findingcode in ('266927001') 
        then 'Unknown'
      END) 

    else null end as  smokingstatusdelivery_derived
  
   ,msd101.*
   ,msd302.person_id_mother as msd302_person_id_mother
   ,msd302.RecordNumber as msd302_RecordNumber
   ,msd302.UniqPregID as msd302_UniqPregID
   ,msd302.MSD302_ID as msd302_MSD302_ID
   ,msd302.ClinInterDateMother as ClinInterDateMother
   ,MSD301.LabourOnsetDate as msd301_LabourOnsetDate
   ,MSD301.CaesareanDate as msd301_CaesareanDate
    from {dbSchema}.MSD101PregnancyBooking msd101
    -- left outer join {dbSchema}.MSD201CareContactPreg msd201
    -- on msd101.person_id_Mother = msd201.person_id_Mother and msd101.uniqpregid = msd201.uniqpregid
    left join {dbSchema}.MSD302CareActivityLabDel msd302
    on msd101.person_id_Mother = msd302.person_id_Mother and msd101.uniqpregid = msd302.uniqpregid --and msd201.careconid = msd202.careconid
    left join {dbSchema}.MSD301LabourDelivery msd301
    on msd101.person_id_Mother = msd301.person_id_Mother and msd101.uniqpregid = msd301.uniqpregid and msd101.OrgCodeProvider = msd301.OrgCodeProvider
    -- where msd101.rpenddate = '{RPEnddate}' 
    -- where msd201.RPEndDate in ('{RPEnddate}', add_months('{RPEnddate}',-1),add_months('{RPEnddate}',-2))
    -- and msd202.RPEndDate = msd201.RPEndDate
    where msd302.RPStartDate in ('{RPStartdate}', add_months('{RPStartdate}',-1),add_months('{RPStartdate}',-2))"""
print(sql)
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,MSD101PregnancyBooking with the field smokingstatusdelivery_derived added
sql = f"""
create or replace global temp view MSD101PregnancyBooking_with_smokingstatusdelivery_derived as
with smokingstatus_derived as
(
SELECT * FROM global_temp.MSD202PregnancyBooking_with_smokingstatusdelivery_derived 
UNION
SELECT * FROM global_temp.MSD302PregnancyBooking_with_smokingstatusdelivery_derived 
)
, smokingstatus_derived_ranked as
(select *  
--  , RANK () OVER(PARTITION BY msd202_person_id_Mother,msd202_uniqpregid,AntenatalAppDate  ORDER BY  cast(msd202_RecordNumber as bigint) desc, cast(msd202_MSD202_ID as bigint) desc) as rnk_202
 , RANK () OVER(PARTITION BY msd202_person_id_Mother,msd202_uniqpregid,AntenatalAppDate 
                ORDER BY CASE WHEN (msd201_CContactDate = msd301_LabourOnsetDate OR msd201_CContactDate = msd301_CaesareanDate) AND smokingstatusdelivery_derived LIKE 'Smoker%' THEN 1 
                              WHEN (msd201_CContactDate = msd301_LabourOnsetDate OR msd201_CContactDate = msd301_CaesareanDate) AND smokingstatusdelivery_derived LIKE '%Smoker%' THEN 2
                              WHEN smokingstatusdelivery_derived LIKE 'Smoker%' THEN 3
                              WHEN smokingstatusdelivery_derived LIKE '%Smoker%' THEN 4 ELSE 5 END ASC
                        ,msd202_MSD202_ID DESC) as rnk_202
 from smokingstatus_derived
 where smokingstatusdelivery_derived is not null)
,msd101_with_smokingstatusdelivery as
(select distinct * from smokingstatus_derived_ranked where rnk_202 = 1)
, msd101_with_smokingstatusdelivery_derived as
(
select * from msd101_with_smokingstatusdelivery
union
select 
  msd101_derived.smokingstatusdelivery_derived
  , msd101.*
  , msd101_derived.msd202_person_id_mother
  , msd101_derived.msd202_RecordNumber
  , msd101_derived.msd202_UniqPregID
  , msd101_derived.msd202_msd202_ID
  , msd101_derived.msd201_CContactDate
  , msd101_derived.msd301_LabourOnsetDate
  , msd101_derived.msd301_CaesareanDate
  , msd101_derived.rnk_202
from {dbSchema}.MSD101PregnancyBooking msd101
left join msd101_with_smokingstatusdelivery msd101_derived
  on msd101.msd101_ID = msd101_derived.msd101_ID
where msd101_derived.msd101_ID is null
)
select * from msd101_with_smokingstatusdelivery_derived"""
print(sql)
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Create the temp table msd405careactivitybaby_derived with the derived field 'birthweight_derived'
sql = f"""
create or replace global temp view msd405careactivitybaby_derived as
with birthWeight_calculated  as
(
select
msd401.MSD401_ID,
case when (msd405.ObsCode in ('364589006','636..')) and
          (cast(msd405.ObsValue as decimal(7,3)) >= 0) and
          (upper(msd405.UCUMUnit) = 'G')  then round(msd405.ObsValue)
     when (msd405.ObsCode in ('364589006','636..')) and
          (cast(msd405.ObsValue as decimal(7,3)) >= 0) and
          (upper(msd405.UCUMUnit) = 'KG' or upper(msd405.UCUMUnit) = 'KILOGRAMS') then round(msd405.ObsValue*1000)
     when (msd405.ObsCode in ('27113001','22A..')) and
          (cast(msd405.ObsValue as decimal(7,3)) >= 0) and
          (upper(msd405.UCUMUnit) = 'G')  and
          msd405.ClininterDateBaby = msd401.PersonBirthDateBaby then round(msd405.ObsValue)
     when (msd405.ObsCode in ('27113001','22A..')) and
          (cast(msd405.ObsValue as decimal(7,3)) >= 0) and
          (upper(msd405.UCUMUnit) = 'KG' or upper(msd405.UCUMUnit) = 'KILOGRAMS') and
          msd405.ClininterDateBaby = msd401.PersonBirthDateBaby then round(msd405.ObsValue*1000)
     else null
     end as birthweight_derived
,msd405.*
from {dbSchema}.msd405careactivitybaby msd405
inner join {dbSchema}.msd401babydemographics msd401
  on msd405.Person_Id_Baby = msd401.Person_Id_Baby 
   and msd405.Person_Id_Mother = msd401.Person_Id_Mother 
   and msd405.UniqPregId = msd401.UniqPregId 
   and msd405.RecordNumber = msd401.RecordNumber
 where msd405.RPEnddate = '{RPEnddate}'
   and msd401.rpenddate = '{RPEnddate}')
,ranking as
(select RANK () OVER(PARTITION BY person_id_Mother, person_id_baby, uniqpregid, RecordNumber ORDER BY cast(MSD405_ID as bigint) desc, cast(MSD401_ID as bigint) desc) as rnk 
  ,*
  from birthWeight_calculated
  where birthweight_derived is not null
)
select * from ranking
where rnk = 1"""
print(sql)
spark.sql(sql)


# COMMAND ----------

# MAGIC %md
# MAGIC MHA 3426 - Apgar Derivations fix with new view msd405careactivity_apgar_records
# MAGIC
# MAGIC - Temporary view on Apgar Measures in MSD405, identifies the records with Obscode 169909004 (Apgar score at 5 minutes) and takes the earliest ApgarScore value (CareActIDBaby) latest recorded (MSD405_ID)
# MAGIC - Finds ApgarScore per baby & unique submission ID because this view will link using those fields to populate BirthsBaseTable in the monthly publication, and that consturction relies on the fields
# MAGIC - A baby with multiple providers submitting MSD401 & MSD405 records already has mutliple rows in BirthsBaseTable, and this view may have different ApgarScore values recorded in each if provider submits records to other

# COMMAND ----------

# DBTITLE 1,Create global view msd405careactivity_apgar_records to consider Apgar from MSD405
sql = f"""
CREATE OR REPLACE GLOBAL TEMP VIEW msd405careactivity_apgar_records AS WITH apgar_records AS (
SELECT MSD405.OrgCodeProvider,
  MSD405.Person_ID_Baby,
  MSD405.Person_ID_Mother,
  MSD405.UniqPregID,
  MSD405.ClinInterDateBaby,
  MSD401.PersonBirthDateBaby,
  CAST(MSD405.ApgarScore AS FLOAT) AS ApgarScore,
  MSD405.MasterSnomedCTObsCode,
  MSD405.RPStartDate,
  MSD405.RecordNumber,
  MSD405.UniqSubmissionID,
  MSD405.CareActIDBaby,
  MSD405.MSD405_ID,
  ROW_NUMBER() OVER (
    PARTITION BY MSD405.UniqPregID,MSD405.Person_ID_Baby,MSD405.Person_ID_Mother,MSD405.UniqSubmissionID
    ORDER BY MSD405.CareActIDBaby ASC,
             CAST(MSD405.MSD405_ID AS BIGINT) DESC
   ) AS ApgarRank
  FROM {dbSchema}.msd405careactivitybaby MSD405
  INNER JOIN {dbSchema}.msd401babydemographics MSD401 ON MSD401.Person_ID_Baby = MSD405.Person_ID_Baby
    AND MSD401.Person_ID_Mother = MSD405.Person_ID_Mother
    AND MSD401.UniqPregID = MSD405.UniqPregID
    AND MSD401.RecordNumber = MSD405.RecordNumber
  WHERE MSD401.RPEndDate = '{RPEnddate}'
    AND MSD405.MasterSnomedCTObsCode = '169909004'
)
SELECT * FROM apgar_records 
  WHERE ApgarRank = 1"""
print(sql)
spark.sql(sql)

# COMMAND ----------

dbutils.notebook.exit("Notebook: create_views_for_derivations ran successfully")