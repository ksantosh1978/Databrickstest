-- Databricks notebook source
-- DBTITLE 1,D_dataset
create or replace temp view D_dataset as
SELECT distinct 
       msd401.MSD401_ID as KeyValue,
       msd401.UniqPregID,
       msd401.Person_ID_Mother,
       msd401.OrgCodeProvider,
       msd301.LabourOnsetMethod,
       msd302.MasterSnomedCTProcedureCode,
       msd302.ProcedureCode,
       msd302.ClinInterDateMother,
      ROW_NUMBER () OVER(PARTITION BY msd401.OrgcodeProvider, msd401.Person_ID_Mother, msd401.UniqPregID ORDER BY msd401.MSD401_ID DESC) AS rank_denominator
FROM   global_temp.msd401BabyDemographics_old_orgs_updated msd401
inner join $dbSchema.msd301labourdelivery msd301
  on msd401.UniqPregID = msd301.UniqPregID 
  and msd401.RPStartDate = msd301.RPStartDate
left join $dbSchema.msd302careactivitylabdel msd302
  on msd401.Person_ID_Mother = msd302.Person_ID_Mother 
where msd401.RPStartDate = '$RPStartdate'
  and msd401.PersonBirthDateBaby between '$RPStartdate' and '$RPEnddate'
  and msd401.PregOutcome = "01"
  and msd401.GestationLengthBirth between 259 and 315
  and msd301.BirthsPerLabandDel = 1

-- COMMAND ----------

-- DBTITLE 1,N_dataset
create or replace temp view N_dataset as
 
select  distinct dd.KeyValue, dd.UniqPregID, dd.Person_ID_Mother, dd.OrgCodeProvider, dd.LabourOnsetMethod, dd.ClinInterDateMother,
ROW_NUMBER () OVER(PARTITION BY dd.Person_ID_Mother, dd.UniqPregID, dd.orgcodeProvider ORDER BY dd.keyValue DESC) AS rank_numerator
  from D_dataset dd
  left join $outSchema.NMPA_induction_codes snomed
         on trim(dd.MasterSnomedCTProcedureCode) = snomed.Code
         and snomed.Measure = 'Induction' 
         and '$RPStartdate' >= snomed.FirstMonth 
         and snomed.Type = 'SNOMED'
         and (snomed.LastMonth is null or '$RPStartdate' <= snomed.LastMonth) 
  left join $outSchema.NMPA_induction_codes ProcCode
        on trim(dd.ProcedureCode) = ProcCode.Code 
        and ProcCode.Measure = 'Induction' 
        and '$RPStartdate' >= ProcCode.FirstMonth 
        and (ProcCode.LastMonth is null or '$RPStartdate' <= ProcCode.LastMonth) 
        and ProcCode.Type <> 'SNOMED'  
  where
     (dd.LabourOnsetMethod in (3,4,5) OR 
     ( (dd.LabourOnsetMethod is null or dd.LabourOnsetMethod = 9) and dd.ClinInterDateMother between add_months('$RPStartdate',-1) and '$RPEnddate' ) and (snomed.Code is not null or ProcCode.Code is not null) and not (snomed.Code is null and ProcCode.Code is null) )

-- COMMAND ----------

-- DBTITLE 1,Create Denominator
-- MAGIC %sql
-- MAGIC /* Produce RAW denominator */
-- MAGIC INSERT INTO $outSchema.Measures_raw
-- MAGIC SELECT  DISTINCT
-- MAGIC         KeyValue
-- MAGIC        ,'$RPStartdate' AS RPStartDate
-- MAGIC        ,'$RPEnddate' AS RPEndDate
-- MAGIC        ,'Birth' AS IndicatorFamily 
-- MAGIC        ,'Induction' AS Indicator
-- MAGIC        ,0 AS isNumerator
-- MAGIC        ,1 AS isDenominator
-- MAGIC        ,Person_ID_Mother as Person_ID
-- MAGIC        ,UniqPregID as PregnancyID
-- MAGIC        ,OrgCodeProvider
-- MAGIC        ,rank_denominator as Rank 
-- MAGIC        ,0 AS IsOverDQThreshold
-- MAGIC        ,current_timestamp() as CreatedAt 
-- MAGIC        ,null as Rank_IMD_Decile
-- MAGIC        ,null as EthnicCategory
-- MAGIC        ,null as EthnicGroup 
-- MAGIC FROM D_dataset

-- COMMAND ----------

-- DBTITLE 1,Create Numerator
-- MAGIC %sql
-- MAGIC /* Produce RAW denominator 
-- MAGIC We need to rank the n-dataset - we can't use the rank from the d-dataset because the numeraotr could have been found from a record different to rank = 1.  However there could have been multiple n-dataset matches therefore multiple keyvalues and we can only have one uniqpregid/person_id_mother from the n-dataset.
-- MAGIC */
-- MAGIC INSERT INTO $outSchema.Measures_raw
-- MAGIC SELECT  DISTINCT
-- MAGIC         KeyValue
-- MAGIC        ,'$RPStartdate' AS RPStartDate
-- MAGIC        ,'$RPEnddate' AS RPEndDate
-- MAGIC        ,'Birth' AS IndicatorFamily 
-- MAGIC        ,'Induction' AS Indicator
-- MAGIC        ,1 AS isNumerator
-- MAGIC        ,0 AS isDenominator
-- MAGIC        ,Person_ID_Mother as Person_ID
-- MAGIC        ,UniqPregID as PregnancyID
-- MAGIC        ,OrgCodeProvider
-- MAGIC        ,rank_numerator as Rank 
-- MAGIC        ,0 AS IsOverDQThreshold
-- MAGIC        ,current_timestamp() as CreatedAt 
-- MAGIC        ,null as Rank_IMD_Decile
-- MAGIC        ,null as EthnicCategory
-- MAGIC        ,null as EthnicGroup 
-- MAGIC FROM N_dataset

-- COMMAND ----------

-- MAGIC %md Two of Five

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit("Notebook: InductionOfLabour_MeetCriteria ran successfully")