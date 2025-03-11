-- Databricks notebook source
-- DBTITLE 1,Global Widgets 
-- MAGIC %python
-- MAGIC
-- MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
-- MAGIC #dbutils.widgets.text("RPStartDate", "2015-04-01", "RPStartDate")
-- MAGIC #dbutils.widgets.text("RPEndDate", "2015-04-30", "RPEndDate")
-- MAGIC #dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
-- MAGIC #dbutils.widgets.text("PatientFilterStartDate", "2012-01-01", "PatientFilterStartDate")
-- MAGIC #dbutils.widgets.text("source", "mat_pre_clear", "source")
-- MAGIC #dbutils.widgets.text("RunTime", "2021-01-25", "RunTime")
-- MAGIC
-- MAGIC outSchema = dbutils.widgets.get("outSchema")
-- MAGIC assert outSchema
-- MAGIC  
-- MAGIC dss_corporate = dbutils.widgets.get("dss_corporate")
-- MAGIC assert dss_corporate
-- MAGIC  
-- MAGIC RPStartDate = dbutils.widgets.get("RPStartDate")
-- MAGIC assert RPStartDate
-- MAGIC  
-- MAGIC RPEndDate = dbutils.widgets.get("RPEndDate")
-- MAGIC assert RPEndDate
-- MAGIC  
-- MAGIC PatientFilterStartDate = dbutils.widgets.get("PatientFilterStartDate")
-- MAGIC assert PatientFilterStartDate
-- MAGIC  
-- MAGIC source = dbutils.widgets.get("source")
-- MAGIC assert source
-- MAGIC  
-- MAGIC RunTime = dbutils.widgets.get("RunTime")
-- MAGIC assert RunTime
-- MAGIC  
-- MAGIC params = {"Target database" : outSchema, "Source database" : source, "Reference database" : dss_corporate, "Start Date" : RPStartDate, "End Date" : RPEndDate, "PatientFilterStartDate": PatientFilterStartDate, "RunTime" : RunTime}
-- MAGIC print(params)
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW NMPAMeetCriteriaForAspirinPatientBase 
-- MAGIC AS 
-- MAGIC WITH LastAntenatalAppointmentForPregnancy AS 
-- MAGIC (
-- MAGIC     SELECT DISTINCT
-- MAGIC           MSD101.MSD101_ID
-- MAGIC          ,MSD101.UniqPregID
-- MAGIC          ,MSD101.Person_ID_Mother
-- MAGIC          ,MSD101.OrgCodeProvider
-- MAGIC          ,MSD101.RecordNumber
-- MAGIC          ,MSD101.AntenatalAppDate
-- MAGIC          ,MSD101.GestAgeBooking
-- MAGIC          ,MSD101.LastMenstrualPeriodDate
-- MAGIC          ,MSD101.DischargeDateMatService
-- MAGIC          ,MSD101.EDDAgreed
-- MAGIC          ,ROW_NUMBER() OVER (
-- MAGIC                               PARTITION BY MSD101.Person_ID_Mother, MSD101.UniqPregID 
-- MAGIC                               ORDER BY MSD101.AntenatalAppDate DESC, MSD101.RecordNumber DESC
-- MAGIC                             ) AS rank
-- MAGIC   FROM $source.MSD101PregnancyBooking MSD101
-- MAGIC   WHERE (RPStartDate = '$RPStartDate')
-- MAGIC     AND (GestAgeBooking > 0)
-- MAGIC     AND (GestAgeBooking + DATEDIFF('$RPStartDate', AntenatalAppDate)) <=119 
-- MAGIC     AND (GestAgeBooking + DATEDIFF('$RPEndDate', AntenatalAppDate)) >=119
-- MAGIC     AND (DischargeDateMatService IS NULL OR DATEDIFF(DischargeDateMatService, RPStartDate) >= 0)
-- MAGIC )
-- MAGIC SELECT  LAAFP.MSD101_ID
-- MAGIC        ,LAAFP.Person_ID_Mother
-- MAGIC        ,LAAFP.UniqPregID
-- MAGIC        ,LAAFP.OrgCodeProvider
-- MAGIC        ,LAAFP.GestAgeBooking
-- MAGIC        ,LAAFP.AntenatalAppDate
-- MAGIC        ,LAAFP.DischargeDateMatService
-- MAGIC        ,LAAFP.Rank
-- MAGIC FROM LastAntenatalAppointmentForPregnancy LAAFP
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Create Denominator
-- MAGIC %sql
-- MAGIC /* Produce RAW denominator */
-- MAGIC INSERT INTO $outSchema.Measures_raw
-- MAGIC SELECT  DISTINCT
-- MAGIC         MCFAPB.MSD101_ID as KeyValue
-- MAGIC        ,'$RPStartDate' AS RPStartDate
-- MAGIC        ,'$RPEndDate' AS RPEndDate
-- MAGIC        ,'Pregnancy' AS IndicatorFamily 
-- MAGIC        ,'Aspirin_MeetCriteria' AS Indicator
-- MAGIC        ,0 AS isNumerator
-- MAGIC        ,1 AS isDenominator
-- MAGIC        ,MCFAPB.Person_ID_Mother as Person_ID
-- MAGIC        ,MCFAPB.UniqPregID as PregnancyID
-- MAGIC        ,MCFAPB.OrgCodeProvider
-- MAGIC        ,1 as Rank 
-- MAGIC        ,0 AS IsOverDQThreshold
-- MAGIC        ,current_timestamp() as CreatedAt 
-- MAGIC        ,null as Rank_IMD_Decile
-- MAGIC        ,null as EthnicCategory
-- MAGIC        ,null as EthnicGroup 
-- MAGIC FROM NMPAMeetCriteriaForAspirinPatientBase MCFAPB
-- MAGIC WHERE MCFAPB.rank = 1

-- COMMAND ----------

-- MAGIC %md #Create Numerator Filters

-- COMMAND ----------

-- DBTITLE 1,Filter A: Has a diagnosis in ('chronic hypertension,'chronic kidney disease','autoimmune disease','diabetes')
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterA
-- MAGIC AS
-- MAGIC SELECT  MSD106.Person_ID_Mother
-- MAGIC        ,MSD106.UniqPregID
-- MAGIC FROM $source.MSD106DiagnosisPreg MSD106
-- MAGIC        JOIN $outSchema.aspirin_code_reference lookup ON MSD106.Diag = lookup.Code AND
-- MAGIC                                                         lookup.Area IN ( 
-- MAGIC                                                                          'Chronic hypertension', -- 75 Codes
-- MAGIC                                                                          'Chronic kidney disease', -- 141 Codes
-- MAGIC                                                                          'Autoimmune disease', -- 462 Codes
-- MAGIC                                                                          'Diabetes (type 1 or type 2)' -- 62 Codes
-- MAGIC                                                                        )  -- 740 Codes
-- MAGIC        JOIN NMPAMeetCriteriaForAspirinPatientBase base ON base.UniqPregID = MSD106.UniqPregID AND 
-- MAGIC                                                           base.Person_ID_Mother = MSD106.Person_ID_Mother
-- MAGIC WHERE MSD106.RPEndDate <= '$RPEndDate'
-- MAGIC GROUP BY  MSD106.Person_ID_Mother
-- MAGIC          ,MSD106.UniqPregID
-- MAGIC ;
-- MAGIC SELECT 'PatientBase_FilterA' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterA
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Filter B: Has previous diagnosis of hypertension
CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterB
AS
SELECT  MSD107.Person_ID_Mother
       ,MSD107.UniqPregID 
FROM $source.msd107MedHistory MSD107 
       JOIN $outSchema.aspirin_code_reference lookup ON MSD107.PrevDiag = lookup.Code AND
                                                        lookup.Area IN ( 
                                                                        'Hypertension in previous pregnancy' -- 57 codes
                                                                       )  
       JOIN NMPAMeetCriteriaForAspirinPatientBase base ON base.UniqPregID = MSD107.UniqPregID AND base.Person_ID_Mother = MSD107.Person_ID_Mother
WHERE MSD107.RPEndDate <= '$RPEndDate'
GROUP BY  MSD107.Person_ID_Mother
         ,MSD107.UniqPregID 
;
SELECT 'PatientBase_FilterB' AS Filter,COUNT(*) AS RowCount
FROM PatientBase_FilterB
;

-- COMMAND ----------

-- MAGIC %md Two of Five

-- COMMAND ----------

-- DBTITLE 1,Filter 3a: Woman's first pregnancy
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterC1 
-- MAGIC AS
-- MAGIC SELECT  MSD101.Person_ID_Mother
-- MAGIC        ,MSD101.UniqPregID
-- MAGIC FROM $source.msd101PregnancyBooking MSD101
-- MAGIC         JOIN NMPAMeetCriteriaForAspirinPatientBase PatientBase ON PatientBase.Person_ID_Mother = MSD101.Person_ID_Mother AND 
-- MAGIC                                                                   PatientBase.UniqPregID = MSD101.UniqPregID
-- MAGIC WHERE MSD101.RPStartDate > '$PatientFilterStartDate' 
-- MAGIC   AND MSD101.RPEndDate <= '$RPEndDate'
-- MAGIC   AND ( MSD101.PreviousLiveBirths + MSD101.PreviousStillBirths ) = 0
-- MAGIC GROUP BY  MSD101.Person_ID_Mother
-- MAGIC          ,MSD101.UniqPregID
-- MAGIC ;
-- MAGIC SELECT 'PatientBase_FilterC1' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterC1
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Filter 3b: This is a multiple pregnancy (more than one fetus) 
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterC2 
-- MAGIC AS
-- MAGIC SELECT  MSD103.Person_ID_Mother
-- MAGIC        ,MSD103.UniqPregID
-- MAGIC FROM $source.msd103DatingScan MSD103
-- MAGIC         JOIN NMPAMeetCriteriaForAspirinPatientBase PatientBase ON PatientBase.Person_ID_Mother = MSD103.Person_ID_Mother AND 
-- MAGIC                                                                   PatientBase.UniqPregID = MSD103.UniqPregID
-- MAGIC WHERE MSD103.RPStartDate > '$PatientFilterStartDate' 
-- MAGIC   AND MSD103.RPEndDate <= '$RPEndDate'
-- MAGIC   AND MSD103.NoFetusesDatingUltrasound > 1
-- MAGIC GROUP BY  MSD103.Person_ID_Mother
-- MAGIC          ,MSD103.UniqPregID
-- MAGIC ;
-- MAGIC SELECT 'PatientBase_FilterC2' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterC2
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Filter 3c: Mother over 40 years old at estimated delivery date
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterC3 
-- MAGIC AS
-- MAGIC SELECT  MSD101.Person_ID_Mother
-- MAGIC        ,MSD101.UniqPregID
-- MAGIC FROM $source.msd101PregnancyBooking MSD101
-- MAGIC       JOIN $source.msd001MotherDemog MSD001 ON MSD101.Person_ID_Mother = MSD001.Person_ID_Mother AND MSD101.UniqSubmissionID = MSD001.UniqSubmissionID     
-- MAGIC       JOIN NMPAMeetCriteriaForAspirinPatientBase PaientBase ON PaientBase.Person_ID_Mother = MSD101.Person_ID_Mother AND 
-- MAGIC                                                                PaientBase.UniqPregID = MSD101.UniqPregID
-- MAGIC                                                                
-- MAGIC WHERE MSD101.RPStartDate > '$PatientFilterStartDate' 
-- MAGIC   AND MSD101.RPEndDate <= '$RPEndDate'
-- MAGIC   AND DATEDIFF(MSD101.EDDAgreed,(Add_Months(MSD001.PersonBirthDateMother, 480))) >= 0 -- 480 Months = 40 Years
-- MAGIC GROUP BY  MSD101.Person_ID_Mother
-- MAGIC          ,MSD101.UniqPregID
-- MAGIC ;
-- MAGIC SELECT 'PatientBase_FilterC3' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterC3

-- COMMAND ----------

-- MAGIC %run ../../BMI/BMI_gestation $weeks=16 $RPStartDate=$RPStartDate $RPEndDate=$RPEndDate $source=$source

-- COMMAND ----------

-- DBTITLE 1,Filter 3d: The mother had a BMI of 35 or more at first visit
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW ASPIRIN_BMI_ALL AS
-- MAGIC
-- MAGIC  Select MSD101_ID, '$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE, CCONTACTDATE AS CARE_DIAGDATE, PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER, BMI AS BMI_RANGES , 'calc_id' as statdetail from BMI_Calc
-- MAGIC  UNION 
-- MAGIC  Select MSD101_ID, '$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE, CCONTACTDATE AS CARE_DIAGDATE, PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER, BMI_RANGES, 'recorded_id' as statdetail from BMI_Recorded
-- MAGIC  UNION 
-- MAGIC  Select MSD101_ID, '$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE, DIAGDATE AS CARE_DIAGDATE, PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER, BMI_RANGES, 'diag_id' as statdetail from BMI_Diagnosed
-- MAGIC ;
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterC4 
-- MAGIC AS
-- MAGIC WITH EarliestAntenatalDate AS (
-- MAGIC   SELECT
-- MAGIC     MSD101.Person_ID_Mother,
-- MAGIC     MSD101.UniqPregID,
-- MAGIC     MSD101.AntenatalAppDate,
-- MAGIC     MSD101.RecordNumber,
-- MAGIC     ROW_NUMBER () OVER (
-- MAGIC       PARTITION BY MSD101.Person_ID_Mother, MSD101.UniqPregID
-- MAGIC       ORDER BY MSD101.AntenatalAppDate ASC--, MSD101.RecordNumber ASC
-- MAGIC     ) as Ranking
-- MAGIC   FROM $source.msd101PregnancyBooking MSD101
-- MAGIC          JOIN NMPAMeetCriteriaForAspirinPatientBase PatientBase ON MSD101.Person_ID_Mother = PAtientBase.Person_ID_Mother AND 
-- MAGIC                                                                    MSD101.UniqPregID = PAtientBase.UniqPregID
-- MAGIC   WHERE MSD101.RPStartDate > '$PatientFilterStartDate' 
-- MAGIC     AND MSD101.RPEndDate <= '$RPEndDate'
-- MAGIC )
-- MAGIC SELECT  EAD.Person_ID_Mother
-- MAGIC        ,EAD.UniqPregID
-- MAGIC FROM EarliestAntenatalDate EAD
-- MAGIC       INNER JOIN ASPIRIN_BMI_ALL BMI ON  EAD.Ranking = 1
-- MAGIC                                                  AND BMI.Person_ID_Mother = EAD.Person_ID_Mother
-- MAGIC                                                  AND BMI.UniqPregID = EAD.UniqPregID
-- MAGIC                                                  AND BMI.RPEndDate <= '$RPEndDate'
-- MAGIC                                                  AND BMI.RPStartDate > '$PatientFilterStartDate'
-- MAGIC                                                  AND (BMI.BMI_RANGES >= 35 OR BMI.BMI_RANGES = 'Body Mass Index 40+ - Severely Obese')
-- MAGIC                                                  AND DATEDIFF(BMI.CARE_DIAGDATE, EAD.AntenatalAppDate) BETWEEN -6 AND 6 -- 7 days EXCLUDING 
-- MAGIC GROUP BY  EAD.Person_ID_Mother
-- MAGIC          ,EAD.UniqPregID;
-- MAGIC SELECT 'PatientBase_FilterC4' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterC4;

-- COMMAND ----------

-- DBTITLE 1,Filter 3e: There is a family history of Pre-eclampsia
-- MAGIC %sql 
-- MAGIC -- History of eclampsia
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_FilterC5 
-- MAGIC AS
-- MAGIC SELECT  MSD108.Person_ID_Mother
-- MAGIC        ,MSD108.UniqPregID
-- MAGIC FROM $source.msd108famhistbooking MSD108
-- MAGIC        JOIN $outSchema.aspirin_code_reference lookup ON MSD108.Situation = lookup.Code AND
-- MAGIC                                                         lookup.Area IN ( 
-- MAGIC                                                                          'History of eclampsia' -- 40 Codes
-- MAGIC                                                                        )                                                                   
-- MAGIC        JOIN NMPAMeetCriteriaForAspirinPatientBase PatientBase ON PatientBase.Person_ID_Mother = MSD108.Person_ID_Mother AND 
-- MAGIC                                                                  PatientBase.UniqPregID = MSD108.UniqPregID
-- MAGIC WHERE MSD108.RPStartDate > '$PatientFilterStartDate'
-- MAGIC   AND MSD108.RPEndDate <= '$RPEndDate' 
-- MAGIC GROUP BY  MSD108.Person_ID_Mother
-- MAGIC          ,MSD108.UniqPregID
-- MAGIC ;
-- MAGIC SELECT 'PatientBase_FilterC5' AS Filter,COUNT(*) AS RowCount
-- MAGIC FROM PatientBase_FilterC5
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Filter3: A and/or B and/or Any two of five C conditions
-- MAGIC %sql
-- MAGIC /* IN Filter A or Filter B or 2 of Filter C */
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW PatientBase_NumeratorFilter
-- MAGIC AS
-- MAGIC SELECT  DISTINCT
-- MAGIC         Person_ID_Mother
-- MAGIC        ,UniqPregID
-- MAGIC FROM 
-- MAGIC (
-- MAGIC   SELECT A.Person_ID_Mother
-- MAGIC         ,A.UniqPregID
-- MAGIC         ,2 AS Weight
-- MAGIC   FROM PatientBase_FilterA A
-- MAGIC   UNION ALL
-- MAGIC   SELECT B.Person_ID_Mother
-- MAGIC         ,B.UniqPregID
-- MAGIC         ,2 AS Weight
-- MAGIC   FROM PatientBase_FilterB B
-- MAGIC   UNION ALL
-- MAGIC   SELECT C1.Person_ID_Mother
-- MAGIC         ,C1.UniqPregID
-- MAGIC         ,1 AS Weight
-- MAGIC   FROM PatientBase_FilterC1 C1
-- MAGIC   UNION ALL
-- MAGIC   SELECT C2.Person_ID_Mother
-- MAGIC         ,C2.UniqPregID
-- MAGIC         ,1 AS Weight
-- MAGIC   FROM PatientBase_FilterC2 C2
-- MAGIC   UNION ALL
-- MAGIC   SELECT C3.Person_ID_Mother
-- MAGIC         ,C3.UniqPregID
-- MAGIC         ,1 AS Weight
-- MAGIC   FROM PatientBase_FilterC3 C3
-- MAGIC   UNION ALL
-- MAGIC   SELECT C4.Person_ID_Mother
-- MAGIC         ,C4.UniqPregID
-- MAGIC         ,1 AS Weight
-- MAGIC   FROM PatientBase_FilterC4 C4
-- MAGIC   UNION ALL
-- MAGIC   SELECT C5.Person_ID_Mother
-- MAGIC         ,C5.UniqPregID
-- MAGIC         ,1 AS Weight
-- MAGIC   FROM PatientBase_FilterC5 C5
-- MAGIC )
-- MAGIC GROUP BY Person_ID_Mother
-- MAGIC       ,UniqPregID
-- MAGIC HAVING SUM(Weight) >= 2

-- COMMAND ----------

-- DBTITLE 1,Set Numerator flags (subset) on the denominators that match the numerator filters
-- MAGIC %sql
-- MAGIC WITH numerator_Measures_raw AS 
-- MAGIC (
-- MAGIC   SELECT  
-- MAGIC           numer.UniqPregID AS PregnancyID
-- MAGIC          ,numer.Person_ID_Mother AS Person_ID
-- MAGIC   FROM PatientBase_NumeratorFilter numer 
-- MAGIC          JOIN NMPAMeetCriteriaForAspirinPatientBase denom ON denom.UniqPregID = numer.UniqPregID AND
-- MAGIC                                                              denom.Person_ID_Mother = numer.Person_ID_Mother AND
-- MAGIC                                                              denom.rank = 1 AND
-- MAGIC                                                              denom.GestAgeBooking <= 112 -- From SPEC
-- MAGIC   GROUP BY  numer.UniqPregID
-- MAGIC            ,numer.Person_ID_Mother
-- MAGIC )
-- MAGIC UPDATE $outSchema.Measures_raw raw
-- MAGIC SET raw.IsNumerator = 1
-- MAGIC WHERE EXISTS ( SELECT 1 
-- MAGIC                FROM numerator_Measures_raw 
-- MAGIC                WHERE numerator_Measures_raw.PregnancyID = raw.PregnancyID 
-- MAGIC                  AND numerator_Measures_raw.Person_ID = raw.Person_ID
-- MAGIC                  AND Indicator = 'Aspirin_MeetCriteria'
-- MAGIC              ) 
-- MAGIC   AND raw.Indicator = 'Aspirin_MeetCriteria'
-- MAGIC ;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit("Notebook: Aspirin_MeetCriteria ran successfully")