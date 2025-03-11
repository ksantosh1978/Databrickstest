-- Databricks notebook source
-- MAGIC %py
-- MAGIC """SQL views to get women's BMI when they reach x weeks gestation in the month."""

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(f"Input reporting period start date: {dbutils.widgets.get('RPStartDate')}")
-- MAGIC print(f"Input reporting period end date: {dbutils.widgets.get('RPEndDate')}")
-- MAGIC print(f"Input weeks: {dbutils.widgets.get('weeks')}")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC assert dbutils.widgets.get('weeks').isdigit(), 'Non-digit value provided for weeks.'

-- COMMAND ----------

-- View to get women who reach x weeks gestation in the month.
CREATE OR REPLACE TEMPORARY VIEW UNIQPREG AS 
SELECT D.MSD101_ID, D.PERSON_ID_MOTHER,D.UNIQPREGID, D.DATE_XWEEK, D.GESTAGEBOOKING, D.DISCHARGEDATEMATSERVICE,D.ORGCODEPROVIDER,
       D.RECORDNUMBER, D.RPSTARTDATE,D.RPENDDATE, D.RANK
FROM 
   (   
     SELECT DISTINCT MSD101_ID, PERSON_ID_MOTHER,UNIQPREGID, DATE_ADD(ANTENATALAPPDATE, cast('$weeks' as int)*7 - cast(GESTAGEBOOKING as int)) DATE_XWEEK,
     GESTAGEBOOKING, DISCHARGEDATEMATSERVICE,ORGCODEPROVIDER,
     RECORDNUMBER, '$RPStartDate' as RPSTARTDATE,'$RPEndDate' AS RPENDDATE,
     ROW_NUMBER () OVER (PARTITION BY PERSON_ID_MOTHER, UNIQPREGID ORDER BY ANTENATALAPPDATE DESC, RECORDNUMBER DESC) RANK
     FROM global_temp.msd101pregnancybooking_old_orgs_updated MSD101
   
     where MSD101.RPStartDate <= '$RPStartDate' and MSD101.GestAgeBooking > 0
     ) AS D
WHERE D.RANK=1
AND D.DATE_XWEEK <= '$RPEndDate' 
AND D.DATE_XWEEK >= '$RPStartDate'
AND (D.DISCHARGEDATEMATSERVICE IS NULL OR D.DISCHARGEDATEMATSERVICE > D.DATE_XWEEK)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  BMI_Calc as 
(SELECT distinct D.MSD101_ID, D.PERSON_ID_MOTHER, D.UNIQPREGID, D.DATE_XWEEK,D.GESTAGEBOOKING
  , D.DISCHARGEDATEMATSERVICE, D.ORGCODEPROVIDER, D.RECORDNUMBER, D.RPSTARTDATE,D.RPENDDATE, D.RANK, D.CCONTACTDATE
  ,CASE WHEN Recorded_BMI IS NOT NULL and Calculated_BMI IS NOT NULL THEN 
            CASE WHEN ABS(Recorded_BMI - Calculated_BMI) <= 2 then Recorded_BMI ELSE NULL END
        WHEN Recorded_BMI IS NOT NULL and Calculated_BMI IS NULL THEN Recorded_BMI
        WHEN Recorded_BMI IS NULL and Calculated_BMI IS NOT NULL THEN 
            CASE WHEN Calculated_BMI <= 100 then Calculated_BMI ELSE NULL END
   ELSE NULL END AS BMI
 FROM
(
 SELECT DISTINCT D.*, CC.CCONTACTDATE, 
 CAST(B.ObsValue AS DECIMAL(10,1)) as Recorded_BMI,
 CAST((CASE WHEN UPPER(REPLACE(B1.UCUMUnit,' ','')) ='G' THEN B1.Obsvalue / 1000 
       ELSE B1.ObsValue END) / 
       NULLIF(((CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) ='CM'  THEN B2.Obsvalue / 100 
          ELSE CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) = 'IN' THEN B2.ObsValue * 0.0254 
                ELSE CASE WHEN B2.ObsValue > 80 THEN B2.ObsValue / 100 
                      ELSE B2.ObsValue END 
                END 
          END)
          *(CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) ='CM' THEN B2.Obsvalue / 100 
                 ELSE CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) = 'IN' THEN B2.ObsValue * 0.0254 
                       ELSE CASE WHEN B2.ObsValue > 80 THEN B2.ObsValue / 100 
                             ELSE B2.ObsValue END 
                       END 
                 END)),0) AS DECIMAL(10,1)) AS Calculated_BMI
 FROM UNIQPREG D
 LEFT JOIN $source.MSD201CARECONTACTPREG CC ON D.PERSON_ID_MOTHER =CC.PERSON_ID_MOTHER AND D.UNIQPREGID=CC.UNIQPREGID 
 
 LEFT JOIN  
   ( 
    SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider , B.RPStartDate ContactDate
    FROM UNIQPREG A 
    LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother
    WHERE (ObsCode IN ('162863004','162864005','301331008','310252000','408512008','412768003','60621009','427090001') AND ObsScheme = '03')
   ) B ON CC.PERSON_ID_MOTHER = B.PERSON_ID_MOTHER AND CC.UNIQPREGID=B.UNIQPREGID AND CC.ORGCODEPROVIDER = B.CAREPROVIDER AND CC.CARECONID=B.CARECONID and cc.rpstartdate =b.contactdate
 
 LEFT JOIN
 (
     SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider, B.RPStartDate ContactDate
     FROM UNIQPREG A
     LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother 
     WHERE (ObsCode IN ('27113001','363808001') AND ObsScheme = '03' )
 )B1 ON CC.UniqPregId = B1.UniqPregId AND CC.Person_Id_Mother = B1.Person_ID_Mother AND CC.OrgCodeProvider = B1.CareProvider AND CC.CARECONID = B1.CARECONID and cc.rpstartdate =b1.contactdate
 LEFT JOIN 
 (
     SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider, B.RPStartDate ContactDate
     FROM UNIQPREG A
     LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother
     WHERE 
     (
         ObsCode IN ('248333004','50373000') AND ObsScheme = '03' 
     )
 )B2 ON B1.UniqPregId = B2.UniqPregId AND B1.Person_ID_Mother = B2.Person_ID_Mother AND B1.CareProvider = B2.CareProvider AND B1.CARECONID = B2.CARECONID and cc.rpstartdate =b2.contactdate
 )D
 )
;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BMI_Recorded AS (
 SELECT DISTINCT D.MSD101_ID, D.RPSTARTDATE, D.RPENDDATE, CCONTACTDATE, Date_Xweek, D.Person_ID_mother, D.UniqPregID, D.OrgCodeProvider,
        CASE WHEN B.FindingCode = '427090001' THEN 'Body Mass Index less than 16.5 - Severely Underweight' 
             WHEN B.FindingCode = '310252000' THEN 'Body Mass Index less than 20 - Underweight' 
             WHEN B.FindingCode = '412768003' THEN 'Body Mass Index 20-24 - Normal' 
             WHEN B.FindingCode = '162863004' THEN 'Body Mass Index 25-29 - Overweight' 
             WHEN B.FindingCode = '162864005' THEN 'Body Mass Index 30+ - Obese' 
             WHEN B.FindingCode = '408512008' THEN 'Body Mass Index 40+ - Severely Obese' ELSE 'Body Mass Index not recorded' END AS BMI_Ranges
             
 FROM UniqPreg D
 LEFT JOIN $source.MSD201CARECONTACTPREG CC ON D.PERSON_ID_MOTHER =CC.PERSON_ID_MOTHER AND D.UNIQPREGID=CC.UNIQPREGID 
 LEFT JOIN $source.MSD202CareActivityPreg B ON CC.UniqPregId = B.UniqPregId AND CC.Person_ID_Mother = B.Person_ID_Mother AND CC.careConID = B.CareConID AND CC.ORGCODEPROVIDER = B.ORGCODEPROVIDER AND CC.rpstartdate=B.rpstartdate
 WHERE (B.FindingCode IN ('162863004','162864005','310252000','408512008','412768003', '427090001') AND B.FindingScheme = '04')
 );

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BMI_Diagnosed AS (
 SELECT DISTINCT D.MSD101_ID, D.RPSTARTDATE, D.RPENDDATE, DIAGDATE, Date_Xweek, D.Person_ID_mother, D.UniqPregID, D.OrgCodeProvider,
        CASE WHEN A.Diag = '427090001' THEN 'Body Mass Index less than 16.5 - Severely Underweight' 
             WHEN A.Diag = '310252000' THEN 'Body Mass Index less than 20 - Underweight' 
             WHEN A.Diag = '412768003' THEN 'Body Mass Index 20-24 - Normal' 
             WHEN A.Diag = '162863004' THEN 'Body Mass Index 25-29 - Overweight' 
             WHEN A.Diag = '162864005' THEN 'Body Mass Index 30+ - Obese' 
             WHEN A.Diag = '408512008' THEN 'Body Mass Index 40+ - Severely Obese' ELSE 'Body Mass Index not recorded' END AS BMI_Ranges 
                    
   
 FROM UniqPreg D
 LEFT JOIN $source.MSD106DIAGNOSISPREG A ON D.PERSON_ID_MOTHER=A.PERSON_ID_MOTHER AND D.UNIQPREGID=A.UNIQPREGID 
 WHERE (Diag IN ('162863004','162864005','310252000','408512008','412768003', '427090001') AND DiagScheme = '06')
 AND A.RPStartDate <='$RPStartDate'
 );