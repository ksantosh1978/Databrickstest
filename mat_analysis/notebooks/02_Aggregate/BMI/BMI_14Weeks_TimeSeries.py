# Databricks notebook source
# DBTITLE 1,Cleanup at start
# MAGIC %python
# MAGIC #dbutils.widgets.removeAll()
# MAGIC

# COMMAND ----------

# MAGIC %run ./MaternityFunctions_BMI

# COMMAND ----------

# DBTITLE 1,Global Widgets 
# MAGIC %python
# MAGIC
# MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
# MAGIC #dbutils.widgets.text("RPStartDate", "2022-02-01", "RPStartDate")
# MAGIC #dbutils.widgets.text("RPEndDate", "2022-02-28", "RPEndDate")
# MAGIC #dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
# MAGIC #dbutils.widgets.text("source", "mat_pre_clear", "source")
# MAGIC
# MAGIC outSchema = dbutils.widgets.get("outSchema")
# MAGIC assert outSchema
# MAGIC  
# MAGIC dss_corporate = dbutils.widgets.get("dss_corporate")
# MAGIC assert dss_corporate
# MAGIC  
# MAGIC RPStartDate = dbutils.widgets.get("RPStartDate")
# MAGIC assert RPStartDate
# MAGIC  
# MAGIC RPEndDate = dbutils.widgets.get("RPEndDate")
# MAGIC assert RPEndDate
# MAGIC  
# MAGIC source = dbutils.widgets.get("source")
# MAGIC assert source
# MAGIC
# MAGIC params = {"Target database" : outSchema, "Source database" : source, "Reference database" : dss_corporate, "Start Date" : RPStartDate, "End Date" : RPEndDate}
# MAGIC print(params)

# COMMAND ----------

# DBTITLE 1,Setup BMI_15 weeks DQ Denominator
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW UNIQPREG AS 
# MAGIC SELECT D.MSD101_ID, D.PERSON_ID_MOTHER,D.UNIQPREGID, D.DATE_15WEEK,D.GESTAGEBOOKING, D.DISCHARGEDATEMATSERVICE,D.ORGCODEPROVIDER,
# MAGIC        D.RECORDNUMBER, D.RPSTARTDATE,D.RPENDDATE, D.RANK
# MAGIC FROM 
# MAGIC    (   
# MAGIC      SELECT DISTINCT MSD101_ID, PERSON_ID_MOTHER,UNIQPREGID, DATE_ADD(ANTENATALAPPDATE, 105-cast(GESTAGEBOOKING as int)) DATE_15WEEK,
# MAGIC      GESTAGEBOOKING, DISCHARGEDATEMATSERVICE,ORGCODEPROVIDER,
# MAGIC      RECORDNUMBER, '$RPStartDate' as RPSTARTDATE,'$RPEndDate' AS RPENDDATE,
# MAGIC      ROW_NUMBER () OVER (PARTITION BY PERSON_ID_MOTHER, UNIQPREGID ORDER BY ANTENATALAPPDATE DESC, RECORDNUMBER DESC) RANK
# MAGIC      FROM global_temp.msd101pregnancybooking_old_orgs_updated MSD101
# MAGIC    
# MAGIC      where MSD101.RPStartDate <='$RPStartDate' and MSD101.GestAgeBooking>0
# MAGIC      ) AS D
# MAGIC WHERE D.RANK=1
# MAGIC AND D.DATE_15WEEK<='$RPEndDate' 
# MAGIC AND D.DATE_15WEEK >='$RPStartDate'
# MAGIC AND (D.DISCHARGEDATEMATSERVICE IS NULL OR D.DISCHARGEDATEMATSERVICE > D.DATE_15WEEK)
# MAGIC ;
# MAGIC
# MAGIC  Select Count(*) as DQDenom from global_temp.UNIQPREG;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  $outSchema.Measures_raw WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartDate';
# MAGIC delete from  $outSchema.Measures_Aggregated WHERE IndicatorFamily like 'BMI%' and RPStartDate = '$RPStartDate'

# COMMAND ----------

# MAGIC %sql
# MAGIC  INSERT INTO $outSchema.Measures_raw
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE, 
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_DQ' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.UNIQPREG
# MAGIC  ;
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW  BMI_Calc as 
# MAGIC (SELECT distinct D.MSD101_ID, D.PERSON_ID_MOTHER, D.UNIQPREGID, D.DATE_15WEEK,D.GESTAGEBOOKING
# MAGIC   , D.DISCHARGEDATEMATSERVICE, D.ORGCODEPROVIDER, D.RECORDNUMBER, D.RPSTARTDATE,D.RPENDDATE, D.RANK, D.CCONTACTDATE
# MAGIC   ,CASE WHEN Recorded_BMI IS NOT NULL and Calculated_BMI IS NOT NULL THEN 
# MAGIC             CASE WHEN ABS(Recorded_BMI - Calculated_BMI) <= 2 then Recorded_BMI ELSE NULL END
# MAGIC         WHEN Recorded_BMI IS NOT NULL and Calculated_BMI IS NULL THEN Recorded_BMI
# MAGIC         WHEN Recorded_BMI IS NULL and Calculated_BMI IS NOT NULL THEN 
# MAGIC             CASE WHEN Calculated_BMI <= 100 then Calculated_BMI ELSE NULL END
# MAGIC    ELSE NULL END AS BMI
# MAGIC  FROM
# MAGIC (
# MAGIC  SELECT DISTINCT D.*, CC.CCONTACTDATE, 
# MAGIC  CAST(B.ObsValue AS DECIMAL(10,1)) as Recorded_BMI,
# MAGIC  CAST((CASE WHEN UPPER(REPLACE(B1.UCUMUnit,' ','')) ='G' THEN B1.Obsvalue / 1000 
# MAGIC        ELSE B1.ObsValue END) / 
# MAGIC        NULLIF(((CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) ='CM'  THEN B2.Obsvalue / 100 
# MAGIC           ELSE CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) = 'IN' THEN B2.ObsValue * 0.0254 
# MAGIC                 ELSE CASE WHEN B2.ObsValue > 80 THEN B2.ObsValue / 100 
# MAGIC                       ELSE B2.ObsValue END 
# MAGIC                 END 
# MAGIC           END)
# MAGIC           *(CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) ='CM' THEN B2.Obsvalue / 100 
# MAGIC                  ELSE CASE WHEN UPPER(REPLACE(B2.UCUMUnit,' ','')) = 'IN' THEN B2.ObsValue * 0.0254 
# MAGIC                        ELSE CASE WHEN B2.ObsValue > 80 THEN B2.ObsValue / 100 
# MAGIC                              ELSE B2.ObsValue END 
# MAGIC                        END 
# MAGIC                  END)),0) AS DECIMAL(10,1)) AS Calculated_BMI
# MAGIC  FROM global_temp.UNIQPREG D
# MAGIC  LEFT JOIN $source.MSD201CARECONTACTPREG CC ON D.PERSON_ID_MOTHER =CC.PERSON_ID_MOTHER AND D.UNIQPREGID=CC.UNIQPREGID 
# MAGIC  
# MAGIC  LEFT JOIN  
# MAGIC    ( 
# MAGIC     SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider , B.RPStartDate ContactDate
# MAGIC     FROM global_temp.UNIQPREG A 
# MAGIC     LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother
# MAGIC     WHERE (ObsCode IN ('162863004','162864005','301331008','310252000','408512008','412768003','60621009','427090001') AND ObsScheme = '03')
# MAGIC    ) B ON CC.PERSON_ID_MOTHER = B.PERSON_ID_MOTHER AND CC.UNIQPREGID=B.UNIQPREGID AND CC.ORGCODEPROVIDER = B.CAREPROVIDER AND CC.CARECONID=B.CARECONID and cc.rpstartdate =b.contactdate
# MAGIC  
# MAGIC  LEFT JOIN
# MAGIC  (
# MAGIC      SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider, B.RPStartDate ContactDate
# MAGIC      FROM global_temp.UNIQPREG A
# MAGIC      LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother 
# MAGIC      WHERE (ObsCode IN ('27113001','363808001') AND ObsScheme = '03' )
# MAGIC  )B1 ON CC.UniqPregId = B1.UniqPregId AND CC.Person_Id_Mother = B1.Person_ID_Mother AND CC.OrgCodeProvider = B1.CareProvider AND CC.CARECONID = B1.CARECONID and cc.rpstartdate =b1.contactdate
# MAGIC  LEFT JOIN 
# MAGIC  (
# MAGIC      SELECT DISTINCT A.*, B.ObsScheme, B.ObsCode, B.CARECONID, B.ObsValue, b.UCUMUnit, B.OrgCodeProvider CareProvider, B.RPStartDate ContactDate
# MAGIC      FROM global_temp.UNIQPREG A
# MAGIC      LEFT JOIN $source.MSD202CareActivityPreg B ON A.UniqPregId = B.UniqPregId AND A.Person_ID_Mother = B.Person_Id_Mother
# MAGIC      WHERE 
# MAGIC      (
# MAGIC          ObsCode IN ('248333004','50373000') AND ObsScheme = '03' 
# MAGIC      )
# MAGIC  )B2 ON B1.UniqPregId = B2.UniqPregId AND B1.Person_ID_Mother = B2.Person_ID_Mother AND B1.CareProvider = B2.CareProvider AND B1.CARECONID = B2.CARECONID and cc.rpstartdate =b2.contactdate
# MAGIC  )D
# MAGIC  )
# MAGIC
# MAGIC ;
# MAGIC Select Count(*)  from global_temp.BMI_Calc ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_Calculated as
# MAGIC  (
# MAGIC  SELECT DISTINCT MSD101_ID,RPSTARTDATE, RPENDDATE,PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER,
# MAGIC         CASE WHEN BMI < 16.5 THEN 'Body Mass Index less than 16.5 - Severely Underweight' 
# MAGIC              WHEN BMI BETWEEN 16.5 AND 19.99 THEN 'Body Mass Index less than 20 - Underweight' 
# MAGIC              WHEN BMI BETWEEN 20 AND 24.99 THEN 'Body Mass Index 20-24 - Normal' 
# MAGIC              WHEN BMI BETWEEN 25 AND 29.99 THEN 'Body Mass Index 25-29 - Overweight' 
# MAGIC              WHEN BMI BETWEEN 30 AND 39.99 THEN 'Body Mass Index 30+ - Obese' 
# MAGIC              WHEN BMI >= 40 THEN 'Body Mass Index 40+ - Severely Obese' ELSE 'Body Mass Index not recorded' END AS BMI_Ranges,
# MAGIC            
# MAGIC              ROW_Number() Over ( Partition by Person_ID_Mother, UniqPregId Order by BMI Desc) AS BMI_Rank    
# MAGIC  FROM global_temp.BMI_Calc
# MAGIC  WHERE CCONTACTDATE <= Date_15week
# MAGIC  ORDER BY PERSON_ID_MOTHER ASC, UNIQPREGID ASC
# MAGIC  )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_Recorded as (
# MAGIC  SELECT DISTINCT D.MSD101_ID,D.RPSTARTDATE,D.RPENDDATE,D.Person_ID_mother, D.UniqPregID, D.OrgCodeProvider,
# MAGIC         CASE WHEN B.FindingCode = '427090001' THEN 'Body Mass Index less than 16.5 - Severely Underweight' 
# MAGIC              WHEN B.FindingCode = '310252000' THEN 'Body Mass Index less than 20 - Underweight' 
# MAGIC              WHEN B.FindingCode = '412768003' THEN 'Body Mass Index 20-24 - Normal' 
# MAGIC              WHEN B.FindingCode = '162863004' THEN 'Body Mass Index 25-29 - Overweight' 
# MAGIC              WHEN B.FindingCode = '162864005' THEN 'Body Mass Index 30+ - Obese' 
# MAGIC              WHEN B.FindingCode = '408512008' THEN 'Body Mass Index 40+ - Severely Obese' ELSE 'Body Mass Index not recorded' END AS BMI_Ranges
# MAGIC              
# MAGIC  FROM global_temp.UniqPreg D
# MAGIC  LEFT JOIN $source.MSD201CARECONTACTPREG CC ON D.PERSON_ID_MOTHER =CC.PERSON_ID_MOTHER AND D.UNIQPREGID=CC.UNIQPREGID 
# MAGIC  LEFT JOIN $source.MSD202CareActivityPreg B ON CC.UniqPregId = B.UniqPregId AND CC.Person_ID_Mother = B.Person_ID_Mother AND CC.careConID = B.CareConID AND CC.ORGCODEPROVIDER = B.ORGCODEPROVIDER AND CC.rpstartdate=B.rpstartdate
# MAGIC  WHERE (B.FindingCode IN ('162863004','162864005','310252000','408512008','412768003', '427090001') AND B.FindingScheme = '04')
# MAGIC  AND CCONTACTDATE <= Date_15week
# MAGIC  )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_Diagnosed AS (
# MAGIC  SELECT DISTINCT D.MSD101_ID,D.RPSTARTDATE,D.RPENDDATE,D.Person_ID_mother, D.UniqPregID, D.OrgCodeProvider,
# MAGIC         CASE WHEN A.Diag = '427090001' THEN 'Body Mass Index less than 16.5 - Severely Underweight' 
# MAGIC              WHEN A.Diag = '310252000' THEN 'Body Mass Index less than 20 - Underweight' 
# MAGIC              WHEN A.Diag = '412768003' THEN 'Body Mass Index 20-24 - Normal' 
# MAGIC              WHEN A.Diag = '162863004' THEN 'Body Mass Index 25-29 - Overweight' 
# MAGIC              WHEN A.Diag = '162864005' THEN 'Body Mass Index 30+ - Obese' 
# MAGIC              WHEN A.Diag = '408512008' THEN 'Body Mass Index 40+ - Severely Obese' ELSE 'Body Mass Index not recorded' END AS BMI_Ranges 
# MAGIC                     
# MAGIC    
# MAGIC  FROM global_temp.UniqPreg D
# MAGIC  LEFT JOIN $source.MSD106DIAGNOSISPREG A ON D.PERSON_ID_MOTHER=A.PERSON_ID_MOTHER AND D.UNIQPREGID=A.UNIQPREGID 
# MAGIC  WHERE (Diag IN ('162863004','162864005','310252000','408512008','412768003', '427090001') AND DiagScheme = '06')
# MAGIC  AND DIAGDATE <= Date_15week and A.RPStartDate <='$RPStartDate'
# MAGIC  )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_ALL AS
# MAGIC
# MAGIC  Select  MSD101_ID,'$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE,PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER,BMI_RANGES , 'calc_id' as statdetail from BMI_Calculated 
# MAGIC  UNION 
# MAGIC  Select  MSD101_ID,'$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE,PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER,BMI_RANGES, 'recorded_id' as statdetail from BMI_Recorded
# MAGIC  UNION 
# MAGIC  Select  MSD101_ID,'$RPStartDate' RPSTARTDATE, '$RPEndDate' RPENDDATE,PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER,BMI_RANGES, 'diag_id' as statdetail from BMI_Diagnosed
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_MAXBMI AS
# MAGIC SELECT *, ROW_NUMBER() OVER (PARTITION BY PERSON_ID_MOTHER, UNIQPREGID, ORGCODEPROVIDER ORDER BY 
# MAGIC CASE WHEN BMI_Ranges = 'Body Mass Index less than 16.5 - Severely Underweight' THEN 1 
# MAGIC             WHEN BMI_Ranges = 'Body Mass Index less than 20 - Underweight'  THEN 2 
# MAGIC             WHEN BMI_Ranges = 'Body Mass Index 20-24 - Normal'  THEN 3 
# MAGIC             WHEN BMI_Ranges = 'Body Mass Index 25-29 - Overweight'  THEN 4 
# MAGIC             WHEN BMI_Ranges = 'Body Mass Index 30+ - Obese'  THEN 5 
# MAGIC             WHEN BMI_Ranges = 'Body Mass Index 40+ - Severely Obese'  THEN 6 
# MAGIC                         
# MAGIC             ELSE 9 END DESC) AS RANK 
# MAGIC FROM BMI_ALL 
# MAGIC WHERE BMI_RANGES NOT IN ('Body Mass Index not recorded');
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW BMI_NUMERATOR_FINAL AS
# MAGIC SELECT PERSON_ID_MOTHER,UNIQPREGID,MSD101_ID,RANK,BMI_RANGES,ORGCODEPROVIDER,RPSTARTDATE,RPENDDATE FROM BMI_MAXBMI
# MAGIC WHERE RANK=1
# MAGIC ORDER BY PERSON_ID_MOTHER,UNIQPREGID,MSD101_ID DESC
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW  BMI_DQ_NUMERATOR AS (
# MAGIC  Select MSD101_ID,RPSTARTDATE,RPENDDATE, PERSON_ID_MOTHER, UNIQPREGID,RANK,BMI_RANGES,ORGCODEPROVIDER
# MAGIC  FROM BMI_NUMERATOR_FINAL 
# MAGIC  ORDER BY PERSON_ID_MOTHER, UNIQPREGID,BMI_RANGES,RANK
# MAGIC ) ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,CALCULATING DQ NUMERATORS
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_raw
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE, 
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_DQ' AS INDICATOR, 
# MAGIC  1 AS ISNUMERATOR, 
# MAGIC  0 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.BMI_DQ_NUMERATOR
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Building Aggregates
# MAGIC %sql
# MAGIC WITH Numerator AS
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider,COUNT(DISTINCT KeyValue, Person_id,Pregnancyid) AS Total
# MAGIC   FROM  $outSchema.Measures_raw
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsNumerator = 1   
# MAGIC     AND IndicatorFamily = 'BMI_14+1Wks' and Indicator='BMI_DQ'
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider
# MAGIC )
# MAGIC ,Denominator AS 
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, COUNT(Person_id,Pregnancyid) AS Total
# MAGIC   FROM $outSchema.Measures_raw
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsDenominator = 1   
# MAGIC     AND IndicatorFamily = 'BMI_14+1Wks' and Indicator='BMI_DQ'
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider
# MAGIC )
# MAGIC
# MAGIC ,cteorg as (
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where ORG_OPEN_DATE <= '$RPEndDate' and (ORG_CLOSE_DATE is NULL or ORG_CLOSE_DATE >= '$RPStartDate')
# MAGIC   and BUSINESS_END_DATE is NULL 
# MAGIC )
# MAGIC ,cteclosed as ( -- If organisation not open during reporting period, resolve to current name
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where BUSINESS_END_DATE is NULL
# MAGIC   and org_type_code='TR'
# MAGIC )
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC SELECT  '$RPStartDate' AS RPStartDate
# MAGIC        ,'$RPEndDate' AS RPEndDate
# MAGIC        ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC        ,'BMI_DQ' AS Indicator
# MAGIC        ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC        ,CASE WHEN C.NAME IS NULL THEN CC.NAME ELSE C.NAME END  AS OrgName
# MAGIC        ,'Provider' AS OrgLevel 
# MAGIC        ,COALESCE(numerator.Total, 0) AS UnroundedNumerator
# MAGIC        ,Denominator.Total AS UnroundedDenominator
# MAGIC        ,MaternityRate(COALESCE(Numerator.Total,0), Denominator.Total, NULL) AS UnroundedRate -- Default Rate
# MAGIC        ,MaternityRounding(COALESCE(Numerator.Total,0))   AS RoundedNumerator      -- Maternity Rounding
# MAGIC        ,MaternityRounding(Denominator.Total)  AS RoundedDenominator    -- Maternity Rounding
# MAGIC        ,MaternityRate(MaternityRounding(COALESCE(Numerator.Total, 0)), MaternityRounding(Denominator.Total), 0) AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC        ,0 AS OverDQThreshold 
# MAGIC        ,current_timestamp() as CreatedAt 
# MAGIC FROM Denominator
# MAGIC       LEFT OUTER JOIN Numerator ON  Denominator.OrgCodeProvider =Numerator.OrgCodeProvider 
# MAGIC       LEFT OUTER JOIN 
# MAGIC       cteorg c
# MAGIC       ON Denominator.OrgCodeProvider = c.Org_Code
# MAGIC       LEFT OUTER JOIN 
# MAGIC       CTECLOSED CC ON Denominator.OrgCodeProvider = CC.Org_Code
# MAGIC       
# MAGIC    ;
# MAGIC    
# MAGIC  Select count(*)  from $outSchema.Measures_Aggregated where IndicatorFamily like 'BMI%';
# MAGIC

# COMMAND ----------

# MAGIC %sql  
# MAGIC
# MAGIC UPDATE $outSchema.Measures_Aggregated 
# MAGIC SET IsOverDQThreshold = CASE 
# MAGIC                         WHEN Rounded_Rate >= 90  
# MAGIC                           THEN 1 
# MAGIC                           ELSE 0 
# MAGIC                        END 
# MAGIC WHERE Indicator = 'BMI_DQ' ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE $outSchema.Measures_raw raw
# MAGIC SET IsOverDQThreshold = 1 
# MAGIC WHERE EXISTS ( 
# MAGIC                SELECT 1 
# MAGIC                FROM $outSchema.Measures_Aggregated agg
# MAGIC                WHERE agg.IndicatorFamily = raw.IndicatorFamily
# MAGIC                  AND agg.Indicator = agg.Indicator
# MAGIC                  AND agg.IsOverDQThreshold = 1
# MAGIC                  AND agg.OrgCodeProvider = raw.OrgCodeProvider
# MAGIC              )
# MAGIC   AND raw.Indicator = 'BMI_DQ'
# MAGIC   AND raw.IndicatorFamily = 'BMI_14+1Wks';
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW DQ_MEASURES AS 
# MAGIC (
# MAGIC
# MAGIC SELECT  RPStartDate AS ReportingPeriodStartDate
# MAGIC            ,RPEndDate AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Denominator' AS MEASURENAME
# MAGIC            ,Rounded_Denominator AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated 
# MAGIC     WHERE Indicator ='BMI_DQ'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC     SELECT  RPStartDate AS ReportingPeriodStartDate
# MAGIC            ,RPEndDate AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Numerator' AS MEASURENAME
# MAGIC            ,Rounded_Numerator AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated 
# MAGIC     WHERE Indicator ='BMI_DQ'
# MAGIC     
# MAGIC   UNION ALL
# MAGIC
# MAGIC     SELECT  RPStartDate AS ReportingPeriodStartDate
# MAGIC            ,RPEndDate AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Rate' AS MEASURENAME
# MAGIC            ,Rounded_Rate AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated
# MAGIC     WHERE Indicator ='BMI_DQ'
# MAGIC
# MAGIC   UNION ALL
# MAGIC   
# MAGIC     SELECT  RPStartDate AS ReportingPeriodStartDate
# MAGIC            ,RPEndDate AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Result' AS MEASURENAME
# MAGIC            ,CASE 
# MAGIC               WHEN Rounded_Rate > 90 
# MAGIC                 THEN 'Pass' 
# MAGIC               ELSE 'Fail' 
# MAGIC             END AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated
# MAGIC     WHERE Indicator ='BMI_DQ'
# MAGIC     )
# MAGIC     ;
# MAGIC     
# MAGIC  create or replace temp view  V_DQ_Measures AS  (
# MAGIC  SELECT  ReportingPeriodStartDate 
# MAGIC         ,ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,MEASURENAME
# MAGIC         ,Value
# MAGIC         ,CreatedAt  
# MAGIC   FROM DQ_MEASURES 
# MAGIC )
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC SELECT COUNT(*) FROM V_DQ_MEASURES;
# MAGIC     
# MAGIC INSERT INTO $outSchema.measures_CSV
# MAGIC SELECT  ReportingPeriodStartDate 
# MAGIC        ,ReportingPeriodEndDate 
# MAGIC        ,IndicatorFamily
# MAGIC        ,Indicator
# MAGIC        ,OrgCodeProvider
# MAGIC        ,OrgName
# MAGIC        ,OrgLevel
# MAGIC        ,MEASURENAME
# MAGIC        ,Value
# MAGIC        ,CreatedAt  
# MAGIC  FROM v_DQ_MEASURES ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PASSED_PROVIDERS AS
# MAGIC SELECT * from $outSchema.Measures_Aggregated WHERE IsOverDQThreshold=1 and IndicatorFamily like 'BMI%' and RPStartDate='$RPStartDate'
# MAGIC ;
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PASS_PROVIDERS AS
# MAGIC SELECT * FROM global_temp.UNIQPREG WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER FROM global_temp.PASSED_PROVIDERS) ;
# MAGIC
# MAGIC SELECT COUNT(DISTINCT ORGCODEPROViDER) FROM  global_temp.PASS_PROVIDERS ;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $outSchema.Measures_raw
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_SeverelyUnderweight' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC UNION ALL 
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_Underweight' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC  UNION ALL 
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_Normal' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC UNION ALL 
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_Overweight' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC  UNION ALL 
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_Obese' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC   UNION ALL
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_SeverelyObese' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC   UNION ALL 
# MAGIC  SELECT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  'BMI_NotRecorded' AS INDICATOR, 
# MAGIC  0 AS ISNUMERATOR, 
# MAGIC  1 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC  CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.PASS_PROVIDERS
# MAGIC  ;
# MAGIC  
# MAGIC    select indicator,count( distinct orgcodeprovider) from $outSchema.measures_raw group by indicator;
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW BMI_NUMERATOR_NOTRECORDED_cohort AS
# MAGIC
# MAGIC select d.*,coalesce(n.BMI_Ranges,'BMI_NotRecorded') Ranges from global_temp.uniqpreg d
# MAGIC left outer join global_temp.bmi_dq_numerator n
# MAGIC on d.msd101_id=n.msd101_id and d.person_id_mother=n.person_id_mother and d.uniqpregid=n.uniqpregid
# MAGIC  ;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW BMI_NUMERATOR_NOTRECORDED AS
# MAGIC   Select MSD101_ID,RPSTARTDATE,RPENDDATE, PERSON_ID_MOTHER, UNIQPREGID,RANK,RANGES,ORGCODEPROVIDER
# MAGIC   FROM BMI_NUMERATOR_NOTRECORDED_cohort where Ranges='BMI_NotRecorded'
# MAGIC  ;
# MAGIC
# MAGIC select orgcodeprovider,count(distinct msd101_id,PERSON_ID_MOTHER,uniqpregid) from BMI_NUMERATOR_NOTRECORDED group by orgcodeprovider order by orgcodeprovider;

# COMMAND ----------

# DBTITLE 1,Numerator 1  - Body Mass Index less than 16.5 - Severely Underweight
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_1
# MAGIC AS
# MAGIC
# MAGIC SELECT *, 'BMI_SeverelyUnderweight' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index less than 16.5 - Severely Underweight'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_2
# MAGIC AS
# MAGIC
# MAGIC SELECT *,'BMI_Underweight' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index less than 20 - Underweight'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_3
# MAGIC AS
# MAGIC
# MAGIC SELECT *,'BMI_Normal' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index 20-24 - Normal'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_4
# MAGIC AS
# MAGIC
# MAGIC SELECT *,'BMI_Overweight' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index 25-29 - Overweight'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_5
# MAGIC AS
# MAGIC
# MAGIC SELECT *,'BMI_Obese' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index 30+ - Obese'

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_6
# MAGIC AS
# MAGIC
# MAGIC SELECT *,'BMI_SeverelyObese' AS BMI_INDICATOR FROM global_temp.BMI_DQ_NUMERATOR 
# MAGIC WHERE ORGCODEPROVIDER IN 
# MAGIC (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC AND BMI_RANGES ='Body Mass Index 40+ - Severely Obese'

# COMMAND ----------

# MAGIC  %sql
# MAGIC  
# MAGIC  CREATE OR REPLACE GLOBAL TEMPORARY VIEW NUM_7
# MAGIC  AS
# MAGIC  
# MAGIC  SELECT *,'BMI_NotRecorded' AS BMI_INDICATOR FROM BMI_NUMERATOR_NOTRECORDED
# MAGIC  WHERE ORGCODEPROVIDER IN 
# MAGIC  (SELECT DISTINCT ORGCODEPROVIDER  FROM global_temp.PASS_PROVIDERS)
# MAGIC  AND RANGES ='BMI_NotRecorded'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW INDICATOR_NUMERATOR
# MAGIC AS
# MAGIC (
# MAGIC SELECT * FROM global_temp.NUM_1
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_2
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_3
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_4
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_5
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_6
# MAGIC UNION ALL
# MAGIC SELECT * FROM global_temp.NUM_7
# MAGIC
# MAGIC );
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_raw
# MAGIC  SELECT DISTINCT MSD101_ID AS KEY, 
# MAGIC  '$RPStartDate' as RPSTARTDATE,
# MAGIC  '$RPEndDate' as RPENDDATE,
# MAGIC  'BMI_14+1Wks' AS INDICATOR_FAMILY, 
# MAGIC  BMI_Indicator AS INDICATOR, 
# MAGIC  1 AS ISNUMERATOR, 
# MAGIC  0 AS ISDENOMINATOR, 
# MAGIC  PERSON_ID_MOTHER AS PERSON_ID, 
# MAGIC  UNIQPREGID AS PREGNANCY_ID,
# MAGIC  ORGCODEPROVIDER,
# MAGIC  RANK AS RANK,
# MAGIC  0 AS ISOVERDQTHRESHOLD, 
# MAGIC CURRENT_TIMESTAMP() AS CREATEDAT, 
# MAGIC  NULL AS RANK_IMD_DECILE, 
# MAGIC  NULL AS ETHNICCATEGORY, 
# MAGIC  NULL AS ETHNICGROUP
# MAGIC  FROM global_temp.INDICATOR_NUMERATOR
# MAGIC  
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Numerator AS
# MAGIC (
# MAGIC   SELECT '$RPStartDate', '$RPEndDate', IndicatorFamily, Indicator, OrgCodeProvider,COUNT(DISTINCT Keyvalue,Person_id,Pregnancyid) AS Total
# MAGIC   FROM  $outSchema.Measures_raw
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsNumerator = 1   
# MAGIC     AND (IndicatorFamily = 'BMI_14+1Wks' and Indicator!='BMI_DQ')
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider
# MAGIC )
# MAGIC ,Denominator AS 
# MAGIC (
# MAGIC   SELECT '$RPStartDate', '$RPEndDate', IndicatorFamily, Indicator, OrgCodeProvider, COUNT(Person_id,Pregnancyid) AS Total
# MAGIC   FROM $outSchema.Measures_raw
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsDenominator = 1   
# MAGIC     AND IndicatorFamily = 'BMI_14+1Wks' and Indicator!='BMI_DQ'
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider
# MAGIC )
# MAGIC ,cteorg as (
# MAGIC      select ORG_CODE, NAME
# MAGIC    from $dss_corporate.ORG_DAILY
# MAGIC    where ORG_OPEN_DATE <= '$RPEndDate' and (ORG_CLOSE_DATE is NULL or ORG_CLOSE_DATE >= '$RPStartDate')
# MAGIC    and BUSINESS_END_DATE is NULL 
# MAGIC  )
# MAGIC  ,cteclosed as ( -- If organisation not open during reporting period, resolve to current name
# MAGIC      select ORG_CODE, NAME
# MAGIC    from $dss_corporate.ORG_DAILY
# MAGIC    where BUSINESS_END_DATE is NULL
# MAGIC    and org_type_code='TR'
# MAGIC  )
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC SELECT  '$RPStartDate' AS RPStartDate
# MAGIC         ,'$RPEndDate' AS RPEndDate
# MAGIC         ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC         ,Denominator.Indicator AS Indicator
# MAGIC         ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC          ,CASE WHEN C.NAME IS NULL THEN CC.NAME ELSE C.NAME END  AS OrgName
# MAGIC         ,'Provider' AS OrgLevel 
# MAGIC         ,COALESCE(numerator.Total, 0) AS UnroundedNumerator
# MAGIC         ,Denominator.Total AS UnroundedDenominator
# MAGIC         ,MaternityRate(COALESCE(Numerator.Total,0), Denominator.Total, NULL) AS UnroundedRate -- Default Rate
# MAGIC         ,MaternityRounding(COALESCE(Numerator.Total,0))   AS RoundedNumerator      -- Maternity Rounding
# MAGIC         ,MaternityRounding(Denominator.Total)  AS RoundedDenominator    -- Maternity Rounding
# MAGIC         ,MaternityRate(MaternityRounding(COALESCE(Numerator.Total, 0)), MaternityRounding(Denominator.Total), 0) AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC         ,0 AS OverDQThreshold 
# MAGIC         ,current_timestamp() as CreatedAt 
# MAGIC    FROM Denominator
# MAGIC        LEFT OUTER JOIN Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator = Numerator.Indicator
# MAGIC        LEFT OUTER JOIN cteorg c 
# MAGIC        on Denominator.OrgCodeProvider =c.Org_Code
# MAGIC        LEFT OUTER JOIN CTECLOSED CC
# MAGIC        ON Denominator.OrgCodeProvider =CC.Org_Code
# MAGIC        ;
# MAGIC        
# MAGIC       
# MAGIC  
# MAGIC       
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE $outSchema.Measures_Aggregated 
# MAGIC SET IsOverDQThreshold = CASE 
# MAGIC                         WHEN Rounded_Rate >= 90  
# MAGIC                           THEN 1 
# MAGIC                           ELSE 0 
# MAGIC                        END 
# MAGIC WHERE IndicatorFamily='BMI_14+1Wks' and Indicator!= 'BMI_DQ'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH MEASURES2 AS 
# MAGIC (
# MAGIC
# MAGIC SELECT  '$RPStartDate' AS ReportingPeriodStartDate
# MAGIC            ,'$RPEndDate' AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Denominator' AS MEASURENAME
# MAGIC            ,Rounded_Denominator AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated 
# MAGIC     WHERE (IndicatorFamily ='BMI_14+1Wks' and Indicator !='BMI_DQ')
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC     SELECT  '$RPStartDate' AS ReportingPeriodStartDate
# MAGIC            ,'$RPEndDate' AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Numerator' AS MEASURENAME
# MAGIC            ,Rounded_Numerator AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated 
# MAGIC     WHERE (IndicatorFamily ='BMI_14+1Wks' and Indicator !='BMI_DQ')
# MAGIC     
# MAGIC   UNION ALL
# MAGIC
# MAGIC     SELECT  '$RPStartDate' AS ReportingPeriodStartDate
# MAGIC            ,'$RPEndDate' AS ReportingPeriodEndDate
# MAGIC            ,IndicatorFamily
# MAGIC            ,Indicator         
# MAGIC            ,OrgCodeProvider
# MAGIC            ,OrgName
# MAGIC            ,OrgLevel
# MAGIC            ,'Rate' AS MEASURENAME
# MAGIC            ,Rounded_Rate AS Value
# MAGIC            ,current_timestamp() AS CreatedAt
# MAGIC     FROM $outSchema.Measures_Aggregated
# MAGIC     WHERE (IndicatorFamily ='BMI_14+1Wks' and Indicator !='BMI_DQ')
# MAGIC
# MAGIC  )
# MAGIC     
# MAGIC     
# MAGIC INSERT INTO $outSchema.measures_CSV
# MAGIC SELECT  '$RPStartDate' as ReportingPeriodStartDate 
# MAGIC        ,'$RPEndDate' as ReportingPeriodEndDate 
# MAGIC        ,IndicatorFamily
# MAGIC        ,Indicator
# MAGIC        ,OrgCodeProvider
# MAGIC        ,OrgName
# MAGIC        ,OrgLevel
# MAGIC        ,MEASURENAME
# MAGIC        ,Value
# MAGIC        ,CreatedAt  
# MAGIC  FROM MEASURES2
# MAGIC  ;
# MAGIC
# MAGIC SELECT INDICATOR,CURRENCY,COUNT( DISTINCT ORGCODEPROVIDER) value FROM $outSchema.measures_CSV where IndicatorFamily like 'BMI%' GROUP BY INDICATOR,CURRENCY 
# MAGIC  union all 
# MAGIC  SELECT INDICATOR,CURRENCY,SUM(VALUE) value FROM $outSchema.measures_CSV where IndicatorFamily like 'BMI%' AND CURRENCY IN ('Denominator','Numerator') GROUP BY INDICATOR,CURRENCY ORDER BY INDICATOR,currency ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW National_Denominator as
# MAGIC (
# MAGIC SELECT  IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,'National' AS OrgCodeProvider
# MAGIC         ,'All Submitters' AS OrgName
# MAGIC         ,'National' AS OrgLevel
# MAGIC         ,'Denominator' AS currency
# MAGIC          ,Sum(Unrounded_Denominator) as Unrounded_Denominator
# MAGIC         ,MaternityRounding(Sum(Unrounded_Denominator)) as value
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM $outSchema.Measures_Aggregated  
# MAGIC   WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ'
# MAGIC   group by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW National_Numerator as 
# MAGIC (
# MAGIC SELECT  IndicatorFamily
# MAGIC         ,Indicator
# MAGIC     ,'National' AS OrgCodeProvider
# MAGIC         ,'All Submitters' AS OrgName
# MAGIC         ,'National' AS OrgLevel
# MAGIC         ,'Numerator' AS currency
# MAGIC         ,Sum(Unrounded_Numerator) as Unrounded_Numerator
# MAGIC         ,MaternityRounding(Sum(Unrounded_Numerator)) as value
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM $outSchema.Measures_Aggregated  
# MAGIC   WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ'
# MAGIC   group by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC    )     ;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW National_Rate as
# MAGIC (
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC         ,'National' AS OrgCodeProvider
# MAGIC         ,'All Submitters' AS OrgName
# MAGIC         ,'National' AS OrgLevel
# MAGIC         ,'Rate' AS currency
# MAGIC          ,MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator)  as Unrounded_Rate
# MAGIC         ,MaternityRate(N.value,D.Value)  as value
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM National_Denominator D
# MAGIC   left outer join  National_Numerator N
# MAGIC   on D.IndicatorFamily=N.IndicatorFamily and D.Indicator=N.Indicator
# MAGIC   
# MAGIC )
# MAGIC      ;
# MAGIC
# MAGIC  INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC  SELECT  '$RPStartDate' AS RPStartDate
# MAGIC         ,'$RPEndDate' AS RPEndDate
# MAGIC         ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC         ,Denominator.Indicator AS Indicator
# MAGIC         ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC          ,Denominator.OrgName  AS OrgName
# MAGIC         ,'National' AS OrgLevel 
# MAGIC         ,Numerator.Unrounded_Numerator
# MAGIC         ,Denominator.Unrounded_Denominator AS UnroundedDenominator
# MAGIC         ,Rate.Unrounded_Rate AS UnroundedRate -- Default Rate
# MAGIC         ,Numerator.value   AS RoundedNumerator      -- Maternity Rounding
# MAGIC         ,Denominator.value  AS RoundedDenominator    -- Maternity Rounding
# MAGIC         ,Rate.value AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC         ,0 AS OverDQThreshold 
# MAGIC         ,current_timestamp() as CreatedAt 
# MAGIC    FROM National_Denominator Denominator
# MAGIC        LEFT OUTER JOIN National_Numerator Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator = Numerator.Indicator
# MAGIC       LEFT OUTER JOIN National_Rate Rate ON Denominator.OrgCodeProvider = Rate.OrgCodeProvider and Denominator.Indicator = Rate.Indicator
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Denominator' as currency
# MAGIC         ,Rounded_Denominator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgCodeProvider='National'
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Numerator' as currency
# MAGIC         ,Rounded_Numerator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgCodeProvider='National'
# MAGIC  
# MAGIC
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Rate' as currency
# MAGIC         ,Rounded_Rate as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgCodeProvider='National'
# MAGIC
# MAGIC         sort by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW NHSE_Denominator as
# MAGIC (
# MAGIC
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.RegionORG as Orgcodeprovider
# MAGIC          ,D.Region as orgname
# MAGIC         ,'Denominator' AS currency
# MAGIC          ,Sum(D.Unrounded_Denominator) as Unrounded_Denominator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Denominator)) as rounded_Denominator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC             ,g.RegionORG    
# MAGIC             ,g.Region
# MAGIC             ,M.Unrounded_Denominator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_ORG
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.RegionORG  ,D.Region
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW NHSE_Numerator as
# MAGIC (
# MAGIC
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.RegionORG as Orgcodeprovider ,D.Region as orgname
# MAGIC         ,'Numerator' AS currency
# MAGIC          ,sum(D.Unrounded_Numerator) as Unrounded_Numerator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Numerator)) as rounded_Numerator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC             ,g.RegionORG    
# MAGIC             ,g.Region
# MAGIC             ,M.Unrounded_Numerator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_ORG
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.RegionORG  ,D.Region
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW NHSE_Rate as
# MAGIC (
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.Orgcodeprovider ,D.orgname
# MAGIC         ,'Rate' AS currency
# MAGIC         ,MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator)  as Unrounded_Rate
# MAGIC         ,MaternityRate(N.rounded_Numerator,D.rounded_Denominator)  as Rounded_Rate
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM NHSE_Denominator D
# MAGIC   left outer join  NHSE_Numerator N
# MAGIC   on D.IndicatorFamily=N.IndicatorFamily and D.Indicator=N.Indicator and D.Orgcodeprovider=N.Orgcodeprovider
# MAGIC   
# MAGIC )
# MAGIC      ;
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC  SELECT  '$RPStartDate' AS RPStartDate
# MAGIC         ,'$RPEndDate' AS RPEndDate
# MAGIC         ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC         ,Denominator.Indicator AS Indicator
# MAGIC         ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC          ,Denominator.OrgName  AS OrgName
# MAGIC         ,'NHS England (Region)' AS OrgLevel 
# MAGIC         ,Numerator.Unrounded_Numerator
# MAGIC         ,Denominator.Unrounded_Denominator AS UnroundedDenominator
# MAGIC         ,Rate.Unrounded_Rate AS UnroundedRate -- Default Rate
# MAGIC         ,Numerator.rounded_Numerator   AS RoundedNumerator      -- Maternity Rounding
# MAGIC         ,Denominator.rounded_Denominator  AS RoundedDenominator    -- Maternity Rounding
# MAGIC         ,Rate.rounded_Rate AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC         ,0 AS OverDQThreshold 
# MAGIC         ,current_timestamp() as CreatedAt 
# MAGIC    FROM NHSE_Denominator Denominator
# MAGIC        LEFT OUTER JOIN NHSE_Numerator Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator = Numerator.Indicator
# MAGIC       LEFT OUTER JOIN NHSE_Rate Rate ON Denominator.OrgCodeProvider = Rate.OrgCodeProvider and Denominator.Indicator = Rate.Indicator
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Denominator' as currency
# MAGIC         ,Rounded_Denominator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='NHS England (Region)'
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Numerator' as currency
# MAGIC         ,Rounded_Numerator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='NHS England (Region)'
# MAGIC  
# MAGIC
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Rate' as currency
# MAGIC         ,Rounded_Rate as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='NHS England (Region)'
# MAGIC
# MAGIC         sort by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW STP_DENOMINATOR AS (SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.STP_Code as Orgcodeprovider
# MAGIC          ,D.STP_Name as orgname
# MAGIC         ,'Denominator' AS currency
# MAGIC          ,Sum(D.Unrounded_Denominator) as Unrounded_Denominator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Denominator)) as rounded_Denominator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC             ,g.STP_Name,g.STP_Code
# MAGIC             ,M.Unrounded_Denominator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_org
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.STP_Code  ,D.STP_Name
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW STP_Numerator as
# MAGIC (
# MAGIC
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator,D.STP_CODE AS ORGcodePROVIDER,D.STP_NAME AS ORGNAME
# MAGIC         ,'Numerator' AS currency
# MAGIC          ,sum(D.Unrounded_Numerator) as Unrounded_Numerator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Numerator)) as rounded_Numerator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC             ,g.STP_Code,g.STP_Name  
# MAGIC            
# MAGIC             ,M.Unrounded_Numerator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_ORG
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.STP_Code  ,D.STP_Name
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW STP_Rate as
# MAGIC (
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.Orgcodeprovider ,D.orgname
# MAGIC         ,'Rate' AS currency
# MAGIC         ,MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator)  as Unrounded_Rate
# MAGIC         ,MaternityRate(N.rounded_Numerator,D.rounded_Denominator)  as Rounded_Rate
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM STP_Denominator D
# MAGIC   left outer join  STP_Numerator N
# MAGIC   on D.IndicatorFamily=N.IndicatorFamily and D.Indicator=N.Indicator and D.Orgcodeprovider=N.Orgcodeprovider
# MAGIC   
# MAGIC )
# MAGIC      ;
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC  SELECT  '$RPStartDate' AS RPStartDate
# MAGIC         ,'$RPEndDate' AS RPEndDate
# MAGIC         ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC         ,Denominator.Indicator AS Indicator
# MAGIC         ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC          ,Denominator.OrgName  AS OrgName
# MAGIC         ,'Local Maternity System' AS OrgLevel 
# MAGIC         ,Numerator.Unrounded_Numerator
# MAGIC         ,Denominator.Unrounded_Denominator AS UnroundedDenominator
# MAGIC         ,Rate.Unrounded_Rate AS UnroundedRate -- Default Rate
# MAGIC         ,Numerator.rounded_Numerator   AS RoundedNumerator      -- Maternity Rounding
# MAGIC         ,Denominator.rounded_Denominator  AS RoundedDenominator    -- Maternity Rounding
# MAGIC         ,Rate.rounded_Rate AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC         ,0 AS OverDQThreshold 
# MAGIC         ,current_timestamp() as CreatedAt 
# MAGIC    FROM STP_Denominator Denominator
# MAGIC        LEFT OUTER JOIN STP_Numerator Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator = Numerator.Indicator
# MAGIC       LEFT OUTER JOIN STP_Rate Rate ON Denominator.OrgCodeProvider = Rate.OrgCodeProvider and Denominator.Indicator = Rate.Indicator
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Denominator' as currency
# MAGIC         ,Rounded_Denominator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='Local Maternity System'
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Numerator' as currency
# MAGIC         ,Rounded_Numerator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='Local Maternity System'
# MAGIC  
# MAGIC
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Rate' as currency
# MAGIC         ,Rounded_Rate as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='Local Maternity System'
# MAGIC
# MAGIC         sort by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW MBR_DENOMINATOR AS (SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.Mbrrace_Grouping_short as Orgcodeprovider
# MAGIC          ,D.Mbrrace_Grouping as orgname
# MAGIC         ,'Denominator' AS currency
# MAGIC          ,Sum(D.Unrounded_Denominator) as Unrounded_Denominator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Denominator)) as rounded_Denominator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC             ,g.Mbrrace_Grouping,g.Mbrrace_Grouping_short
# MAGIC             ,M.Unrounded_Denominator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_org
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.Mbrrace_Grouping_short,D.Mbrrace_Grouping
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW MBR_Numerator as
# MAGIC (
# MAGIC
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator  ,d.Mbrrace_Grouping_short as Orgcodeprovider
# MAGIC          ,D.Mbrrace_Grouping as orgname
# MAGIC         ,'Numerator' AS currency
# MAGIC          ,sum(D.Unrounded_Numerator) as Unrounded_Numerator
# MAGIC         ,MaternityRounding(Sum(D.Unrounded_Numerator)) as rounded_Numerator
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM( 
# MAGIC            SELECT  IndicatorFamily
# MAGIC             ,Indicator
# MAGIC            ,g.Mbrrace_Grouping,g.Mbrrace_Grouping_short
# MAGIC            
# MAGIC             ,M.Unrounded_Numerator
# MAGIC            FROM $outSchema.Measures_Aggregated  M
# MAGIC            LEFT OUTER JOIN $outSchema.geogtlrr g
# MAGIC             on M.orgcodeprovider= g.Trust_ORG
# MAGIC            WHERE IndicatorFamily='BMI_14+1Wks' and Indicator !='BMI_DQ' and OrgLevel='Provider'
# MAGIC       ) D
# MAGIC   group by d.IndicatorFamily,D.Indicator,D.Mbrrace_Grouping_short,D.Mbrrace_Grouping
# MAGIC );
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW MBR_Rate as
# MAGIC (
# MAGIC SELECT  D.IndicatorFamily
# MAGIC         ,D.Indicator
# MAGIC          ,d.Orgcodeprovider ,D.orgname
# MAGIC         ,'Rate' AS currency
# MAGIC         ,MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator)  as Unrounded_Rate
# MAGIC         ,MaternityRate(N.rounded_Numerator,D.rounded_Denominator)  as Rounded_Rate
# MAGIC          ,CURRENT_TIMESTAMP() AS CREATEDAT
# MAGIC   FROM MBR_Denominator D
# MAGIC   left outer join  MBR_Numerator N
# MAGIC   on D.IndicatorFamily=N.IndicatorFamily and D.Indicator=N.Indicator and D.Orgcodeprovider=N.Orgcodeprovider
# MAGIC   
# MAGIC )
# MAGIC      ;
# MAGIC
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC  SELECT  '$RPStartDate' AS RPStartDate
# MAGIC         ,'$RPEndDate' AS RPEndDate
# MAGIC         ,'BMI_14+1Wks' AS IndicatorFamily
# MAGIC         ,Denominator.Indicator AS Indicator
# MAGIC         ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC          ,Denominator.OrgName  AS OrgName
# MAGIC         ,'MBRRACE Grouping' AS OrgLevel 
# MAGIC         ,Numerator.Unrounded_Numerator
# MAGIC         ,Denominator.Unrounded_Denominator AS UnroundedDenominator
# MAGIC         ,Rate.Unrounded_Rate AS UnroundedRate -- Default Rate
# MAGIC         ,Numerator.rounded_Numerator   AS RoundedNumerator      -- Maternity Rounding
# MAGIC         ,Denominator.rounded_Denominator  AS RoundedDenominator    -- Maternity Rounding
# MAGIC         ,Rate.rounded_Rate AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC         ,0 AS OverDQThreshold 
# MAGIC         ,current_timestamp() as CreatedAt 
# MAGIC    FROM MBR_Denominator Denominator
# MAGIC        LEFT OUTER JOIN MBR_Numerator Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator =    Numerator.Indicator
# MAGIC       LEFT OUTER JOIN MBR_Rate Rate ON Denominator.OrgCodeProvider = Rate.OrgCodeProvider and Denominator.Indicator = Rate.Indicator
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Denominator' as currency
# MAGIC         ,Rounded_Denominator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='MBRRACE Grouping'
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Numerator' as currency
# MAGIC         ,Rounded_Numerator as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='MBRRACE Grouping'
# MAGIC  
# MAGIC
# MAGIC   union all 
# MAGIC SELECT  RPStartDate as ReportingPeriodStartDate 
# MAGIC         ,RPEndDate as ReportingPeriodEndDate 
# MAGIC         ,IndicatorFamily
# MAGIC         ,Indicator
# MAGIC         ,OrgCodeProvider
# MAGIC         ,OrgName
# MAGIC         ,OrgLevel
# MAGIC         ,'Rate' as currency
# MAGIC         ,Rounded_Rate as value
# MAGIC         ,current_timestamp() as CreatedAt  
# MAGIC   FROM $outSchema.Measures_Aggregated  where Indicator like 'BMI%' and OrgLevel='MBRRACE Grouping'
# MAGIC
# MAGIC         sort by IndicatorFamily
# MAGIC         ,Indicator
# MAGIC
# MAGIC
# MAGIC
# MAGIC