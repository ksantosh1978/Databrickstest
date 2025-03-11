# Databricks notebook source
# DBTITLE 1,Global Widgets 
# MAGIC %python
# MAGIC
# MAGIC dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
# MAGIC dbutils.widgets.text("RPStartDate", "2022-02-01", "RPStartDate")
# MAGIC dbutils.widgets.text("RPEndDate", "2022-02-28", "RPEndDate")
# MAGIC dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
# MAGIC dbutils.widgets.text("source", "mat_pre_clear", "source")
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
# MAGIC SELECT * from $outSchema.Measures_raw WHERE IsOverDQThreshold=1
# MAGIC
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
# MAGIC AND BMI_RANGES ='Body Mass Index less than 20 - Underweight"'

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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC         ,MaternityRounding(MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator))  as Rounded_Rate
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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC         ,MaternityRounding(MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator))  as Rounded_Rate
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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC            LEFT OUTER JOIN mat_analysis.geogtlrr g
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
# MAGIC         ,MaternityRounding(MaternityRate(N.Unrounded_Numerator,D.Unrounded_Denominator))  as Rounded_Rate
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
# MAGIC        LEFT OUTER JOIN MBR_Numerator Numerator ON Denominator.OrgCodeProvider = Numerator.OrgCodeProvider and Denominator.Indicator = Numerator.Indicator
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