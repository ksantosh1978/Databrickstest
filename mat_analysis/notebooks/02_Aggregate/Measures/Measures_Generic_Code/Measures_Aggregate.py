# Databricks notebook source
# DBTITLE 1,Cleanup at start
# MAGIC %python
# MAGIC #dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Import Functions
# MAGIC %run ../MaternityFunctions

# COMMAND ----------

# DBTITLE 1,Global Widgets 
# MAGIC %python
# MAGIC
# MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
# MAGIC #dbutils.widgets.text("RPStartDate", "2015-04-01", "RPStartDate")
# MAGIC #dbutils.widgets.text("RPEndDate", "2015-04-30", "RPEndDate")
# MAGIC #dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
# MAGIC #dbutils.widgets.text("PatientFilterStartDate", "2010-02-01", "PatientFilterStartDate")
# MAGIC #dbutils.widgets.text("source", "mat_pre_clear", "source")
# MAGIC #dbutils.widgets.text("RunTime", "2021-01-28", "RunTime")
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
# MAGIC PatientFilterStartDate = dbutils.widgets.get("PatientFilterStartDate")
# MAGIC assert PatientFilterStartDate
# MAGIC
# MAGIC source = dbutils.widgets.get("source")
# MAGIC assert source
# MAGIC
# MAGIC RunTime = dbutils.widgets.get("RunTime")
# MAGIC assert RunTime
# MAGIC  
# MAGIC params = {"Target database" : outSchema, "Source database" : source, "Reference database" : dss_corporate, "Start Date" : RPStartDate, "End Date" : RPEndDate, "PatientFilterStartDate": PatientFilterStartDate, "RunTime" : RunTime}
# MAGIC print(params)

# COMMAND ----------

# DBTITLE 1,Set Ethnicity and IMD on raw data - Generic
# MAGIC %sql
# MAGIC WITH denominator AS 
# MAGIC (
# MAGIC   -- Mothers of interest
# MAGIC   -- Assumes numerator is a subset of denominator
# MAGIC   SELECT  raw.Person_ID
# MAGIC          ,raw.OrgCodeProvider 
# MAGIC   FROM $outSchema.Measures_raw raw
# MAGIC   GROUP BY  raw.Person_ID
# MAGIC            ,raw.OrgCodeProvider
# MAGIC )
# MAGIC ,person AS 
# MAGIC (
# MAGIC   SELECT Person_ID 
# MAGIC   FROM denominator 
# MAGIC   GROUP BY Person_ID
# MAGIC ) 
# MAGIC ,demographicsAll AS 
# MAGIC (
# MAGIC   SELECT  001md.Person_ID_Mother
# MAGIC          ,COALESCE(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) AS OrgCodeProvider
# MAGIC          ,001md.EthnicCategoryMother
# MAGIC          ,CASE WHEN 001md.EthnicCategoryMother IN ('D', 'E', 'F', 'H', 'J', 'K', 'M', 'L', 'N', 'P') THEN 'BAME'
# MAGIC                WHEN 001md.EthnicCategoryMother IN ('A', 'B', 'C') THEN 'White'
# MAGIC                WHEN 001md.EthnicCategoryMother IN ('G', 'R', 'S') THEN 'Other'
# MAGIC                ELSE 'Missing_Unknown'
# MAGIC           END AS EthnicGroup
# MAGIC          ,eid.DECI_IMD
# MAGIC          ,row_number() OVER (
# MAGIC                               PARTITION BY 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) 
# MAGIC                               ORDER BY 001md.RecordNumber DESC
# MAGIC                             ) AS rank
# MAGIC   FROM $source.msd001motherdemog AS 001md
# MAGIC         INNER JOIN person AS p on 001md.Person_ID_Mother = p.Person_ID -- filter to mothers identified in PCSP_Numerator_Raw
# MAGIC         LEFT JOIN $dss_corporate.org_relationship_daily AS ord ON 001md.OrgCodeProvider = ord.REL_TO_ORG_CODE AND 
# MAGIC                                                                   ord.REL_IS_CURRENT = 1 AND 
# MAGIC                                                                   ord.REL_TYPE_CODE = 'P' AND 
# MAGIC                                                                   ord.REL_FROM_ORG_TYPE_CODE = 'TR' AND 
# MAGIC                                                                   ord.REL_OPEN_DATE <= '$RPEndDate'  AND 
# MAGIC                                                                   (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEndDate')
# MAGIC         LEFT JOIN $dss_corporate.english_indices_of_dep_v02 AS eid ON 001md.LSOAMother2011 = eid.LSOA_CODE_2011 AND 
# MAGIC                                                                       eid.IMD_YEAR = 2019
# MAGIC   WHERE RPStartDate <= '$RPEndDate' 
# MAGIC )
# MAGIC ,demographics AS 
# MAGIC (
# MAGIC   SELECT  da.Person_ID_Mother
# MAGIC          ,da.OrgCodeProvider
# MAGIC          ,COALESCE(i.Description, 'Missing_Unknown') AS Rank_IMD_Decile
# MAGIC          ,da.EthnicCategoryMother, da.EthnicGroup 
# MAGIC   FROM demographicsAll AS da
# MAGIC         LEFT JOIN $outSchema.IMD AS i ON da.DECI_IMD = i.Rank_IMD_Decile
# MAGIC   WHERE da.rank = 1
# MAGIC )
# MAGIC MERGE 
# MAGIC INTO $outSchema.Measures_raw AS raw
# MAGIC USING (
# MAGIC         SELECT  Person_ID_Mother
# MAGIC                ,OrgCodeProvider
# MAGIC                ,Rank_IMD_Decile
# MAGIC                ,EthnicCategoryMother
# MAGIC                ,EthnicGroup 
# MAGIC         FROM demographics
# MAGIC ) AS demo
# MAGIC ON raw.Person_ID = demo.Person_ID_Mother AND 
# MAGIC    raw.OrgCodeProvider = demo.OrgCodeProvider
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET  raw.Rank_IMD_Decile = demo.Rank_IMD_Decile
# MAGIC                   ,raw.EthnicCategory = demo.EthnicCategoryMother
# MAGIC                   ,raw.EthnicGroup = demo.EthnicGroup;
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC Aggregates = {
# MAGIC   "Aspirin" : {"IndicatorFamily":"Pregnancy", "Indicator":"Aspirin_MeetCriteria", "OverDQ":"5"}
# MAGIC }
# MAGIC print(Aggregates)

# COMMAND ----------

# DBTITLE 1,Populate aggregated provider 

  for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = ("\
      WITH Numerator AS \
      ( \
        SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total \
        FROM  {outSchema}.Measures_Geographies \
        WHERE Rank = 1 \
          AND IsNumerator = 1 \
          AND Indicator = '{Indicator}' \
          AND RPStartDate = '{RPStartDate}' \
        GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust \
      ), \
      Denominator AS \
      ( \
        SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total \
        FROM {outSchema}.Measures_Geographies \
        WHERE Rank = 1 \
          AND IsDenominator = 1 \
          AND Indicator = '{Indicator}' \
          AND RPStartDate = '{RPStartDate}' \
        GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust \
      ), \
      cteclosed AS \
      ( \
        SELECT  ORG_CODE \
               ,NAME \
        FROM {dss_corporate}.ORG_DAILY \
        WHERE BUSINESS_END_DATE IS NULL \
          AND org_type_code='TR' \
      ) \
      INSERT INTO {outSchema}.Measures_Aggregated \
      SELECT  '{RPStartDate}' AS RPStartDate \
             ,'{RPEndDate}' AS RPEndDate \
             ,'{IndicatorFamily}' AS IndicatorFamily \
             ,'{Indicator}' AS Indicator \
             ,Denominator.OrgCodeProvider AS OrgCodeProvider \
             ,COALESCE(Denominator.Trust, c.NAME) AS OrgName \
             ,'Provider' AS OrgLevel \
             ,COALESCE(numerator.Total, 0) AS UnroundedNumerator \
             ,Denominator.Total AS UnroundedDenominator \
             ,MaternityRate(COALESCE(Numerator.Total,0), Denominator.Total, NULL) AS UnroundedRate \
             ,MaternityRounding(COALESCE(Numerator.Total,0))   AS RoundedNumerator \
             ,MaternityRounding(Denominator.Total)  AS RoundedDenominator \
             ,MaternityRate(MaternityRounding(COALESCE(Numerator.Total, 0)), MaternityRounding(Denominator.Total), 0) AS RoundedRate \
             ,0 AS OverDQThreshold \
             ,current_timestamp() as CreatedAt \
      FROM Denominator \
            LEFT OUTER JOIN Numerator ON Numerator.OrgCodeProvider = Denominator.OrgCodeProvider \
            LEFT OUTER JOIN cteclosed AS c on Denominator.OrgCodeProvider = c.ORG_CODE"
    ).format(outSchema=outSchema,IndicatorFamily=IndicatorFamily,Indicator=Indicator,RPStartDate=RPStartDate,RPEndDate=RPEndDate,dss_corporate=dss_corporate)
    #print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Determine if provider is over DQ threshold

for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = ("\
      UPDATE {outSchema}.Measures_Aggregated \
      SET IsOverDQThreshold =  CASE \
                                 WHEN Rounded_Rate >= {OverDQ} \
                                   THEN 1 \
                                   ELSE 0 \
                               END \
      WHERE Indicator = '{Indicator}' \
      AND OrgLevel = 'Provider'"
    ).format(outSchema=outSchema, Indicator=Indicator, OverDQ=OverDQ)
    #print(sql)
    spark.sql(sql)           
;

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold on measures_raw based on org code provider in aggregate table
for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = ("\
      WITH ProviderOverDQThreshold \
      AS \
      ( \
        SELECT OrgCodeProvider \
        FROM {outSchema}.Measures_Aggregated \
        WHERE Indicator = '{Indicator}' \
          AND RPStartDate = '{RPStartDate}' \
          AND IsOverDQThreshold = 1 \
        GROUP BY OrgCodeProvider \
      ) \
      UPDATE {outSchema}.Measures_raw raw \
      SET IsOverDQThreshold = 1 \
      WHERE EXISTS ( SELECT 1 \
                     FROM ProviderOverDQThreshold overDQ \
                     WHERE raw.OrgCodeProvider = overDQ.OrgCodeProvider \
                   ) \
        AND raw.Indicator = '{Indicator}' \
        AND raw.RPStartDate = '{RPStartDate}'"
    ).format(outSchema=outSchema, Indicator=Indicator, OverDQ=OverDQ, RPStartDate=RPStartDate)
    #print(sql)
    spark.sql(sql)           
;