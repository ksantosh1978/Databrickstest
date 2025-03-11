# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./../MaternityFunctions

# COMMAND ----------

# DBTITLE 1,Global Widgets 
# MAGIC %python
# MAGIC
# MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
# MAGIC #dbutils.widgets.text("RPStartDate", "2015-04-01", "RPStartDate")
# MAGIC #dbutils.widgets.text("RPEndDate", "2015-04-30", "RPEndDate")
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
# MAGIC
# MAGIC params = {"Target database" : outSchema, "Source database" : source, "Reference database" : dss_corporate, "Start Date" : RPStartDate, "End Date" : RPEndDate}
# MAGIC print(params)

# COMMAND ----------

Aggregates = {
  "Induction" : {"IndicatorFamily":"Pregnancy", "Indicator":"Aspirin_MeetCriteria", "OverDQ":"5"}
}
print(Aggregates)

# COMMAND ----------

# DBTITLE 1,Populate aggregated provider 
# MAGIC %sql
# MAGIC WITH Numerator AS
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total
# MAGIC   FROM  $outSchema.Measures_Geographies
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsNumerator = 1
# MAGIC     AND Indicator = 'Aspirin_MeetCriteria'
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust
# MAGIC ), 
# MAGIC Denominator AS 
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total
# MAGIC   FROM $outSchema.Measures_Geographies
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsDenominator = 1   
# MAGIC     AND Indicator = 'Aspirin_MeetCriteria'
# MAGIC     AND RPStartDate = '$RPStartDate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust
# MAGIC ),
# MAGIC cteclosed AS 
# MAGIC (
# MAGIC   SELECT  ORG_CODE
# MAGIC          ,NAME 
# MAGIC   FROM $dss_corporate.ORG_DAILY
# MAGIC   WHERE BUSINESS_END_DATE IS NULL
# MAGIC   AND org_type_code='TR' 
# MAGIC )
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC SELECT  '$RPStartDate' AS RPStartDate
# MAGIC        ,'$RPEndDate' AS RPEndDate
# MAGIC        ,'Pregnancy' AS IndicatorFamily
# MAGIC        ,'Aspirin_MeetCriteria' AS Indicator
# MAGIC        ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC        ,COALESCE(Denominator.Trust, c.NAME) AS OrgName
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
# MAGIC       LEFT OUTER JOIN Numerator ON Numerator.OrgCodeProvider = Denominator.OrgCodeProvider
# MAGIC       LEFT OUTER JOIN cteclosed AS c on Denominator.OrgCodeProvider = c.ORG_CODE   

# COMMAND ----------

# DBTITLE 1,Determine if provider is over DQ threshold - from Measures_Aggregate
for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = (f"\
      UPDATE {outSchema}.Measures_Aggregated \
      SET IsOverDQThreshold =  CASE \
                                 WHEN Rounded_Rate > {OverDQ} \
                                   THEN 1 \
                                   ELSE 0 \
                               END \
      WHERE Indicator = '{Indicator}' \
      AND OrgLevel = 'Provider'"
    )
    print(sql)
    spark.sql(sql)           
;

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold on measures_raw based on org code provider in aggregate table
for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = (f"\
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
    )
    print(sql)
    spark.sql(sql)           
;

# COMMAND ----------

dbutils.notebook.exit("Notebook: Aspirin_Aggregate ran successfully")