# Databricks notebook source
# DBTITLE 1,Cleanup at start
# MAGIC %python
# MAGIC #dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Import Functions
# MAGIC %run ../MaternityFunctions

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %python
# MAGIC
# MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
# MAGIC #dbutils.widgets.text("RPStartDate", "2015-04-01", "RPStartDate")
# MAGIC #dbutils.widgets.text("RPEndDate", "2015-04-30", "RPEndDate")
# MAGIC #dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
# MAGIC #dbutils.widgets.text("PatientFilterStartDate", "2013-02-01", "PatientFilterStartDate")
# MAGIC #dbutils.widgets.text("source", "testdata_mat_analysis_mat_pre_clear", "source")
# MAGIC #dbutils.widgets.text("RunTime", "2021-01-25", "RunTime")
# MAGIC
# MAGIC RPBegindate = dbutils.widgets.get("RPStartDate")
# MAGIC print(RPBegindate)
# MAGIC assert(RPBegindate)
# MAGIC RPEnddate = dbutils.widgets.get("RPEndDate")
# MAGIC print(RPEnddate)
# MAGIC assert(RPEnddate)
# MAGIC dbSchema = dbutils.widgets.get("source")
# MAGIC print(dbSchema)
# MAGIC assert(dbSchema)
# MAGIC outSchema = dbutils.widgets.get("outSchema")
# MAGIC print(outSchema)
# MAGIC assert(outSchema)
# MAGIC dss_corporate = dbutils.widgets.get("dss_corporate")
# MAGIC print(dss_corporate)
# MAGIC assert(dss_corporate)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC AllMeasures = ["Aspirin_MeetCriteria", "Induction"]
# MAGIC #AllMeasures = ["Induction"]

# COMMAND ----------

# DBTITLE 1,For Aspirin LRegionORG should be re-introduced. See cmd below
Geographies = {
  "RegionORG" : {"Org_Level":"'NHS England (Region)'", "Org_Name_Column":"Region", "Provider_Name_Column" : "RegionOrg"}
  ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Org_Name_Column":"MBRRACE_Grouping", "Provider_Name_Column" : "Mbrrace_Grouping_Short"}
  ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Org_Name_Column":"STP_Name", "Provider_Name_Column" : "STP_Code"}  
}
print(Geographies)

# COMMAND ----------

# Geographies = {
#   "RegionORG" : {"Org_Level":"'NHS England (Region)'", "Org_Name_Column":"Region", "Provider_Name_Column" : "RegionOrg"}
#   ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Org_Name_Column":"MBRRACE_Grouping", "Provider_Name_Column" : "Mbrrace_Grouping_Short"}
#   ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Org_Name_Column":"STP_Name", "Provider_Name_Column" : "STP_Code"}  
#   ,"Trust_ORG" : {"Org_Level": "'Provider'", "Org_Name_Column":"Trust", "Provider_Name_Column" : "Trust_ORG"}
#   ,"LRegionORG" : {"Org_Level": "'Local Maternity System'", "Org_Name_Column":"Lregion", "Provider_Name_Column" : "LRegionORG"}
# }
# print(Geographies)

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator and geography - needs to be refactored to become generic (lines 12, 25 and 30)
# Aggregate for each indicator, each geography
for Indicator in AllMeasures:
  for key, value in Geographies.items():
    print(key, value)
    Geog_Org_Level = value["Org_Level"]
    Geog_Org_Name_Column = value["Org_Name_Column"]
    Geog_Provider_Name_Column = value["Provider_Name_Column"]
    sql = ("\
    INSERT INTO {outSchema}.Measures_Aggregated \
    SELECT  '{RPBegindate}' as RPStartDate \
           ,'{RPEnddate}' as RPEndDate \
           ,COALESCE(den.IndicatorFamily, 'Induction') as IndicatorFamily \
           ,den.Indicator \
           ,den.{Geog_Provider_Name} AS OrgCodeProvider \
           ,den.{Geog_Org_Name_Column} AS OrgName \
           ,{Geog_Org_Level} AS OrgLevel \
           ,COALESCE(num.Total,0) AS Unrounded_Numerator \
           ,COALESCE(den.Total,0) AS Unrounded_Denominator \
           ,MaternityRate(COALESCE(num.Total,0), den.Total, 0) AS Unrounded_Rate \
           ,MaternityRounding(COALESCE(num.Total,0)) AS Rounded_Numerator \
           ,MaternityRounding(COALESCE(den.Total,0)) AS Rounded_Denominator \
           ,MaternityRate(MaternityRounding(COALESCE(num.Total,0)), MaternityRounding(COALESCE(den.Total,0)), 0)  AS Rounded_Rate \
           ,0 AS IsOverDQThreshold \
           ,current_timestamp() as CreatedAt \
    FROM ( SELECT  {Geog_Org_Name_Column}, {Geog_Provider_Name}, count(distinct PregnancyID, Person_ID) as Total \
           FROM {outSchema}.Measures_Geographies \
           WHERE Rank = 1 AND Indicator = '{Indicator}' AND isNumerator = 1 AND isOverDQThreshold = 1 \
           GROUP BY RPStartDate, RPEndDate, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name} \
         ) AS num \
           RIGHT JOIN ( SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name}, count(distinct PregnancyID, Person_ID) as Total \
                        FROM {outSchema}.Measures_Geographies \
                        WHERE Rank = 1 AND Indicator = '{Indicator}' AND isDenominator = 1 AND isOverDQThreshold = 1 \
                        GROUP BY RPStartDate, RPEndDate, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name} \
                      ) as den ON num.{Geog_Org_Name_Column} = den.{Geog_Org_Name_Column}"
).format(outSchema=outSchema,Geog_Org_Level=Geog_Org_Level,Geog_Org_Name_Column=Geog_Org_Name_Column,Geog_Provider_Name=Geog_Provider_Name_Column,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
   #print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,National aggregation per indicator - needs to be refactored to become generic (lines 7, 20 and 25)
# use cross join to avoid 'detected implicit cartesian product' message
for Indicator in AllMeasures:
  sql = (" \
  INSERT INTO {outSchema}.Measures_Aggregated \
  SELECT  '{RPBegindate}' as RPStartDate \
         ,'{RPEnddate}' as RPEndDate \
         ,COALESCE(den.IndicatorFamily, 'Induction') AS IndicatorFamily \
         ,den.Indicator \
         ,'National' AS OrgCodeProvider \
         ,'All Submitters' AS OrgName \
         ,'National' AS OrgLevel \
         ,COALESCE(num.Total,0) AS Unrounded_Numerator \
         ,COALESCE(den.Total,0) AS Unrounded_Denominator \
         ,MaternityRate(COALESCE(num.Total,0), den.Total, 0) AS Unrounded_Rate \
         ,MaternityRounding(COALESCE(num.Total,0)) AS Rounded_Numerator \
         ,MaternityRounding(COALESCE(den.Total,0)) AS Rounded_Denominator \
         ,MaternityRate(MaternityRounding(COALESCE(num.Total,0)), MaternityRounding(COALESCE(den.Total,0)), 0) AS Rounded_Rate \
         ,1 AS IsOverDQThreshold \
         ,current_timestamp() AS CreatedAt \
    FROM ( SELECT count(distinct PregnancyID, Person_ID) AS Total \
           FROM {outSchema}.Measures_Raw \
           WHERE Rank = 1 AND IsOverDQThreshold = 1 AND Indicator = '{Indicator}' AND isNumerator = 1 \
           GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator  \
         ) AS num \
           CROSS JOIN ( SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, count(distinct PregnancyID, Person_ID) AS Total \
                        FROM {outSchema}.Measures_Raw \
                        WHERE Rank = 1 AND IsOverDQThreshold = 1 AND Indicator = '{Indicator}' AND isDenominator = 1\
                        GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator \
                      ) AS den \
").format(outSchema=outSchema, Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
  #print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Populate Measures_CSV - has been moved to the measures_final notebook
#   # 4 statements to populate CSV output table
#   for Indicator in AllMeasures:
#     csvStatement = ("INSERT INTO {outSchema}.Measures_CSV \
#       SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Denominator' AS Currency, \
#       COALESCE(REPLACE(CAST (ROUND(Rounded_Denominator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
#       FROM {outSchema}.Measures_Aggregated where Indicator = '{indicator}'").format(outSchema=outSchema,indicator=Indicator)
# #     print(csvStatement)
#     spark.sql(csvStatement)
#     csvStatement = ("INSERT INTO {outSchema}.Measures_CSV \
#       SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Numerator' AS Currency, \
#       COALESCE(REPLACE(CAST (ROUND(Rounded_Numerator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
#       FROM {outSchema}.Measures_Aggregated where Indicator = '{indicator}'").format(outSchema=outSchema,indicator=Indicator)
# #     print(csvStatement)
#     spark.sql(csvStatement)
#     csvStatement = ("INSERT INTO {outSchema}.Measures_CSV \
#       SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Rate' AS Currency, \
#       COALESCE(REPLACE(CAST (ROUND(Rounded_Rate, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
#       FROM {outSchema}.Measures_Aggregated where Indicator = '{indicator}'").format(outSchema=outSchema,indicator=Indicator)
# #     print(csvStatement)
#     spark.sql(csvStatement)
#     csvStatement = ("INSERT INTO {outSchema}.Measures_CSV \
#       SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Result' AS Currency, \
#       CASE WHEN (Rounded_Numerator = 0) OR (IsOverDQThreshold = 0) THEN 'Fail' ELSE 'Pass' END AS Value, current_timestamp() as CreatedAt \
#       FROM {outSchema}.Measures_Aggregated where Indicator = '{indicator}'").format(outSchema=outSchema,indicator=Indicator)
# #     print(csvStatement)
#     spark.sql(csvStatement)
  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM $outSchema.Measures_CSV

# COMMAND ----------

dbutils.notebook.exit("Notebook: Measures_Geographies ran successfully")