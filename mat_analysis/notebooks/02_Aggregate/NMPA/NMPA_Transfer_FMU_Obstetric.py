# Databricks notebook source
# DBTITLE 1,Transfer_FMU_Obstetric metric - Denominator
# MAGIC %sql
# MAGIC /* Mbrrace cases only & make a distinct count of Person_ID_Mother where SettingIntraCare is 04 (Home, NHS) or 05 (Home, private) */
# MAGIC
# MAGIC INSERT INTO $outSchema.NMPA_Transfer_FMU_Obstetric_Denominator
# MAGIC -- Mbrrace
# MAGIC SELECT Mbrrace_Grouping_Short AS Org_Code
# MAGIC   , Mbrrace_Grouping AS Org_Name
# MAGIC   , 'MBRRACE Grouping' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Denominator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03'
# MAGIC GROUP BY Mbrrace_Grouping_Short, Mbrrace_Grouping
# MAGIC
# MAGIC UNION 
# MAGIC -- Provider
# MAGIC SELECT Trust_Org AS Org_Code
# MAGIC   , Trust AS Org_Name
# MAGIC   , 'Provider' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Denominator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03'
# MAGIC GROUP BY Trust_Org, Trust
# MAGIC
# MAGIC UNION
# MAGIC -- Region
# MAGIC SELECT
# MAGIC   RegionOrg AS Org_Code,
# MAGIC   Region AS Org_Name,
# MAGIC   'NHS England (Region)' AS Org_Level,
# MAGIC   '$RPStartdate' AS ReportingPeriodStartDate,
# MAGIC   '$RPEnddate' AS ReportingPeriodEndDate,
# MAGIC   'Birth' AS IndicatorFamily,
# MAGIC   'Transfer_FMU_Obstetric' Indicator,
# MAGIC   'Denominator' Currency,
# MAGIC   COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC FROM  $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03'
# MAGIC GROUP BY RegionOrg, Region
# MAGIC
# MAGIC UNION
# MAGIC -- Local Maternity System
# MAGIC SELECT STP_Code AS Org_Code
# MAGIC   , STP_Name AS Org_Name
# MAGIC   , 'Local Maternity System' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Denominator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03'
# MAGIC GROUP BY STP_Code, STP_Name
# MAGIC
# MAGIC UNION
# MAGIC -- National
# MAGIC SELECT 'National' AS Org_Code
# MAGIC   , 'All Submitters' AS Org_Name
# MAGIC   , 'National' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Denominator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transfer_FMU_Obstetric metric - Numerator
# MAGIC %sql
# MAGIC /* Mbrrace cases only & make a distinct count of Person_ID_Mother where SettingIntraCare is 04 (Home, NHS) or 05 (Home, private) */
# MAGIC
# MAGIC INSERT INTO $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator
# MAGIC
# MAGIC --Mbrrace
# MAGIC SELECT Mbrrace_Grouping_Short AS Org_Code
# MAGIC   , Mbrrace_Grouping AS Org_Name
# MAGIC   , 'MBRRACE Grouping' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Numerator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' AND SettingPlaceBirth = '01'
# MAGIC GROUP BY Mbrrace_Grouping_Short, Mbrrace_Grouping
# MAGIC
# MAGIC UNION
# MAGIC -- Provider
# MAGIC SELECT Trust_Org AS Org_Code
# MAGIC   , Trust AS Org_Name
# MAGIC   , 'Provider' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Numerator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' AND SettingPlaceBirth = '01'
# MAGIC GROUP BY Trust_Org, Trust
# MAGIC
# MAGIC UNION
# MAGIC -- Region
# MAGIC SELECT
# MAGIC   RegionOrg AS Org_Code,
# MAGIC   Region AS Org_Name,
# MAGIC   'NHS England (Region)' AS Org_Level,
# MAGIC   '$RPStartdate' AS ReportingPeriodStartDate,
# MAGIC   '$RPEnddate' AS ReportingPeriodEndDate,
# MAGIC   'Birth' AS IndicatorFamily,
# MAGIC   'Transfer_FMU_Obstetric' Indicator,
# MAGIC   'Numerator' Currency,
# MAGIC   COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC FROM  $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' AND SettingPlaceBirth = '01'
# MAGIC GROUP BY RegionOrg, Region
# MAGIC
# MAGIC UNION
# MAGIC -- Local Maternity System
# MAGIC SELECT STP_Code AS Org_Code
# MAGIC   , STP_Name AS Org_Name
# MAGIC   , 'Local Maternity System' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Numerator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' AND SettingPlaceBirth = '01'
# MAGIC GROUP BY STP_Code, STP_Name
# MAGIC
# MAGIC UNION
# MAGIC -- National
# MAGIC SELECT 'National' AS Org_Code
# MAGIC   , 'All Submitters' AS Org_Name
# MAGIC   , 'National' AS Org_Level
# MAGIC   , '$RPStartdate' AS ReportingPeriodStartDate
# MAGIC   , '$RPEnddate' AS ReportingPeriodEndDate
# MAGIC   , 'Birth' AS IndicatorFamily
# MAGIC   , 'Transfer_FMU_Obstetric' Indicator
# MAGIC   , 'Numerator' Currency
# MAGIC   , COUNT(DISTINCT Person_ID_Mother) AS Value
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC WHERE SettingIntraCare = '03' AND SettingPlaceBirth = '01'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Handle cases where values are absent in either Numerator but present in Denominator
# MAGIC %sql
# MAGIC
# MAGIC /* This section handles the cases where a row exists in Denominator table but not present in Numerator table. However, the reverse case will never happen because Numerator query is exactly same except an additional filter criteria and hence there should be no case where a row exist in Numerator but not present in Denominator table. */
# MAGIC
# MAGIC INSERT INTO $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator
# MAGIC
# MAGIC SELECT x.Org_Code
# MAGIC   , x.Org_Name
# MAGIC   , x.Org_Level
# MAGIC   , x.ReportingPeriodStartDate
# MAGIC   , x.ReportingPeriodEndDate
# MAGIC   , x.IndicatorFamily
# MAGIC   , x.Indicator
# MAGIC   , 'Numerator' Currency
# MAGIC   , 0 AS Value
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT Org_Code, Org_Name, Org_Level, ReportingPeriodStartDate, ReportingPeriodEndDate, IndicatorFamily, Indicator
# MAGIC   FROM $outSchema.NMPA_Transfer_FMU_Obstetric_Denominator
# MAGIC   EXCEPT
# MAGIC   SELECT Org_Code, Org_Name, Org_Level, ReportingPeriodStartDate, ReportingPeriodEndDate, IndicatorFamily, Indicator
# MAGIC   FROM $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator
# MAGIC ) x
# MAGIC

# COMMAND ----------

# DBTITLE 1,Apply suppression
# MAGIC %sql
# MAGIC create or replace global temp view csvMbrrace as
# MAGIC
# MAGIC with den as (
# MAGIC  
# MAGIC  
# MAGIC select 
# MAGIC   d.Org_Code
# MAGIC   ,d.Org_Name
# MAGIC   ,d.Org_Level
# MAGIC   ,d.ReportingPeriodStartDate
# MAGIC   ,d.ReportingPeriodEndDate
# MAGIC   ,d.Indicator
# MAGIC   ,d.IndicatorFamily
# MAGIC   ,d.Currency
# MAGIC   ,Case 
# MAGIC     when
# MAGIC       d.Value = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       d.Value >= 1 and d.Value <=7
# MAGIC     then 
# MAGIC       5
# MAGIC     when 
# MAGIC       d.Value > 7 
# MAGIC     then
# MAGIC       round(d.Value/5,0)*5
# MAGIC end as 
# MAGIC   Value
# MAGIC   
# MAGIC from 
# MAGIC   $outSchema.NMPA_Transfer_FMU_Obstetric_Denominator d),
# MAGIC  
# MAGIC num as (
# MAGIC  
# MAGIC select 
# MAGIC   n.Org_Code
# MAGIC   ,n.Org_Name
# MAGIC   ,n.Org_Level
# MAGIC   ,n.ReportingPeriodStartDate
# MAGIC   ,n.ReportingPeriodEndDate
# MAGIC   ,n.Indicator
# MAGIC   ,n.IndicatorFamily
# MAGIC   ,n.Currency
# MAGIC   ,Case 
# MAGIC     when
# MAGIC       n.Value = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       n.Value >= 1 and n.Value <=7
# MAGIC     then 
# MAGIC       5
# MAGIC     when 
# MAGIC       n.Value > 7 
# MAGIC     then
# MAGIC       round(n.Value/5,0)*5
# MAGIC end as 
# MAGIC   Value
# MAGIC from 
# MAGIC   $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator n)
# MAGIC   
# MAGIC select 
# MAGIC   den.Org_Code
# MAGIC   ,den.Org_Name
# MAGIC   ,den.Org_Level
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,CASE WHEN (num.Org_Code is null) THEN 0
# MAGIC         WHEN (CAST(num.Value AS FLOAT) = 0) AND (CAST(Den.Value AS FLOAT) = 0) THEN 0
# MAGIC         WHEN (CAST(num.Value AS FLOAT) > 0) AND (CAST(Den.Value AS FLOAT) = 0) THEN 0
# MAGIC         WHEN (CAST(num.Value AS FLOAT) = 0) AND (CAST(Den.Value AS FLOAT) > 0) THEN 0
# MAGIC         WHEN (CAST(num.Value AS FLOAT) > 0) AND (CAST(Den.Value AS FLOAT) > 0) THEN round( (CAST(num.Value AS FLOAT)/CAST(Den.Value AS FLOAT))*100 ,1)
# MAGIC    END AS Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.Org_Code = num.Org_Code
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC   
# MAGIC union all
# MAGIC  
# MAGIC select * from den
# MAGIC  
# MAGIC union all
# MAGIC  
# MAGIC select * from num

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $outSchema.NMPA_CSV
# MAGIC SELECT Org_Code, Org_Name, Org_Level, ReportingPeriodStartDate, ReportingPeriodEndDate, IndicatorFamily, Indicator, Currency, Value, current_timestamp() AS CreatedAt
# MAGIC FROM global_temp.csvMbrrace

# COMMAND ----------

dbutils.notebook.exit("Notebook: NMPA_Transfer_FMU_Obstetric ran successfully")