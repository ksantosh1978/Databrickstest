# Databricks notebook source
RPBegindate = dbutils.widgets.get("RPBegindate")
print(RPBegindate)
assert(RPBegindate)
RPEnddate = dbutils.widgets.get("RPEnddate")
print(RPEnddate)
assert(RPEnddate)
dbSchema = dbutils.widgets.get("dbSchema")
print(dbSchema)
assert(dbSchema)
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)
type(RPBegindate)

# COMMAND ----------

# insert into cqim_output_rates from cqim_measures_and_rates

# COMMAND ----------

# DBTITLE 1,Measures configuration
# TO DO Remove MinRate, MaxRate, Inclusive. These are now in view CQIM_Suppression_Rule. Remove once Somking work is complete.
# MeasureDNS: Not DNS when measure numerator > 0 or measure denominator > 0 or additional measure (MeasureDNS, if specified) numerator > 0
# 06/09/23 CQIMDQ19,35,40,41,42 have been removed from the pipeline
CQIMMeasures = {
  "CQIMDQ02" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''} 
  ,"CQIMDQ03" : {"MinRate" : 0.5, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ04" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ03'}
  ,"CQIMDQ05" : {"MinRate" : 0.005, "MaxRate" : 0.5, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ03'} 
  ,"CQIMDQ06" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ02'}
  ,"CQIMDQ07" : {"MinRate" : 0.005, "MaxRate" : 0.5, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ02'}
  ,"CQIMDQ08" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ09'}
  ,"CQIMDQ09" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ10" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ11" : {"MinRate" : None, "MaxRate" : 0.6, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ12" : {"MinRate" : None, "MaxRate" : 0.2, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ13" : {"MinRate" : 1, "MaxRate" : 1, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ14" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ15" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ16" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ18" : {"MinRate" : 0.4, "MaxRate" : None, "Inclusive" : 0, 'MeasureDNS' : 'CQIMDQ14'}
  #,"CQIMDQ19" : {"MinRate" : None, "MaxRate" : 0.5, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ20" : {"MinRate" : None, "MaxRate" : 0.1, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ21" : {"MinRate" : 1, "MaxRate" : 1, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ22" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ23" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : ''}
  ,"CQIMDQ24" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ25" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ09'}
  ,"CQIMDQ26" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}					
  ,"CQIMDQ27" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ28" : {"MinRate" : 0.2, "MaxRate" : 0.7, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ14'}
  ,"CQIMDQ29" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ09'}
  ,"CQIMDQ30" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ31" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ32" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ33" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ34" : {"MinRate" : 0.4, "MaxRate" : None, "Inclusive" : 0, 'MeasureDNS' : 'CQIMDQ30'}
  #,"CQIMDQ35" : {"MinRate" : 0.005, "MaxRate" : 0.4, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ36" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ37" : {"MinRate" : 0.2, "MaxRate" : 0.7, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ38" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  ,"CQIMDQ39" : {"MinRate" : 0.7, "MaxRate" : None, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  #,"CQIMDQ40" : {"MinRate" : None, "MaxRate" : 0.3, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  #,"CQIMDQ41" : {"MinRate" : 0.005, "MaxRate" : 0.6, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
  #,"CQIMDQ42" : {"MinRate" : 0.5, "MaxRate" : 0.95, "Inclusive" : 1, 'MeasureDNS' : 'CQIMDQ30'}
}

# COMMAND ----------

# DBTITLE 1,Rates configuration
# Measures: List of measures comprising the rate
# RateDNS: DNS is when rate numerator = 0 and rate denominator = 0 and RateDNS measure (if specified) numerator = 0
# RateFlag: 1 = output Rate, 0 = output Rate per Thousand
# ResultColumn use Result (unrounded) or Unrounded_Result column in cqim_output_rates
CQIMRates =  {
 'CQIMApgar': {'Measures' : ('CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ24'), 'RateDNS' : 'CQIMDQ14', 'RateFlag' : '0', 'ResultColumn' : 'Result'}
  ,'CQIMBreastfeeding': {'Measures' : ('CQIMDQ08','CQIMDQ09'), 'RateDNS' : 'CQIMDQ09', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMPPH': {'Measures' : ('CQIMDQ10','CQIMDQ11','CQIMDQ12','CQIMDQ13'), 'RateDNS' : '', 'RateFlag' : '0', 'ResultColumn' : 'Result'}
  ,'CQIMPreterm': {'Measures' : ('CQIMDQ09','CQIMDQ22','CQIMDQ23'), 'RateDNS' : 'CQIMDQ09', 'RateFlag' : '0', 'ResultColumn' : 'Result'}
  ,'CQIMSmokingBooking': {'Measures' : ('CQIMDQ03','CQIMDQ04','CQIMDQ05'), 'RateDNS' : 'CQIMDQ03', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMSmokingDelivery': {'Measures' : ('CQIMDQ02','CQIMDQ06','CQIMDQ07'), 'RateDNS' : 'CQIMDQ02', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMTears': {'Measures' : ('CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ18','CQIMDQ20','CQIMDQ21'), 'RateDNS' : 'CQIMDQ14', 'RateFlag' : '0', 'ResultColumn' : 'Result'}
  ,'CQIMBWOI': {'Measures' : ('CQIMDQ09','CQIMDQ15','CQIMDQ16','CQIMDQ29','CQIMDQ18','CQIMDQ25'), 'RateDNS' : 'CQIMDQ09', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMVBAC': {'Measures' : ('CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ26','CQIMDQ18','CQIMDQ27','CQIMDQ28'), 'RateDNS' : 'CQIMDQ14', 'RateFlag' : '1', 'ResultColumn' : 'Result'}
#   GBT: amended rows below while some DQ measures are under discussion with providers - originals further below
  ,'CQIMRobson01': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMRobson02': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  ,'CQIMRobson05': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'} 
#   GBT: original rows commented out below - DMS001-1155
#     ,'CQIMRobson01': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ40'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
#   ,'CQIMRobson02': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ41'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
#   ,'CQIMRobson05': {'Measures' : ('CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ42'), 'RateDNS' : 'CQIMDQ30', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
  #SL: 06/09/23 Removing CQIMDQ19, original construction of measures below
#    ,'CQIMTears': {'Measures' : ('CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ18','CQIMDQ19','CQIMDQ20','CQIMDQ21'), 'RateDNS' : 'CQIMDQ14', 'RateFlag' : '0', 'ResultColumn' : 'Result'}
#   ,'CQIMBWOI': {'Measures' : ('CQIMDQ09','CQIMDQ15','CQIMDQ16','CQIMDQ29','CQIMDQ18','CQIMDQ19','CQIMDQ25'), 'RateDNS' : 'CQIMDQ09', 'RateFlag' : '1', 'ResultColumn' : 'Rounded_Result'}
#   ,'CQIMVBAC': {'Measures' : ('CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ26','CQIMDQ18','CQIMDQ19','CQIMDQ27','CQIMDQ28'), 'RateDNS' : 'CQIMDQ14', 'RateFlag' : '1', 'ResultColumn' : 'Result'}
}

# COMMAND ----------

# DBTITLE 1,Geographies
# Org_Level - text inserted into cqim_output_rates.Org_Level
# Code_Column - column in view geogltrr containing organisation code to insert into cqim_output_rates.Org_Code
# Name_Column - column in view geogltrr containing organisation name to insert into cqim_output_rates.Org_Name
Geographies = {
      "RegionORG" : {"Org_Level":"'NHS England (Region)'", "Name_Column":"Region", "Code_Column" : "RegionOrg"}
      ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Name_Column":"MBRRACE_Grouping", "Code_Column" : "Mbrrace_Grouping_Short"}
      ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Name_Column":"STP_Name", "Code_Column" : "STP_Code"}
}
print(Geographies)

# COMMAND ----------

# DBTITLE 1,Add measures to cqim_output_rates
# For each measure row in cqim_measures_and_rates, insert a row into cqim_output_rates
# Inner join on CQIM_Suppression_Rule filters out rates
sql = ("INSERT INTO {outSchema}.cqim_output_rates \
select cmr.OrgCodeProvider, cmr.Org_Name, 'Provider' as Org_Level, cmr.RPStartDate, cmr.RPEndDate, cmr.ADJStartDate, cmr.ADJEndDate, cmr.IndicatorFamily, cmr.Indicator, \
case when cmr.Numerator > 0 then cmr.Numerator else 0 end as Unrounded_Numerator, \
case when cmr.Denominator > 0 then cmr.Denominator else 0 end as Unrounded_Denominator, \
case when cmr.Numerator > 0 then round((cmr.Numerator/cmr.Denominator)*100, 1) else 0 end as Unrounded_Rate, \
case when cmr.Numerator > 0 then round((cmr.Numerator/cmr.Denominator)*1000, 0) else 0 end as Unrounded_RateperThousand, \
'' as Result, \
case when cmr.Numerator is null then null when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 ELSE 0 end as Rounded_Numerator, \
case when cmr.Denominator is null then null when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 ELSE 0 end as Rounded_Denominator, \
case when cmr.Numerator is null or cmr.Denominator is null or cmr.Denominator = 0 then null \
else round((case when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 end / \
case when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 end) * 100, 1) end as Rounded_Rate, \
case when cmr.Numerator is null or cmr.Denominator is null or cmr.Denominator = 0 then null \
else round((case when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 end / \
case when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 end) * 1000, 0) end as Rounded_RateperThousand, \
'' as Rounded_Result \
from {outSchema}.cqim_measures_and_rates as cmr \
inner join {outSchema}.CQIM_Suppression_Rule as csr on cmr.Indicator=csr.Indicator \
where cmr.RPStartDate = '{RPBegindate}'").format(outSchema=outSchema, RPBegindate=RPBegindate)
print(sql)
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Update cqim_output_rates.Result with Pass/Fail - measure within minimum and maximum DQ thresholds
# MAGIC %sql
# MAGIC --Could include this in the insert statement but it's easier to maintain as an update
# MAGIC --Update statement available with DataBricks 7 runtime - https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-update.html
# MAGIC MERGE INTO $outSchema.cqim_output_rates as cor
# MAGIC USING
# MAGIC (SELECT cmr.Indicator
# MAGIC ,cmr.OrgCodeProvider
# MAGIC , RPStartDate
# MAGIC ,case when csr.Inclusive = 1 and ((((round((cmr.Unrounded_Numerator/coalesce(cmr.Unrounded_Denominator, 0)),3) >= csr.MinRate) OR csr.MinRate is null) and ((round((cmr.Unrounded_Numerator/coalesce(cmr.Unrounded_Denominator, 0)),3) <= csr.MaxRate) OR csr.MaxRate is null)) OR (cmr.Unrounded_Numerator > 0 and cmr.Unrounded_Denominator = 0 and csr.MaxRate is null) OR (cmr.Unrounded_Numerator > 0 and cmr.Unrounded_Denominator = 0 and csr.MaxRate is null))
# MAGIC then 'Pass'
# MAGIC when csr.Inclusive = 0 and ((((round((cmr.Unrounded_Numerator/coalesce(cmr.Unrounded_Denominator, 0)),3) > csr.MinRate) OR csr.MinRate is null) and ((round((cmr.Unrounded_Numerator/coalesce(cmr.Unrounded_Denominator, 0)),3) < csr.MaxRate) OR csr.MaxRate is null)) OR (cmr.Unrounded_Numerator > 0 and cmr.Unrounded_Denominator = 0))
# MAGIC then 'Pass'
# MAGIC else 'Fail'
# MAGIC end as Result
# MAGIC ,case when csr.Inclusive = 1 and ((((round((cmr.Rounded_Numerator/coalesce(cmr.Rounded_Denominator, 0)),3) >= csr.MinRate) OR csr.MinRate is null) and ((round((cmr.Rounded_Numerator/coalesce(cmr.Rounded_Denominator, 0)),3) <= csr.MaxRate) OR csr.MaxRate is null)) OR (cmr.Rounded_Numerator > 0 and coalesce(cmr.Rounded_Denominator, 0) = 0 and csr.MaxRate is null) OR (coalesce(cmr.Rounded_Numerator, 0) = 0 and coalesce(cmr.Rounded_Denominator, 0) > 0 and csr.MinRate is null))
# MAGIC then 'Pass'
# MAGIC when csr.Inclusive = 0 and ((((round((cmr.Rounded_Numerator/coalesce(cmr.Rounded_Denominator, 0)),3) > csr.MinRate) OR csr.MinRate is null) and ((round((cmr.Rounded_Numerator/coalesce(cmr.Rounded_Denominator, 0)),3) < csr.MaxRate) OR csr.MaxRate is null)) OR (cmr.Rounded_Numerator > 0 and coalesce(cmr.Rounded_Denominator, 0) = 0 and csr.MaxRate is null) OR (coalesce(cmr.Rounded_Numerator, 0) = 0 and coalesce(cmr.Rounded_Denominator, 0) > 0 and csr.MinRate is null))
# MAGIC then 'Pass'
# MAGIC else 'Fail'
# MAGIC end as Rounded_Result
# MAGIC from $outSchema.cqim_output_rates as cmr
# MAGIC inner join $outSchema.CQIM_Suppression_Rule as csr on cmr.Indicator=csr.Indicator
# MAGIC where RPStartDate = '$RPBegindate'
# MAGIC ) as res
# MAGIC ON cor.Indicator = res.Indicator and cor.OrgCodeProvider = res.OrgCodeProvider and  cor.RPStartDate = res.RPStartDate
# MAGIC WHEN MATCHED THEN UPDATE SET cor.Result = res.Result, cor.Rounded_Result = res.Rounded_Result
# MAGIC

# COMMAND ----------

# DBTITLE 1,Add rates per provider to cqim_output_rates
# For each rate
# For each row in cqim_measures_and_rates, insert row in cqim_output_rates
# cor.OrgCodeprovider is null if Rounded_Result for any of the measures comprising the CQIM rate is Fail
for rateKey, value in CQIMRates.items():
  inCondition=value["Measures"]
  sql = ("INSERT INTO {outSchema}.cqim_output_rates \
select cmr.OrgCodeProvider, cmr.Org_Name, 'Provider' as Org_Level, cmr.RPStartDate, cmr.RPEndDate, cmr.ADJStartDate, cmr.ADJEndDate, 'CQIM_Rate' as IndicatorFamily, '{rateKey}' as Indicator \
,case when cmr.Numerator > 0 then cmr.Numerator else 0 end as Unrounded_Numerator \
,case when cmr.Denominator > 0 then cmr.Denominator else 0 end as Unrounded_Denominator \
,case when cmr.Numerator > 0 and cmr.Denominator > 0 then round((cmr.Numerator/cmr.Denominator)*100, 1) else 0 end as Unrounded_Rate \
,case when cmr.Numerator > 0 and cmr.Denominator > 0 then round((cmr.Numerator/cmr.Denominator)*1000, 0) else 0 end as Unrounded_RateperThousand \
,case when cor.OrgCodeProvider is not null then 'Pass' else 'Fail' end as Result \
,case when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 else 0 end as Rounded_Numerator \
,case when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 else 0 end as Rounded_Denominator \
,case when cmr.Numerator is null or cmr.Denominator is null or cmr.Denominator = 0 then null \
else round((case when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 end / \
case when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 end) * 100, 1) end as Rounded_Rate \
,case when cmr.Numerator is null or cmr.Denominator is null or cmr.Denominator = 0 then null \
else round((case when cmr.Numerator between 1 and 7 then 5 when cmr.Numerator > 7 then round(cmr.Numerator/5,0)*5 end / \
case when cmr.Denominator between 1 and 7 then 5 when cmr.Denominator > 7 then round(cmr.Denominator/5,0)*5 end) * 1000, 0) end as Rounded_RateperThousand \
,case when cor.OrgCodeProvider is not null then 'Pass' else 'Fail' end as Rounded_Result \
from ( \
select OrgCodeProvider, Org_Name, RPStartDate, RPEndDate, ADJStartDate, ADJEndDate, Numerator, Numerator, Denominator from {outSchema}.cqim_measures_and_rates \
where RPStartDate = '{RPBegindate}' and Indicator='{rateKey}') as cmr \
left join (select OrgCodeProvider \
from {outSchema}.cqim_measures_and_rates as c \
where RPStartDate = '{RPBegindate}' and Indicator='{rateKey}' \
and not exists (select * from {outSchema}.cqim_output_rates as cr where Indicator in {inCondition} and Rounded_Result = 'Fail' and cr.OrgCodeProvider = c.OrgCodeProvider and RPStartDate = '{RPBegindate}') \
) as cor \
on cmr.OrgCodeProvider = cor.OrgCodeProvider").format(outSchema=outSchema, RPBegindate=RPBegindate, inCondition=inCondition, rateKey=rateKey)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Add rates National to cqim_output_rates
# For each rate
# cte is the CQIM rate rows from cqim_measures_and_rates, excluding those for which any comprising measure had Rounded_Result = Fail
# ctecqim groups the rate for providers and aggregates the rate for the providers (excluding those providers where a comprising measure had Rounded_Result = Fail)
for rateKey, value in CQIMRates.items():
  inCondition=value["Measures"]
  resultColumn=value["ResultColumn"] # either Result or Rounded_Result
  sql = ("with cte as ( \
select c.RPStartDate \
,c.RPEndDate \
,c.ADJStartDate \
,c.ADJEndDate \
,c.Numerator \
,c.Denominator \
,o.Result \
,round(c.Numerator/5,0)*5 as Rounded_Numerator \
,round(c.Denominator/5,0)*5 as Rounded_Denominator \
,o.Rounded_Result \
from {outSchema}.cqim_measures_and_rates as c \
left join {outSchema}.cqim_output_rates as o \
  on c.OrgCodeProvider = o.OrgCodeProvider and c.RPStartDate = o.RPStartDate and c.Indicator = o.Indicator \
where c.RPStartDate = '{RPBegindate}' \
and c.Indicator = '{rateKey}' \
and not exists (select * from {outSchema}.cqim_output_rates as cr where Indicator in {inCondition} and Rounded_Result = 'Fail' and cr.OrgCodeProvider = c.OrgCodeProvider and RPStartDate = '{RPBegindate}') \
) \
,ctecqim as ( \
select RPStartDate \
,RPEndDate \
,ADJStartDate \
,ADJEndDate \
,sum(case when Result = 'Pass' then Numerator end) as Unrounded_Numerator_Sum \
,sum(case when Result = 'Pass' then Denominator end) as Unrounded_Denominator_Sum \
,sum(case when Rounded_Result = 'Pass' then Rounded_Numerator end) as Rounded_Numerator_Sum \
,sum(case when Rounded_Result = 'Pass' then Rounded_Denominator end) as Rounded_Denominator_Sum \
from cte \
group by RPStartDate, RPEndDate, ADJStartDate, ADJEndDate \
) \
         INSERT INTO {outSchema}.cqim_output_rates \
select 'National' as OrgCodeProvider, 'All Submitters' as Org_Name, 'National' as Org_Level, RPStartDate, RPEndDate, ADJStartDate, ADJEndDate, 'CQIM_Rate' as IndicatorFamily, '{rateKey}' as Indicator \
,Unrounded_Numerator_Sum as Unrounded_Numerator\
,Unrounded_Denominator_Sum as Unrounded_Denominator \
,case when Unrounded_Numerator_Sum > 0 then round((Unrounded_Numerator_Sum/Unrounded_Denominator_Sum)*100, 1) else 0 end as Unrounded_Rate \
,case when Unrounded_Numerator_Sum > 0 then round((Unrounded_Numerator_Sum/Unrounded_Denominator_Sum)*1000, 0) else 0 end as Unrounded_RateperThousand \
,'Pass' as Result \
,case when Unrounded_Numerator_Sum is null then null ELSE Rounded_Numerator_Sum end as Rounded_Numerator \
,case when Unrounded_Denominator_Sum is null then null ELSE Rounded_Denominator_Sum end as Rounded_Denominator \
,case when Unrounded_Numerator_Sum is null or Unrounded_Denominator_Sum is null or Unrounded_Denominator_Sum = 0 then null \
else round(((Rounded_Numerator_Sum / Rounded_Denominator_Sum) * 100), 1) end as Rounded_Rate \
,case when Unrounded_Numerator_Sum is null or Unrounded_Denominator_Sum is null or Unrounded_Denominator_Sum = 0 then null \
else round(((Rounded_Numerator_Sum / Rounded_Denominator_Sum) * 1000), 0) end as Rounded_RateperThousand \
,'Pass' as Rounded_Result \
from ctecqim").format(outSchema=outSchema, RPBegindate=RPBegindate, inCondition=inCondition, rateKey=rateKey, resultColumn=resultColumn)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Add Rates by Geography to cqim_output_rates
# For each rate
# For each geography (see geogltrr)
# cte is the rate rows from cqim_measures_and_rates, excluding those for which any comprising measure had Rounded_Result = Fail
# ctecqim groups the rate for providers comprising the geography and aggregates the rate for the providers in the geography (excluding those providers where a comprising measure had Rounded_Result = Fail)
for rateKey, value in CQIMRates.items():
  inCondition=value["Measures"]
  resultColumn=value["ResultColumn"] # either Result or Rounded_Result
  for geogKey, geogValue in Geographies.items():
    Geog_Org_Level = geogValue["Org_Level"]
    Geog_Org_Name_Column = geogValue["Name_Column"]
    Geog_Org_Code_Column = geogValue["Code_Column"]
    sql = ("with cte as ( \
select c.RPStartDate \
,c.RPEndDate \
,c.ADJStartDate \
,c.ADJEndDate \
,c.Numerator \
,c.Denominator \
,o.Result \
,round(c.Numerator/5,0)*5 as Rounded_Numerator \
,round(c.Denominator/5,0)*5 as Rounded_Denominator \
,o.Rounded_Result \
,geo.{Geog_Org_Code_Column} as OrgCode \
,geo.{Geog_Org_Name_Column} as Org_Name \
from {outSchema}.cqim_measures_and_rates as c \
left join {outSchema}.cqim_output_rates as o \
  on c.OrgCodeProvider = o.OrgCodeProvider and c.RPStartDate = o.RPStartDate and c.Indicator = o.Indicator \
left join {outSchema}.geogtlrr geo on c.OrgCodeProvider = geo.Trust_ORG \
where c.RPStartDate = '{RPBegindate}' \
and c.Indicator = '{rateKey}' \
and not exists (select * from {outSchema}.cqim_output_rates as cr where Indicator in {inCondition} and Rounded_Result = 'Fail' and cr.OrgCodeProvider = c.OrgCodeProvider and RPStartDate = '{RPBegindate}') \
) \
,ctecqim as ( \
select RPStartDate \
,RPEndDate \
,ADJStartDate \
,ADJEndDate \
,OrgCode as OrgCodeProvider \
,Org_Name \
,sum(case when Result = 'Pass' then Numerator end) as Unrounded_Numerator_Sum \
,sum(case when Result = 'Pass' then Denominator end) as Unrounded_Denominator_Sum \
,sum(case when Rounded_Result = 'Pass' then Rounded_Numerator end) as Rounded_Numerator_Sum \
,sum(case when Rounded_Result = 'Pass' then Rounded_Denominator end) as Rounded_Denominator_Sum \
from cte \
group by RPStartDate, RPEndDate, ADJStartDate, ADJEndDate, OrgCodeProvider, Org_Name \
) \
         INSERT INTO {outSchema}.cqim_output_rates \
select OrgCodeProvider, Org_Name, {Geog_Org_Level} as Org_Level, RPStartDate, RPEndDate, ADJStartDate, ADJEndDate, 'CQIM_Rate' as IndicatorFamily, '{rateKey}' as Indicator \
,Unrounded_Numerator_Sum as Unrounded_Numerator\
,Unrounded_Denominator_Sum as Unrounded_Denominator \
,case when Unrounded_Numerator_Sum > 0 then round((Unrounded_Numerator_Sum/Unrounded_Denominator_Sum)*100, 1) else 0 end as Unrounded_Rate \
,case when Unrounded_Numerator_Sum > 0 then round((Unrounded_Numerator_Sum/Unrounded_Denominator_Sum)*1000, 0) else 0 end as Unrounded_RateperThousand \
,'Pass' as Result \
,case when Unrounded_Numerator_Sum is null then null ELSE Rounded_Numerator_Sum end as Rounded_Numerator \
,case when Unrounded_Denominator_Sum is null then null ELSE Rounded_Denominator_Sum end as Rounded_Denominator \
,case when Unrounded_Numerator_Sum is null or Unrounded_Denominator_Sum is null or Unrounded_Denominator_Sum = 0 then null \
else round(((Rounded_Numerator_Sum / Rounded_Denominator_Sum) * 100), 1) end as Rounded_Rate \
,case when Unrounded_Numerator_Sum is null or Unrounded_Denominator_Sum is null or Unrounded_Denominator_Sum = 0 then null \
else round(((Rounded_Numerator_Sum / Rounded_Denominator_Sum) * 1000), 0) end as Rounded_RateperThousand \
,'Pass' as Rounded_Result \
from ctecqim").format(outSchema=outSchema, RPBegindate=RPBegindate, inCondition=inCondition, rateKey=rateKey, Geog_Org_Code_Column=Geog_Org_Code_Column, Geog_Org_Name_Column=Geog_Org_Name_Column, Geog_Org_Level=Geog_Org_Level, resultColumn=resultColumn)
    print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Format measures into cqim_dq_csv
# For each measure
# cteMeasure - rows from cqim_output_rates for the current measure
# cteMeasureDNS if measureDNS is provided from CQIMMeasures, then add additional filter to where clause that determines IsDNS (DNS = did not submit)
# cteDNS - identify IsDNS to flag if provider submitted data for the measure
# 4 rows inserted per measure - Numerator, Denominator, Rate, Result
for measureKey, value in CQIMMeasures.items():
  measureDNS=value["MeasureDNS"]
  # if measureDNS given, then create then add additional filter to where clause
  # with a workaround for not being able to use exists statement
  if measureDNS != '':
    measureDNS = "select OrgCodeProvider from cteMeasure where Indicator = '" + measureDNS + "' and coalesce(numerator, 0) > 0"
  else:
    measureDNS = "select OrgCodeProvider from cteMeasure where 1 = 2"
  sql = ("with cteMeasure as ( \
    SELECT OrgCodeProvider, Org_Name, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator \
    ,coalesce(Rounded_Numerator, 0) as Numerator \
    ,coalesce(Rounded_Denominator, 0) as Denominator \
    ,coalesce(Rounded_Rate, 0) as Rate \
    ,Rounded_Result as Result \
    FROM {outSchema}.cqim_output_rates \
    WHERE Indicator = '{measureKey}' \
    AND RPStartDate = '{RPBegindate}' \
  ) \
  ,cteMeasureDNS as ( \
    {measureDNS} \
  ) \
  ,cteDNS as ( \
    SELECT cm.OrgCodeProvider \
    ,cm.Indicator \
    ,case when cm.numerator > 0 or cm.denominator > 0 or cmd.OrgCodeProvider is not null then 0 else 1 end as IsDNS \
    from cteMeasure as cm left join cteMeasureDNS as cmd on cm.OrgCodeProvider = cmd.OrgCodeProvider \
  ) \
  INSERT INTO {outSchema}.cqim_dq_csv \
  SELECT cm.OrgCodeProvider, cm.Org_Name, cm.Org_Level, cm.RPStartDate, cm.RPEndDate, cm.IndicatorFamily, cm.Indicator, 'Numerator' as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' \
  else replace(cast(round(coalesce(cm.Numerator, 0), 0) as string), '.0', '') end as Value \
  from cteMeasure as cm \
  inner join cteDNS as cdns on cm.OrgCodeProvider = cdns.OrgCodeProvider and cm.Indicator = cdns.Indicator \
  UNION ALL \
  SELECT cm.OrgCodeProvider, cm.Org_Name, cm.Org_Level, cm.RPStartDate, cm.RPEndDate, cm.IndicatorFamily, cm.Indicator, 'Denominator' as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' \
  else replace(cast(round(coalesce(cm.Denominator, 0), 0) as string), '.0', '') end as Value \
  from cteMeasure as cm \
  inner join cteDNS as cdns on cm.OrgCodeProvider = cdns.OrgCodeProvider and cm.Indicator = cdns.Indicator \
  UNION ALL \
  SELECT cm.OrgCodeProvider, cm.Org_Name, cm.Org_Level, cm.RPStartDate, cm.RPEndDate, cm.IndicatorFamily, cm.Indicator, 'Rate' as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' \
  else replace(cast(round(coalesce(cm.Rate, 0), 1) as string), '.0', '') end as Value \
  from cteMeasure as cm \
  inner join cteDNS as cdns on cm.OrgCodeProvider = cdns.OrgCodeProvider and cm.Indicator = cdns.Indicator \
  UNION ALL \
  SELECT OrgCodeProvider, Org_Name, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, 'Result' as Currency \
  ,Result as Value \
  from cteMeasure").format(outSchema=outSchema, RPBegindate=RPBegindate, measureDNS=measureDNS, measureKey=measureKey)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Format rates into cqim_dq_csv
# For each rate
# cteRate - rows from cqim_output_rates for the current rate. Note that some rates use Result (i.e. unrounded result) and others use Rounded_Result (Pass/Fail). Rate per thousand is Unrounded_RateperThousand at provider and national level and Rounded_RateperThousand for geographic aggregations (Region, MBBRACE, LMS). CQIMBWOI is not output unless Unrounded_Numerator > 0.
# cteMeasure - filter down to the measures (e.g. 'CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ24') comprising the rate (e.g. CQIMApgar) in the current iteration (contains one row per measure and provider/geography - i.e. 4 measure rows for CQIMApgar per OrgCodeProvider, see inCondition)
# cteDQ - used with cteMeasure to identify Result='Low DQ' i.e. any measure in the group for that provider/geography has Result='Fail' (contains 1 row per provider/geography)
# cterateDNS if rateDNS is provided from CQIMRates, then add additional filter to where clause that determines IsDNS (DNS = did not submit)
# cteDNS - identify IsDNS
# TO DO remove  - left join cteDQ as cdq on cr.OrgCodeProvider = cdq.OrgCodeProvider - not used in 3 of the statements in the union
# 4 rows inserted per rate - Numerator, Denominator, Rate (Rate per Thousand), Result
for rateKey, value in CQIMRates.items():
  inCondition=value["Measures"]
  rateDNS=value["RateDNS"]
  rateFlag=value["RateFlag"]
  resultColumn=value["ResultColumn"] # either Result or Rounded_Result
  # if rateDNS given, then create then add additional filter to where clause
  # with a workaround for not being able to use exists statement
  if rateDNS != '':
    rateDNS = "select OrgCodeProvider from cteMeasure where Indicator = '" + rateDNS + "' and coalesce(numerator, 0) > 0"
  else:
    rateDNS = "select OrgCodeProvider from cteMeasure where 1 = 2" # will create empty result set
  sql = ("with cteRate as ( \
    SELECT OrgCodeProvider, Org_Name, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator \
    ,coalesce(Rounded_Numerator, 0) as Numerator \
    ,coalesce(Rounded_Denominator, 0) as Denominator \
    ,coalesce(Rounded_Rate, 0) as Rate \
    ,case when Org_Level in ('Provider', 'National') then coalesce(Unrounded_RateperThousand, 0) else coalesce(Rounded_RateperThousand, 0) end as RateperThousand \
    ,{resultColumn} as Result \
    FROM {outSchema}.cqim_output_rates \
    WHERE Indicator = '{rateKey}' \
    AND RPStartDate = '{RPBegindate}' \
    AND NOT (Indicator = 'CQIMBWOI' and coalesce(Unrounded_Numerator, 0) = 0) \
  ) \
  ,cteMeasure as ( \
    SELECT OrgCodeProvider, Org_Name, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator \
    ,coalesce(Rounded_Numerator, 0) as Numerator \
    ,coalesce(Rounded_Denominator, 0) as Denominator \
    ,coalesce(Rounded_Rate, 0) as Rate \
    ,Rounded_Result as Result \
    FROM {outSchema}.cqim_output_rates \
    WHERE Indicator in {inCondition} \
    AND RPStartDate = '{RPBegindate}' \
  ) \
  ,cteDQ as ( \
    SELECT OrgCodeProvider \
    ,Result \
    from cteMeasure where Result='Fail' group by OrgCodeProvider, Result \
  ) \
  ,cteRateDNS as ( \
    {rateDNS} \
  ) \
  ,cteDNS as ( \
    SELECT cr.OrgCodeProvider \
    ,cr.Indicator \
    ,case when cr.Numerator > 0 or cr.Denominator > 0 or cmd.OrgCodeProvider is not null or cr.Org_Level <> 'Provider' then 0 else 1 end as IsDNS \
    from cteRate as cr left join cteRateDNS as cmd on cr.OrgCodeProvider = cmd.OrgCodeProvider \
  ) \
  INSERT INTO {outSchema}.cqim_dq_csv \
  SELECT cr.OrgCodeProvider, cr.Org_Name, cr.Org_Level, cr.RPStartDate, cr.RPEndDate, cr.IndicatorFamily, cr.Indicator, 'Numerator' as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' when cdq.Result = 'Fail' then cast('0' as string) \
  else replace(cast(round(coalesce(cr.Numerator, 0), 0) as string), '.0', '') end as Value \
  from cteRate as cr \
  inner join cteDNS as cdns on cr.OrgCodeProvider = cdns.OrgCodeProvider and cr.Indicator = cdns.Indicator \
  left join cteDQ as cdq on cr.OrgCodeProvider = cdq.OrgCodeProvider \
  UNION ALL \
  SELECT cr.OrgCodeProvider, cr.Org_Name, cr.Org_Level, cr.RPStartDate, cr.RPEndDate, cr.IndicatorFamily, cr.Indicator, 'Denominator' as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' when cdq.Result = 'Fail' then cast('0' as string) \
  else replace(cast(round(coalesce(cr.Denominator, 0), 0) as string), '.0', '') end as Value \
  from cteRate as cr \
  inner join cteDNS as cdns on cr.OrgCodeProvider = cdns.OrgCodeProvider and cr.Indicator = cdns.Indicator \
  left join cteDQ as cdq on cr.OrgCodeProvider = cdq.OrgCodeProvider \
  UNION ALL \
  SELECT cr.OrgCodeProvider, cr.Org_Name, cr.Org_Level, cr.RPStartDate, cr.RPEndDate, cr.IndicatorFamily, cr.Indicator, case when {rateFlag} = 1 then 'Rate' else 'Rate per Thousand' end as Currency \
  ,case when cdns.IsDNS = 1 then 'DNS' \
  when cdq.Result is not null then 'Low DQ' \
  when {rateFlag} = 1 then replace(cast(round(coalesce(cr.Rate, 0), 1) as string), '.0', '') \
  else \
    case when cr.Numerator > 7 then replace(cast(round(coalesce(cr.RateperThousand, 0), 1) as string), '.0', '') \
    when cr.Numerator = 0 then '0' \
    else '*' \
    end \
  end as Value \
  from cteRate as cr \
  inner join cteDNS as cdns on cr.OrgCodeProvider = cdns.OrgCodeProvider and cr.Indicator = cdns.Indicator \
  left join cteDQ as cdq on cr.OrgCodeProvider = cdq.OrgCodeProvider \
  UNION ALL \
  SELECT cr.OrgCodeProvider, cr.Org_Name, cr.Org_Level, cr.RPStartDate, cr.RPEndDate, cr.IndicatorFamily, cr.Indicator, 'Result' as Currency \
  ,coalesce(cr.Result, 'Pass') as Value \
  from cteRate as cr \
  left join cteDQ as cddq on cr.OrgCodeProvider = cddq.OrgCodeProvider \
  where cr.Org_Level = 'Provider' \
  ").format(outSchema=outSchema, RPBegindate=RPBegindate, inCondition=inCondition, rateKey=rateKey, rateDNS=rateDNS, rateFlag=rateFlag, resultColumn=resultColumn)
  print(sql)
  spark.sql(sql)