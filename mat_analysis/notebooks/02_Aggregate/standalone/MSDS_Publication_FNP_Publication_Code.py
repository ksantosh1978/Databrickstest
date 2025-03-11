# Databricks notebook source
# MAGIC %md # Family Nurse Partnership (FNP) measure
# MAGIC
# MAGIC Denominator:
# MAGIC  - Number of women in reporting perios who have reached 37 weeks gestation  
# MAGIC
# MAGIC Numerator:
# MAGIC  - Number of women in denominator who have an FNP indicator code in either MSD109 or MSD202
# MAGIC
# MAGIC
# MAGIC ###Template notes:
# MAGIC #####Final denominator dataframe should include OrgCodeProvider, (UniqPregID + Person_ID_Mother or Person_ID_Baby), (BookingSite or DeliverySite), RecordNumber and should have 1 row per (UniqPregID + Person_ID_Mother or Person_ID_Baby) and any breakdown (as used in data_csv) included as field = Breakdown     
# MAGIC #####Final numerator dataframe should include the (UniqPregID + Person_ID_Mother or Person_ID_Baby) identifiers as Num_XYZ and should include 1 row per Num_(UniqPregID + Person_ID_Mother or Person_ID_Baby). The numerator cohort does not need to contain organisational data (OrgCodeProvider etc.) which is already given in the denominator cohort.
# MAGIC  
# MAGIC Other cells of the notebook add geographical data, calculate the metric (counts & rates for measure; counts for data) and formats and outputs the suppressed data into the data table.     
# MAGIC
# MAGIC There is a known widgets issue in Databricks, where widgets may not refresh / update as expected when the Run All command is used. (And the fix requires a more recent Databricks version than the one currently - 2022/03 - in DAE). (May need to run the first cells separately, and then the rest of the notebook.) 
# MAGIC
# MAGIC For data metrics the cohort is assumed to be defined by the msd_denom only.

# COMMAND ----------

#Import necessary pyspark + other functions

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import *


# COMMAND ----------

#Remove widgets (RPStartDate)
#dbutils.widgets.removeAll();

# COMMAND ----------

#Set up widgets for measure
#Update for the name of the Indicator Family / Indicator / Output table (personal database) details

#startchoices = [str(r[0]) for r in spark.sql("select distinct RPStartDate from mat_pre_clear.msd000header order by RPStartDate desc").collect()]
#endchoices = [str(r[0]) for r in spark.sql("select distinct RPEndDate from mat_pre_clear.msd000header order by RPEndDate desc").collect()]

#dbutils.widgets.dropdown("RPStartDate", startchoices[0], startchoices)
#dbutils.widgets.dropdown("RPEndDate", endchoices[0], endchoices)
#dbutils.widgets.text("outschema","mat_clear_collab")
#dbutils.widgets.text("outtable","fnp_csv")
#dbutils.widgets.text("IndicatorFamily","Pregnancy")
#dbutils.widgets.text("Indicator","FNP_support")
#dbutils.widgets.dropdown("OutputType", "measure", ["measure", "data"])
#dbutils.widgets.dropdown("OutputCount", "Women", ["Women", "Babies"])
#dbutils.widgets.text("outschema_geog","olivia_dennis1_102274")
#dbutils.widgets.text("source","mat_pre_clear")


StartDate = dbutils.widgets.get("RPStartDate")
EndDate = dbutils.widgets.get("RPEnddate")
STPEndDate = '2022-06-30' #This is fixed as the date of STP/ICB change
IndicaFam = dbutils.widgets.get("IndicatorFamily")
Indicator = dbutils.widgets.get("Indicator")
OutputType = dbutils.widgets.get("OutputType")
OutputCount = dbutils.widgets.get("OutputCount")
dss_corporate = dbutils.widgets.get("dss_corporate")
outSchema = dbutils.widgets.get("outSchema")
outtable = dbutils.widgets.get("outtable")
dbSchema = dbutils.widgets.get("dbSchema")


# COMMAND ----------

# try:
#   StartDate
# except NameError:
#   None
# else:
#   del StartDate
  
# try:
#   EndDate
# except NameError:
#   None
# else:
#   del EndDate
  
# try:
#   IndicaFam
# except NameError:
#   None
# else:
#   del IndicaFam
  
# try:
#   Indicator
# except NameError:
#   None
# else:
#   del Indicator

# try:
#   OutputType
# except NameError:
#   None
# else:
#   del OutputType

# try:
#   OutputCount
# except NameError:
#   None
# else:
#   del OutputCount
  
# try:
#   outschema
# except NameError:
#   None
# else:
#   del outschema
  
# try:
#   outtable
# except NameError:
#   None
# else:
#   del outtable
  
# try:
#   geog_schema
# except NameError:
#   None
# else:
#   del geog_schema

# COMMAND ----------

#Collect widget values

#StartDate = dbutils.widgets.get("RPStartDate")
#EndDate = dbutils.widgets.get("RPEndDate")
#BorderDate = '2022-03-31' #This is fixed as the date that LMS changed organisation type 
#STPEndDate = '2022-06-30' #This is fixed as the date of STP/ICB change
#IndicaFam = dbutils.widgets.get("IndicatorFamily")
#Indicator = dbutils.widgets.get("Indicator")
#OutputType = dbutils.widgets.get("OutputType")
#OutputCount = dbutils.widgets.get("OutputCount")
#outschema = dbutils.widgets.get("outschema")
#outtable = dbutils.widgets.get("outtable")
#geog_schema = dbutils.widgets.get("outschema_geog")
#source = dbutils.widgets.get("source")

# COMMAND ----------

# MAGIC %md ### Get FNP-related codes

# COMMAND ----------

# list of fnp codes
list_fnp_codes = ['836171000000102', '9Nh3.', 'XaZIy']
fnp_code_search = ((F.col('FindingCode').isin(list_fnp_codes)) |
                    (F.col('ObsCode').isin(list_fnp_codes)))

# COMMAND ----------

# The tables below are needed in the FNP measure build

# Original from measure build
#msd001 = table("mat_pre_clear.msd001motherdemog")
#msd101 = table("mat_pre_clear.msd101pregnancybooking")
#msd109 = table("mat_pre_clear.msd109findingobsmother")
#msd201 = table("mat_pre_clear.msd201carecontactpreg")
#msd202 = table("mat_pre_clear.msd202careactivitypreg")

msd001 = table(dbSchema + ".msd001motherdemog")
msd002 = table(dbSchema + ".msd002GP")
msd101 = table(dbSchema + ".msd101pregnancybooking")
msd109 = table(dbSchema + ".msd109findingobsmother")
msd201 = table(dbSchema + ".msd201carecontactpreg")
msd202 = table(dbSchema + ".msd202careactivitypreg")


# COMMAND ----------

# Create spark dataframe for geogltrr and org_relationship_daily_spdf
# geogtlrr_spdf = spark.read.table(f'{geog_schema}.geogtlrr')
# org_relationship_daily_spdf = spark.read.table('dss_corporate.org_relationship_daily')

# Create dataframes for MSD tables of interest
msd101_spdf = msd101
msd109_spdf = msd109
msd202_spdf = msd202

# COMMAND ----------

msd101_month = msd101_spdf.where((F.col('RPStartDate') <= StartDate) & (F.col('GestAgeBooking') > 0))
msd109_month = msd109_spdf.where(F.col('RPStartDate').between(StartDate, EndDate))
msd202_month = msd202_spdf.where(F.col('RPStartDate').between(StartDate, EndDate))

# COMMAND ----------

# DBTITLE 1,Rank records
msd101_month = msd101_month.withColumn('Rank', F.row_number().over(Window.partitionBy(['UniqPregID', 'Person_ID_Mother']).orderBy(F.desc('AntenatalAppDate'), F.desc('RecordNumber'))))

# COMMAND ----------

wks_37 = (36 + 1) * 7

gestation_start_month_37_wks = (F.col('GestAgeBooking') + F.datediff(F.to_date(F.lit(StartDate)), F.col('AntenatalAppDate')) <= wks_37)
gestation_end_month_37_wks = (F.col('GestAgeBooking') + F.datediff(F.to_date(F.lit(EndDate)), F.col('AntenatalAppDate')) >= wks_37)
gestation_in_month_37_wks = gestation_start_month_37_wks & gestation_end_month_37_wks

woman_not_discharged = F.col('DischargeDateMatService').isNull()
discharge_after_37_wks = (F.col('GestAgeBooking') + F.datediff('DischargeDateMatService', 'AntenatalAppDate') > wks_37)

denominator_conditions = (gestation_in_month_37_wks) & (woman_not_discharged | discharge_after_37_wks)

# COMMAND ----------

# DBTITLE 1,Denominator cohort
#Code to identify the denominator in this cell
#Output - Unique row per pregnancy+mother or baby. Must include OrgCode Provider and either (OrgSiteIDBooking or OrgSiteIDActualDelivery)


#Final output dataframe for the denominator cohort must be set to have the name msd_denom
msd_denom = msd101_month.where((F.col('Rank') == 1) & (denominator_conditions))

# COMMAND ----------

fnp_msd202 = (
    msd_denom
    .join(other=msd202_month,
          on=['UniqPregID', 'Person_ID_Mother', 'RecordNumber'],
          how='left')
    .drop()
    .select(msd_denom['*'], F.when(fnp_code_search, 1).otherwise(0).alias('msd202_code_search'))
)

# COMMAND ----------

fnp_search_result = (
    fnp_msd202
    .join(other=msd109_month,
         on=['UniqPregID', 'Person_ID_Mother', 'RecordNumber'],
         how='left')
    .select(fnp_msd202['*'], F.when(fnp_code_search, 1).otherwise(0).alias('msd109_code_search'))
)

# COMMAND ----------

# DBTITLE 1,Numerator cohort
#Code to identify the numerator in this cell

#Final output dataframe for the numerator cohort must be set to have the name msd_num
fnp_search_result = fnp_search_result.select('*', F.when((F.col('msd202_code_search') == 1) | (F.col('msd109_code_search') == 1), 1).otherwise(0).alias('Numerator'))
msd_num = fnp_search_result.where((F.col('msd202_code_search') == 1) | (F.col('msd109_code_search') == 1)).withColumnRenamed("OrgCodeProvider","FNPProvider").withColumnRenamed("UniqPregID","Num_UniqPregID").withColumnRenamed("Person_ID_Mother","Num_Person_ID_Mother").withColumnRenamed("RecordNumber","FNPRecordNumber")

# COMMAND ----------

#If the numerator cohort has not had the counting units - Person_ID_Mother, UniqPregID, Person_ID_Baby - prefixed by Num_ (for later calculations) then those columns are renamed here.
try:
  msd_num
except NameError:
  None
else:
  msd_num = msd_num.withColumnRenamed("Person_ID_Mother","Num_Person_ID_Mother").withColumnRenamed("UniqPregID","Num_UniqPregID").withColumnRenamed("Person_ID_Baby","Num_Person_ID_Baby")

# COMMAND ----------

#Add organisational data to the denominator cohort

#Add additional breakdowns (SubICB / CCG) 
#msd002 = table("mat_pre_clear.msd002gp")
msd002 = msd002.alias("p").join(msd_denom, (msd_denom.Person_ID_Mother == msd002.Person_ID_Mother) & (msd_denom.RecordNumber == msd002.RecordNumber), "inner" ).select(["p.*"]).distinct()

msd002 = msd002.filter(msd002.RPEndDate == EndDate).filter((msd002.EndDateGMPReg.isNull() == 'true') | ((msd002.StartDateGMPReg.isNull() == 'true') & (msd002.EndDateGMPReg > EndDate) ) | ((msd002.StartDateGMPReg < EndDate) & (msd002.EndDateGMPReg > EndDate)))

msd002 = msd002.withColumn("Commissioner",F.when(msd002.RPEndDate > STPEndDate, msd002.OrgIDSubICBLocGP).otherwise(msd002.CCGResponsibilityMother))

msd002 = msd002.withColumn("Commissioner",F.when(msd002.Commissioner.isNull() == 'false', msd002.Commissioner).otherwise(lit("Unknown"))).select(msd002.Person_ID_Mother, msd002.RecordNumber,"Commissioner")


#Add additional breakdowns (Local Authority)
#msd001 = table("mat_pre_clear.msd001motherdemog")
msd001 = msd001.alias("q").join(msd_denom, (msd_denom.Person_ID_Mother == msd001.Person_ID_Mother) & (msd_denom.RecordNumber == msd001.RecordNumber), "inner" ).select(["q.*"]).distinct()

msd001 = msd001.withColumn("LocalAuthority",F.when(msd001.LAD_UAMother.isNull() == 'false', msd001.LAD_UAMother).otherwise(lit("Unknown"))).select(msd001.Person_ID_Mother, msd001.RecordNumber,"LocalAuthority")


#Add geographies
msd_denom = msd_denom.alias("r").join(msd001, (msd_denom.Person_ID_Mother == msd001.Person_ID_Mother) & (msd_denom.RecordNumber == msd001.RecordNumber), "left" ).join(msd002, (msd_denom.Person_ID_Mother == msd002.Person_ID_Mother) & (msd_denom.RecordNumber == msd002.RecordNumber), "left" ).select(["r.*", msd001.LocalAuthority, msd002.Commissioner]).distinct()

msd_denom = msd_denom.withColumn("LocalAuthority",F.when(msd_denom.LocalAuthority.isNull() == 'false', msd_denom.LocalAuthority).otherwise(lit("Unknown"))).withColumn("Commissioner",F.when(msd_denom.Commissioner.isNull() == 'false', msd_denom.Commissioner).otherwise(lit("Unknown")))


#Add columns where they do not exist
if 'BookingSite' not in msd_denom.columns:
    msd_denom = msd_denom.withColumn('BookingSite', lit(None))
if 'DeliverySite' not in msd_denom.columns:
    msd_denom = msd_denom.withColumn('DeliverySite', lit(None))
if 'Breakdown' not in msd_denom.columns:
    msd_denom = msd_denom.withColumn('Breakdown', lit(""))
  


#Add count unit column (either mother/pregnancy or baby)
if OutputCount == 'Women':
    msd_denom = msd_denom.withColumn('CountUnit_ID', concat(msd_denom.UniqPregID,lit('-'),msd_denom.Person_ID_Mother))
else:
    if OutputCount == 'Babies':
      msd_denom = msd_denom.withColumn('CountUnit_ID', msd_denom.Person_ID_Baby)

#Measure is either booking related (BookingSite exists) or delivery related (DeliverySite exists)
msd_denom = msd_denom.withColumn('MeasureType', F.when(msd_denom.BookingSite.isNull() == 'false',lit('1')).otherwise(lit('0')))


# COMMAND ----------

#Add organisational data to the numerator cohort

#Add count unit column (either mother/pregnancy or baby)
if (OutputCount == 'Women') & (OutputType == 'measure'):
    msd_num = msd_num.withColumn('CountUnit_ID_Num', concat(msd_num.Num_UniqPregID,lit('-'),msd_num.Num_Person_ID_Mother))
else:
    if (OutputCount == 'Babies') & (OutputType == 'measure'):
      msd_num = msd_num.withColumn('CountUnit_ID_Num', msd_num.Num_Person_ID_Baby)

# COMMAND ----------

#Create look up between predecessor / successor org -  provider trusts

#org_rel_daily = table("dss_corporate.org_relationship_daily")
org_rel_daily = table(dss_corporate + ".org_relationship_daily")
org_rel_daily = org_rel_daily.filter(org_rel_daily.REL_IS_CURRENT == '1').filter(org_rel_daily.REL_TYPE_CODE == 'P').filter(org_rel_daily.REL_FROM_ORG_TYPE_CODE == 'TR').filter(org_rel_daily.REL_OPEN_DATE <= EndDate)
org_rel_daily = org_rel_daily.filter((org_rel_daily.REL_CLOSE_DATE > EndDate) | (org_rel_daily.REL_CLOSE_DATE.isNull() == 'true'))

org_rel_daily = org_rel_daily.select(org_rel_daily.REL_FROM_ORG_CODE, org_rel_daily.REL_TO_ORG_CODE).withColumnRenamed("REL_FROM_ORG_CODE", "Succ_Provider").withColumnRenamed("REL_TO_ORG_CODE", "Pred_Provider")

#Some predecessor organisations map to multiple successors (which one is correct?)
#Exclude those orgs (RW6 is most recent affected predecesor)
dup_pred_to_succ_exc = org_rel_daily.groupBy("Pred_Provider").agg(count(org_rel_daily.Pred_Provider).alias("count")).where("count == 1")

org_rel_daily = org_rel_daily.alias("g").join(dup_pred_to_succ_exc, dup_pred_to_succ_exc.Pred_Provider == org_rel_daily.Pred_Provider, "inner").select("g.Pred_Provider", "g.Succ_Provider")

#Sometimes the successor organisation isn't the latest / current org 
#Loop around to find the current successor
count_provider = org_rel_daily.count()
loop_count = 0

while count_provider > 0:
  org_rel_daily_inter = org_rel_daily.alias('u').join(org_rel_daily.alias('v'), col("v.Pred_Provider") == col("u.Succ_Provider"), "inner").withColumn("New_Provider", F.when(col("v.Succ_Provider").isNull() == 'true', col("u.Succ_Provider")).otherwise(col("v.Succ_Provider"))).select("u.Pred_Provider","New_Provider").withColumnRenamed("Pred_Provider","Old_Provider")
  org_rel_daily = org_rel_daily.join(org_rel_daily_inter, org_rel_daily.Pred_Provider == org_rel_daily_inter.Old_Provider, "left").withColumn("Succ_Provider_2", F.when(org_rel_daily_inter.New_Provider.isNull() == 'true', org_rel_daily.Succ_Provider).otherwise(org_rel_daily_inter.New_Provider)).select(org_rel_daily.Pred_Provider, "Succ_Provider_2").withColumnRenamed("Succ_Provider_2","Succ_Provider")
  loop_count = loop_count + 1
  count_provider = org_rel_daily_inter.count()

# COMMAND ----------

#Create look up between predecessor / successor organisation - SubICB / CCG

#org_rel_daily2 = table("dss_corporate.org_relationship_daily")
org_rel_daily2 = table(dss_corporate + ".org_relationship_daily")
org_rel_daily2 = org_rel_daily2.filter(org_rel_daily2.REL_IS_CURRENT == '1').filter(org_rel_daily2.REL_TYPE_CODE == 'P').filter((org_rel_daily2.REL_FROM_ORG_TYPE_CODE == 'CC') | (org_rel_daily2.REL_FROM_ORG_TYPE_CODE == 'NC') ).filter(org_rel_daily2.REL_OPEN_DATE <= EndDate)
org_rel_daily2 = org_rel_daily2.filter((org_rel_daily2.REL_CLOSE_DATE > EndDate) | (org_rel_daily2.REL_CLOSE_DATE.isNull() == 'true'))

org_rel_daily2 = org_rel_daily2.select(org_rel_daily2.REL_FROM_ORG_CODE, org_rel_daily2.REL_TO_ORG_CODE).withColumnRenamed("REL_FROM_ORG_CODE", "Succ_Org").withColumnRenamed("REL_TO_ORG_CODE", "Pred_Org")

#Some predecessor organisations map to multiple successors (which one is correct?)
#Exclude those orgs (RW6 is most recent affected predecessor)
dup_pred_to_succ_exc2 = org_rel_daily2.groupBy("Pred_Org").agg(count(org_rel_daily2.Pred_Org).alias("count")).where("count == 1")

org_rel_daily2 = org_rel_daily2.alias("r").join(dup_pred_to_succ_exc2, dup_pred_to_succ_exc2.Pred_Org == org_rel_daily2.Pred_Org, "inner").select("r.Pred_Org", "r.Succ_Org")

#Sometimes the successor organisation isn't the latest / current org 
#Loop around to find the current successor
count_check = org_rel_daily2.count()

while count_check > 0:
  org_rel_daily2_inter = org_rel_daily2.alias('s').join(org_rel_daily2.alias('t'), col("t.Pred_Org") == col("s.Succ_Org"), "inner").withColumn("New_Org", F.when(col("t.Succ_Org").isNull() == 'true', col("s.Succ_Org")).otherwise(col("t.Succ_Org"))).select("s.Pred_Org","New_Org").withColumnRenamed("Pred_Org","Old_Org")
  org_rel_daily2 = org_rel_daily2.join(org_rel_daily2_inter, org_rel_daily2.Pred_Org == org_rel_daily2_inter.Old_Org, "left").withColumn("Succ_Org_2", F.when(org_rel_daily2_inter.New_Org.isNull() == 'true', org_rel_daily2.Succ_Org).otherwise(org_rel_daily2_inter.New_Org)).select(org_rel_daily2.Pred_Org, "Succ_Org_2").withColumnRenamed("Succ_Org_2","Succ_Org")
  count_check = org_rel_daily2_inter.count()

# COMMAND ----------

#Join denominator and numerator cohorts
#Add mapping to new provider organisations

#msd_final = msd_denom.alias("f").join(msd_num, (msd_denom.UniqPregID == msd_num.Num_UniqPregID) & (msd_denom.Person_ID_Mother == msd_num.Num_Person_ID_Mother), "left" )
if OutputType == 'data':
  msd_final = msd_denom.withColumn("CountUnit_ID_Num", msd_denom.CountUnit_ID)
else:
  msd_final = msd_denom.alias("f").join(msd_num, (msd_denom.CountUnit_ID == msd_num.CountUnit_ID_Num) , "left" )

msd_final = msd_final.alias("h").join(org_rel_daily, (msd_final.OrgCodeProvider == org_rel_daily.Pred_Provider), "left").withColumn("FinalProviderCode", F.when(org_rel_daily.Succ_Provider.isNotNull() == 'true', org_rel_daily.Succ_Provider).otherwise(msd_final.OrgCodeProvider)).select(["h.*","FinalProviderCode"])
msd_final = msd_final.alias("h").join(org_rel_daily2, (msd_final.Commissioner == org_rel_daily2.Pred_Org), "left").withColumn("FinalCommissionerCode", F.when(org_rel_daily2.Succ_Org.isNotNull() == 'true', org_rel_daily2.Succ_Org).otherwise(msd_final.Commissioner)).select(["h.*","FinalCommissionerCode"])

#Create provider level counts for numerator and 
#msd_count = msd_final.groupBy("OrgCodeProvider").agg(count("UniqPregID"),count("Num_UniqPregID")).withColumnRenamed("count(UniqPregID)","Denominator").withColumnRenamed("count(Num_UniqPregID)","Numerator")

#display(msd_final.filter("CompDate > AntenatalAppDate"))

# COMMAND ----------

#%run /data_analysts_collaboration/CMH_TURING_Team(Create)/code_sharing/geogtlrr/geogtlrr_table_creation $endperiod=$RPEndDate $outSchema=$outschema_geog

# COMMAND ----------

#Bring in the current geography table
geog_add = outSchema + ".geogtlrr"
geography = table(geog_add)

geography = geography.withColumnRenamed("Trust","Trust_Name").withColumnRenamed("LRegion","LRegion_Name").withColumnRenamed("Region","Region_Name")


geog_flat = geography.select(geography.Trust_ORG.alias("Org1"), geography.Trust_ORG, geography.Trust_Name, lit("Provider")).union(geography.select(geography.Trust_ORG, geography.STP_Code, geography.STP_Name, lit("Local Maternity System"))).union(geography.select(geography.Trust_ORG, geography.RegionORG, geography.Region_Name, lit("NHS England (Region)"))).union(geography.select(geography.Trust_ORG, geography.Mbrrace_Grouping_Short, geography.Mbrrace_Grouping, lit('MBRRACE Grouping')))

geog_flat = geog_flat.withColumnRenamed("Provider","OrgGrouping").distinct()

# COMMAND ----------

#Create geography lookup table

#Reference data tables
#org_daily = table("dss_corporate.org_daily")
#org_rela = table("dss_corporate.org_relationship_daily")


#Look up data for hospital/trust
#org_hosp = org_daily.alias("a").join(org_rela.alias("b"), org_rela.REL_FROM_ORG_CODE == org_daily.ORG_CODE, "left").\
#      filter(org_daily.BUSINESS_START_DATE <= EndDate).\
#      filter(org_daily.ORG_OPEN_DATE <= EndDate).\
#      filter(org_daily.ORG_TYPE_CODE == 'TS').\
#      filter(org_rela.REL_OPEN_DATE <= EndDate).\
#      filter(org_rela.REL_TO_ORG_TYPE_CODE == 'TR').\
#      filter((org_daily.BUSINESS_END_DATE.isNull() == 'true') | (org_daily.BUSINESS_END_DATE >= EndDate) ).\
#      filter((org_daily.ORG_CLOSE_DATE.isNull() == 'true') | (org_daily.ORG_CLOSE_DATE >= EndDate) ).\
#      filter((org_rela.REL_CLOSE_DATE.isNull() == 'true') | (org_rela.REL_CLOSE_DATE >= EndDate) ).\
#      select("a.ORG_CODE","a.ORG_TYPE_CODE","a.NAME","b.REL_FROM_ORG_CODE", "b.REL_TO_ORG_CODE").withColumnRenamed("ORG_CODE", "Hospital_ORG").withColumnRenamed("ORG_TYPE_CODE", "Hospital_ORGTYPE").withColumnRenamed("NAME", "Hospital_Name")

#org_daily_2 = org_daily.withColumnRenamed("BUSINESS_END_DATE", "BUSINESS_END_DATE2").withColumnRenamed("ORG_OPEN_DATE", "ORG_OPEN_DATE2").withColumnRenamed("BUSINESS_START_DATE", "BUSINESS_START_DATE2").withColumnRenamed("ORG_CLOSE_DATE", "ORG_CLOSE_DATE2").withColumnRenamed("COUNTRY_CODE", "COUNTRY_CODE2").withColumnRenamed("NAME", "NAME2")

#org_hosp = org_hosp.join(org_daily_2.alias("c"), org_hosp.REL_TO_ORG_CODE == org_daily_2.ORG_CODE,"left").\
#      filter((org_daily_2.BUSINESS_END_DATE2.isNull() == 'true') | (org_daily_2.BUSINESS_END_DATE2 >= EndDate) ).\
#      filter((org_daily_2.ORG_OPEN_DATE2 <= EndDate) ).\
#      filter((org_daily_2.BUSINESS_START_DATE2 <= EndDate) ).\
#      filter((org_daily_2.ORG_CLOSE_DATE2.isNull() == 'true') | (org_daily_2.ORG_CLOSE_DATE2 >= EndDate) ).\
#      filter((org_daily_2.COUNTRY_CODE2.isNull() == 'true') | (org_daily_2.COUNTRY_CODE2 == 'E')).\
#      select(org_hosp.Hospital_ORG, org_hosp.Hospital_ORGTYPE, org_hosp.Hospital_Name, org_daily_2.NAME2).withColumnRenamed("NAME2","Trust").distinct()


#Look up data for trust/lregion
#org_daily = org_daily. withColumn("Flag1", lit(EndDate))

#org_trust = org_daily.alias("a").join(org_rela.alias("b"), org_rela.REL_FROM_ORG_CODE == org_daily.ORG_CODE, "left").\
#      filter(org_daily.BUSINESS_START_DATE <= EndDate).\
#      filter(org_daily.ORG_OPEN_DATE <= EndDate).\
#      filter(org_daily.ORG_TYPE_CODE == 'TR').\
#      filter(org_rela.REL_OPEN_DATE <= EndDate).\
#      filter(((org_rela.REL_TO_ORG_TYPE_CODE == 'CF') & (org_daily.Flag1 <= BorderDate)) | ((org_rela.REL_TO_ORG_TYPE_CODE == 'ST') & (org_daily.Flag1 > BorderDate)) ).\
#      filter((org_daily.BUSINESS_END_DATE.isNull() == 'true') | (org_daily.BUSINESS_END_DATE >= EndDate) ).\
#      filter((org_daily.ORG_CLOSE_DATE.isNull() == 'true') | (org_daily.ORG_CLOSE_DATE >= EndDate) ).\
#      filter((org_rela.REL_CLOSE_DATE.isNull() == 'true') | (org_rela.REL_CLOSE_DATE >= EndDate) ).\
#      select("a.ORG_CODE","a.ORG_TYPE_CODE","a.NAME","b.REL_FROM_ORG_CODE", "b.REL_TO_ORG_CODE").withColumnRenamed("ORG_CODE", "Trust_ORG").withColumnRenamed("ORG_TYPE_CODE", "Trust_ORGTYPE").withColumnRenamed("NAME", "Trust_Name")

#org_trust = org_trust.join(org_daily_2.alias("c"), org_trust.REL_TO_ORG_CODE == org_daily_2.ORG_CODE,"left").\
#      filter((org_daily_2.BUSINESS_END_DATE2.isNull() == 'true') | (org_daily_2.BUSINESS_END_DATE2 >= EndDate) ).\
#      filter((org_daily_2.ORG_OPEN_DATE2 <= EndDate)).\
#      filter((org_daily_2.BUSINESS_START_DATE2 <= EndDate)).\
#      filter((org_daily_2.ORG_CLOSE_DATE2.isNull() == 'true') | (org_daily_2.ORG_CLOSE_DATE2 >= EndDate) ).\
#select(org_trust.Trust_ORG, org_trust.Trust_ORGTYPE, org_trust.Trust_Name, org_daily_2.NAME2).withColumnRenamed("NAME2","LRegion").distinct()

#Look up for lregion/region
#org_lreg = org_daily.alias("a").join(org_rela.alias("b"), org_rela.REL_FROM_ORG_CODE == org_daily.ORG_CODE, "left").\
#      filter(org_daily.BUSINESS_START_DATE <= EndDate).\
#      filter(org_daily.ORG_OPEN_DATE <= EndDate).\
#      filter(((org_daily.ORG_TYPE_CODE == 'CF') & (org_daily.Flag1 <= BorderDate)) | ((org_daily.ORG_TYPE_CODE == 'ST') & (org_daily.Flag1 > BorderDate)) ).\
#      filter(org_rela.REL_OPEN_DATE <= EndDate).\
#      filter(org_rela.REL_TO_ORG_TYPE_CODE == 'CE').\
#      filter((org_daily.BUSINESS_END_DATE.isNull() == 'true') | (org_daily.BUSINESS_END_DATE >= EndDate) ).\
#      filter((org_daily.ORG_CLOSE_DATE.isNull() == 'true') | (org_daily.ORG_CLOSE_DATE >= EndDate) ).\
#      filter((org_rela.REL_CLOSE_DATE.isNull() == 'true') | (org_rela.REL_CLOSE_DATE >= EndDate) ).\
#      select("a.ORG_CODE","a.ORG_TYPE_CODE","a.NAME","b.REL_FROM_ORG_CODE", "b.REL_TO_ORG_CODE").withColumnRenamed("ORG_CODE", "LRegion_ORG").withColumnRenamed("ORG_TYPE_CODE", "LRegion_ORGTYPE").withColumnRenamed("NAME", "LRegion_Name")

#org_lreg = org_lreg.join(org_daily_2.alias("c"), org_lreg.REL_TO_ORG_CODE == org_daily_2.ORG_CODE,"left").\
#      filter((org_daily_2.BUSINESS_END_DATE2.isNull() == 'true') | (org_daily_2.BUSINESS_END_DATE2 >= EndDate) ).\
#      filter((org_daily_2.ORG_OPEN_DATE2 <= EndDate) ).\
#      filter((org_daily_2.BUSINESS_START_DATE2 <= EndDate) ).\
#      filter((org_daily_2.ORG_CLOSE_DATE2.isNull() == 'true') | (org_daily_2.ORG_CLOSE_DATE2 >= EndDate) ).\
#select(org_lreg.LRegion_ORG, org_lreg.LRegion_ORGTYPE, org_lreg.LRegion_Name, "C.ORG_CODE", "C.ORG_TYPE_CODE", org_daily_2.NAME2).withColumnRenamed("NAME2","Region").withColumnRenamed("ORG_CODE","Region_ORG").withColumnRenamed("ORG_TYPE_CODE","Region_ORGTYPE").distinct()


#Look up for MBRRACE groups
#mbr_grp = table('dss_corporate.maternity_mbrrace_groups')
#mbr_grp = mbr_grp.withColumn("Current",when(mbr_grp.REL_CLOSE_DATE.isNull() == 'true', lit(1)).otherwise(0))
#mbr_grp = mbr_grp.filter(mbr_grp.REL_OPEN_DATE <= EndDate).withColumn("Ranking",row_number().over( Window.partitionBy(mbr_grp.ORG_CODE).orderBy(mbr_grp.Current.desc(), mbr_grp.REL_CLOSE_DATE.asc() )))
#mbr_grp = mbr_grp.filter(mbr_grp.Ranking == '1')


#Combine for geography lookup
#geography = org_hosp.alias("i").join(org_trust.alias("j"), org_trust.Trust_Name  == org_hosp.Trust, "left").\
#                                join(org_lreg.alias("l"), org_lreg.LRegion_Name == org_trust.LRegion, "left" ).\
#                                join(mbr_grp.alias("m"), org_trust.Trust_ORG == mbr_grp.ORG_CODE , "left" ).\
#                                select("i.Hospital_ORG", "i.Hospital_ORGTYPE", "i.Hospital_Name","j.Trust_ORG", "j.Trust_ORGTYPE", "j.Trust_Name", "l.LRegion_ORG", "l.LRegion_ORGTYPE", "l.LRegion_Name","l.Region_ORG", "l.Region_ORGTYPE", "l.Region", "m.MBRRACE_GROUPING_SHORT", "m.MBRRACE_GROUPING").withColumnRenamed("Region","Region_Name").distinct()

#geography = geography.withColumn("MBRRACE_GROUPING", when(geography.MBRRACE_GROUPING.isNull() == 'true', lit("Group 6. Unknown")).otherwise(geography.MBRRACE_GROUPING))\
#                     .withColumn("MBRRACE_GROUPING_SHORT", when(geography.MBRRACE_GROUPING_SHORT.isNull() == 'true', lit('Group 6. Unknown')).otherwise(geography.MBRRACE_GROUPING_SHORT))

# COMMAND ----------

#Add general name look-up for current organisations.
#Used for data_csv specific measures as they do not appear in the geogtlrr tables

#org_daily = table("dss_corporate.org_daily")
org_daily = table(dss_corporate + ".org_daily")
org_daily = org_daily. withColumn("Flag1", lit(EndDate))

org_name_list = org_daily.filter(org_daily.BUSINESS_START_DATE <= org_daily.Flag1).filter( (org_daily.ORG_CLOSE_DATE > org_daily.Flag1 ) | (org_daily.BUSINESS_END_DATE > org_daily.Flag1) | (org_daily.BUSINESS_END_DATE.isNull() == 'true')).select(org_daily.ORG_CODE, org_daily.ORG_TYPE_CODE, org_daily.NAME, org_daily.SHORT_NAME).withColumnRenamed("NAME","ORG_NAME").withColumnRenamed("SHORT_NAME","ORG_SHORT_NAME").distinct()

#org_lsoa_name = table("dss_corporate.ons_chd_geo_listings")
org_lsoa_name = table(dss_corporate + ".ons_chd_geo_listings")


org_lsoa_name = org_lsoa_name. withColumn("Flag2", lit(EndDate))

#org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DSS_RECORD_START_DATE <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DSS_RECORD_END_DATE > org_lsoa_name.Flag2 ) | (org_lsoa_name.DSS_RECORD_END_DATE.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.LADNM)).select(org_lsoa_name.LADCD, "ORG_TYPE", upper(org_lsoa_name.LADNM),"SHORT_NAME" ).distinct()

org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DATE_OF_OPERATION <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DATE_OF_TERMINATION > org_lsoa_name.Flag2 ) | (org_lsoa_name.DATE_OF_TERMINATION.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.GEOGRAPHY_NAME)).select(org_lsoa_name.GEOGRAPHY_CODE, "ORG_TYPE", upper(org_lsoa_name.GEOGRAPHY_NAME),"SHORT_NAME" ).distinct()


org_name_list = org_name_list.union(org_lsoa_name)

#display(org_name_list)

# COMMAND ----------

#Find all the providers submitting to MSDS in reporting period

#sub_orgs = table("mat_pre_clear.msd000header")
sub_orgs = table(dbSchema + ".msd000header")
sub_orgs = sub_orgs.filter((sub_orgs.RPStartDate.between(StartDate,EndDate)) | (sub_orgs.RPEndDate.between(StartDate,EndDate)))

sub_orgs = sub_orgs.select(sub_orgs.OrgCodeProvider).distinct().withColumnRenamed("OrgCodeProvider","SubmittingProvider")

sub_orgs = sub_orgs.alias("h").join(org_rel_daily, (sub_orgs.SubmittingProvider == org_rel_daily.Pred_Provider), "left").withColumn("FinalSubmitterCode", F.when(org_rel_daily.Succ_Provider.isNotNull() == 'true', org_rel_daily.Succ_Provider).otherwise(sub_orgs.SubmittingProvider)).select(["h.*","FinalSubmitterCode"])

#Add in the higher geographies
all_org_high = geog_flat.alias("allorg").join(sub_orgs, sub_orgs.FinalSubmitterCode == geog_flat.Org1, "inner").select(["allorg.*"]).distinct()
all_org_high = all_org_high.withColumnRenamed("Trust_ORG","Org_Code").withColumnRenamed("Trust_Name","Org_Name").withColumnRenamed("OrgGrouping","Org_Level").drop("Org1").distinct()

# COMMAND ----------

#Assign each record a geography breakdown (provider, LMS, MBR, Region, National)

msd_final_prov = msd_final.join(geography, geography.Trust_ORG == msd_final.FinalProviderCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.FinalProviderCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalProviderCode", "Org_Code").withColumn("Org_Level",lit("Provider")).distinct()

msd_final_mbr = msd_final.join(geography, geography.Trust_ORG == msd_final.FinalProviderCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, geography.Mbrrace_Grouping_Short, geography.Mbrrace_Grouping).withColumnRenamed("Mbrrace_Grouping", "Org_Name").withColumnRenamed("Mbrrace_Grouping_Short", "Org_Code").withColumn("Org_Level",lit("MBRRACE Grouping")).distinct()

msd_final_lms = msd_final.join(geography, geography.Trust_ORG == msd_final.FinalProviderCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, geography.LRegionORG, geography.LRegion_Name).withColumnRenamed("LRegion_Name", "Org_Name").withColumnRenamed("LRegionORG", "Org_Code").withColumn("Org_Level",lit("Local Maternity System")).distinct()

msd_final_reg = msd_final.join(geography, geography.Trust_ORG == msd_final.FinalProviderCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, geography.RegionORG, geography.Region_Name).withColumnRenamed("Region_Name", "Org_Name").withColumnRenamed("Region_ORG", "Org_Code").withColumn("Org_Level",lit("NHS England (Region)")).distinct()

msd_final_nat = msd_final.join(geography, geography.Trust_ORG == msd_final.FinalProviderCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown).withColumn("Org_Code",lit("National")).withColumn("Org_Name",lit("All Submitters")).withColumn("Org_Level",lit("National")).distinct()

msd_final_combined = msd_final_prov.union(msd_final_mbr).union(msd_final_lms).union(msd_final_reg).union(msd_final_nat)



#If the measure if for the Data file csv, then it has the following additional breakdowns - SubICB, LA and (one of) Booking or Delivery Site

if OutputType == 'data':
  msd_final_com = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.FinalCommissionerCode, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.FinalCommissionerCode, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", F.when(msd_final.FinalCommissionerCode =='Unknown', lit("UNKNOWN")).otherwise(org_name_list.ORG_NAME)).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("FinalCommissionerCode", "Org_Code").withColumn("Org_Level",lit("SubICB of Responsibility")).distinct()
  
  msd_final_la = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.LocalAuthority, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.LocalAuthority, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", F.when(msd_final.LocalAuthority =='ZZ201', lit("HOME")).otherwise(F.when(msd_final.LocalAuthority == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(F.when(msd_final.LocalAuthority == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(F.when(msd_final.LocalAuthority == 'ZZ999', lit('OTHER')).otherwise(F.when(msd_final.LocalAuthority == 'Unknown', lit('LOCAL AUTHORITY UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("LocalAuthority", "Org_Code").withColumn("Org_Level",lit("Local Authority of Residence")).distinct()
  msd_final_la = msd_final_la.withColumn("Org_Name",F.when(msd_final_la.Org_Name.isNull() == 'false', msd_final_la.Org_Name).otherwise(lit("LOCAL AUTHORITY UNKNOWN")))
  
  msd_final_combined = msd_final_combined.union(msd_final_com).union(msd_final_la)

  
  
if (OutputCount == 'Women') & (OutputType == 'data') :
  msd_final_bks = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.BookingSite, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.BookingSite, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", F.when(msd_final.BookingSite =='ZZ201', lit("HOME")).otherwise(F.when(msd_final.BookingSite == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(F.when(msd_final.BookingSite == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(F.when(msd_final.BookingSite == 'ZZ999', lit('OTHER')).otherwise(F.when(msd_final.BookingSite == 'Unknown', lit('BOOKING SITE UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("BookingSite", "Org_Code").withColumn("Org_Level",lit("BookingSite")).distinct()
  
  msd_final_bks = msd_final_bks.withColumn("Org_Name",F.when(msd_final_bks.Org_Name.isNull() == 'false', msd_final_bks.Org_Name).otherwise(lit("BOOKING SITE UNKNOWN")))
  msd_final_combined = msd_final_combined.union(msd_final_bks)
  
  
if (OutputCount == 'Babies') & (OutputType == 'data') :
  msd_final_del = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.DeliverySite, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.DeliverySite, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", F.when(msd_final.DeliverySite =='ZZ201', lit("HOME")).otherwise(F.when(msd_final.DeliverySite == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(F.when(msd_final.DeliverySite == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(F.when(msd_final.DeliverySite == 'ZZ999', lit('OTHER')).otherwise(F.when(msd_final.DeliverySite == 'Unknown', lit('DELIVERY SITE UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("DeliverySite", "Org_Code").withColumn("Org_Level",lit("DeliverySite")).distinct()
  
  msd_final_del = msd_final_del.withColumn("Org_Name",F.when(msd_final_del.Org_Name.isNull() == 'false', msd_final_del.Org_Name).otherwise(lit("DELIVERY SITE UNKNOWN")))
  msd_final_combined = msd_final_combined.union(msd_final_del)

  
  
#Add in the months of the reporting period
msd_final_combined = msd_final_combined.withColumn("RPStartDate",lit(StartDate)).withColumn("RPEndDate",lit(EndDate))

# COMMAND ----------

#Unsuppressed figures - for measures file

if OutputType == 'measure':
  
  msd_count_denom = msd_final_combined.groupBy("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate").agg(count("CountUnit_ID")).withColumnRenamed("count(CountUnit_ID)","Value").withColumn("Currency",lit("Denominator"))
  
  msd_count_num = msd_final_combined.groupBy("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate").agg(count("CountUnit_ID_Num")).withColumnRenamed("count(CountUnit_ID_Num)","Value").withColumn("Currency",lit("Numerator"))
  
  msd_count_rate = msd_count_denom.alias("n").join(msd_count_num.alias("o"), (msd_count_denom.Org_Code == msd_count_num.Org_Code) &  (msd_count_denom.Org_Level == msd_count_num.Org_Level)).withColumn("Rate", 100 * col("o.Value")/ col("n.Value")).select("n.Org_Code", "n.Org_Name", "n.Org_Level", "n.RPStartDate", "n.RPEndDate", "Rate").withColumn("Value",round(col("Rate"),1)).withColumn("Currency", lit("Rate")).drop("Rate")
  
  msd_count_all = msd_count_denom.union(msd_count_num).union(msd_count_rate).withColumn("IndicatorFamily", lit(IndicaFam)).withColumn("Indicator", lit(Indicator)).select("Org_Code", "Org_Name", "Org_Level", "RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Currency", "Value")
  
  sub_org_1 = all_org_high.withColumn("Currency",lit("Denominator"))
  sub_org_2 = all_org_high.withColumn("Currency",lit("Numerator"))
  sub_org_3 = all_org_high.withColumn("Currency",lit("Rate"))
   
#  sub_org_1 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
#            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Denominator")).distinct()
  
#  sub_org_2 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
#            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Numerator")).distinct()
  
#  sub_org_3 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
#            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Rate")).distinct()



  sub_org_frame = sub_org_1.union(sub_org_2).union(sub_org_3)
  
  sub_org_frame = sub_org_frame.withColumn("RPStartDate",lit(StartDate)).withColumn("RPEndDate",lit(EndDate)).withColumn("IndicatorFamily",lit(IndicaFam)).withColumn("Indicator",lit(Indicator)).withColumn("Value",lit("0.0"))#.withColumn("Org_Level",lit("Provider"))
  sub_org_frame = sub_org_frame.select("Org_Code", "Org_Name", "Org_Level", "RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Currency", "Value")
  
  #Add in any organisations which do not otherwise appear in the data
  
  msd_count_add = msd_count_all.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all.Org_Level) &  (sub_org_frame.RPStartDate == msd_count_all.RPStartDate) &  (sub_org_frame.RPEndDate == msd_count_all.RPEndDate) &  (sub_org_frame.IndicatorFamily == msd_count_all.IndicatorFamily) &  (sub_org_frame.Indicator == msd_count_all.Indicator) & (sub_org_frame.Currency == msd_count_all.Currency), "right" ).filter(msd_count_all.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all = msd_count_all.union(msd_count_add)
  
  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.IndicatorFamily.asc(), msd_count_all.Indicator.asc(), msd_count_all.Currency.asc()))

# COMMAND ----------

# DBTITLE 1,Final Output
#Suppressed figures - for measures file

if OutputType == 'measure':
  
  msd_count_denom_sup = msd_count_denom.withColumn("Value", F.when(col("Value").between(1,7), 5).otherwise(5 * round(col("Value") / 5)))
  
  msd_count_num_sup = msd_count_num.withColumn("Value", F.when(col("Value").between(1,7), 5).otherwise(5 * round(col("Value") / 5)))
  
  msd_count_rate_sup = msd_count_denom_sup.alias("n").join(msd_count_num_sup.alias("o"), (msd_count_denom_sup.Org_Code == msd_count_num_sup.Org_Code) &  (msd_count_denom_sup.Org_Level == msd_count_num_sup.Org_Level)).withColumn("Rate", 100 * col("o.Value")/ col("n.Value")).select("n.Org_Code", "n.Org_Name", "n.Org_Level", "n.RPStartDate", "n.RPEndDate", "Rate").withColumn("Value",round(col("Rate"),1)).withColumn("Currency", lit("Rate")).drop("Rate")
  
  msd_count_all_sup = msd_count_denom_sup.union(msd_count_num_sup).union(msd_count_rate_sup).withColumn("IndicatorFamily",lit(IndicaFam)).withColumn("Indicator",lit(Indicator)).select("Org_Code", "Org_Name", "Org_Level", "RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Currency", "Value")
  
  msd_count_add_sup = msd_count_all_sup.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all_sup.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all_sup.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all_sup.Org_Level) &  (sub_org_frame.RPStartDate == msd_count_all_sup.RPStartDate) &  (sub_org_frame.RPEndDate == msd_count_all_sup.RPEndDate) &  (sub_org_frame.IndicatorFamily == msd_count_all_sup.IndicatorFamily) &  (sub_org_frame.Indicator == msd_count_all_sup.Indicator) & (sub_org_frame.Currency == msd_count_all_sup.Currency), "right" ).filter(msd_count_all_sup.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all_sup = msd_count_all_sup.union(msd_count_add_sup)
  
  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.IndicatorFamily.asc(), msd_count_all_sup.Indicator.asc(), msd_count_all_sup.Currency.asc()))

# COMMAND ----------

#Unsuppressed figures - for data file

if OutputType == 'data':
  
  msd_count_denom = msd_final_combined.groupBy("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate","Breakdown").agg(count("CountUnit_ID")).withColumnRenamed("count(CountUnit_ID)","Final_value").withColumn("Count_Of",lit(OutputCount))
  msd_count_denom = msd_count_denom.withColumn("Org_Name",upper(msd_count_denom.Org_Name))
    
  msd_count_all = msd_count_denom.withColumn("Dimension", lit(Indicator)).withColumnRenamed("RPStartDate", "ReportingPeriodStartDate").withColumnRenamed("Breakdown", "Measure").withColumnRenamed("RPEndDate", "ReportingPeriodEndDate").select("ReportingPeriodStartDate", "ReportingPeriodEndDate", "Dimension", "Org_Level", "Org_Code", "Org_Name", "Measure", "Count_Of", "Final_value")
  
  #Referenece orgs
  sub_org_1 = all_org_high.withColumn("Org_Name",upper(all_org_high.Org_Name))
  sub_org_1 = sub_org_1.select("Org_Code","Org_Name","Org_Level")
  #Value orgs
  sub_org_2 = msd_count_all.select("Org_Code","Org_Name","Org_Level").distinct()
  #Combined orgs
  sub_org_1 = sub_org_1.union(sub_org_2)
    
#  sub_org_1 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
#            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").distinct()
#  sub_org_1 = sub_org_1.withColumn("Org_Name",upper(sub_org_1.Org_Name))

  breakdown = msd_count_denom.select(msd_count_denom.Breakdown).distinct()
  
  sub_org_frame = sub_org_1.crossJoin(breakdown)
  
  sub_org_frame = sub_org_frame.withColumn("ReportingPeriodStartDate",lit(StartDate)).withColumn("ReportingPeriodEndDate",lit(EndDate)).withColumn("Dimension",lit(Indicator)).withColumn("Final_value",lit("0")).withColumn("Count_Of",lit(OutputCount)).withColumnRenamed("Breakdown", "Measure")#.withColumn("Org_Level",lit("Provider"))
  sub_org_frame = sub_org_frame.select("ReportingPeriodStartDate", "ReportingPeriodEndDate", "Dimension", "Org_Level", "Org_Code", "Org_Name", "Measure", "Count_Of", "Final_value")
   
  #Add in any providers which do not otherwise appear in the data
  
  msd_count_add = msd_count_all.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all.Org_Level) &  (sub_org_frame.ReportingPeriodStartDate == msd_count_all.ReportingPeriodStartDate) &  (sub_org_frame.ReportingPeriodEndDate == msd_count_all.ReportingPeriodEndDate) &  (sub_org_frame.Dimension == msd_count_all.Dimension) & (sub_org_frame.Measure == msd_count_all.Measure), "right" ).filter(msd_count_all.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all = msd_count_all.union(msd_count_add)
  
  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.Dimension.asc(), msd_count_all.Count_Of.asc()))

# COMMAND ----------

#Suppressed figures - for data file

if OutputType == 'data':
  
  msd_count_all_sup = msd_count_all.withColumn("Final_value", F.when(col("Final_value").between(1,7), 5).otherwise(5 * round(col("Final_value") / 5)))
  
  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.Dimension.asc(), msd_count_all_sup.Count_Of.asc()))

# COMMAND ----------

#Convert dataframe output into a temporary view

msd_count_all_sup.createOrReplaceTempView('msd_count_all_sup')

# COMMAND ----------

fnp_spdf = (
    msd_count_all_sup
    .orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.IndicatorFamily.asc(), msd_count_all_sup.Indicator.asc(), msd_count_all_sup.Currency.asc())
    .select('Org_Code',
            'Org_Name',
            'Org_Level',
            'RPStartDate',
            'RPEndDate',
            'IndicatorFamily',
            'Indicator',
            'Currency',
            'Value')
  )
fnp_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.fnp_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT
# MAGIC RPStartDate,
# MAGIC RPEndDate,
# MAGIC IndicatorFamily,
# MAGIC Indicator,
# MAGIC Org_Code as OrgCodeProvider,
# MAGIC Org_Name as OrgName,
# MAGIC Org_Level as OrgLevel,
# MAGIC Currency,
# MAGIC Value,
# MAGIC current_timestamp() AS CreatedAt 
# MAGIC
# MAGIC from $outSchema.fnp_data where RPStartDate = '$RPStartDate'

# COMMAND ----------

#Create the storage table (if it does not already exist)
#Clear the table of old results for the metric / month

#if OutputType == 'measure':
  #string = "CREATE TABLE IF NOT EXISTS " + outSchema + "." + outtable + " (Org_Code STRING, Org_Name STRING, Org_Level STRING, RPStartDate DATE, RPEndDate DATE, IndicatorFamily STRING, Indicator #STRING, Currency STRING, Value STRING) USING DELTA"
  #spark.sql(string)
  
  #string2 = "DELETE FROM " + outSchema + "." + outtable + " WHERE RPStartDate = '" + StartDate + "' AND IndicatorFamily = '" + IndicaFam + "' AND Indicator = '" + Indicator + "'"  
  #spark.sql(string2)
  
  
#if OutputType == 'data':
  #string = "CREATE TABLE IF NOT EXISTS " + outSchema + "." + outtable + " (ReportingPeriodStartDate DATE, ReportingPeriodEndDate DATE, Dimension STRING, Org_Level STRING, Org_Code STRING, Org_Name STRING, Measure STRING, Count_Of STRING, Final_value STRING) USING DELTA"
 # spark.sql(string)
  
 # otherstring = "DELETE FROM " + outSchema + "." + outtable + " WHERE ReportingPeriodStartDate = '" + StartDate + "' AND Dimension = '" + Indicator + "'"
#  spark.sql(otherstring)

# COMMAND ----------

#Check the format of the table is correct 
#Then store the output

#string3 = outSchema + "." + outtable
#outputform = table(string3)

#if outputform.columns == msd_count_all_sup.columns:
  #string4 = "INSERT INTO " + outSchema + "." + outtable + " SELECT * FROM msd_count_all_sup"
  #spark.sql(string4)
#else:
#  print("Table is not in the appropriate format. Metric was not loaded.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --INSERT INTO $outschema.$outtable
# MAGIC --SELECT *
# MAGIC --FROM msd_count_all_sup

# COMMAND ----------

# DBTITLE 1,This includes FNP Pathway and Support do not use this for published measure use "Final Ouput" cell - FNP Support Only
# MAGIC %sql
# MAGIC
# MAGIC --SELECT *
# MAGIC --FROM $outSchema.$outtable
# MAGIC --WHERE RPStartDate = '2023-05-01'
# MAGIC --AND RPEndDate = '2023-05-31'
# MAGIC --ORDER BY Org_Level, Org_code, Currency

# COMMAND ----------

