# Databricks notebook source
# MAGIC %md ##### Spontaneous births following cephalic presentation - as of April 2023
# MAGIC
# MAGIC Denominator: Singleton, cephalic, full term births, where the delivery method is known.    
# MAGIC Numerator: Singleton, cephalic, full term births, where the delivery method is spontaneous.

# COMMAND ----------

# MAGIC %md
# MAGIC How to run:
# MAGIC - Run Cmd 3 & 4
# MAGIC - Update Cmd 5 as required (only regular change should be personal database)
# MAGIC - Run whole notebook from Cmd 6
# MAGIC - Download suppressed data file for publication from Cmd 24

# COMMAND ----------

#Import necessary pyspark + other functions

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when, lit, col, round, concat, upper, max, countDistinct
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


# COMMAND ----------

# DBTITLE 1,Setting the parameters - Reading from widgets to variables
#Collect widget values

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

# MAGIC %sql
# MAGIC
# MAGIC -- Create denominator
# MAGIC --Identify births (live, full term, cephalic) in reporting period
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW Denominator_births AS
# MAGIC
# MAGIC SELECT
# MAGIC   S.Person_ID_Baby,
# MAGIC   S.UniqPregID,
# MAGIC   S.Person_ID_Mother,
# MAGIC   S.LabourDeliveryID,
# MAGIC   S.OrgCodeProvider,
# MAGIC   S.RecordNumber,
# MAGIC   S.DeliveryMethodCode,
# MAGIC   S.PersonBirthDateBaby,
# MAGIC   S.GestationLengthBirth,
# MAGIC   s.FetusPresentation,
# MAGIC   L.BirthsPerLabandDel,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY S.Person_ID_Baby,
# MAGIC     S.UniqPregID,
# MAGIC     S.Person_ID_Mother
# MAGIC     ORDER BY
# MAGIC       S.RecordNumber DESC
# MAGIC   ) Ranking
# MAGIC FROM
# MAGIC   $dbSchema.MSD401BabyDemographics S
# MAGIC   LEFT JOIN $dbSchema.msd301labourdelivery L ON S.Person_ID_Mother = L.Person_ID_Mother
# MAGIC   AND S.UniqPregID = L.UniqPregID
# MAGIC   AND S.LabourDeliveryID = L.LabourDeliveryID
# MAGIC   AND S.RecordNumber = L.RecordNumber
# MAGIC WHERE
# MAGIC   S.RPStartDate = '$RPStartDate'
# MAGIC   AND PersonBirthDateBaby BETWEEN S.RPStartDate
# MAGIC   AND S.RPEndDate
# MAGIC   AND GestationLengthBirth BETWEEN 259
# MAGIC   AND 315
# MAGIC   AND FetusPresentation = '01'
# MAGIC   AND BirthsPerLabandDel = '1'
# MAGIC   AND DeliveryMethodCode IN ('0', '1', '2', '3', '4', '5', '6', '7', '8');
# MAGIC --Does not include 9 (Other), invalid DeliveryMethodCodes or unrecorded, where could not determine whether the delivery involved intervention or not
# MAGIC
# MAGIC   --Where mulitple records exist for baby/pregnancy then keep the latest submitted record (max RecordNumber)
# MAGIC   CREATE OR REPLACE GLOBAL TEMPORARY VIEW Denominator_Cohort AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC CASE WHEN DeliveryMethodCode IN ('0', '1', '5') THEN 'Spontaneous'
# MAGIC ELSE CASE WHEN DeliveryMethodCode IN ('2', '3', '4', '6', '7', '8') THEN 'Intervention'
# MAGIC ELSE 'Unknown/Invalid'
# MAGIC END
# MAGIC END BirthStatus
# MAGIC FROM
# MAGIC   global_temp.Denominator_births
# MAGIC WHERE
# MAGIC   Ranking = '1';
# MAGIC
# MAGIC -- Summary of count of births identified in the denominator 
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT Person_ID_Baby, UniqPregID) Births
# MAGIC FROM
# MAGIC   global_temp.Denominator_Cohort;
# MAGIC   
# MAGIC   select * from global_temp.Denominator_Cohort;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create numerator
# MAGIC --Identifying which births in the denominator cohort have a spontaneous birth (0, 1, 5 - delivery code)
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW Numerator_Cohort AS
# MAGIC SELECT 
# MAGIC UniqPregID, 
# MAGIC Person_ID_Baby,
# MAGIC Person_ID_Mother,
# MAGIC LabourDeliveryID,
# MAGIC -- OrgCodeProvider,
# MAGIC RecordNumber,
# MAGIC DeliveryMethodCode,
# MAGIC PersonBirthDateBaby,
# MAGIC GestationLengthBirth,
# MAGIC FetusPresentation,
# MAGIC BirthsPerLabandDel,
# MAGIC Ranking,
# MAGIC BirthStatus
# MAGIC FROM global_temp.Denominator_Cohort
# MAGIC WHERE BirthStatus = 'Spontaneous'

# COMMAND ----------

#For SQL defined measures: Converts the denominator and numerator cohort views into dataframes

msd_denom = table("global_temp.Denominator_Cohort")
msd_num = table("global_temp.Numerator_Cohort")

# COMMAND ----------

#If the numerator cohort has not had the counting units - Person_ID_Mother, UniqPregID, Person_ID_Baby - prefixed by Num_ (for later calculations) then those columns are renamed here.
try:
  msd_num
except NameError:
  None
else:
  msd_num = msd_num.withColumnRenamed("Person_ID_Mother","Num_Person_ID_Mother").withColumnRenamed("UniqPregID","Num_UniqPregID").withColumnRenamed("Person_ID_Baby","Num_Person_ID_Baby")

# COMMAND ----------

# MAGIC %sql
# MAGIC select '%RPEnddate',* FROM $dbSchema.msd002gp
# MAGIC where StartDateGMPReg<'%RPEnddate' and EndDateGMPReg >'%RPEnddate'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Add organisational data to the denominator cohort

#Add additional breakdowns (SubICB / CCG) 
gptable = dbSchema + ".msd002gp"
print(gptable)
msd002 = table(gptable)
display(msd002)
msd002 = msd002.alias("p").join(msd_denom, (msd_denom.Person_ID_Mother == msd002.Person_ID_Mother) & (msd_denom.RecordNumber == msd002.RecordNumber), "inner" ).select(["p.*"]).distinct()
display(msd002)
msd002 = msd002.filter(msd002.RPEndDate == EndDate).filter((msd002.EndDateGMPReg.isNull() == 'true') | ((msd002.StartDateGMPReg.isNull() == 'true') & (msd002.EndDateGMPReg > EndDate) ) | ((msd002.StartDateGMPReg < EndDate) & (msd002.EndDateGMPReg > EndDate)))
display(msd002)
msd002 = msd002.withColumn("Commissioner",when(msd002.RPEndDate > STPEndDate, msd002.OrgIDSubICBLocGP).otherwise(msd002.CCGResponsibilityMother))
display(msd002)
msd002 = msd002.withColumn("Commissioner",when(msd002.Commissioner.isNull() == 'false', msd002.Commissioner).otherwise(lit("Unknown"))).select(msd002.Person_ID_Mother, msd002.RecordNumber,"Commissioner")
display(msd002)

#Add additional breakdowns (Local Authority)
demogtable = dbSchema + ".msd001motherdemog"
print(demogtable )
msd001 = table(demogtable)

msd001 = msd001.alias("q").join(msd_denom, (msd_denom.Person_ID_Mother == msd001.Person_ID_Mother) & (msd_denom.RecordNumber == msd001.RecordNumber), "inner" ).select(["q.*"]).distinct()

msd001 = msd001.withColumn("LocalAuthority",when(msd001.LAD_UAMother.isNull() == 'false', msd001.LAD_UAMother).otherwise(lit("Unknown"))).select(msd001.Person_ID_Mother, msd001.RecordNumber,"LocalAuthority")


#Add geographies
msd_denom = msd_denom.alias("r").join(msd001, (msd_denom.Person_ID_Mother == msd001.Person_ID_Mother) & (msd_denom.RecordNumber == msd001.RecordNumber), "left" ).join(msd002, (msd_denom.Person_ID_Mother == msd002.Person_ID_Mother) & (msd_denom.RecordNumber == msd002.RecordNumber), "left" ).select(["r.*", msd001.LocalAuthority, msd002.Commissioner]).distinct()

msd_denom = msd_denom.withColumn("LocalAuthority",when(msd_denom.LocalAuthority.isNull() == 'false', msd_denom.LocalAuthority).otherwise(lit("Unknown"))).withColumn("Commissioner",when(msd_denom.Commissioner.isNull() == 'false', msd_denom.Commissioner).otherwise(lit("Unknown")))


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
msd_denom = msd_denom.withColumn('MeasureType', when(msd_denom.BookingSite.isNull() == 'false',lit('1')).otherwise(lit('0')))


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
dss = dss_corporate + ".org_relationship_daily"
print(dss )
org_rel_daily = table(dss)
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
  org_rel_daily_inter = org_rel_daily.alias('u').join(org_rel_daily.alias('v'), col("v.Pred_Provider") == col("u.Succ_Provider"), "inner").withColumn("New_Provider", when(col("v.Succ_Provider").isNull() == 'true', col("u.Succ_Provider")).otherwise(col("v.Succ_Provider"))).select("u.Pred_Provider","New_Provider").withColumnRenamed("Pred_Provider","Old_Provider")
  org_rel_daily = org_rel_daily.join(org_rel_daily_inter, org_rel_daily.Pred_Provider == org_rel_daily_inter.Old_Provider, "left").withColumn("Succ_Provider_2", when(org_rel_daily_inter.New_Provider.isNull() == 'true', org_rel_daily.Succ_Provider).otherwise(org_rel_daily_inter.New_Provider)).select(org_rel_daily.Pred_Provider, "Succ_Provider_2").withColumnRenamed("Succ_Provider_2","Succ_Provider")
  loop_count = loop_count + 1
  count_provider = org_rel_daily_inter.count()

# COMMAND ----------

#Create look up between predecessor / successor organisation - SubICB / CCG
dss2= dss_corporate+".org_relationship_daily"
org_rel_daily2 = table(dss2)
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
  org_rel_daily2_inter = org_rel_daily2.alias('s').join(org_rel_daily2.alias('t'), col("t.Pred_Org") == col("s.Succ_Org"), "inner").withColumn("New_Org", when(col("t.Succ_Org").isNull() == 'true', col("s.Succ_Org")).otherwise(col("t.Succ_Org"))).select("s.Pred_Org","New_Org").withColumnRenamed("Pred_Org","Old_Org")
  org_rel_daily2 = org_rel_daily2.join(org_rel_daily2_inter, org_rel_daily2.Pred_Org == org_rel_daily2_inter.Old_Org, "left").withColumn("Succ_Org_2", when(org_rel_daily2_inter.New_Org.isNull() == 'true', org_rel_daily2.Succ_Org).otherwise(org_rel_daily2_inter.New_Org)).select(org_rel_daily2.Pred_Org, "Succ_Org_2").withColumnRenamed("Succ_Org_2","Succ_Org")
  count_check = org_rel_daily2_inter.count()

# COMMAND ----------

#Join denominator and numerator cohorts
#Add mapping to new provider organisations

#msd_final = msd_denom.alias("f").join(msd_num, (msd_denom.UniqPregID == msd_num.Num_UniqPregID) & (msd_denom.Person_ID_Mother == msd_num.Num_Person_ID_Mother), "left" )
if OutputType == 'data':
  msd_final = msd_denom.withColumn("CountUnit_ID_Num", msd_denom.CountUnit_ID)
else:
  msd_final = msd_denom.alias("f").join(msd_num, (msd_denom.CountUnit_ID == msd_num.CountUnit_ID_Num) , "left" )

msd_final = msd_final.alias("h").join(org_rel_daily, (msd_final.OrgCodeProvider == org_rel_daily.Pred_Provider), "left").withColumn("FinalProviderCode", when(org_rel_daily.Succ_Provider.isNotNull() == 'true', org_rel_daily.Succ_Provider).otherwise(msd_final.OrgCodeProvider)).select(["h.*","FinalProviderCode"])
msd_final = msd_final.alias("h").join(org_rel_daily2, (msd_final.Commissioner == org_rel_daily2.Pred_Org), "left").withColumn("FinalCommissionerCode", when(org_rel_daily2.Succ_Org.isNotNull() == 'true', org_rel_daily2.Succ_Org).otherwise(msd_final.Commissioner)).select(["h.*","FinalCommissionerCode"])

#Create provider level counts for numerator and 
#msd_count = msd_final.groupBy("OrgCodeProvider").agg(count("UniqPregID"),count("Num_UniqPregID")).withColumnRenamed("count(UniqPregID)","Denominator").withColumnRenamed("count(Num_UniqPregID)","Numerator")

#display(msd_final.filter("CompDate > AntenatalAppDate"))

# COMMAND ----------

#Bring in the current geography table
geog_add = outSchema + ".geogtlrr"
geography = table(geog_add)

geography = geography.withColumnRenamed("Trust","Trust_Name").withColumnRenamed("LRegion","LRegion_Name").withColumnRenamed("Region","Region_Name")

# COMMAND ----------

#Add general name look-up for current organisations.
#Used for data_csv specific measures as they do not appear in the geogtlrr tables
org_daily = table(dss_corporate+".org_daily")
org_daily = org_daily. withColumn("Flag1", lit(EndDate))

org_name_list = org_daily.filter(org_daily.BUSINESS_START_DATE <= org_daily.Flag1).filter( (org_daily.ORG_CLOSE_DATE > org_daily.Flag1 ) | (org_daily.BUSINESS_END_DATE > org_daily.Flag1) | (org_daily.BUSINESS_END_DATE.isNull() == 'true')).select(org_daily.ORG_CODE, org_daily.ORG_TYPE_CODE, org_daily.NAME, org_daily.SHORT_NAME).withColumnRenamed("NAME","ORG_NAME").withColumnRenamed("SHORT_NAME","ORG_SHORT_NAME").distinct()

org_lsoa_name = table(dss_corporate+".ons_chd_geo_listings")


org_lsoa_name = org_lsoa_name. withColumn("Flag2", lit(EndDate))

#org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DSS_RECORD_START_DATE <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DSS_RECORD_END_DATE > org_lsoa_name.Flag2 ) | (org_lsoa_name.DSS_RECORD_END_DATE.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.LADNM)).select(org_lsoa_name.LADCD, "ORG_TYPE", upper(org_lsoa_name.LADNM),"SHORT_NAME" ).distinct()

org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DATE_OF_OPERATION <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DATE_OF_TERMINATION > org_lsoa_name.Flag2 ) | (org_lsoa_name.DATE_OF_TERMINATION.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.GEOGRAPHY_NAME)).select(org_lsoa_name.GEOGRAPHY_CODE, "ORG_TYPE", upper(org_lsoa_name.GEOGRAPHY_NAME),"SHORT_NAME" ).distinct()


org_name_list = org_name_list.union(org_lsoa_name)

#display(org_name_list)

# COMMAND ----------

#Find all the providers submitting to MSDS in reporting period
sub = dbSchema + ".msd000header"
sub_orgs = table(sub)
sub_orgs = sub_orgs.filter((sub_orgs.RPStartDate.between(StartDate,EndDate)) | (sub_orgs.RPEndDate.between(StartDate,EndDate)))

sub_orgs = sub_orgs.select(sub_orgs.OrgCodeProvider).distinct().withColumnRenamed("OrgCodeProvider","SubmittingProvider")

sub_orgs = sub_orgs.alias("h").join(org_rel_daily, (sub_orgs.SubmittingProvider == org_rel_daily.Pred_Provider), "left").withColumn("FinalSubmitterCode", when(org_rel_daily.Succ_Provider.isNotNull() == 'true', org_rel_daily.Succ_Provider).otherwise(sub_orgs.SubmittingProvider)).select(["h.*","FinalSubmitterCode"])

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
            withColumn("ORG_NAME", when(msd_final.FinalCommissionerCode =='Unknown', lit("UNKNOWN")).otherwise(org_name_list.ORG_NAME)).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("FinalCommissionerCode", "Org_Code").withColumn("Org_Level",lit("SubICB of Responsibility")).distinct()
  
  msd_final_la = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.LocalAuthority, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.LocalAuthority, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", when(msd_final.LocalAuthority =='ZZ201', lit("HOME")).otherwise(when(msd_final.LocalAuthority == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(when(msd_final.LocalAuthority == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(when(msd_final.LocalAuthority == 'ZZ999', lit('OTHER')).otherwise(when(msd_final.LocalAuthority == 'Unknown', lit('LOCAL AUTHORITY UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("LocalAuthority", "Org_Code").withColumn("Org_Level",lit("Local Authority of Residence")).distinct()
  msd_final_la = msd_final_la.withColumn("Org_Name",when(msd_final_la.Org_Name.isNull() == 'false', msd_final_la.Org_Name).otherwise(lit("LOCAL AUTHORITY UNKNOWN")))
  
  msd_final_combined = msd_final_combined.union(msd_final_com).union(msd_final_la)

  
  
if (OutputCount == 'Women') & (OutputType == 'data') :
  msd_final_bks = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.BookingSite, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.BookingSite, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", when(msd_final.BookingSite =='ZZ201', lit("HOME")).otherwise(when(msd_final.BookingSite == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(when(msd_final.BookingSite == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(when(msd_final.BookingSite == 'ZZ999', lit('OTHER')).otherwise(when(msd_final.BookingSite == 'Unknown', lit('BOOKING SITE UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("BookingSite", "Org_Code").withColumn("Org_Level",lit("BookingSite")).distinct()
  
  msd_final_bks = msd_final_bks.withColumn("Org_Name",when(msd_final_bks.Org_Name.isNull() == 'false', msd_final_bks.Org_Name).otherwise(lit("BOOKING SITE UNKNOWN")))
  msd_final_combined = msd_final_combined.union(msd_final_bks)
  
  
if (OutputCount == 'Babies') & (OutputType == 'data') :
  msd_final_del = msd_final.join(org_name_list, org_name_list.ORG_CODE == msd_final.DeliverySite, "left").\
            select(msd_final.CountUnit_ID, msd_final.CountUnit_ID_Num, msd_final.Breakdown, msd_final.DeliverySite, org_name_list.ORG_NAME).\
            withColumn("ORG_NAME", when(msd_final.DeliverySite =='ZZ201', lit("HOME")).otherwise(when(msd_final.DeliverySite == 'ZZ888', lit('NON-NHS ORGANISATION')).otherwise(when(msd_final.DeliverySite == 'ZZ203', lit('NOT KNOWN (NOT RECORDED)')).otherwise(when(msd_final.DeliverySite == 'ZZ999', lit('OTHER')).otherwise(when(msd_final.DeliverySite == 'Unknown', lit('DELIVERY SITE UNKNOWN')).otherwise(org_name_list.ORG_NAME)))))).\
            withColumnRenamed("ORG_NAME", "Org_Name").withColumnRenamed("DeliverySite", "Org_Code").withColumn("Org_Level",lit("DeliverySite")).distinct()
  
  msd_final_del = msd_final_del.withColumn("Org_Name",when(msd_final_del.Org_Name.isNull() == 'false', msd_final_del.Org_Name).otherwise(lit("DELIVERY SITE UNKNOWN")))
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
  
  
  sub_org_1 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Denominator")).distinct()
  
  sub_org_2 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Numerator")).distinct()
  
  sub_org_3 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").withColumn("Currency",lit("Rate")).distinct()
  
  sub_org_frame = sub_org_1.union(sub_org_2).union(sub_org_3)
  
  sub_org_frame = sub_org_frame.withColumn("RPStartDate",lit(StartDate)).withColumn("RPEndDate",lit(EndDate)).withColumn("IndicatorFamily",lit(IndicaFam)).withColumn("Indicator",lit(Indicator)).withColumn("Value",lit("0.0")).withColumn("Org_Level",lit("Provider"))
  sub_org_frame = sub_org_frame.select("Org_Code", "Org_Name", "Org_Level", "RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Currency", "Value")
  
  #Add in any providers which do not otherwise appear in the data
  
  msd_count_add = msd_count_all.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all.Org_Level) &  (sub_org_frame.RPStartDate == msd_count_all.RPStartDate) &  (sub_org_frame.RPEndDate == msd_count_all.RPEndDate) &  (sub_org_frame.IndicatorFamily == msd_count_all.IndicatorFamily) &  (sub_org_frame.Indicator == msd_count_all.Indicator) & (sub_org_frame.Currency == msd_count_all.Currency), "right" ).filter(msd_count_all.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all = msd_count_all.union(msd_count_add)
  
display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.IndicatorFamily.asc(), msd_count_all.Indicator.asc(), msd_count_all.Currency.asc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Download data file from Cmd 24

# COMMAND ----------

#Suppressed figures - for measures file

if OutputType == 'measure':
  
  msd_count_denom_sup = msd_count_denom.withColumn("Value", when(col("Value").between(1,7), 5).otherwise(5 * round(col("Value") / 5)))
  
  msd_count_num_sup = msd_count_num.withColumn("Value", when(col("Value").between(1,7), 5).otherwise(5 * round(col("Value") / 5)))
  
  msd_count_rate_sup = msd_count_denom_sup.alias("n").join(msd_count_num_sup.alias("o"), (msd_count_denom_sup.Org_Code == msd_count_num_sup.Org_Code) &  (msd_count_denom_sup.Org_Level == msd_count_num_sup.Org_Level)).withColumn("Rate", 100 * col("o.Value")/ col("n.Value")).select("n.Org_Code", "n.Org_Name", "n.Org_Level", "n.RPStartDate", "n.RPEndDate", "Rate").withColumn("Value",round(col("Rate"),1)).withColumn("Currency", lit("Rate")).drop("Rate")
  
  msd_count_all_sup = msd_count_denom_sup.union(msd_count_num_sup).union(msd_count_rate_sup).withColumn("IndicatorFamily",lit(IndicaFam)).withColumn("Indicator",lit(Indicator)).select("Org_Code", "Org_Name", "Org_Level", "RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Currency", "Value")
  
  msd_count_add_sup = msd_count_all_sup.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all_sup.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all_sup.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all_sup.Org_Level) &  (sub_org_frame.RPStartDate == msd_count_all_sup.RPStartDate) &  (sub_org_frame.RPEndDate == msd_count_all_sup.RPEndDate) &  (sub_org_frame.IndicatorFamily == msd_count_all_sup.IndicatorFamily) &  (sub_org_frame.Indicator == msd_count_all_sup.Indicator) & (sub_org_frame.Currency == msd_count_all_sup.Currency), "right" ).filter(msd_count_all_sup.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all_sup = msd_count_all_sup.union(msd_count_add_sup)

  ceph_spdf = (
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
  ceph_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.ceph_data')

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
# MAGIC from $outSchema.ceph_data

# COMMAND ----------

#Unsuppressed figures - for data file

if OutputType == 'data':
  
  msd_count_denom = msd_final_combined.groupBy("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate","Breakdown").agg(count("CountUnit_ID")).withColumnRenamed("count(CountUnit_ID)","Final_value").withColumn("Count_Of",lit(OutputCount))
  msd_count_denom = msd_count_denom.withColumn("Org_Name",upper(msd_count_denom.Org_Name))
  
  msd_count_denom = msd_count_denom.filter(msd_count_denom.Org_Code != 'RYJ')
  
  msd_count_all = msd_count_denom.withColumn("Dimension", lit(Indicator)).withColumnRenamed("RPStartDate", "ReportingPeriodStartDate").withColumnRenamed("Breakdown", "Measure").withColumnRenamed("RPEndDate", "ReportingPeriodEndDate").select("ReportingPeriodStartDate", "ReportingPeriodEndDate", "Dimension", "Org_Level", "Org_Code", "Org_Name", "Measure", "Count_Of", "Final_value")
  
  
  sub_org_1 = sub_orgs.join(geography, geography.Trust_ORG == sub_orgs.FinalSubmitterCode, "left").\
            select(sub_orgs.FinalSubmitterCode, geography.Trust_Name).withColumnRenamed("Trust_Name", "Org_Name").withColumnRenamed("FinalSubmitterCode", "Org_Code").distinct()
  sub_org_1 = sub_org_1.withColumn("Org_Name",upper(sub_org_1.Org_Name))
  
  breakdown = msd_count_denom.select(msd_count_denom.Breakdown).distinct()
  
  sub_org_frame = sub_org_1.crossJoin(breakdown)
  
  sub_org_frame = sub_org_frame.withColumn("ReportingPeriodStartDate",lit(StartDate)).withColumn("ReportingPeriodEndDate",lit(EndDate)).withColumn("Dimension",lit(Indicator)).withColumn("Final_value",lit("0")).withColumn("Org_Level",lit("Provider")).withColumn("Count_Of",lit(OutputCount)).withColumnRenamed("Breakdown", "Measure")
  sub_org_frame = sub_org_frame.select("ReportingPeriodStartDate", "ReportingPeriodEndDate", "Dimension", "Org_Level", "Org_Code", "Org_Name", "Measure", "Count_Of", "Final_value")
   
  #Add in any providers which do not otherwise appear in the data
  
  msd_count_add = msd_count_all.join(sub_org_frame.alias("v"), (sub_org_frame.Org_Code == msd_count_all.Org_Code) &  (sub_org_frame.Org_Name == msd_count_all.Org_Name) &  (sub_org_frame.Org_Level == msd_count_all.Org_Level) &  (sub_org_frame.ReportingPeriodStartDate == msd_count_all.ReportingPeriodStartDate) &  (sub_org_frame.ReportingPeriodEndDate == msd_count_all.ReportingPeriodEndDate) &  (sub_org_frame.Dimension == msd_count_all.Dimension) & (sub_org_frame.Measure == msd_count_all.Measure), "right" ).filter(msd_count_all.Org_Code.isNull() == 'true').\
                select(["v.*"])
  
  msd_count_all = msd_count_all.union(msd_count_add)
  
  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.Dimension.asc(), msd_count_all.Count_Of.asc()))

# COMMAND ----------

#Suppressed figures - for data file

if OutputType == 'data':
  
  msd_count_all_sup = msd_count_all.withColumn("Final_value", when(col("Final_value").between(1,7), 5).otherwise(5 * round(col("Final_value") / 5)))
  
  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.Dimension.asc(), msd_count_all_sup.Count_Of.asc()))

# COMMAND ----------

dbutils.notebook.exit("Notebook: MSDS_Ceph_pres_Spon_birth_Publication_Code ran successfully")