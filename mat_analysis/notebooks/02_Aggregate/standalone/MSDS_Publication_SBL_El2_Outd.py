# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ###Saving Babies Lives - Element 2 Outcome Indicator d (formerly Element 2 Outcome Indicator 1)     
# MAGIC
# MAGIC Denominator: Babies born in the reporting month whose birthweight is under the 3rd centile    
# MAGIC Numerator: Babies born in the reporting month whose birthweight is under the 3rd centile and has a birth gestation of more than 37+6 weeks     
# MAGIC
# MAGIC Identification of centile:     
# MAGIC  - Primary: Provider has recorded birth weight centile in MSD405.    
# MAGIC  - Secondary: Provider has recorded birth weight (MSD405) and birth gestation (MSD401). Do not use this method to identify low-centile weight babies if the provider has recorded the birth centile directly.     
# MAGIC  
# MAGIC Reference data limitation:     
# MAGIC  - Calculations of birth weight centile use the estimated fetal weight charts published (https://journals.plos.org/plosmedicine/article?id=10.1371/journal.pmed.1002220). This only considers baby gestation in determining centiles for weight. Other more complicated models of baby / child centile exist (GROW etc). This set of data only contains values up to 40+6 weeks gestation. To avoid missing very low weight babies of 41-45 weeks gestation, this metric uses the 40 week limit for all births at 40-45 weeks. This will be an approximation for late term babies.    
# MAGIC
# MAGIC DQ concerns: Provider does not record gestation / birth centile / birth weight.
# MAGIC
# MAGIC
# MAGIC There is a known widgets issue in Databricks, where widgets may not refresh / update as expected when the Run All command is used. (And the fix requires a more recent Databricks version than the one currently - 2022/03 - in DAE). (May need to run the first cells separately, and then the rest of the notebook.) 

# COMMAND ----------

#Import necessary pyspark + other functions

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when, lit, col, round, concat, upper, max, date_add, min, regexp_replace, datediff, length, substring, floor
from pyspark.sql.types import DoubleType, IntegerType, DateType, StructType,StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from datetime import datetime

# COMMAND ----------

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

#Define MSDS tables, create dataframes

msd000 = table(dbSchema + ".msd000header")
msd001 = table(dbSchema + ".msd001motherdemog")
msd002 = table(dbSchema + ".msd002gp")
msd401 = table(dbSchema + ".msd401babydemographics")
msd405 = table(dbSchema + ".msd405careactivitybaby")

# COMMAND ----------

#Lists, dictionaries, imported reference data related to the measure
#Last updated 2023-04-21

#Set codes and units required for centile / weight
Weight_Centile_Codes_Obs = ['170005003', '248356008', '301332001', '301334000', '926111000000100', '.647.', '64k..', 'X76CL', 'Xa7wH', 'Xa7wJ', 'Xabdv']
Birth_Weight_Codes_Obs = ['364589006', '636..', '27113001', '22A..']
BirthWeightExact = ['364589006', '636..']
Weight_Units_Low = ["G"]
Weight_Units_High = ["KG", "KILOGRAMS"]


#Data is taken from https://journals.plos.org/plosmedicine/article?id=10.1371/journal.pmed.1002220 for estimated fetal weight (independent of sex) 
#And then linear interpolation to find the weight at 3rd centile
#Reference data had given weights for the 2.5nd and 5th centiles. Assuming a linear rate of change between the 2.5nd and 5th centile then the 3rd centile is worked out as follows -
#[3rd centile weight] = [2.5nd centile weight] + ( ( ( 3 - 2.5 ) / ( 5 - 2.5 ) ) * ( [5th centile weight] - [2.5nd centile weight] ) )

estimated_fetal_growth_3rdtile_table11 = [(14,70.6), (15,89.8), (16, 113.8), (17, 142.0), (18, 175.4), (19, 215.8), (20, 262.2), (21, 316.6), (22, 378.4), (23, 449.0), (24, 528.0), (25, 617.0), (26, 714.2), (27, 821.4), (28, 938.6), (29, 1064.0), (30, 1197.4), (31, 1339.6), (32, 1488.0), (33, 1642.4), (34, 1802.4), (35, 1966.0), (36, 2131.4), (37, 2298.4), (38, 2464.0), (39, 2628.8), (40, 2789.8)  ]

schema1 = StructType([ \
    StructField("GestationAge",IntegerType(),True), \
    StructField("ThirdCentile",DoubleType(),True) \
  ])
 
efw = spark.createDataFrame(data=estimated_fetal_growth_3rdtile_table11,schema=schema1)

# COMMAND ----------

#Comment out if using SQL to build measure
#Code to identify the denominator in this cell
#Output - Unique row per pregnancy+mother or baby. Must include OrgCode Provider and either (OrgSiteIDBooking or OrgSiteIDActualDelivery)

#Records from the MSD401 table
#msd401 = table("mat_pre_clear.msd401babydemographics")

#Births occurring in and recorded for the reporting month
msd401 = msd401.filter(msd401.RPStartDate.between(StartDate, EndDate)).filter(msd401.PersonBirthDateBaby.between(StartDate, EndDate))

#Ranking records - some Person_ID_Baby(s) have multiple records in the reporting month
#Prioritise the latest record (RecordNumber / MSD401_ID)
msd401 = msd401.withColumn("RankingRec", row_number().over( Window.partitionBy(msd401.Person_ID_Baby).orderBy(msd401.RecordNumber.desc(), msd401.MSD401_ID.desc())))
msd401 = msd401.filter(msd401.RankingRec == '1')


msd_denom_base = msd401.select(msd401.Person_ID_Baby, msd401.Person_ID_Mother, msd401.UniqPregID, msd401.OrgCodeProvider, msd401.OrgSiteIDActualDelivery, msd401.RecordNumber, msd401.MSD401_ID, msd401.PersonBirthDateBaby, msd401.GestationLengthBirth).withColumnRenamed("OrgSiteIDActualDelivery", "DeliverySite").distinct()


#Where the centile is directly recorded by the provider
#msd405 = table("mat_pre_clear.msd405careactivitybaby")

#Only records for babies in the cohort (born in reporting month)
msd405 = msd405.alias("a").join(msd401.alias("b"), ["Person_ID_Baby"], "inner").select(["a.*", "b.PersonBirthDateBaby", "b.GestationLengthBirth"])

#Record is from this month (or earlier); recorded by the end of the reporting month; with code stating a centile has been recorded
msd405_o = msd405.filter(msd405.RPStartDate <= StartDate).filter(msd405.ClinInterDateBaby >= msd405.PersonBirthDateBaby).filter(msd405.ClinInterDateBaby <= EndDate).filter((msd405.ObsCode.isin(Weight_Centile_Codes_Obs) == 'true')).\
            select(msd405.Person_ID_Baby, msd405.Person_ID_Mother, msd405.UniqPregID, msd405.PersonBirthDateBaby, msd405.ClinInterDateBaby, msd405.GestationLengthBirth, msd405.ObsCode, msd405.ObsValue, msd405.UCUMUnit, msd405.RecordNumber, msd405.MSD405_ID, msd405.OrgCodeProvider)

#Only keep records with ObsValue has a numeric value recorded (some records are of from <1.0 etc. and will be removed). No unit requirement.
msd405_o = msd405_o.withColumn("ObsValue_Num", msd405_o.ObsValue.cast(DoubleType())).filter("ObsValue_Num is not null")

#Only keep records with ObsValue in centile range (some records have out of scale values - 252) 
msd405_o = msd405_o.filter(msd405_o.ObsValue_Num.between(0,100))

#Flag records where centile is recorded on the birth date of the baby; how many days after centile was recorded; whether centile value is below 3 
msd405_o = msd405_o.withColumn("OnDateFlag", when(msd405_o.ClinInterDateBaby == msd405_o.PersonBirthDateBaby, '1').otherwise("0")).withColumn("DateDifference", datediff(msd405_o.ClinInterDateBaby, msd405_o.PersonBirthDateBaby)).withColumn("Under3", when(msd405_o.ObsValue_Num < 3, "1").otherwise("0"))

#Add ranking - preference to records with measurement on the day of the birth, earlier records (so nearer to birth date), latest submitted records
msd405_o = msd405_o.withColumn("BW_Ranking", row_number().over( Window.partitionBy(msd405_o.Person_ID_Baby).orderBy(msd405_o.OnDateFlag.desc(), msd405_o.DateDifference.asc(), msd405_o.RecordNumber.desc(), msd405_o.MSD405_ID.desc())))

#Filter to single record for each baby
msd405_o = msd405_o.filter("BW_Ranking = 1")

#Summarise the centile data
msd_cent = msd405_o.select(msd405_o.Person_ID_Baby, msd405_o.UniqPregID, msd405_o.Person_ID_Mother, msd405_o.PersonBirthDateBaby, msd405_o.GestationLengthBirth, msd405_o.ClinInterDateBaby, msd405_o.ObsCode, msd405_o.ObsValue_Num, msd405_o.Under3)



#Weight + gestation only for people without a centile recorded (not in msd405_o)

#Record is from this month (or earlier); recorded by the end of the reporting month; with code stating weight has been recorded
msd405_w = msd405.filter(msd405.RPStartDate <= StartDate).filter(msd405.ClinInterDateBaby >= msd405.PersonBirthDateBaby).filter(msd405.ClinInterDateBaby <= EndDate).filter((msd405.ObsCode.isin(Birth_Weight_Codes_Obs) == 'true')).\
            select(msd405.Person_ID_Baby, msd405.Person_ID_Mother, msd405.UniqPregID, msd405.PersonBirthDateBaby, msd405.ClinInterDateBaby, msd405.ObsCode, msd405.ObsValue, msd405.UCUMUnit, msd405.RecordNumber, msd405.MSD405_ID, msd405.OrgCodeProvider, msd405.RPStartDate, msd405.GestationLengthBirth)

#Only include records where the baby doesn't have a centile recorded.
msd_help = msd405_w.alias("h").join(msd405_o.alias("k"), (msd405_o.Person_ID_Baby == msd405_w.Person_ID_Baby), "left").filter(col("k.Person_ID_Baby").isNull() == 'true').select(["h.*"])

#Only keep records with ObsValue has a numeric value recorded. Format unit recording.
msd_help = msd_help.withColumn("ObsValue_Num", msd_help.ObsValue.cast(DoubleType())).filter("ObsValue_Num is not null").withColumn("UCUMUnit", upper(msd_help.UCUMUnit))

#Convert weight to grams (where originally recorded in kilograms). Only keeping weights in g/kg.
msd_help = msd_help.withColumn("Weight_Grams", when(msd_help.UCUMUnit.isin(Weight_Units_High), 1000 * msd_help.ObsValue_Num).otherwise(when(msd_help.UCUMUnit.isin(Weight_Units_Low), msd_help.ObsValue_Num))).drop(msd_help.ObsValue_Num)

#Follow birthweight logic as in other MSDS measures - either birthweight specific weight code, or generatl weight code recorded on birth date
msd_help = msd_help.filter( (msd_help.PersonBirthDateBaby == msd_help.ClinInterDateBaby) | (msd_help.ObsCode.isin(BirthWeightExact)) )

#Remove any weights without a value recorded, or negative weight value. Weights above 10kg are excluded as invalid (too high).
msd_help = msd_help.filter(msd_help.Weight_Grams.isNull() == 'false')
msd_help = msd_help.filter(msd_help.Weight_Grams >= 0).filter(msd_help.Weight_Grams <= 10000)

#Add gestation (full weeks of) column for later calculation
msd_help = msd_help.withColumn("GestWeeks", floor(msd405.GestationLengthBirth / 7))

#Add flags for birthweight recorded on date of birth; days to weight recording.
msd_help = msd_help.withColumn("OnDateFlag", when(msd_help.ClinInterDateBaby == msd_help.PersonBirthDateBaby, '1').otherwise("0")).withColumn("DateDifference", datediff(msd_help.ClinInterDateBaby, msd_help.PersonBirthDateBaby))

#Add ranking - preference to records with measurement on the day of the birth, earlier records (so nearer to birth date), latest submitted records
msd_help = msd_help.withColumn("WGT_Ranking", row_number().over( Window.partitionBy(msd_help.Person_ID_Baby).orderBy(msd_help.OnDateFlag.desc(), msd_help.DateDifference.asc(), msd_help.RecordNumber.desc(), msd_help.MSD405_ID.desc())))

#Filter to single record for each baby
msd_help = msd_help.filter("WGT_Ranking = 1")



#Join to estimated fetal weight reference data
msd_help = msd_help.withColumn("GestLimitedAge", when(msd_help.GestWeeks > 40, lit(40)).otherwise(msd_help.GestWeeks))
#msd_help = msd_help.alias("g").join(efw.alias("h"), efw.GestationAge == msd_help.GestWeeks, "left")
msd_help = msd_help.alias("g").join(efw.alias("h"), efw.GestationAge == msd_help.GestLimitedAge, "left")

#Add flag for whether a baby is under 3rd centile, at or above 3rd centile, not eligible (because there is a limited range of applicable gestations in the reference data)
#msd_help = msd_help.withColumn("Under3Centile", when(msd_help.Weight_Grams < msd_help.ThirdCentile, "1").otherwise(when(msd_help.GestWeeks.between(19,40) == 'true',"0").otherwise("99")))
msd_help = msd_help.withColumn("Under3Centile", when(msd_help.Weight_Grams < msd_help.ThirdCentile, "1").otherwise(when(msd_help.GestLimitedAge.between(19,40) == 'true',"0").otherwise("99")))

#Summarise weight data
msd_weighted = msd_help.select(msd_help.Person_ID_Baby, msd_help.UniqPregID, msd_help.Person_ID_Mother, msd_help.PersonBirthDateBaby, msd_help.ClinInterDateBaby, msd_help.GestationLengthBirth, msd_help.Weight_Grams, msd_help.Under3Centile).distinct() 



#From babies with centile (recorded or calculated)
#Which are under 3rd centile
msd_under3_cent = msd_cent.filter(msd_cent.Under3 == '1').select(msd_cent.Person_ID_Mother, msd_cent.Person_ID_Baby, msd_cent.UniqPregID)
msd_under3_weig = msd_weighted.filter(msd_weighted.Under3Centile == '1').select(msd_weighted.Person_ID_Mother, msd_weighted.Person_ID_Baby, msd_weighted.UniqPregID)
msd_under3 = msd_under3_cent.union(msd_under3_weig)



#Final output dataframe for the denominator cohort must be set to have the name msd_denom

msd_denom = msd_denom_base.alias("r").join(msd_under3.alias("p"), ["Person_ID_Baby"], "inner").select(["r.Person_ID_Baby", "r.Person_ID_Mother", "r.UniqPregID", "r.OrgCodeProvider", "r.RecordNumber", "r.DeliverySite", "r.GestationLengthBirth"])

# COMMAND ----------

#Comment out if using SQL to build measure
#Code to identify the numerator in this cell

#Final output dataframe for the numerator cohort must be set to have the name msd_num
# Identify babies in the denominaotr with a birth gestation > 37+6 weeks
msd_num_1 = msd_denom.filter(msd_denom.GestationLengthBirth > 265)

msd_num = msd_num_1.select(msd_num_1.Person_ID_Baby).distinct()

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

msd002 = msd002.withColumn("Commissioner",when(msd002.RPEndDate > STPEndDate, msd002.OrgIDSubICBLocGP).otherwise(msd002.CCGResponsibilityMother))

msd002 = msd002.withColumn("Commissioner",when(msd002.Commissioner.isNull() == 'false', msd002.Commissioner).otherwise(lit("Unknown"))).select(msd002.Person_ID_Mother, msd002.RecordNumber,"Commissioner")


#Add additional breakdowns (Local Authority)
#msd001 = table("mat_pre_clear.msd001motherdemog")
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
  org_rel_daily_inter = org_rel_daily.alias('u').join(org_rel_daily.alias('v'), col("v.Pred_Provider") == col("u.Succ_Provider"), "inner").withColumn("New_Provider", when(col("v.Succ_Provider").isNull() == 'true', col("u.Succ_Provider")).otherwise(col("v.Succ_Provider"))).select("u.Pred_Provider","New_Provider").withColumnRenamed("Pred_Provider","Old_Provider")
  org_rel_daily = org_rel_daily.join(org_rel_daily_inter, org_rel_daily.Pred_Provider == org_rel_daily_inter.Old_Provider, "left").withColumn("Succ_Provider_2", when(org_rel_daily_inter.New_Provider.isNull() == 'true', org_rel_daily.Succ_Provider).otherwise(org_rel_daily_inter.New_Provider)).select(org_rel_daily.Pred_Provider, "Succ_Provider_2").withColumnRenamed("Succ_Provider_2","Succ_Provider")
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

sub_orgs = sub_orgs.alias("h").join(org_rel_daily, (sub_orgs.SubmittingProvider == org_rel_daily.Pred_Provider), "left").withColumn("FinalSubmitterCode", when(org_rel_daily.Succ_Provider.isNotNull() == 'true', org_rel_daily.Succ_Provider).otherwise(sub_orgs.SubmittingProvider)).select(["h.*","FinalSubmitterCode"])

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
  
#  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.IndicatorFamily.asc(), msd_count_all.Indicator.asc(), msd_count_all.Currency.asc()))

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
  
#  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.Dimension.asc(), msd_count_all.Count_Of.asc()))

# COMMAND ----------

#Suppressed figures - for data file

if OutputType == 'data':
  
  msd_count_all_sup = msd_count_all.withColumn("Final_value", when(col("Final_value").between(1,7), 5).otherwise(5 * round(col("Final_value") / 5)))
  
#  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.Dimension.asc(), msd_count_all_sup.Count_Of.asc()))

# COMMAND ----------

#Convert dataframe output into a temporary view

msd_count_all_sup.createOrReplaceTempView('msd_count_all_sup')

# COMMAND ----------

sble2_o1_spdf = (
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
sble2_o1_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.{outtable}')

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
# MAGIC from $outSchema.$outtable where RPStartDate = '$RPStartDate'