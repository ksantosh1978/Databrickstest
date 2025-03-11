# Databricks notebook source
# MAGIC %md ### Spec (from Jira ticket)
# MAGIC
# MAGIC Task: As an analyst, I need to produce a final DAE code notebook that will produce publication-ready data for the agreed London Measure of Unplanned Pregnancy (LMUP) measure. This notebook should also be re-usable, and therefore able to act as a template  that can be used for other measures.
# MAGIC
# MAGIC I will base this on the measure logic finalised as part of MHA-3623.
# MAGIC
# MAGIC This notebook will contain a space for the LMUP numerator and denominator (that can be swapped out for other denominators/numerators so that the notebook can be re-used for other measures), and will also bring the resulting data together and organise and tidy it appropriately. Including:
# MAGIC
# MAGIC Applying the MSDS Suppression Rules (https://nhsd-confluence.digital.nhs.uk/display/MHA/MSDS+Suppression+Rules).
# MAGIC Using the relevant reference information to produce the necessary publication geographic breakdowns. The LMUP measure will be published as part of the "Measures" file but the template notebook should have the ability to produce the extra geographic breakdowns used in the "CSV Data" file. For future-proofing. 
# MAGIC The data should come out the notebook in a publication-ready format, to be added into the relevant CSV file (as in https://digital.nhs.uk/data-and-information/publications/statistical/maternity-services-monthly-statistics).
# MAGIC
# MAGIC Notes: Alongside the publication pipeline code itself; there are some existing useful examples of publication code to refer to. They include :- 
# MAGIC
# MAGIC Workspaces > mat_clear_collab > CoC > New_DQ_MetricWork > multiple notebooks
# MAGIC
# MAGIC Workspaces > mat_clear_collab > BMI by 14 weeks > "Updated_14WeeksBMI_Geog"
# MAGIC
# MAGIC Peer review: Turing analyst
# MAGIC
# MAGIC Definition of done:  Final publication-ready LMUP DAE notebook has been built, reviewed, and is ready to use to produce publication data.  It has been constructed so it can also form a re-usable 'template' for other measures.  Product owner (Kate) has been notified of notebook completion.

# COMMAND ----------

# MAGIC %md #### Build ticket (https://db.core.data.digital.nhs.uk/#notebook/12899145/command/12899160)

# COMMAND ----------

# MAGIC %md ##### LMUP
# MAGIC
# MAGIC Notes:     
# MAGIC Denominator = Women/pregnancies with a booking date in the reporting period (month)    
# MAGIC Numerator = Women/pregnancies with a booking date in the reporting period (month) and a LMUP assessment completed by the end of that month.     
# MAGIC
# MAGIC No other date restrictions: This will include LMUP assessments recorded after booking if the assessment and booking took place in the same month - i.e. Booking = 12th Sep 2022; LMUP = 28th Sep 2022.

# COMMAND ----------

# MAGIC %md ### Template
# MAGIC
# MAGIC To use for other MSDS measures / data breakdowns     
# MAGIC
# MAGIC For all measures:     
# MAGIC  - Cmd7-9 - Sets widget values to define whether the metric is a measures/data file output; name (and where appropriate the family) of the metric; start and end of the reporting period; database and table where the output should be stored; whether the metric is counting in terms of women (mother+pregnancy) or babies       
# MAGIC
# MAGIC For measures originally coded in SQL:    
# MAGIC  - Cmd10 - Defines denominator cohort   
# MAGIC  - Cmd11 - Defines numerator cohort     
# MAGIC  - Cmd12 - Converts cohort views into dataframes (msd_denom and msd_num)     
# MAGIC         
# MAGIC For measures originally coded in python:     
# MAGIC  - Cmd13 - Defines denominator cohort (msd_denom)     
# MAGIC  - Cmd14 - Defines numerator cohort (msd_num)    
# MAGIC
# MAGIC Comment out / delete the notebook cells Cmd10-12 or Cmd13-14 which are not relevant to the coding language that's being used.    
# MAGIC      
# MAGIC       
# MAGIC      
# MAGIC #####Final denominator dataframe should include OrgCodeProvider, (UniqPregID + Person_ID_Mother or Person_ID_Baby), (BookingSite or DeliverySite), RecordNumber and should have 1 row per (UniqPregID + Person_ID_Mother or Person_ID_Baby) and any breakdown (as used in data_csv) included as field = Breakdown     
# MAGIC #####Final numerator dataframe should include the (UniqPregID + Person_ID_Mother or Person_ID_Baby) identifiers as Num_XYZ and should include 1 row per Num_(UniqPregID + Person_ID_Mother or Person_ID_Baby). The numerator cohort does not need to contain organisational data (OrgCodeProvider etc.) which is already given in the denominator cohort.
# MAGIC  
# MAGIC Other cells of the notebook add geographical data, calculate the metric (counts & rates for measure; counts for data) and formats and outputs the suppressed data into the data table.     
# MAGIC
# MAGIC For data metrics the cohort is assumed to be defined by the msd_denom only.

# COMMAND ----------

#Import necessary pyspark + other functions

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when, lit, col, round, concat, upper, max
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
 

# COMMAND ----------

#Remove widgets 
#dbutils.widgets.removeAll();

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
#dss_corporate = dbutils.widgets.get("dss_corporate")
#outschema = dbutils.widgets.get("outschema")
#outtable = dbutils.widgets.get("outtable")
#geog_schema = dbutils.widgets.get("outschema_geog")


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
# MAGIC --For SQL defined measures: Define denominator and create view (one row per count-unit with appropriate provider, etc.)
# MAGIC
# MAGIC --CREATE OR REPLACE GLOBAL TEMPORARY VIEW Denominator_Cohort AS
# MAGIC --SELECT *
# MAGIC --FROM
# MAGIC --(
# MAGIC --    select Person_ID_Mother, UniqPregID, OrgCodeProvider, RecordNumber, RPStartDate, RPEndDate, AntenatalAppDate, coalesce(upper(OrgSiteIDBooking),'Unknown') BookingSite, row_number() over(partition by Person_ID_Mother, UniqPregID order by AntenatalAppDate asc, RecordNumber desc) Ranking
# MAGIC --    from mat_pre_clear.msd101pregnancybooking
# MAGIC --    where RPStartDate = '$RPStartDate' AND AntenatalAppDate BETWEEN '$RPStartDate' AND '$RPEndDate' --and UniqPregID = '1443dab5900746e99fef89e2d25bf582'
# MAGIC --) W
# MAGIC --WHERE Ranking = '1';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --For SQL defined measures: Define numerator cohort and create view (one row per count-unit)
# MAGIC
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW MSD203_Cohort AS
# MAGIC --SELECT *
# MAGIC --FROM
# MAGIC --(
# MAGIC --    select a.Person_ID_Mother, a.UniqPregID, a.OrgCodeProvider, a.AntenatalAppDate, b.CContactDate, d.UniqSubmissionID, d.ToolTypeSNOMED, CAST(d.Score as float) Score_Number, d.RPStartDate, d.OrgCodeProvider LMUPProvider, d.RecordNumber LMUPRecordNumber, row_number() over (partition by d.Person_ID_Mother, d.UniqPregID order by b.CContactDate asc, d.UniqSubmissionID asc, d.MSD203_ID asc) Ranking
# MAGIC --    from global_temp.Denominator_Cohort a
# MAGIC --    inner join mat_pre_clear.msd201carecontactpreg b on b.UniqPregID = a.UniqPregID AND a.Person_ID_Mother = b.Person_ID_Mother
# MAGIC --    inner join mat_pre_clear.msd202careactivitypreg c on c.UniqPregID = b.UniqPregID AND c.Person_ID_Mother = b.Person_ID_Mother AND c.CareConID = b.CareConID AND c.RecordNumber = b.RecordNumber 
# MAGIC --    inner join mat_pre_clear.msd203codedscoreasscontact d on d.UniqPregID = c.UniqPregID AND d.Person_ID_Mother = c.Person_ID_Mother AND d.CareActIDMother = c.CareActIDMother AND d.RecordNumber = c.RecordNumber
# MAGIC --    where b.RPStartDate <= '$RPStartDate' and d.ToolTypeSNOMED == '145041000000108' and CAST(Score AS FLOAT) BETWEEN 0 AND 12
# MAGIC --)P
# MAGIC --WHERE Ranking = '1';
# MAGIC
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW MSD104_Cohort AS
# MAGIC --SELECT *
# MAGIC --FROM
# MAGIC --(
# MAGIC --    select a.Person_ID_Mother, a.UniqPregID, a.OrgCodeProvider, a.AntenatalAppDate, b.CompDate, b.UniqSubmissionID, b.ToolTypeSNOMED, CAST(b.Score as float) Score_Number, b.RPStartDate, b.OrgCodeProvider LMUPProvider, b.RecordNumber LMUPRecordNumber, row_number() over (partition by b.Person_ID_Mother, b.UniqPregID order by b.CompDate asc, b.UniqSubmissionID asc, b.MSD104_ID asc) Ranking
# MAGIC --    from global_temp.Denominator_Cohort a
# MAGIC --    inner join mat_pre_clear.msd104codedscoreasspreg b on b.UniqPregID = a.UniqPregID AND a.Person_ID_Mother = b.Person_ID_Mother
# MAGIC --    where b.RPStartDate <= '$RPStartDate' and b.ToolTypeSNOMED == '145041000000108' and CAST(Score AS FLOAT) BETWEEN 0 AND 12
# MAGIC --)P
# MAGIC --WHERE Ranking = '1';
# MAGIC
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW Combined_Cohort AS
# MAGIC --SELECT *
# MAGIC --FROM MSD104_Cohort
# MAGIC --UNION
# MAGIC --SELECT *
# MAGIC --FROM MSD203_Cohort;
# MAGIC
# MAGIC
# MAGIC --CREATE OR REPLACE GLOBAL TEMPORARY VIEW Numerator_Cohort AS
# MAGIC --SELECT LMUPProvider, UniqPregID AS Num_UniqPregID, Person_ID_Mother AS Num_Person_ID_Mother, LMUPRecordNumber, ToolTypeSNOMED, CompDate, Score_Number, UniqSubmissionID, Ranking_Overall
# MAGIC --FROM
# MAGIC --(
# MAGIC --    SELECT *, ROW_NUMBER() OVER (PARTITION BY Person_ID_Mother, UniqPregID ORDER BY CompDate ASC, UniqSubmissionID ASC) Ranking_Overall
# MAGIC --    FROM Combined_Cohort
# MAGIC --)P
# MAGIC --WHERE Ranking_Overall = '1'

# COMMAND ----------

#For SQL defined measures: Converts the denominator and numerator cohort views into dataframes

#msd_denom = table("global_temp.Denominator_Cohort")
#msd_num = table("global_temp.Numerator_Cohort")

# COMMAND ----------

#Code to identify the denominator in this cell
#Output - Unique row per pregnancy+mother or baby. Must include OrgCode Provider and either (OrgSiteIDBooking or OrgSiteIDActualDelivery)

#For LMUP - antenatal booking in the current reporting period:

#Look in booking table

msd101table = dbSchema + ".msd101pregnancybooking"
print(msd101table)
msd101 = table(msd101table)
#msd101 = table(dbSchema + ".msd101pregnancybooking")

#Find bookings in the current reporting month
msd101 = msd101.filter(msd101.RPStartDate == StartDate).filter(msd101.AntenatalAppDate.between(StartDate,EndDate))

#Select only the relevant columns from MSD101
msd101 = msd101.select(msd101.Person_ID_Mother, msd101.UniqPregID, msd101.OrgCodeProvider, msd101.RecordNumber, msd101.RPStartDate, msd101.RPEndDate, msd101.AntenatalAppDate, msd101.MSD101_ID, msd101.OrgSiteIDBooking).withColumn("OrgSiteIDBooking", when(msd101.OrgSiteIDBooking.isNull() == 'false', upper(msd101.OrgSiteIDBooking)).otherwise(lit("Unknown"))).withColumnRenamed("OrgSiteIDBooking", "BookingSite")

#Find the latest record of the earliest antenatal booking date (as 'the best' record of the first antenatal booking )
#Added in MSD101_ID as a tie breaker
msd101 = msd101.withColumn("Ranking", row_number().over( Window.partitionBy(msd101.Person_ID_Mother, msd101.UniqPregID).orderBy(msd101.AntenatalAppDate.asc(), msd101.RecordNumber.desc(), msd101.MSD101_ID.desc() )))
msd101 = msd101.filter(msd101.Ranking == '1')
msd101 = msd101.drop("MSD101_ID")

#Set output as denominator cohort
msd_denom = msd101


# COMMAND ----------

#Code to identify the numerator in this cell

#For LMUP - antenatal booking in the current reporting period (with LMUP recorded score in reporting period)

#Look in the care contact table for records associated with people/pregnancies in the denominator cohort
#msd201 = table("mat_pre_clear.msd201carecontactpreg")
msd201 = table(dbSchema + ".msd201carecontactpreg")

msd201 = msd201.alias("a").join(msd_denom, (msd_denom.UniqPregID == msd201.UniqPregID) & (msd_denom.Person_ID_Mother == msd201.Person_ID_Mother), "inner" ).select(["a.*"])\

#Only allow records from reporting month or before
msd201 = msd201.filter(msd201.RPStartDate <= StartDate)

#Take the relevant fields from MSD201
msd201 = msd201.select(msd201.UniqPregID, msd201.Person_ID_Mother,  msd201.CareConID,  msd201.RecordNumber, msd201.CContactDate, msd201.RPStartDate)


#Look in the care activity table 
#msd202 = table("mat_pre_clear.msd202careactivitypreg")

msd202 = table(dbSchema + ".msd202careactivitypreg")
msd202 = msd202.alias("b").join(msd201, (msd201.UniqPregID == msd202.UniqPregID) & (msd202.Person_ID_Mother == msd201.Person_ID_Mother) & (msd202.CareConID == msd201.CareConID) & (msd202.RecordNumber == msd201.RecordNumber), "inner" ).select(["b.*", msd201.CContactDate])\

#Take the relevant fields from MSD202
msd202 = msd202.select(msd202.UniqPregID, msd202.Person_ID_Mother, msd202.CareConID, msd202.RecordNumber, msd202.CareActIDMother, msd202.RPStartDate, msd202.CContactDate)


#Look in the care assessment table 
#msd203 = table("mat_pre_clear.msd203codedscoreasscontact")

msd203 = table(dbSchema + ".msd203codedscoreasscontact")


msd203 = msd203.alias("c").join(msd202, (msd203.UniqPregID == msd202.UniqPregID) & (msd202.Person_ID_Mother == msd203.Person_ID_Mother) & (msd202.CareActIDMother == msd203.CareActIDMother) & (msd202.RecordNumber == msd203.RecordNumber), "inner" ).select(["c.*", msd202.CContactDate])\

#Take the relevant fields from MSD203
msd203 = msd203.select(msd203.OrgCodeProvider, msd203.UniqPregID, msd203.Person_ID_Mother, msd203.CareActIDMother, msd203.RecordNumber,msd203.RPStartDate, msd203.Score, msd203.ToolTypeSNOMED, msd203.CContactDate, msd203.UniqSubmissionID).withColumn("Score_Numeric", msd203.Score.cast(DoubleType()).alias("Score"))

#Find records for LMUP assessment
msd203 = msd203.filter(msd203.ToolTypeSNOMED == '145041000000108')
msd203 = msd203.filter(msd203.Score_Numeric.between(0,12))

#Find the earliest test recorded
msd203 = msd203.withColumn("Ranking", row_number().over( Window.partitionBy(msd203.Person_ID_Mother, msd203.UniqPregID).orderBy(msd203.CContactDate.asc(), msd203.UniqSubmissionID.asc() )))
msd203 = msd203.filter(msd203.Ranking == '1')

#Look in the care contact table for records associated with people/pregnancies in the denominator cohort
msd104 = table(dbSchema + ".msd104codedscoreasspreg")

msd104 = msd104.alias("a").join(msd_denom, (msd_denom.UniqPregID == msd104.UniqPregID) & (msd_denom.Person_ID_Mother == msd104.Person_ID_Mother), "inner" ).select(["a.*"])\

#Only allow records from reporting month or before
msd104 = msd104.filter(msd104.RPStartDate <= StartDate)

#Take the relevant fields from MSD104
msd104 = msd104.select(msd104.OrgCodeProvider, msd104.UniqPregID, msd104.Person_ID_Mother, msd104.RecordNumber, msd104.ToolTypeSNOMED, msd104.Score, msd104.CompDate, msd104.RPStartDate, msd104.UniqSubmissionID).withColumn("Score_Numeric", msd104.Score.cast(DoubleType()).alias("Score"))

#Find records for LMUP assessment
msd104 = msd104.filter(msd104.ToolTypeSNOMED == '145041000000108')
msd104 = msd104.filter(msd104.Score_Numeric.between(0,12))

#Find the earliest test recorded
msd104 = msd104.withColumn("Ranking", row_number().over( Window.partitionBy(msd104.Person_ID_Mother, msd104.UniqPregID).orderBy(msd104.CompDate.asc(), msd104.UniqSubmissionID.asc() )))
msd104 = msd104.filter(msd104.Ranking == '1')


#Combine scores from MSD203 and MSD104
msd_score1 = msd104.select(msd104.OrgCodeProvider, msd104.UniqPregID, msd104.Person_ID_Mother, msd104.RecordNumber, msd104.ToolTypeSNOMED, msd104.CompDate, msd104.Score_Numeric, msd104.UniqSubmissionID)
msd_score1 = msd_score1.withColumnRenamed("OrgCodeProvider","LMUPProvider").withColumnRenamed("UniqPregID","Num_UniqPregID").withColumnRenamed("Person_ID_Mother","Num_Person_ID_Mother").withColumnRenamed("RecordNumber","LMUPRecordNumber")
msd_score2 = msd203.select(msd203.OrgCodeProvider, msd203.UniqPregID, msd203.Person_ID_Mother, msd203.RecordNumber, msd203.ToolTypeSNOMED, msd203.CContactDate, msd203.Score_Numeric, msd203.UniqSubmissionID)

msd_score = msd_score1.union(msd_score2)

#Find the earliest test recorded (in case results are recorded in both tables)
msd_score = msd_score.withColumn("Ranking", row_number().over( Window.partitionBy(msd_score.Num_Person_ID_Mother, msd_score.Num_UniqPregID).orderBy(msd_score.CompDate.asc(), msd_score.UniqSubmissionID.asc() )))
msd_score = msd_score.filter(msd_score.Ranking == '1')

msd_num = msd_score

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
msd002 = table(dbSchema + ".msd002gp")
msd002 = msd002.alias("p").join(msd_denom, (msd_denom.Person_ID_Mother == msd002.Person_ID_Mother) & (msd_denom.RecordNumber == msd002.RecordNumber), "inner" ).select(["p.*"]).distinct()

msd002 = msd002.filter(msd002.RPEndDate == EndDate).filter((msd002.EndDateGMPReg.isNull() == 'true') | ((msd002.StartDateGMPReg.isNull() == 'true') & (msd002.EndDateGMPReg > EndDate) ) | ((msd002.StartDateGMPReg < EndDate) & (msd002.EndDateGMPReg > EndDate)))

msd002 = msd002.withColumn("Commissioner",when(msd002.RPEndDate > STPEndDate, msd002.OrgIDSubICBLocGP).otherwise(msd002.CCGResponsibilityMother))

msd002 = msd002.withColumn("Commissioner",when(msd002.Commissioner.isNull() == 'false', msd002.Commissioner).otherwise(lit("Unknown"))).select(msd002.Person_ID_Mother, msd002.RecordNumber,"Commissioner")


#Add additional breakdowns (Local Authority)
msd001 = table(dbSchema + ".msd001motherdemog")
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

org_daily = table(dss_corporate + ".org_daily")
org_daily = org_daily. withColumn("Flag1", lit(EndDate))

org_name_list = org_daily.filter(org_daily.BUSINESS_START_DATE <= org_daily.Flag1).filter( (org_daily.ORG_CLOSE_DATE > org_daily.Flag1 ) | (org_daily.BUSINESS_END_DATE > org_daily.Flag1) | (org_daily.BUSINESS_END_DATE.isNull() == 'true')).select(org_daily.ORG_CODE, org_daily.ORG_TYPE_CODE, org_daily.NAME, org_daily.SHORT_NAME).withColumnRenamed("NAME","ORG_NAME").withColumnRenamed("SHORT_NAME","ORG_SHORT_NAME").distinct()

org_lsoa_name = table(dss_corporate + ".ons_chd_geo_listings")


org_lsoa_name = org_lsoa_name. withColumn("Flag2", lit(EndDate))

#org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DSS_RECORD_START_DATE <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DSS_RECORD_END_DATE > org_lsoa_name.Flag2 ) | (org_lsoa_name.DSS_RECORD_END_DATE.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.LADNM)).select(org_lsoa_name.LADCD, "ORG_TYPE", upper(org_lsoa_name.LADNM),"SHORT_NAME" ).distinct()

org_lsoa_name = org_lsoa_name.filter(org_lsoa_name.DATE_OF_OPERATION <= org_lsoa_name.Flag2).filter( (org_lsoa_name.DATE_OF_TERMINATION > org_lsoa_name.Flag2 ) | (org_lsoa_name.DATE_OF_TERMINATION.isNull() == 'true')).withColumn("ORG_TYPE",lit('LocA')).withColumn("SHORT_NAME",upper(org_lsoa_name.GEOGRAPHY_NAME)).select(org_lsoa_name.GEOGRAPHY_CODE, "ORG_TYPE", upper(org_lsoa_name.GEOGRAPHY_NAME),"SHORT_NAME" ).distinct()


org_name_list = org_name_list.union(org_lsoa_name)

#display(org_name_list)

# COMMAND ----------

#Find all the providers submitting to MSDS in reporting period

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
  
  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.IndicatorFamily.asc(), msd_count_all.Indicator.asc(), msd_count_all.Currency.asc()))

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
  
  display(msd_count_all.orderBy(msd_count_all.Org_Level.asc(), msd_count_all.Org_Code.asc(), msd_count_all.Dimension.asc(), msd_count_all.Count_Of.asc()))

# COMMAND ----------

#Suppressed figures - for data file

if OutputType == 'data':
  
  msd_count_all_sup = msd_count_all.withColumn("Final_value", when(col("Final_value").between(1,7), 5).otherwise(5 * round(col("Final_value") / 5)))
  
  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.Dimension.asc(), msd_count_all_sup.Count_Of.asc()))

# COMMAND ----------

#Convert dataframe output into a temporary view

msd_count_all_sup.createOrReplaceTempView('msd_count_all_sup')

# COMMAND ----------

lmup_spdf = (
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
lmup_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.lmup_data')

# COMMAND ----------

#Create the storage table (if it does not already exist)
#Clear the table of old results for the metric / month

# if OutputType == 'measure':
#   string = "CREATE TABLE IF NOT EXISTS " + outSchema + "." + outtable + " (Org_Code STRING, Org_Name STRING, Org_Level STRING, RPStartDate DATE, RPEndDate DATE, IndicatorFamily STRING, Indicator STRING, Currency STRING, Value STRING) USING DELTA"
#   spark.sql(string)
#   print(string)
  
#   string2 = "DELETE FROM " + outSchema + "." + outtable + " WHERE RPStartDate = '" + StartDate + "' AND IndicatorFamily = '" + IndicaFam + "' AND Indicator = '" + Indicator + "'"  
#   spark.sql(string2)
  
  
# if OutputType == 'data':
#   string = "CREATE TABLE IF NOT EXISTS " + outSchema + "." + outtable + " (ReportingPeriodStartDate DATE, ReportingPeriodEndDate DATE, Dimension STRING, Org_Level STRING, Org_Code STRING, Org_Name STRING, Measure STRING, Count_Of STRING, Final_value STRING) USING DELTA"
#   spark.sql(string)
#   print(string)
  
#   otherstring = "DELETE FROM " + outSchema + "." + outtable + " WHERE ReportingPeriodStartDate = '" + StartDate + "' AND Dimension = '" + Indicator + "'"
#   spark.sql(otherstring)
#   print(otherstring)

# COMMAND ----------

#Check the format of the table is correct 
#Then store the output

# string3 = outSchema + "." + outtable
# outputform = table(string3)

# if outputform.columns == msd_count_all_sup.columns:
#   string4 = "INSERT INTO " + outSchema + "." + outtable + " SELECT * FROM msd_count_all_sup"
#   spark.sql(string4)
# else:
#   print("Table is not in the appropriate format. Metric was not loaded.")

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
# MAGIC from $outSchema.lmup_data where RPStartDate = '$RPStartDate'