# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #####Measure Definition     
# MAGIC  - Reporting Period: The month prior to the current month (i.e. current/latest month is Dec 2022, measure is for people with a pregnancy at 36 weeks in Nov 2022)     
# MAGIC  - Denominator: Anyone with a pregnancy reaching 36 weeks in reporting period; pregnancy was not discharged before 36 weeks; pregnancy was not booked after 36 weeks.     
# MAGIC  - Numerator: Anyone in the denominator with a booking CO reading <4 ppm, and a 36 weeks CO reading <4 ppm (where reading was +/- 7 days of date); with no evidence of smoking (cigarette consumption > 0 or recorded smoking status as current smoker) in either the booking or 36 week window.
# MAGIC  
# MAGIC  - Enter current month (e.g. Dec-2022) and set MonthShift as -1 to take activity found for 36 weeks gestation in the previous month (e.g.Nov-2022). Final output will show under Dec-2022 (as is for CQIMs reporting on previous month periods). Code amended so joins to MSD002 etc. tables are to record month etc.

# COMMAND ----------

# Uncomment if running the notebook in silo for testing purpose
# dbutils.widgets.text("RPStartDate","2023-01-01","RPStartDate")
# dbutils.widgets.text("RPEnddate","2023-01-31","RPEnddate")
# dbutils.widgets.text("IndicatorFamily","Pregnancy","IndicatorFamily")
# dbutils.widgets.text("Indicator","Smokefree_pregnancy","Indicator")
# dbutils.widgets.text("OutputType","measure","OutputType")
# dbutils.widgets.text("OutputCount","Women","OutputCount")
# dbutils.widgets.text("dss_corporate","dss_corporate","dss_corporate")
# dbutils.widgets.text("outSchema","mat_analysis","outSchema")
# dbutils.widgets.text("outtable","smokefreepreg_csv","outtable")
# dbutils.widgets.text("dbSchema","testdata_mat_analysis_mat_pre_clear","dbSchema")


# COMMAND ----------

#Import necessary pyspark + other functions

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when, lit, col, round, concat, upper, max, date_add, min, regexp_replace, datediff, length, substring
from pyspark.sql.types import DoubleType, IntegerType, DateType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from datetime import datetime

# COMMAND ----------

#Set up widgets for measure
#Update for the name of the Indicator Family / Indicator / Output table (personal database) details

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
msd101_all = table(dbSchema + ".msd101pregnancybooking")
msd109_all = table(dbSchema + ".msd109findingobsmother")
msd201_all = table(dbSchema + ".msd201carecontactpreg")
msd202_all = table(dbSchema + ".msd202careactivitypreg")
msd302_all = table(dbSchema + ".msd302careactivitylabdel")

# COMMAND ----------

#Comment out if using SQL to build measure
#Code to identify the denominator in this cell

#Code added for reporting/current month difference
#This creates the variables for the reporting period start and end (of the previous month)
#strptime amends the StartDate string to a number format, to allow the arimethic operation (subtraction of 1 month)
#relativedelta allows the arithmetic operation (subtraction of 1 month)

#MonthPlus = datetime.strptime(StartDate, "%Y-%m-%d") + relativedelta(months=int(MonthShifted))
#MonthPlusEnd = datetime.strptime(StartDate, "%Y-%m-%d") + relativedelta(months=int(MonthShifted)+1) + relativedelta(days=-1)
MonthPlus = datetime.strptime(StartDate, "%Y-%m-%d") + relativedelta(months=-1)
MonthPlusEnd = datetime.strptime(StartDate, "%Y-%m-%d") + relativedelta(days=-1)

#Add in strings for current month (will be used in final outputs)
#SL 29/06/2023 - commented out as these variables aren't being used anywhere else in the code
#OutputMonthStart = MonthPlus.strftime("%Y-%m-%d")
#OutputMonthEnd = MonthPlusEnd.strftime("%Y-%m-%d")

#Output - Unique row per pregnancy+mother or baby. Must include OrgCode Provider and either (OrgSiteIDBooking or OrgSiteIDActualDelivery)
#All booking records
#msd101_all = table("mat_pre_clear.msd101pregnancybooking")

#With valid gestation recorded; recorded by reporting month
msd101 = msd101_all.filter(msd101_all.RPStartDate <= MonthPlus).filter(msd101_all.GestAgeBooking > 0)

#Take relevant columns from the booking table
msd101 = msd101.select(msd101.Person_ID_Mother, msd101.UniqPregID, msd101.OrgCodeProvider, msd101.DischargeDateMatService, msd101.RPStartDate, msd101.RPEndDate, msd101.RecordNumber, msd101.AntenatalAppDate, msd101.OrgSiteIDBooking, msd101.GestAgeBooking, msd101.MSD101_ID)
msd101 = msd101.withColumn("OrgSiteIDBooking", upper(msd101.OrgSiteIDBooking)).withColumn("GestAgeBooking", msd101.GestAgeBooking.cast(DoubleType()))

#Add date when pregnancy reached 36 weeks (252 days)
msd101 = msd101.withColumn("Date36Weeks", msd101.AntenatalAppDate + 252 - msd101.GestAgeBooking.cast(IntegerType()))

#Add ranking for booking records; prioritise latest booking date (AntenatalAppDate), then latest submitted MSD101 record (RecordNumber, MSD101_ID)
msd101 = msd101.withColumn("RankingRec", row_number().over( Window.partitionBy(msd101.Person_ID_Mother, msd101.UniqPregID).orderBy(msd101.AntenatalAppDate.desc(), msd101.RecordNumber.desc(), msd101.MSD101_ID.desc())))

#Keep latest record; pregnancy reached 36 weeks in reporting period; pregnancy not discharged before 36 weeks
msd101 = msd101.filter(msd101.RankingRec == 1).filter(msd101.Date36Weeks.between(MonthPlus,MonthPlusEnd)).filter((msd101.Date36Weeks < msd101.DischargeDateMatService) | (msd101.DischargeDateMatService.isNull() == 'true'))


#Above code identifies some pregnancies which were booked after 36 weeks if the pregnancy was booked in the month = i.e. pregnancy booked on 21st June (at 38 weeks) would be identified for June.

#Find all booking records for people in the above group of pregnancies; recorded by reporting month
msd101_exc = msd101.alias("a").join(msd101_all.alias("b"), ["Person_ID_Mother", "UniqPregID"], "inner").withColumn("CheckingDate",col("b.AntenatalAppDate")).filter(col("b.RPStartDate") <= MonthPlus).select(["a.*","CheckingDate"])

#Find earliest booking date for each pregnancy
msd101_exc_group = msd101_exc.groupBy(msd101_exc.Person_ID_Mother, msd101_exc.UniqPregID).agg(min(msd101_exc.CheckingDate)).withColumnRenamed("min(CheckingDate)","EarliestBookingDate")

#Find pregnancies where the first booking date is after the pregnancy reached 36 weeks
exclusions = msd101_exc_group.alias("c").join(msd101, ["Person_ID_Mother", "UniqPregID"], "inner").filter(msd101.Date36Weeks < msd101_exc_group.EarliestBookingDate).select(["c.*",msd101.Date36Weeks])

#Exclude pregnancies recorded after 36 weeks
msd101_in = msd101.alias("d").join(exclusions.alias("e"),  (col("e.Person_ID_Mother") == col("d.Person_ID_Mother")) & (col("e.UniqPregID") == col("d.UniqPregID")) , "left").filter(col("e.Person_ID_Mother").isNull() == 'true').select(["d.*"])


#Set up denominator cohort to feed template output

msd_denom = msd101_in.select(msd101_in.Person_ID_Mother, msd101_in.UniqPregID, msd101_in.OrgCodeProvider, msd101_in.OrgSiteIDBooking, msd101.RecordNumber).withColumnRenamed("OrgSiteIDBooking","BookingSite").distinct()
                             

# COMMAND ----------

#Comment out if using SQL to build measure
#Code to identify the numerator in this cell


#Codes for CO monitoring
#CO_Reading_Codes = ['251900003','.4I90','4I90.','X77Qd']
CO_Reading_Codes = ['251900003'] #, '413753009'

CO_Unit_Codes = ['COPPM','PPM'] #A number of records are recorded with other units (PPM = 52K, COPPM = 73K, COPPM² = 20K, COPPMÂ² = 2K  in MSD202, Dec 2022)

#All MSD109 records
#msd109_all = table("mat_pre_clear.msd109findingobsmother")

#With codes for CO reading; recorded by reporting month+1
msd109 = msd109_all.filter(msd109_all.RPStartDate <= StartDate).filter(msd109_all.ObsCode.isin(CO_Reading_Codes))
#With a CO value of greater than or equal to 0
msd109 = msd109.withColumn("ObsValue_Num", msd109.ObsValue.cast(DoubleType())).filter("ObsValue_Num >= '0'")
#In units of PPM / COPPM
msd109 = msd109.withColumn("Unit_Clean", regexp_replace(upper(msd109.UCUMUnit)," ","")).withColumn("Unit_Clean1", substring("Unit_Clean", 0, 5)).withColumn("Unit_Clean2", substring("Unit_Clean1", -3, 3)).drop("Unit_Clean")
msd109 = msd109.filter((msd109.Unit_Clean1.isin(CO_Unit_Codes)) | (msd109.Unit_Clean2.isin(CO_Unit_Codes)))

#For pregnancies in the denominator cohort
msd109 = msd109.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(msd109.Person_ID_Mother, msd109.UniqPregID, msd109.OrgCodeProvider, msd109.ObsCode, msd109.ObsDate, msd109.ObsValue_Num, msd109.UCUMUnit, msd109.RecordNumber, msd109.MSD109_ID).withColumnRenamed("ObsDate","COReadingDate").withColumnRenamed("OrgCodeProvider","COReadingProvider").withColumnRenamed("RecordNumber","COReadingRecordNumber")

#msd109 = msd109.filter("Person_ID_Mother = -1")

#All MSD302 records
#msd302_all = table("mat_pre_clear.msd302careactivitylabdel")

#With codes for CO reading; recorded by reporting month+1
msd302 = msd302_all.filter(msd302_all.RPStartDate <= StartDate).filter(msd302_all.ObsCode.isin(CO_Reading_Codes))
#With a CO value of greater than or equal to 0
msd302 = msd302.withColumn("ObsValue_Num", msd302.ObsValue.cast(DoubleType())).filter("ObsValue_Num >= '0'")
#In units of PPM / COPPM
msd302 = msd302.withColumn("Unit_Clean", regexp_replace(upper(msd302.UCUMUnit)," ","")).withColumn("Unit_Clean1", substring("Unit_Clean", 0, 5)).withColumn("Unit_Clean2", substring("Unit_Clean1", -3, 3)).drop("Unit_Clean")
msd302 = msd302.filter((msd302.Unit_Clean1.isin(CO_Unit_Codes)) | (msd302.Unit_Clean2.isin(CO_Unit_Codes)))

#For pregnancies in the denominator cohort
msd302 = msd302.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(msd302.Person_ID_Mother, msd302.UniqPregID, msd302.OrgCodeProvider, msd302.ObsCode, msd302.ClinInterDateMother, msd302.ObsValue_Num, msd302.UCUMUnit, msd302.RecordNumber, msd302.MSD302_ID).withColumnRenamed("ClinInterDateMother", "COReadingDate").withColumnRenamed("OrgCodeProvider", "COReadingProvider").withColumnRenamed("RecordNumber","COReadingRecordNumber")


#All MSD201/2 records
#msd201_all = table("mat_pre_clear.msd201carecontactpreg")
#msd202_all = table("mat_pre_clear.msd202careactivitypreg")

#With codes for CO reading; recorded by reporting month+1
msd202 = msd202_all.filter(msd202_all.RPStartDate <= StartDate).filter(msd202_all.ObsCode.isin(CO_Reading_Codes))
#With a CO value of greater than or equal to 0
msd202 = msd202.withColumn("ObsValue_Num", msd202.ObsValue.cast(DoubleType())).filter("ObsValue_Num >= '0'")
#In units of PPM / COPPM
msd202 = msd202.withColumn("Unit_Clean", regexp_replace(upper(msd202.UCUMUnit)," ","")).withColumn("Unit_Clean1", substring("Unit_Clean", 0, 5)).withColumn("Unit_Clean2", substring("Unit_Clean1", -3, 3)).drop("Unit_Clean")
msd202 = msd202.filter((msd202.Unit_Clean1.isin(CO_Unit_Codes)) | (msd202.Unit_Clean2.isin(CO_Unit_Codes)))

#For pregnancies in the denominator cohort
msd201_202 = msd202.alias("f").join(msd201_all, ["Person_ID_Mother","UniqPregID", "RecordNumber", "CareConID"], "inner").select(msd202.Person_ID_Mother, msd202.UniqPregID, msd202.RecordNumber, msd202.CareConID, msd202.MSD202_ID, msd202.OrgCodeProvider, msd202.ObsCode, msd202.ObsValue_Num, msd202.UCUMUnit, msd201_all.CContactDate)

msd201_202 = msd201_202.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(msd201_202.Person_ID_Mother, msd201_202.UniqPregID, msd201_202.OrgCodeProvider, msd201_202.ObsCode, msd201_202.CContactDate, msd201_202.ObsValue_Num, msd201_202.UCUMUnit, msd201_202.RecordNumber, msd201_202.MSD202_ID).withColumnRenamed("CContactDate", "COReadingDate").withColumnRenamed("OrgCodeProvider", "COReadingProvider").withColumnRenamed("RecordNumber","COReadingRecordNumber")


#Combine all records of CO readings
co_reading_all = msd201_202.union(msd302).union(msd109).withColumnRenamed("MSD202_ID","MSD_ID")


#Finding records where the booking CO is < 4 ppm

#Join CO readings to denominator cohort
msd_co_booking = msd101_in.join(co_reading_all, ["Person_ID_Mother","UniqPregID"], "inner")

#Keep CO readings within -7/+7 days of booking
msd_co_booking = msd_co_booking.filter(msd_co_booking.COReadingDate.between(date_add(msd_co_booking.AntenatalAppDate,days=-7), date_add(msd_co_booking.AntenatalAppDate,days=7)))
msd_co_booking = msd_co_booking.withColumn("OnDateFlag", when(msd_co_booking.AntenatalAppDate == msd_co_booking.COReadingDate, '1').otherwise("0")).withColumn("DateDifference", datediff(msd_co_booking.AntenatalAppDate, msd_co_booking.COReadingDate))

#If the pregnancy has multiple records within the interval then rank - first by CO recorded on booking date, then CO recorded closest to booking date, then reading with the latest submitted RecordNumber, then latest record by table/row ID
msd_co_booking = msd_co_booking.withColumn("Rank_CO", row_number().over( Window.partitionBy(msd_co_booking.Person_ID_Mother, msd_co_booking.UniqPregID).orderBy(msd_co_booking.OnDateFlag.desc(), msd_co_booking.DateDifference.asc(), msd_co_booking.COReadingRecordNumber.desc(), msd_co_booking.MSD_ID.desc())))

msd_co_booking = msd_co_booking.filter(msd_co_booking.Rank_CO == 1).filter(msd_co_booking.ObsValue_Num < 4)
msd_co_booking = msd_co_booking.select("Person_ID_Mother", "UniqPregID", "AntenatalAppDate", "COReadingProvider", "COReadingDate", "ObsValue_Num", "ObsCode" ).withColumnRenamed("COReadingProvider", "BookingCOProvider").withColumnRenamed("COReadingDate", "BookingCODate").withColumnRenamed("ObsValue_Num", "BookingCOValue").withColumnRenamed("ObsCode", "BookingCOObsCode")



#Finding records where the CO reading at 36 weeks is < 4 ppm

#Join CO readings to denominator cohort
msd_co_threesix = msd101_in.join(co_reading_all, ["Person_ID_Mother","UniqPregID"], "inner")

#Keep CO readings within -7/+7 days of 252 days gestation
msd_co_threesix = msd_co_threesix.filter(msd_co_threesix.COReadingDate.between(date_add(msd_co_threesix.Date36Weeks,days=-7), date_add(msd_co_threesix.Date36Weeks,days=7)))
msd_co_threesix = msd_co_threesix.withColumn("OnDateFlag", when(msd_co_threesix.Date36Weeks == msd_co_threesix.COReadingDate, '1').otherwise("0")).withColumn("DateDifference", datediff(msd_co_threesix.Date36Weeks, msd_co_threesix.COReadingDate))

#If the pregnancy has multiple records within the interval then rank - first by CO recorded on gestation 252 days, then CO recorded closest to that date, then reading with the latest submitted RecordNumber, then latest record by table/row ID
msd_co_threesix = msd_co_threesix.withColumn("Rank_CO", row_number().over( Window.partitionBy(msd_co_threesix.Person_ID_Mother, msd_co_threesix.UniqPregID).orderBy(msd_co_threesix.OnDateFlag.desc(), msd_co_threesix.DateDifference.asc(), msd_co_threesix.COReadingRecordNumber.desc(), msd_co_threesix.MSD_ID.desc())))

msd_co_threesix = msd_co_threesix.filter(msd_co_threesix.Rank_CO == 1).filter(msd_co_threesix.ObsValue_Num < 4)
msd_co_threesix = msd_co_threesix.select("Person_ID_Mother", "UniqPregID", "Date36Weeks", "COReadingProvider", "COReadingDate", "ObsValue_Num", "ObsCode" ).withColumnRenamed("COReadingProvider", "Week36COProvider").withColumnRenamed("COReadingDate", "Week36CODate").withColumnRenamed("ObsValue_Num", "Week36COValue").withColumnRenamed("ObsCode", "Week36ObsCode")


#Pregnancies with both 36 and booking readings under 4 ppm

msd_under4 = msd_co_booking.join(msd_co_threesix, ["Person_ID_Mother","UniqPregID"], "inner")


#Output for the numerator cohort
msd_num = msd_denom.alias("g").join(msd_under4, ["Person_ID_Mother","UniqPregID"], "inner").select(["g.*"])



#Codes for cigarette consumption
#Cigarette_Codes = ['230056004', '.137X', '137X.', 'Ub1tI']
Cigarette_Codes = ['230056004', '401201003']

#All MSD109 records
#With codes for cigarette consumption; recorded by reporting month+1
cig109 = msd109_all.filter(msd109_all.RPStartDate <= StartDate).filter(msd109_all.ObsCode.isin(Cigarette_Codes))
#With a cigarettes value of greater than 0 (only want to identify smokers)
cig109 = cig109.withColumn("ObsValue_Num", cig109.ObsValue.cast(DoubleType())).filter("ObsValue_Num > '0'")
#No specified units required (users sometimes record in cigarettes per day / per week or packs per day or total or unspecified) as any value over 0 is enough to count as a smoker, units are not required.

#For pregnancies in the denominator cohort
cig109 = cig109.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(cig109.Person_ID_Mother, cig109.UniqPregID, cig109.OrgCodeProvider, cig109.ObsCode, cig109.ObsDate, cig109.ObsValue_Num, cig109.RecordNumber, cig109.MSD109_ID).withColumnRenamed("ObsDate","CigaretteDate").withColumnRenamed("OrgCodeProvider","CigaretteProvider").withColumnRenamed("RecordNumber","CigaretteRecordNumber")

#cig109 = cig109.filter("Person_ID_Mother = -1")

#All MSD302 records
#With codes for cigarette consumption; recorded by reporting month+1
cig302 = msd302_all.filter(msd302_all.RPStartDate <= StartDate).filter(msd302_all.ObsCode.isin(Cigarette_Codes))
#With a cigarettes value of greater than 0 (only want to identify smokers)
cig302 = cig302.withColumn("ObsValue_Num", cig302.ObsValue.cast(DoubleType())).filter("ObsValue_Num > '0'")
#No specified units required (users sometimes record in cigarettes per day / per week or packs per day or total or unspecified) as any value over 0 is enough to count as a smoker, units are not required.

#For pregnancies in the denominator cohort
cig302 = cig302.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(cig302.Person_ID_Mother, cig302.UniqPregID, cig302.OrgCodeProvider, cig302.ObsCode, cig302.ClinInterDateMother, cig302.ObsValue_Num, cig302.RecordNumber, cig302.MSD302_ID).withColumnRenamed("ClinInterDateMother", "CigaretteDate").withColumnRenamed("OrgCodeProvider", "CigaretteProvider").withColumnRenamed("RecordNumber","CigaretteRecordNumber")


#All MSD201/2 records
#With codes for cigarette consumption; recorded by reporting month+1
cig202 = msd202_all.filter(msd202_all.RPStartDate <= StartDate).filter(msd202_all.ObsCode.isin(Cigarette_Codes))
#With a cigarettes value of greater than 0 (only want to identify smokers)
cig202 = cig202.withColumn("ObsValue_Num", cig202.ObsValue.cast(DoubleType())).filter("ObsValue_Num > '0'")
#No specified units required (users sometimes record in cigarettes per day / per week or packs per day or total or unspecified) as any value over 0 is enough to count as a smoker, units are not required.

#For pregnancies in the denominator cohort
cig201_202 = cig202.alias("f").join(msd201_all, ["Person_ID_Mother","UniqPregID", "RecordNumber", "CareConID"], "inner").select(cig202.Person_ID_Mother, cig202.UniqPregID, cig202.RecordNumber, cig202.CareConID, cig202.MSD202_ID, cig202.OrgCodeProvider, cig202.ObsCode, cig202.ObsValue_Num, msd201_all.CContactDate)

cig201_202 = cig201_202.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(cig201_202.Person_ID_Mother, cig201_202.UniqPregID, cig201_202.OrgCodeProvider, cig201_202.ObsCode, cig201_202.CContactDate, cig201_202.ObsValue_Num, cig201_202.RecordNumber, cig201_202.MSD202_ID).withColumnRenamed("CContactDate", "CigaretteDate").withColumnRenamed("OrgCodeProvider", "CigaretteProvider").withColumnRenamed("RecordNumber", "CigaretteRecordNumber")


#Combine all records of cigarettes
cigarettes_all = cig201_202.union(cig302).union(cig109).withColumnRenamed("MSD202_ID","MSD_ID")


#Codes for smoking (only codes identifying the person as a smoker)
#Uses the same list of smoking codes used to flag someone as a smoker in CQIM (not updated in a while; may be other relevant codes)
Smoker_Codes = ['110483000', '160603005', '160604004', '160605003', '160606002', '160616005', '160619003', '169940006', '203191000000107', '230059006', '230060001', '230062009', '230063004', '230064005', '230065006', '266920004', '266929003', '30310000', '308438006', '365981007', '365982000', '394871007', '394873005', '428041000124106', '446172000', '449345000', '56294008', '56578002', '56771006', '59978006', '65568007', '697956009', '77176002', '82302008', 'F17', 'Z716', 'Z720',# '1372.', '1373.', '1374.', '1375.', '1376.', '137a.', '137b.', '137C.', '137c.', '137d.', '137e.', '137f.', '137G.', '137H.', '137h.', '137J.', '137M.', '137P.', '137Q.', '137R.', '137V.', '137X.', '137Y.', '137Z.', '13p..', '13p0.', '13p1.', '13p2.', '13p3.', '13p4.', '13p5.', '13p6.', '38DH.', '67A3.', '67H6.', '745H.', '745H0', '745H1', '745H2', '745H3', '745H4', '745Hy', '745Hz', '8B2B.', '8B3f.', '8B3Y.', '8BP3.', '8CAg.', '8CAL.', '8H7i.', '8HBM.', '8HkQ.', '8HTK.', '8I2I.', '8I2J.', '8I39.', '8I3M.', '9kc..', '9kc0.', '9ko..', '9N2k.', '9N4M.', '9OO..', '9OO1.', '9OO2.', '9OO3.', '9OO4.', '9OO5.', '9OO6.', '9OO7.', '9OO8.', '9OO9.', '9OOA.', '9OOZ.', 'E023.', 'E251.', 'E2510', 'E2511', 'E251z', 'Eu17.', 'Eu171', 'Ub0pJ', 'Ub0pT', 'Xaa26', 'XagO3', 'XaIIu', 'XaItg', 'XaJX2', 'XaLQh', 'XaWNE', 'XE0oq', 'XE0or', 'ZG233', 'ZRaM.', 'ZRao.', 'ZRBm2', 'ZRh4.', 'ZV4K0', 'ZV6D8',\
               #Other relevant smoker codes
               'F17.0', 'F170', 'F17.1', 'F171', 'F17.2', 'F172', 'Z71.6', 'Z72.0', '134406006', '394872000', '449868002']

#All MSD109 records
#With codes for being a smoker; recorded by reporting month+1
smk109 = msd109_all.filter(msd109_all.RPStartDate <= StartDate).filter(msd109_all.FindingCode.isin(Smoker_Codes))

#For pregnancies in the denominator cohort
smk109 = smk109.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(smk109.Person_ID_Mother, smk109.UniqPregID, smk109.OrgCodeProvider, smk109.FindingCode, smk109.FindingDate, smk109.RecordNumber, smk109.MSD109_ID).withColumnRenamed("ObsDate","SmokerDate").withColumnRenamed("OrgCodeProvider","SmokerProvider").withColumnRenamed("RecordNumber","SmokerRecordNumber")

#smk109 = smk109.filter("Person_ID_Mother = -1")
#All MSD302 records
#With codes for being a smoker; recorded by reporting month+1
smk302 = msd302_all.filter(msd302_all.RPStartDate <= StartDate).filter(msd302_all.FindingCode.isin(Smoker_Codes))

#For pregnancies in the denominator cohort
smk302 = smk302.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(smk302.Person_ID_Mother, smk302.UniqPregID, smk302.OrgCodeProvider, smk302.FindingCode, smk302.ClinInterDateMother, smk302.RecordNumber, smk302.MSD302_ID).withColumnRenamed("ClinInterDateMother", "SmokerDate").withColumnRenamed("OrgCodeProvider", "SmokerProvider").withColumnRenamed("RecordNumber","SmokerRecordNumber")


#All MSD201/2 records
#With codes for being a smoker; recorded by reporting month+1
smk202 = msd202_all.filter(msd202_all.RPStartDate <= StartDate).filter(msd202_all.FindingCode.isin(Smoker_Codes))

#For pregnancies in the denominator cohort
smk201_202 = smk202.alias("f").join(msd201_all, ["Person_ID_Mother","UniqPregID", "RecordNumber", "CareConID"], "inner").select(smk202.Person_ID_Mother, smk202.UniqPregID, smk202.RecordNumber, smk202.CareConID, smk202.MSD202_ID, smk202.OrgCodeProvider, smk202.FindingCode, msd201_all.CContactDate)

smk201_202 = smk201_202.join(msd101_in, ["Person_ID_Mother","UniqPregID"], "inner").select(smk201_202.Person_ID_Mother, smk201_202.UniqPregID, smk201_202.OrgCodeProvider, smk201_202.FindingCode, smk201_202.CContactDate, smk201_202.RecordNumber, smk201_202.MSD202_ID).withColumnRenamed("CContactDate", "SmokerDate").withColumnRenamed("OrgCodeProvider", "SmokerProvider").withColumnRenamed("RecordNumber", "SmokerRecordNumber")


#Combine all records of smokers
smoker_all = smk201_202.union(smk302).union(smk109).withColumnRenamed("MSD202_ID","MSD_ID")



#List of people / pregnancies who have an indication of smoking within -/+7 days of the booking appointment or within -/+7 days of 36 weeks gestation

#Any indication of smoking
smoking_any_smk = smoker_all.select(smoker_all.Person_ID_Mother, smoker_all.UniqPregID, smoker_all.SmokerDate)
smoking_any_cig = cigarettes_all.select(cigarettes_all.Person_ID_Mother, cigarettes_all.UniqPregID, cigarettes_all.CigaretteDate)
smoking_any = smoking_any_smk.union(smoking_any_cig)


#Join smoking to denominator cohort
msd_smoker_booking = msd101_in.join(smoking_any, ["Person_ID_Mother","UniqPregID"], "inner")

#Keep smoking status within -7/+7 days of booking
msd_smoker_booking = msd_smoker_booking.filter(msd_smoker_booking.SmokerDate.between(date_add(msd_smoker_booking.AntenatalAppDate,days=-7), date_add(msd_smoker_booking.AntenatalAppDate,days=7)))
msd_smoker_booking = msd_smoker_booking.withColumn("OnDateFlag", when(msd_smoker_booking.AntenatalAppDate == msd_smoker_booking.SmokerDate, '1').otherwise("0")).withColumn("DateDifference", datediff(msd_smoker_booking.AntenatalAppDate, msd_smoker_booking.SmokerDate))

#Finding records where the CO reading at 36 weeks is < 4 ppm

#Join smoking to denominator cohort
msd_smoker_threesix = msd101_in.join(smoking_any, ["Person_ID_Mother","UniqPregID"], "inner")

#Keep smoking within -7/+7 days of 252 days gestation
msd_smoker_threesix = msd_smoker_threesix.filter(msd_smoker_threesix.SmokerDate.between(date_add(msd_smoker_threesix.Date36Weeks,days=-7), date_add(msd_smoker_threesix.Date36Weeks,days=7)))
msd_smoker_threesix = msd_smoker_threesix.withColumn("OnDateFlag", when(msd_smoker_threesix.Date36Weeks == msd_smoker_threesix.SmokerDate, '1').otherwise("0")).withColumn("DateDifference", datediff(msd_smoker_threesix.Date36Weeks, msd_smoker_threesix.SmokerDate))


#If a person has any record of being a smoker in either timeframe then exclude
msd_exc_smoker = msd_smoker_booking.union(msd_smoker_threesix)
msd_exc_smoker = msd_exc_smoker.select(msd_exc_smoker.Person_ID_Mother, msd_exc_smoker.UniqPregID).distinct()



msd_num_1 = msd_num.alias("j").join(msd_exc_smoker.alias("k"), (msd_num.Person_ID_Mother == msd_exc_smoker.Person_ID_Mother) & (msd_num.UniqPregID == msd_exc_smoker.UniqPregID) , "left").filter(col("k.Person_ID_Mother").isNull() == 'true').select(["j.*"])
msd_num_1 = msd_num_1.select(msd_num_1.Person_ID_Mother, msd_num_1.UniqPregID)

#Final output dataframe for the numerator cohort must be set to have the name msd_num
msd_num = msd_num_1

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

msd002 = msd002.filter(msd002.RPEndDate == MonthPlusEnd).filter((msd002.EndDateGMPReg.isNull() == 'true') | ((msd002.StartDateGMPReg.isNull() == 'true') & (msd002.EndDateGMPReg > MonthPlusEnd) ) | ((msd002.StartDateGMPReg < MonthPlusEnd) & (msd002.EndDateGMPReg > MonthPlusEnd)))

msd002 = msd002.withColumn("Commissioner",when(msd002.RPEndDate > STPEndDate, msd002.OrgIDSubICBLocGP).otherwise(msd002.CCGResponsibilityMother))

msd002 = msd002.withColumn("Commissioner",when(msd002.Commissioner.isNull() == 'false', msd002.Commissioner).otherwise(lit("Unknown"))).select(msd002.Person_ID_Mother, msd002.RecordNumber,"Commissioner")


#Add additional breakdowns (Local Authority)
#msd001 = table("mat_pre_clear.msd001motherdemog")
#msd001 = table(dbSchema + ".msd001motherdemog")
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

#%run /data_analysts_collaboration/CMH_TURING_Team(Create)/code_sharing/geogtlrr/geogtlrr_table_creation $endperiod=$RPEndDate $outSchema=$outschema_geog

# COMMAND ----------

#Bring in the current geography table
geog_add = outSchema + ".geogtlrr"
geography = table(geog_add)

geography = geography.withColumnRenamed("Trust","Trust_Name").withColumnRenamed("LRegion","LRegion_Name").withColumnRenamed("Region","Region_Name")


geog_flat = geography.select(geography.Trust_ORG.alias("Org1"), geography.Trust_ORG, geography.Trust_Name, lit("Provider")).union(geography.select(geography.Trust_ORG, geography.STP_Code, geography.STP_Name, lit("Local Maternity System"))).union(geography.select(geography.Trust_ORG, geography.RegionORG, geography.Region_Name, lit("NHS England (Region)"))).union(geography.select(geography.Trust_ORG, geography.Mbrrace_Grouping_Short, geography.Mbrrace_Grouping, lit('MBRRACE Grouping')))

geog_flat = geog_flat.withColumnRenamed("Provider","OrgGrouping").distinct()

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
sub_orgs = sub_orgs.filter((sub_orgs.RPStartDate.between(MonthPlus,MonthPlusEnd)) | (sub_orgs.RPEndDate.between(MonthPlus,MonthPlusEnd)))

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
#msd_final_combined = msd_final_combined.withColumn("RPStartDate",lit(StartDate)).withColumn("RPEndDate",lit(EndDate))

#Amended from template to add in for current month <> activity month
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
  
#  display(msd_count_all_sup.orderBy(msd_count_all_sup.Org_Level.asc(), msd_count_all_sup.Org_Code.asc(), msd_count_all_sup.IndicatorFamily.asc(), msd_count_all_sup.Indicator.asc(), msd_count_all_sup.Currency.asc()))

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

smokefreepreg_spdf = (
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
smokefreepreg_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.{outtable}')

# COMMAND ----------

sql = f"""
INSERT INTO {outSchema}.measures_csv
SELECT
RPStartDate,
RPEndDate,
IndicatorFamily,
Indicator,
Org_Code as OrgCodeProvider,
Org_Name as OrgName,
Org_Level as OrgLevel,
Currency,
Value,
current_timestamp() AS CreatedAt 

from {outSchema}.{outtable} where RPStartDate = '{StartDate}'"""

display(spark.sql(sql))

# COMMAND ----------

#Create the storage table (if it does not already exist)
#Clear the table of old results for the metric / month

# if OutputType == 'measure':
#   string = "CREATE TABLE IF NOT EXISTS " + outschema + "." + outtable + " (Org_Code STRING, Org_Name STRING, Org_Level STRING, RPStartDate DATE, RPEndDate DATE, IndicatorFamily STRING, Indicator STRING, Currency STRING, Value STRING) USING DELTA"
#   spark.sql(string)
  
#   string2 = "DELETE FROM " + outschema + "." + outtable + " WHERE RPStartDate = '" + StartDate + "' AND IndicatorFamily = '" + IndicaFam + "' AND Indicator = '" + Indicator + "'"  
#   spark.sql(string2)
  
  
# if OutputType == 'data':
#   string = "CREATE TABLE IF NOT EXISTS " + outschema + "." + outtable + " (ReportingPeriodStartDate DATE, ReportingPeriodEndDate DATE, Dimension STRING, Org_Level STRING, Org_Code STRING, Org_Name STRING, Measure STRING, Count_Of STRING, Final_value STRING) USING DELTA"
#   spark.sql(string)
  
#   otherstring = "DELETE FROM " + outschema + "." + outtable + " WHERE ReportingPeriodStartDate = '" + StartDate + "' AND Dimension = '" + Indicator + "'"
#   spark.sql(otherstring)

# COMMAND ----------

#Check the format of the table is correct 
#Then store the output

# string3 = outschema + "." + outtable
# outputform = table(string3)

# if outputform.columns == msd_count_all_sup.columns:
#   string4 = "INSERT INTO " + outschema + "." + outtable + " SELECT * FROM msd_count_all_sup"
#   spark.sql(string4)
# else:
#   print("Table is not in the appropriate format. Metric was not loaded.")

# COMMAND ----------

# final_output = table(string3)

# if OutputType == 'measure':
#   final_output = final_output.filter(final_output.RPStartDate == StartDate).filter(final_output.RPEndDate == EndDate).filter(final_output.Indicator == Indicator)
#   display(final_output.orderBy("RPStartDate", "RPEndDate", "IndicatorFamily", "Indicator", "Org_Level", "Org_Code", "Currency"))
  
  
# if OutputType == 'data':
#   final_output = final_output.filter(final_output.ReportingPeriodStartDate == StartDate).filter(final_output.ReportingPeriodEndDate == EndDate).filter(final_output.Dimension == Indicator)
#   display(final_output.orderBy("ReportingPeriodStartDate", "ReportingPeriodEndDate", "Dimension", "Measure" "Org_Level", "Org_Code"))