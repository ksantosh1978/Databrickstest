# Databricks notebook source
# DBTITLE 1,Widgets
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

# COMMAND ----------

# DBTITLE 1,Libraries
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from datetime import datetime
from datetime import date

# COMMAND ----------

# DBTITLE 1,Measures
# See also https://nhsd-confluence.digital.nhs.uk/pages/viewpage.action?spaceKey=KH&title=CQIM+Definitions+V2.0+-+Measures

# Configuration of each measure.
CQIM = {}
# Measure
# Numerator SQL
# Denominator SQL - initially blank where set by HES Denominator cell
# adjStart - the number of months prior to the current month for the measure to cover
# length - the number of months for the measure to cover

# COMMAND ----------

# DBTITLE 1,CQIMDQ02 - Women booking in MSDS in the previous reporting period as a percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ02 = {    
      "Measure": "CQIMDQ02"
      ,"Numerator": "SELECT h.OrgCodeProviderMerge as OrgCodeProvider, count (DISTINCT Person_ID_Mother) as Numerator FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h on a.OrgCodeProvider = h.OrgCodeProvider and h.RPEndDate = '{RPEnd}' and a.RPStartDate between '{start}' and '{end}' where PersonBirthDateBaby between '{start}' and '{end}' and (a.Pregoutcome IN ('01','02','03','04')) group by h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 1
      ,"length" : 1
}
CQIM['CQIMDQ02'] = CQIMDQ02

# COMMAND ----------

# DBTITLE 1,CQIMDQ03 - Women booking in MSDS in the reporting period as a percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ03 = { 
      "Measure": "CQIMDQ03"
      ,"Numerator": "SELECT h.OrgCodeProviderMerge AS OrgCodeProvider, COUNT (DISTINCT Person_ID_Mother) AS Numerator  FROM {dbSchema}.msd101pregnancybooking a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ03'] = CQIMDQ03

# COMMAND ----------

# DBTITLE 1,CQIMDQ04 - Percentage of women who had a booking appointment in the reporting period for whose smoking status was known
CQIMDQ04 = { 
      "Measure": "CQIMDQ04"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}' AND a.SmokingStatusBooking_derived in ('Smoker','Non-Smoker / Ex-Smoker')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ04'] = CQIMDQ04

# COMMAND ----------

# DBTITLE 1,CQIMDQ05 - Percentage of women who had a booking appointment in the reporting period for whose smoking status was recorded as known that were current smokers
CQIMDQ05 = { 
      "Measure": "CQIMDQ05"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') AND a.SmokingStatusBooking_derived = 'Smoker' GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') AND (a.SmokingStatusBooking_derived in ('Smoker','Non-Smoker / Ex-Smoker') ) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ05'] = CQIMDQ05

# COMMAND ----------

# DBTITLE 1,CQIMDQ06 - Percentage of women giving birth in the previous period for whom status was known
CQIMDQ06 = { 
      "Measure": "CQIMDQ06"
      ,"Numerator": "select COUNT (DISTINCT a.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN global_temp.MSD101PregnancyBooking_with_smokingstatusdelivery_derived smokingstatusdelivery_derived ON a.UniqPregID = smokingstatusdelivery_derived.UniqPregID INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') WHERE  (a.PersonBirthDateBaby between '{start}' and '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND (smokingstatusdelivery_derived.SmokingStatusDelivery_derived IN ('Smoker','Non-Smoker / Ex-Smoker')) AND a.RPStartDate in ('{start}',add_months('{start}',-1)) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}' AND a.Pregoutcome IN ('01','02','03','04')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 1
      ,"length" : 1
    }
CQIM['CQIMDQ06'] = CQIMDQ06

# COMMAND ----------

# DBTITLE 1,CQIMDQ07 - Percentage of women giving birth in the previous reporting period for whose smoking status at delivery was recorded as known that were current smokers
CQIMDQ07 = { 
      "Measure": "CQIMDQ07"
      ,"Numerator": "SELECT count(distinct a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') AND a.RPStartDate BETWEEN '{start}' AND '{end}' INNER JOIN global_temp.MSD101PregnancyBooking_with_smokingstatusdelivery_derived b ON a.UniqPregID=b.UniqPregID and a.OrgCodeProvider=b.OrgCodeProvider WHERE (a.PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.RPStartDate BETWEEN '{start}' and '{end}') and (a.PregOutcome in ('01', '02', '03', '04')) and (b.SmokingStatusDelivery_derived = 'Smoker') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count(distinct a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') AND a.RPStartDate BETWEEN '{start}' AND '{end}'  INNER JOIN global_temp.MSD101PregnancyBooking_with_smokingstatusdelivery_derived b ON a.UniqPregID=b.UniqPregID and a.OrgCodeProvider=b.OrgCodeProvider WHERE (a.PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.RPStartDate BETWEEN '{start}' and '{end}') and (a.PregOutcome in ('01', '02', '03', '04')) and (b.SmokingStatusDelivery_derived  IN ('Smoker','Non-Smoker / Ex-Smoker')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 1
      ,"length" : 1
    }
CQIM['CQIMDQ07'] = CQIMDQ07

# COMMAND ----------

# DBTITLE 1,CQIMDQ08 - Percentage of Babies whose first feed type was recorded
CQIMDQ08 = { 
      "Measure": "CQIMDQ08"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Baby) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}' AND a.BabyFirstFeedIndCode IN ('01','02','03')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}'  WHERE (PersonBirthDateBaby between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ08'] = CQIMDQ08

# COMMAND ----------

# DBTITLE 1,CQIMDQ09 - Women giving birth in  MSDS in the reporting period as a percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ09 = { 
      "Measure": "CQIMDQ09"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start} ' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ09'] = CQIMDQ09

# COMMAND ----------

# DBTITLE 1,CQIMDQ10 - Women giving birth in the previous 3 months reporting period as a percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ10 = { 
      "Measure": "CQIMDQ10"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE ((PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04'))) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 3
      ,"length" : 3
    }
CQIM['CQIMDQ10'] = CQIMDQ10

# COMMAND ----------

# DBTITLE 1,CQIMDQ11 - Percentage of  women with a recorded postpartum haemorrhage  of 500ml or more in the previous 3 months or  Maternal Critical Incident recorded in the previous 3 months
CQIMDQ11 = { 
      "Measure": "CQIMDQ11"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' INNER JOIN {dbSchema}.msd302careactivitylabdel ON msd302careactivitylabdel.Person_ID_Mother = a.Person_ID_Mother WHERE ((PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND (msd302careactivitylabdel.MasterSnomedCTObsCode in (47821001, 27214003, 1033591000000101, 1033571000000100, 23171006, 200025008, 11452009, 49177006, 724496000, 119891000119105,785322012,1216656013, 1218146019, 79691012, 494929013, 1230526013, 719051004, 3314819019, 3314820013, 494930015)) and ObsValue >= 500) and (ClinInterDateMother BETWEEN '{start}' AND '{end}') or ((PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND ((msd302careactivitylabdel.MasterSnomedCTProcedureCode in ('236886002', '305351004')) or (msd302careactivitylabdel.MasterSnomedCTFindingCode in ('237235009', '33211000', '17263003', '429098002', '59282003', '274126009'))) AND (a.RPStartDate between '{start}' and '{end}')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 3
      ,"length" : 3
    }
CQIM['CQIMDQ11'] = CQIMDQ11

# COMMAND ----------

# DBTITLE 1,CQIMDQ12 - Percentage of  women with a recorded postpartum haemorrhage of 1,500ml or more  in the previous 3 months or  Maternal Critical Incident recorded in the previous 3 months
CQIMDQ12 = { 
      "Measure": "CQIMDQ12"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' INNER JOIN {dbSchema}.msd302careactivitylabdel ON msd302careactivitylabdel.Person_ID_Mother = a.Person_ID_Mother WHERE ((PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND (msd302careactivitylabdel.MasterSnomedCTObsCode in (47821001, 27214003, 1033591000000101, 1033571000000100, 23171006, 200025008, 11452009, 49177006, 724496000, 119891000119105,785322012,1216656013, 1218146019, 79691012, 494929013, 1230526013, 719051004, 3314819019, 3314820013, 494930015)) and ObsValue >= 1500) and (ClinInterDateMother BETWEEN '{start}' AND '{end}') or ((PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND ((msd302careactivitylabdel.MasterSnomedCTProcedureCode in ('236886002', '305351004')) or (msd302careactivitylabdel.MasterSnomedCTFindingCode in ('237235009', '33211000', '17263003', '429098002', '59282003', '274126009'))) AND (a.RPStartDate between '{start}' and '{end}')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 3
      ,"length" : 3
    }
CQIM['CQIMDQ12'] = CQIMDQ12

# COMMAND ----------

# DBTITLE 1,CQIMDQ13 - At least one postpartum haemorrhage recorded in the previous 6 month
CQIMDQ13 = { 
      "Measure": "CQIMDQ13"
      ,"Numerator": "SELECT CASE WHEN COUNT(DISTINCT Person_ID_Mother) > 0 THEN 1 END AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.msd302careactivitylabdel a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (ClinInterDateMother between '{start}' AND '{end}') AND (a.MasterSnomedCTObsCode in (47821001, 27214003, 1033591000000101, 1033571000000100, 23171006, 200025008, 11452009, 49177006, 724496000, 119891000119105,785322012,1216656013, 1218146019, 79691012, 494929013, 1230526013, 719051004, 3314819019, 3314820013, 494930015)) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT 1 AS Denominator, OrgCodeProviderMerge AS OrgCodeProvider from {outSchema}.msd000header WHERE RPEndDate = '{end}' group by OrgCodeProviderMerge"
      ,"adjStart" : 6
      ,"length" : 6
    }
CQIM['CQIMDQ13'] = CQIMDQ13

# COMMAND ----------

# DBTITLE 1,CQIMDQ14 - Women giving birth in MSDS in the reporting period as percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ14 = { 
      "Measure": "CQIMDQ14"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ14'] = CQIMDQ14

# COMMAND ----------

# DBTITLE 1,CQIMDQ15 - Percentage of babies with a valid gestational age  between 154 days (22wks) and 315 days (45wks) in the current 3 months reporting period (Singleton)
CQIMDQ15 = { 
      "Measure": "CQIMDQ15"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between '154' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}')  AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ15'] = CQIMDQ15

# COMMAND ----------

# DBTITLE 1,CQIMDQ16 - Percentage of babies with a gestational age between 259 days (37wks) and 315 days (45wks) in the current 3 months reporting period (Singleton)
CQIMDQ16 = { 
      "Measure": "CQIMDQ16"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between '259' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between '154' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ16'] = CQIMDQ16

# COMMAND ----------

# DBTITLE 1,CQIMDQ18 - Percentage of babies born as vaginal births  in the current 3 months reporting period (Singleton)
CQIMDQ18 = { 
      "Measure": "CQIMDQ18" 
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ18'] = CQIMDQ18

# COMMAND ----------

# DBTITLE 1,CQIMDQ19 - Percentage of babies born as caesarean section births in the current 3 months reporting period (Singleton)
# 06/09/23 CQIMDQ19 has been removed from the pipeline
# CQIMDQ19 = { 
#       "Measure": "CQIMDQ19"
#       ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('7','8')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
#       ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
#       ,"adjStart" : 2
#       ,"length" : 3
#     }
# CQIM['CQIMDQ19'] = CQIMDQ19

# COMMAND ----------

# DBTITLE 1,CQIMDQ20 - Percentage of babies born as vaginal births with a 3rd or 4th degree tear  in the current 3 months reporting period (Singleton)
CQIMDQ20 = { 
      "Measure": "CQIMDQ20"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' INNER JOIN {dbSchema}.msd302careactivitylabdel ON a.Person_ID_Mother = msd302careactivitylabdel.Person_ID_Mother AND ld.RPStartDate = msd302careactivitylabdel.RPStartDate WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND (a.GestationLengthBirth between '259' AND '315') AND msd302careactivitylabdel.GenitalTractTraumaticLesion in ('10217006', '399031001')  AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}'  WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02','03','04')) AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND (a.GestationLengthBirth between '259' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ20'] = CQIMDQ20

# COMMAND ----------

# DBTITLE 1,CQIMDQ21 - Provider has at least one 3rd or 4th degree tear recorded in the previous 6 months reporting period
# group by added to denominator due to poor data in DAE Ref
CQIMDQ21 = { 
      "Measure": "CQIMDQ21"
      ,"Numerator": "SELECT CASE WHEN COUNT(DISTINCT Person_ID_Mother) > 0 THEN 1 END AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.msd302careactivitylabdel a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (ClinInterDateMother between '{start}' AND '{end}') AND (GenitalTractTraumaticLesion in ('10217006', '399031001')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT 1 AS Denominator, OrgCodeProviderMerge AS OrgCodeProvider from {outSchema}.msd000header WHERE RPEndDate = '{RPEnd}' group by OrgCodeProviderMerge"
      ,"adjStart" : 6
      ,"length" : 6
    }
CQIM['CQIMDQ21'] = CQIMDQ21

# COMMAND ----------

# DBTITLE 1,CQIMDQ22 - Percentage of babies with a valid gestational age  between 154 days (22wks) and 315 days (45wks) in the current reporting period (Singleton)
CQIMDQ22 = { 
      "Measure": "CQIMDQ22"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between '154' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND ld.BirthsPerLabandDel = 1 GROUP BY OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ22'] = CQIMDQ22

# COMMAND ----------

# DBTITLE 1,CQIMDQ23 - Percentage of babies with a gestational age between 259 days (37wks) and 315 days (45wks) in the current reporting period (Singleton)
CQIMDQ23 = { 
      "Measure": "CQIMDQ23"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between '259' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND ld.BirthsPerLabandDel = 1 GROUP BY OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ23'] = CQIMDQ23

# COMMAND ----------

# DBTITLE 1,CQIMDQ24 - Percentage of babies with an Apgar score recorded between 0-10 and a valid gestational age between 259 days (37wks) and 315 days (45wks) in the current 3 months reporting period  (Singleton)
CQIMDQ24 = { 
      "Measure": "CQIMDQ24"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD405CareActivityBaby c ON a.UniqPregID=c.UniqPregID AND a.RPStartDate=c.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND a.PregOutcome in ('01', '02', '03', '04') and a.GestationLengthBirth between 259 AND 315 AND ld.BirthsPerLabandDel = 1 AND c.ApgarScore between 0 and 10 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD405CareActivityBaby c ON a.UniqPregID=c.UniqPregID AND a.RPStartDate=c.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND a.PregOutcome in ('01', '02', '03', '04') and a.GestationLengthBirth between 259 AND 315 AND ld.BirthsPerLabandDel = 1  GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ24'] = CQIMDQ24

# COMMAND ----------

# DBTITLE 1,CQIMDQ25 - Percentage of babies born who have a valid fetus presentation recorded in the current reporting period
CQIMDQ25 = { 
      "Measure": "CQIMDQ25"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Baby) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'   WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND  (MSD401.PersonBirthDateBaby between '{start}' AND '{end}') and MSD401.FETUSPRESENTATION in ('01', '02', '03', '04', 'XX')   GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT MSD401.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' WHERE MSD401.RPStartDate between '{start}' AND '{end}' AND (MSD401.PersonBirthDateBaby between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ25'] = CQIMDQ25

# COMMAND ----------

# DBTITLE 1,CQIMDQ26 - Percentage of babies born that have a valid delivery method recorded  in the current  3 months reporting period (Singleton)
CQIMDQ26 = { 
      "Measure": "CQIMDQ26"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ26'] = CQIMDQ26

# COMMAND ----------

# DBTITLE 1,CQIMDQ27 - Percentage of women that have a valid previous live birth and still birth data recorded in current 3 months reporting period
CQIMDQ27 = { 
      "Measure": "CQIMDQ27"
      ,"Numerator": "SELECT h.OrgCodeProviderMerge AS OrgCodeProvider, COUNT (DISTINCT Person_ID_Mother) AS Numerator  FROM {dbSchema}.msd101pregnancybooking a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') AND PreviousLiveBirths BETWEEN 0 AND 20 AND PreviousStillBirths BETWEEN 0 AND 20 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT h.OrgCodeProviderMerge AS OrgCodeProvider, COUNT (DISTINCT Person_ID_Mother) AS Denominator  FROM {dbSchema}.msd101pregnancybooking a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ27'] = CQIMDQ27

# COMMAND ----------

# DBTITLE 1,CQIMDQ28 - Percentage of women that have a valid previous live births and still births data recorded had zero previous births in current 3 months reporting period
CQIMDQ28 = { 
      "Measure": "CQIMDQ28"
      ,"Numerator": "SELECT h.OrgCodeProviderMerge AS OrgCodeProvider, COUNT (DISTINCT Person_ID_Mother) AS Numerator  FROM {dbSchema}.msd101pregnancybooking a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') AND PreviousLiveBirths = 0 AND PreviousStillBirths = 0  GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT h.OrgCodeProviderMerge AS OrgCodeProvider, COUNT (DISTINCT Person_ID_Mother) AS Denominator  FROM {dbSchema}.msd101pregnancybooking a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (AntenatalAppDate between '{start}' AND '{end}') AND PreviousLiveBirths BETWEEN 0 AND 20 AND PreviousStillBirths BETWEEN 0 AND 20 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ28'] = CQIMDQ28

# COMMAND ----------

# DBTITLE 1,CQIMDQ29 - Percentage of babies born that have a valid delivery method recorded in the current reporting period (Singleton)
CQIMDQ29 = { 
      "Measure": "CQIMDQ29"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
    }
CQIM['CQIMDQ29'] = CQIMDQ29

# COMMAND ----------

# DBTITLE 1,CQIMDQ30 - Women giving birth in MSDS  in current 3 months reporting period as percentage of HES average monthly deliveries
# HES denominator - see HES denominator cell below
CQIMDQ30 = { 
      "Measure": "CQIMDQ30"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (a.PersonBirthDateBaby between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": ""
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ30'] = CQIMDQ30

# COMMAND ----------

# DBTITLE 1,CQIMDQ31 - Percentage of babies with a valid gestational age  between 141 days (20wks) and 315 days (45wks) in the current 3 months reporting period
CQIMDQ31 = { 
      "Measure": "CQIMDQ31"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (a.PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between 141 AND 315) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Baby) AS Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (a.PersonBirthDateBaby between '{start}' AND '{end}')  GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ31'] = CQIMDQ31

# COMMAND ----------

# DBTITLE 1,CQIMDQ32 - Percentage of babies with a gestational age between 259 days (37wks) and 315 days (45wks) in the current 3 months reporting period
CQIMDQ32 = { 
      "Measure": "CQIMDQ32"
      ,"Numerator": "SELECT COUNT (DISTINCT Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (a.PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between 259 AND 315) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (a.PersonBirthDateBaby between '{start}' AND '{end}') AND (a.GestationLengthBirth between 141 AND 315) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ32'] = CQIMDQ32

# COMMAND ----------

# DBTITLE 1,CQIMDQ33 - Percentage of babies born that have a valid delivery method recorded in the current  3 months reporting period
CQIMDQ33 = { 
      "Measure": "CQIMDQ33"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a  INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE PersonBirthDateBaby between '{start}' AND '{end}' GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ33'] = CQIMDQ33

# COMMAND ----------

# DBTITLE 1,CQIMDQ34 - Percentage of babies born as vaginal births  in the current 3 months reporting period
CQIMDQ34 = { 
      "Measure": "CQIMDQ34"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4')) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ34'] = CQIMDQ34

# COMMAND ----------

# DBTITLE 1,CQIMDQ35 - Percentage of babies born as caesarean section births in the current 3 months reporting period
# 06/09/23 CQIMDQ35 has been removed from the pipeline
# CQIMDQ35 = { 
#       "Measure": "CQIMDQ35"
#       ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('7','8')) GROUP BY h.OrgCodeProviderMerge"
#       ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) GROUP BY h.OrgCodeProviderMerge"
#       ,"adjStart" : 2
#       ,"length" : 3
#     }
# CQIM['CQIMDQ35'] = CQIMDQ35

# COMMAND ----------

# DBTITLE 1,CQIMDQ36 - Percentage of women that have a valid previous live births and still births data recorded had previous births in current 3 months reporting period
CQIMDQ36 = { 
      "Measure": "CQIMDQ36"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and ((MSD101.PreviousCaesareanSections between 0 and 20) or MSD101.PreviousCaesareanSections is null) and ((MSD101.PreviousLiveBirths between 0 and 20) or MSD101.PreviousLiveBirths is null) and ((MSD101.PreviousStillBirths between 0 and 20) or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' WHERE MSD401.RPStartDate between '{start}' AND '{end}' AND MSD401.PersonBirthDateBaby between '{start}' AND '{end}' GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ36'] = CQIMDQ36

# COMMAND ----------

# DBTITLE 1,CQIMDQ37 - Percentage of women that have a valid previous live births and still births data recorded had zero previous births in current 3 months reporting period
CQIMDQ37 = { 
      "Measure": "CQIMDQ37"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count (distinct MSD401.Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and ((MSD101.PreviousCaesareanSections between 0 and 20) or MSD101.PreviousCaesareanSections is null) and ((MSD101.PreviousLiveBirths between 0 and 20) or MSD101.PreviousLiveBirths is null) and ((MSD101.PreviousStillBirths between 0 and 20) or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null)  GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ37'] = CQIMDQ37

# COMMAND ----------

# DBTITLE 1,CQIMDQ38 - Percentage of babies born who have a valid fetus presentation recorded in the current 3 months reporting period
CQIMDQ38 = { 
      "Measure": "CQIMDQ38"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Baby) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'   WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND  (MSD401.PersonBirthDateBaby between '{start}' AND '{end}') and MSD401.FETUSPRESENTATION in ('01', '02', '03', '04', 'XX')   GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT MSD401.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' WHERE MSD401.RPStartDate between '{start}' AND '{end}' AND (MSD401.PersonBirthDateBaby between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
    }
CQIM['CQIMDQ38'] = CQIMDQ38

# COMMAND ----------

# DBTITLE 1,CQIMDQ39 - Percentage of births that have a valid Labour Onset method data recorded in the current 3 months reporting period
CQIMDQ39 = { 
      "Measure": "CQIMDQ39"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN   {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID AND  MSD401.UniqPregID  = MSD301.UniqPregID WHERE  MSD401.RPStartDate between '{start}' AND '{end}'  and (MSD401.PersonBirthDateBaby between '{start}' AND '{end}') and (MSD301.RPStartDate between '{start}' AND '{end}') AND MSD301.LABOURONSETMETHOD in ('1', '2', '3', '4', '5') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT MSD401.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics as MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' WHERE MSD401.RPStartDate between '{start}' AND '{end}' and (PersonBirthDateBaby between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
     }
CQIM['CQIMDQ39'] = CQIMDQ39

# COMMAND ----------

# DBTITLE 1,CQIMDQ40 - Caesarean section delivery rate in Robson group 1 women
# 06/09/23 CQIMDQ40 has been removed from the pipeline
# CQIMDQ40 = { 
#       "Measure": "CQIMDQ40"
#       ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID AND  MSD401.UniqPregID    = MSD301.UniqPregID  WHERE  (MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD301.RPStartDate between '{start}' AND '{end}' and MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 AND  MSD401.DeliveryMethodCode in ('7', '8') AND  MSD301.LABOURONSETMETHOD = '1'  AND  MSD301.LABOURONSETDATE  IS NOT NULL AND (MSD301.CAESAREANDATE> MSD301.LABOURONSETDATE   OR   MSD301.CAESAREANDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
#       ,"Denominator": "SELECT count (distinct MSD401.Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID AND  MSD401.UniqPregID    = MSD301.UniqPregID  WHERE  (MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD301.RPStartDate between '{start}' AND '{end}' and MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  (MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315) AND  MSD301.LABOURONSETMETHOD = '1'  AND  MSD301.LABOURONSETDATE  IS NOT NULL AND (MSD301.CAESAREANDATE> MSD301.LABOURONSETDATE   OR   MSD301.CAESAREANDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
#       ,"adjStart" : 2
#       ,"length" : 3
#      }
# CQIM['CQIMDQ40'] = CQIMDQ40

# COMMAND ----------

# DBTITLE 1,CQIMDQ41 - Caesarean section delivery rate in Robson group 2 women
# 06/09/23 CQIMDQ41 has been removed from the pipeline
# CQIMDQ41 = { 
#       "Measure": "CQIMDQ41"
#       ,"Numerator": "SELECT  count (distinct MSD401.Person_ID_Mother) as Numerator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN    {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber = MSD101.RecordNumber INNER JOIN {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID = MSD301.UniqPregID  WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 AND  MSD401.DeliveryMethodCode in ('7', '8') AND  (MSD301.LABOURONSETMETHOD   IN ('2','3','4','5') OR   (MSD301.CAESAREANDATE  IS NOT NULL AND MSD301.LABOURONSETDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
#       ,"Denominator": "SELECT  count (distinct MSD401.Person_ID_Mother) as Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM   {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN    {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber = MSD101.RecordNumber INNER JOIN {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID = MSD301.UniqPregID WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315  AND (MSD301.LABOURONSETMETHOD      IN ('2','3','4','5') OR   (MSD301.CAESAREANDATE  IS NOT NULL AND MSD301.LABOURONSETDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
#       ,"adjStart" : 2
#       ,"length" : 3
#      }
# CQIM['CQIMDQ41'] = CQIMDQ41

# COMMAND ----------

# DBTITLE 1,CQIMDQ42 - Caesarean section delivery rate in Robson group 5 women
# 06/09/23 CQIMDQ42 has been removed from the pipeline
# CQIMDQ42 = { 
#       "Measure": "CQIMDQ42"
#       ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN  {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID    = MSD301.UniqPregID WHERE ( MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.DeliveryMethodCode in ('7', '8') AND (MSD101.PreviousCaesareanSections >= 1 AND  MSD101.PreviousCaesareanSections <= 20) AND  MSD401.FETUSPRESENTATION  = '01'  AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 ) group by h.OrgCodeProviderMerge"
#       ,"Denominator": "SELECT count (distinct MSD401.Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN  {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID    = MSD301.UniqPregID WHERE ( MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND (MSD101.PreviousCaesareanSections >= 1 AND  MSD101.PreviousCaesareanSections <= 20) AND  MSD401.FETUSPRESENTATION  = '01'  AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 ) group by h.OrgCodeProviderMerge"
#       ,"adjStart" : 2
#       ,"length" : 3
#     }
# CQIM['CQIMDQ42'] = CQIMDQ42

# COMMAND ----------

# DBTITLE 1,Add ADJStartDate, ADJEndDate, Diff to items in CQIM dictionary
for key, value in CQIM.items():
  RPStart = datetime.strptime(RPBegindate, "%Y-%m-%d")

  AdjStart = RPStart - relativedelta(months=value["adjStart"])
  AdjEnd = AdjStart + relativedelta(months=value["length"]) - timedelta (days = 1)
  DateDiff = (AdjEnd-AdjStart + timedelta(days=1) ).days
  
  CQIM[key]['ADJStartDate'] = str(AdjStart.strftime("%Y-%m-%d"))
  CQIM[key]['ADJEndDate'] = str(AdjEnd.strftime("%Y-%m-%d"))
  CQIM[key]['Diff'] = str(DateDiff)


# COMMAND ----------

# DBTITLE 1,HES Denominator
yrLength = 365
for key in 'CQIMDQ02',  'CQIMDQ03', 'CQIMDQ09', 'CQIMDQ10', 'CQIMDQ14', 'CQIMDQ30':
  Denominator = "SELECT h.OrgCodeProviderMerge as OrgCodeProvider, FLOOR(Sum(TotalDeliveries_HES) * {Diff} / " + str(yrLength) + ") as Denominator FROM \
{outSchema}.msd000header as h left join {outSchema}.HESAnnualBirths as a on h.OrgCodeProviderMerge = a.OrgCodeProviderMerge \
where (org_level = 'Provider') and RPEndDate = '{RPEnd}' \
group by h.OrgCodeProviderMerge"
  print(Denominator)
  CQIM[key]['Denominator'] = Denominator


# COMMAND ----------

# DBTITLE 1,Populate cqim_measures_and_rates with measures - add rates later
RPStart = datetime.strptime(RPBegindate, "%Y-%m-%d")
RPEnd = date(year=(RPStart.year + (RPStart.month == 12)), month=((RPStart.month + 1 if RPStart.month < 12 else 1)), day=1) - timedelta(1)
IndicatorFamily='CQIM_DQ_Measure'
for key, value in CQIM.items():
  Measure = value["Measure"]
  ADJStartDate = value["ADJStartDate"]
  ADJEndDate = value["ADJEndDate"]
  Diff = value["Diff"]
  Denominator = (value["Denominator"]).format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, IndicatorFamily=IndicatorFamily, Indicator=Measure, start=ADJStartDate, end=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, Diff=Diff, dbSchema=dbSchema, outSchema=outSchema)
  Numerator = (value["Numerator"]).format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, IndicatorFamily=IndicatorFamily, Indicator=Measure, start=ADJStartDate, end=ADJEndDate, dbSchema=dbSchema, outSchema=outSchema)
  statement = ("with cteorg as ( \
select distinct org_code, NAME from {dss_corporate}.ORG_DAILY \
WHERE ORG_OPEN_DATE <= '{RPEnddate}' \
AND BUSINESS_END_DATE IS NULL \
AND ((ORG_CLOSE_DATE >= '{RPBegindate}') OR (ORG_CLOSE_DATE IS NULL)) \
) \
, cte2 as ( \
select OrgCodeProvider from {dbSchema}.msd000header \
group by OrgCodeProvider \
) \
, cte3 as ( \
select co.OrgCodeProvider, o.NAME as Org_Name from cte2 as co inner join cteorg as o on co.orgcodeprovider = o.ORG_CODE \
) \
INSERT INTO {outSchema}.cqim_measures_and_rates \
select den.OrgCodeProvider, den.Org_Name, '{RPBegindate}', '{RPEnddate}', '{ADJStartDate}', '{ADJEndDate}', '{IndicatorFamily}', '{Indicator}', num.Numerator, den.Denominator \
from (select c.OrgCodeProvider, c.Org_Name, d.Denominator from cte3 as c left join ({Denominator}) as d on c.OrgCodeProvider = d.OrgCodeProvider) as den \
left join ({Numerator}) as num on den.OrgCodeProvider = num.OrgCodeProvider \
").format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, IndicatorFamily=IndicatorFamily, Indicator=Measure, Numerator=Numerator, Denominator=Denominator, start=ADJStartDate, end=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, dbSchema=dbSchema, outSchema=outSchema)
  print(statement)
  spark.sql(statement)

# COMMAND ----------

# DBTITLE 1,Rates
# Configuration of each measure.
Rates = {}
# Measure - rate name
# Numerator SQL
# Denominator SQL - initially blank where set by HES Denominator cell
# adjStart - the number of months prior to the current month for the measure to cover
# length - the number of months for the measure to cover

# COMMAND ----------

# DBTITLE 1,CQIMApgar
CQIMApgar = { 
      "Measure": "CQIMApgar"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD405CareActivityBaby c ON a.UniqPregID=c.UniqPregID AND a.RPStartDate=c.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND a.PregOutcome in ('01', '02', '03', '04') and a.GestationLengthBirth between 259 AND 315 AND ld.BirthsPerLabandDel = 1 AND c.ApgarScore between 0 and 6 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD405CareActivityBaby c ON a.UniqPregID=c.UniqPregID AND a.RPStartDate=c.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND a.PregOutcome in ('01', '02', '03', '04') and a.GestationLengthBirth between 259 AND 315 AND ld.BirthsPerLabandDel = 1 AND c.ApgarScore between 0 and 10 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMApgar'] = CQIMApgar

# COMMAND ----------

# DBTITLE 1,CQIMBreastfeeding
CQIMBreastfeeding = { 
      "Measure": "CQIMBreastfeeding"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' and '{end}') and (BabyFirstFeedIndCode in ('01','02')) group by h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' and '{end}') and (BabyFirstFeedIndCode in ('01','02', '03')) group by h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
}
Rates['CQIMBreastfeeding'] = CQIMBreastfeeding

# COMMAND ----------

# DBTITLE 1,CQIMPPH
CQIMPPH = { 
      "Measure": "CQIMPPH"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' INNER JOIN {dbSchema}.MSD302CareActivityLabDel b ON (a.UniqPregID = b.UniqPregID AND a.Person_ID_Mother = b.Person_ID_Mother AND a.OrgCodeProvider = b.OrgCodeProvider) WHERE   a.PersonBirthDateBaby BETWEEN '{start}' AND '{end}' AND a.RPStartDate BETWEEN '{start}' AND '{end}' AND a.Pregoutcome IN ('01','02','03','04') AND b.RPStartDate BETWEEN '{start}' AND '{end}' AND b.MasterSnomedCTObsCode IN ( '47821001', '27214003', '1033591000000101', '1033571000000100', '23171006', '200025008','111452009', '49177006', '724496000', '119891000119105', '785322012', '1216656013', '1218146019', '79691012', '494929013', '1230526013', '719051004', '3314819019', '3314820013', '494930015') AND (INT(b.ObsValue) >= 1500 ) and (ClinInterDateMother BETWEEN '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE   PersonBirthDateBaby BETWEEN '{start}' AND '{end}' AND a.RPStartDate BETWEEN '{start}' AND '{end}' AND Pregoutcome IN ('01','02','03','04') GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 3
      ,"length" : 3
}
Rates['CQIMPPH'] = CQIMPPH

# COMMAND ----------

# DBTITLE 1,CQIMPreterm
CQIMPreterm = { 
      "Measure": "CQIMPreterm"
      ,"Numerator": "SELECT count (DISTINCT a.Person_ID_Baby) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate BETWEEN '{start}' AND '{end}' INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate WHERE (PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.GestationLengthBirth BETWEEN '154' and '258') and (a.Pregoutcome = '01') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count (DISTINCT a.Person_ID_Baby) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate BETWEEN '{start}' AND '{end}' INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate WHERE (PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.GestationLengthBirth BETWEEN '154' and '315') and (a.Pregoutcome = '01') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
}
Rates['CQIMPreterm'] = CQIMPreterm

# COMMAND ----------

# DBTITLE 1,CQIMSmokingBooking
CQIMSmokingBooking = { 
      "Measure": "CQIMSmokingBooking"
      ,"Numerator": "SELECT count (DISTINCT Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate BETWEEN '{start}' AND '{end}'WHERE (AntenatalAppDate BETWEEN '{start}' and '{end}') and SmokingStatusBooking_derived = 'Smoker' GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count (DISTINCT Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate BETWEEN '{start}' AND '{end}'WHERE (AntenatalAppDate BETWEEN '{start}' and '{end}') and a.SmokingStatusBooking_derived is not null GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
}
Rates['CQIMSmokingBooking'] = CQIMSmokingBooking

# COMMAND ----------

# DBTITLE 1,CQIMSmokingDelivery
CQIMSmokingDelivery = { 
      "Measure": "CQIMSmokingDelivery"
      ,"Numerator": "SELECT count(distinct a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') AND a.RPStartDate BETWEEN '{start}' AND '{end}' INNER JOIN global_temp.MSD101PregnancyBooking_with_smokingstatusdelivery_derived b ON a.UniqPregID=b.UniqPregID and a.OrgCodeProvider=b.OrgCodeProvider WHERE (a.PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.RPStartDate BETWEEN '{start}' and '{end}') and (a.PregOutcome in ('01', '02', '03', '04')) and (b.SmokingStatusDelivery_derived = 'Smoker') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count(distinct a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate in ('{RPEnddate}' ,'{end}') AND a.RPStartDate BETWEEN '{start}' AND '{end}'  INNER JOIN global_temp.MSD101PregnancyBooking_with_smokingstatusdelivery_derived b ON a.UniqPregID=b.UniqPregID and a.OrgCodeProvider=b.OrgCodeProvider WHERE (a.PersonBirthDateBaby BETWEEN '{start}' and '{end}') and (a.RPStartDate BETWEEN '{start}' and '{end}') and (a.PregOutcome in ('01', '02', '03', '04')) and (b.SmokingStatusDelivery_derived  IN ('Smoker','Non-Smoker / Ex-Smoker')) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 1
      ,"length" : 1
}
Rates['CQIMSmokingDelivery'] = CQIMSmokingDelivery

# COMMAND ----------

# DBTITLE 1,CQIMTears
CQIMTears = { 
      "Measure": "CQIMTears"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' INNER JOIN {dbSchema}.msd302careactivitylabdel ON a.Person_ID_Mother = msd302careactivitylabdel.Person_ID_Mother AND ld.RPStartDate = msd302careactivitylabdel.RPStartDate WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02', '03', '04')) AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND (a.GestationLengthBirth between '259' AND '315') AND msd302careactivitylabdel.GenitalTractTraumaticLesion in ('10217006', '399031001' )  AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider  FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}'  WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.Pregoutcome IN ('01','02', '03', '04')) AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND (a.GestationLengthBirth between '259' AND '315') AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMTears'] = CQIMTears

# COMMAND ----------

# DBTITLE 1,CQIMBWOI
CQIMBWOI = { 
      "Measure": "CQIMBWOI"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD302CareActivityLabDel ca ON ld.UniqPregID=ca.UniqPregID AND ld.RPStartDate=ca.RPStartDate INNER JOIN {dbSchema}.msd101pregnancybooking b ON a.UniqPregID=b.UniqPregID AND a.OrgCodeProvider=b.OrgCodeProvider INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1')) AND (a.GestationLengthBirth between '259' AND '315') AND a.Pregoutcome IN ('01','02','03','04') AND FetusPresentation = '01' AND (labourInductionMethod IS NULL AND NOT EXISTS (SELECT Person_ID_Mother FROM {dbSchema}.MSD302CareActivityLabDel WHERE labourInductionMethod in ('177136006','177135005','408818004','308037008', '177136006', '1105781000000106', '1105791000000108', '236971007', '288189000', '288190009', '288191008' ) AND RPStartDate between '{start}' AND '{end}')) AND (GenitalTractTraumaticLesion in ('85548006', '236992007', '40219000', '26313002', '63407004', '15413009', '17860005', '25828002', '288194000', '249221003', '262935001', '57759005', '6234006', '10217006', '399031001', '199972004', '237329006', '1105801000000107' ) AND NOT EXISTS (SELECT Person_ID_Mother FROM {dbSchema}.MSD302CareActivityLabDel WHERE GenitalTractTraumaticLesion in ('236992007', '40219000', '26313002', '63407004', '15413009', '17860005', '25828002', '288194000', '85548006') or (GenitalTractTraumaticLesion IS NULL AND Episiotomy IS NULL) AND RPStartDate between '{start}' AND '{end}')) AND (LabourAnaesthesiaType in ('68248001','241717009') AND NOT EXISTS (SELECT Person_ID_Mother FROM {dbSchema}.MSD302CareActivityLabDel WHERE LabourAnaesthesiaType in ('16388003', '241717009', '50697003', '67716003', '231249005', '68248001') and (AROM = '408816000' or AROM = '408818004' or AROM IS NULL) AND RPStartDate between '{start}' AND '{end}') AND (AROM = '408816000' or AROM = '408818004' or AROM IS NULL)) AND ld.BirthsPerLabandDel = 1 AND NOT EXISTS (SELECT Person_ID_Mother FROM {dbSchema}.MSD302CareActivityLabDel WHERE OxytocinAdministeredInd = 'Y' AND RPStartDate between '{start}' AND '{end}') GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate LEFT JOIN {dbSchema}.MSD302CareActivityLabDel ca ON ld.UniqPregID=ca.UniqPregID AND ld.RPStartDate=ca.RPStartDate INNER JOIN {dbSchema}.msd101pregnancybooking b ON a.UniqPregID=b.UniqPregID AND a.OrgCodeProvider=b.OrgCodeProvider INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1')) AND (a.GestationLengthBirth between '259' AND '315') AND a.Pregoutcome IN ('01','02','03','04') AND FetusPresentation = '01' AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 0
      ,"length" : 1
}
Rates['CQIMBWOI'] = CQIMBWOI

# COMMAND ----------

# DBTITLE 1,CQIMVBAC
CQIMVBAC = { 
      "Measure": "CQIMVBAC"
      ,"Numerator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {dbSchema}.msd101pregnancybooking b ON a.UniqPregID=b.UniqPregID AND a.OrgCodeProvider=b.OrgCodeProvider INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (b.RPStartDate between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4')) AND (a.GestationLengthBirth between '259' AND '315') AND (PreviousLiveBirths = 1 OR PreviousStillBirths = 1) AND PreviousCaesareanSections = 1 AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT COUNT (DISTINCT a.Person_ID_Mother) AS Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BabyDemographics a INNER JOIN {dbSchema}.MSD301LabourDelivery ld ON a.UniqPregID=ld.UniqPregID AND a.RPStartDate=ld.RPStartDate INNER JOIN {dbSchema}.msd101pregnancybooking b ON a.UniqPregID=b.UniqPregID AND a.OrgCodeProvider=b.OrgCodeProvider INNER JOIN {outSchema}.msd000header h ON a.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' AND a.RPStartDate between '{start}' AND '{end}' WHERE (PersonBirthDateBaby between '{start}' AND '{end}') AND (b.RPStartDate between '{start}' AND '{end}') AND (a.DeliveryMethodCode in ('0','1','2','3','4','5','6','7','8','9')) AND (a.GestationLengthBirth between '259' AND '315') AND (PreviousLiveBirths = 1 OR PreviousStillBirths = 1) AND PreviousCaesareanSections = 1 AND ld.BirthsPerLabandDel = 1 GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMVBAC'] = CQIMVBAC

# COMMAND ----------

# DBTITLE 1,CQIMRobson01
CQIMRobson01 = { 
      "Measure": "CQIMRobson01"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider  FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID AND  MSD401.UniqPregID    = MSD301.UniqPregID  WHERE  (MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD301.RPStartDate between '{start}' AND '{end}' and MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 AND  MSD401.DeliveryMethodCode in ('7', '8') AND  MSD301.LABOURONSETMETHOD = '1'  AND  MSD301.LABOURONSETDATE  IS NOT NULL AND (MSD301.CAESAREANDATE> MSD301.LABOURONSETDATE   OR   MSD301.CAESAREANDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count (distinct MSD401.Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM  {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}'  INNER JOIN {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID AND  MSD401.UniqPregID    = MSD301.UniqPregID  WHERE  (MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD301.RPStartDate between '{start}' AND '{end}' and MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  (MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315) AND  MSD301.LABOURONSETMETHOD = '1'  AND  MSD301.LABOURONSETDATE  IS NOT NULL AND (MSD301.CAESAREANDATE> MSD301.LABOURONSETDATE   OR   MSD301.CAESAREANDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMRobson01'] = CQIMRobson01

# COMMAND ----------

# DBTITLE 1,CQIMRobson02
CQIMRobson02 = { 
      "Measure": "CQIMRobson02"
      ,"Numerator": "SELECT  count (distinct MSD401.Person_ID_Mother) as Numerator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN    {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber = MSD101.RecordNumber INNER JOIN {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID = MSD301.UniqPregID  WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 AND  MSD401.DeliveryMethodCode in ('7', '8') AND  (MSD301.LABOURONSETMETHOD   IN ('2','3','4','5') OR   (MSD301.CAESAREANDATE  IS NOT NULL AND MSD301.LABOURONSETDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT  count (distinct MSD401.Person_ID_Mother) as Denominator,  h.OrgCodeProviderMerge AS OrgCodeProvider FROM   {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN    {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber = MSD101.RecordNumber INNER JOIN {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID = MSD301.UniqPregID WHERE    MSD401.RPStartDate between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.PersonBirthDateBaby between '{start}' AND '{end}' and (MSD101.PreviousCaesareanSections = 0 or MSD101.PreviousCaesareanSections is null) and (MSD101.PreviousLiveBirths = 0 or MSD101.PreviousLiveBirths is null) and (MSD101.PreviousStillBirths = 0 or MSD101.PreviousStillBirths is null) and (MSD101.PreviousLiveBirths is not null or  MSD101.PreviousStillBirths is not null or  MSD101.PreviousCaesareanSections is not null) AND  MSD401.FETUSPRESENTATION  = '01' AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315  AND (MSD301.LABOURONSETMETHOD      IN ('2','3','4','5') OR   (MSD301.CAESAREANDATE  IS NOT NULL AND MSD301.LABOURONSETDATE IS NULL)) GROUP BY h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMRobson02'] = CQIMRobson02

# COMMAND ----------

# DBTITLE 1,CQIMRobson05
CQIMRobson05 = { 
      "Measure": "CQIMRobson05"
      ,"Numerator": "SELECT count (distinct MSD401.Person_ID_Mother) as Numerator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN  {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID    = MSD301.UniqPregID WHERE ( MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND MSD401.DeliveryMethodCode in ('7', '8') AND (MSD101.PreviousCaesareanSections >= 1 AND  MSD101.PreviousCaesareanSections <= 20) AND  MSD401.FETUSPRESENTATION  = '01'  AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 ) group by h.OrgCodeProviderMerge"
      ,"Denominator": "SELECT count (distinct MSD401.Person_ID_Mother) as Denominator, h.OrgCodeProviderMerge AS OrgCodeProvider FROM {dbSchema}.MSD401BABYDEMOGRAPHICS AS MSD401 INNER JOIN {outSchema}.msd000header h ON MSD401.OrgCodeProvider = h.OrgCodeProvider AND h.RPEndDate = '{RPEnd}' INNER JOIN  {dbSchema}.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD401.UniqPregID = MSD101.UniqPregID  AND  MSD401.RecordNumber    = MSD101.RecordNumber INNER JOIN    {dbSchema}.MSD301LABOURDELIVERY   AS MSD301 ON MSD401.LabourDeliveryID = MSD301.LabourDeliveryID  AND  MSD401.UniqPregID    = MSD301.UniqPregID WHERE ( MSD401.RPStartDate between '{start}' AND '{end}' AND  MSD401.PersonBirthDateBaby between '{start}' AND '{end}' AND MSD301.RPStartDate between '{start}' AND '{end}' AND (MSD101.PreviousCaesareanSections >= 1 AND  MSD101.PreviousCaesareanSections <= 20) AND  MSD401.FETUSPRESENTATION  = '01'  AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315 ) group by h.OrgCodeProviderMerge"
      ,"adjStart" : 2
      ,"length" : 3
}
Rates['CQIMRobson05'] = CQIMRobson05

# COMMAND ----------

# DBTITLE 1,Add ADJStartDate, ADJEndDate, Diff to items in CQIM dictionary
for key, value in Rates.items():
  RPStart = datetime.strptime(RPBegindate, "%Y-%m-%d")

  AdjStart = RPStart - relativedelta(months=value["adjStart"])
  AdjEnd = AdjStart + relativedelta(months=value["length"]) - timedelta (days = 1)
  DateDiff = (AdjEnd-AdjStart + timedelta(days=1) ).days
  
  Rates[key]['ADJStartDate'] = str(AdjStart.strftime("%Y-%m-%d"))
  Rates[key]['ADJEndDate'] = str(AdjEnd.strftime("%Y-%m-%d"))
  Rates[key]['Diff'] = str(DateDiff)


# COMMAND ----------

# DBTITLE 1,Populate cqim_measures_and_rates with rates
# cteorg/cte3 used to populate org_name, if organisation name hasn't been resolved (due to it being closed)
# cte2 organisations to report on
# For each rate, get configuration values (Measure etc), then build and execute query to populate cqim_measures_and_rates
RPStart = datetime.strptime(RPBegindate, "%Y-%m-%d")
RPEnd = date(year=(RPStart.year + (RPStart.month == 12)), month=((RPStart.month + 1 if RPStart.month < 12 else 1)), day=1) - timedelta(1)
IndicatorFamily = 'CQIM_Rate'
for key, value in Rates.items():
  Measure = value["Measure"]
  ADJStartDate = value["ADJStartDate"]
  ADJEndDate = value["ADJEndDate"]
  Diff = value["Diff"]
  Denominator = (value["Denominator"]).format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, IndicatorFamily=IndicatorFamily, Indicator=Measure, start=ADJStartDate, end=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, Diff=Diff, dbSchema=dbSchema, outSchema=outSchema)
  Numerator = (value["Numerator"]).format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, IndicatorFamily=IndicatorFamily, Indicator=Measure, start=ADJStartDate, end=ADJEndDate, dbSchema=dbSchema, outSchema=outSchema)
  statement = ("with cteorg as ( \
select distinct org_code, NAME from {dss_corporate}.ORG_DAILY \
WHERE ORG_OPEN_DATE <= '{RPEnddate}' \
AND BUSINESS_END_DATE IS NULL \
AND ((ORG_CLOSE_DATE >= '{RPBegindate}') OR (ORG_CLOSE_DATE IS NULL)) \
) \
, cte2 as ( \
select OrgCodeProvider from {dbSchema}.msd000header \
where rpstartdate between add_months('{RPBegindate}',-12) and '{RPBegindate}' \
group by OrgCodeProvider \
) \
, cte3 as ( \
select co.OrgCodeProvider, o.NAME as Org_Name from cte2 as co inner join cteorg as o on co.orgcodeprovider = o.ORG_CODE \
) \
INSERT INTO {outSchema}.cqim_measures_and_rates \
select den.OrgCodeProvider, den.Org_Name, '{RPBegindate}', '{RPEnddate}', '{ADJStartDate}', '{ADJEndDate}', '{IndicatorFamily}', '{Indicator}', num.Numerator, den.Denominator \
from (select c.OrgCodeProvider, c.Org_Name, d.Denominator from cte3 as c left join ({Denominator}) as d on c.OrgCodeProvider = d.OrgCodeProvider) as den \
left join ({Numerator}) as num on den.OrgCodeProvider = num.OrgCodeProvider \
").format(dss_corporate=dss_corporate, RPBegindate=RPBegindate, RPEnddate=RPEnddate, ADJStartDate=ADJStartDate, ADJEndDate=ADJEndDate, IndicatorFamily=IndicatorFamily, Indicator=Measure, Numerator=Numerator, Denominator=Denominator, start=ADJStartDate, end=ADJEndDate, RPStart=RPStart, RPEnd=RPEnd, dbSchema=dbSchema, outSchema=outSchema)
  print(statement)
  spark.sql(statement)