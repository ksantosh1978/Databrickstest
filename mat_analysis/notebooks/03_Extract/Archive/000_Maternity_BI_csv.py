# Databricks notebook source
# DBTITLE 1,Run all
# MAGIC %md
# MAGIC #### The __trust profile excel workbook__ is a core part of the maternity PowerBI dashboard.
# MAGIC  
# MAGIC It contains 7 tabs;
# MAGIC * **Org_Base**
# MAGIC * Ref_MBRRACEPowe
# MAGIC * **Ref_Org_Stat_Base**
# MAGIC * Ref_Org_Stat_ValueOf
# MAGIC * **Ref_All_Stat**
# MAGIC * **Ref_MBRRACE_Stat**
# MAGIC * Ref_Org_VODIM
# MAGIC    
# MAGIC Of these only 4 (in **bold** above) are used in the dashboard at present, these each have a query (or queries) below. 
# MAGIC  
# MAGIC The end result will be a file named __"Trust Profile Abstracted.xlsb"__ 
# MAGIC # Please note: The final conversion to xlsb, if the file is output as xlsx, may have to be manual.

# COMMAND ----------

import pandas as pd
import numpy as np
import pyspark.sql.functions as F

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#dbutils.widgets.text("RPBegindate","2022-09-01")
#dbutils.widgets.text("RPEnddate", "2022-09-30")
#dbutils.widgets.text("dbSchema", "mat_pre_clear")
#dbutils.widgets.text("outSchema", "mat_analysis")


# COMMAND ----------

RPBegindate = dbutils.widgets.get("RPBegindate")
RPEnddate = dbutils.widgets.get("RPEnddate")
dbSchema = dbutils.widgets.get("dbSchema")
outSchema = dbutils.widgets.get("outSchema")
print(RPBegindate)

# COMMAND ----------

# DBTITLE 1,Check end date widget correct
# Get the period end from the period start
startdateasdate = datetime.strptime(RPBegindate, "%Y-%m-%d")
ReportingEndPeriod = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")

enddateasdate = datetime.strptime(RPEnddate, "%Y-%m-%d").strftime("%Y-%m-%d")

assert ReportingEndPeriod == enddateasdate, f"ERROR: widget date is {enddateasdate} and should be {ReportingEndPeriod} based on the start date"

# COMMAND ----------

# %md 
# Rebuild the Geography view
# Earlier version was referring to a local copy of geography view (geogtlrr table). It has now been updated to refer to mat_analysis.geogtlrr table.

# COMMAND ----------

#get columns from header table
msd000 = spark.table(f"{dbSchema}.msd000header")
msd000 = msd000.select('OrgCodeProvider','RPStartDate','UniqSubmissionID')
msd000 = msd000.filter(f"RPStartdate = '{RPBegindate}'").toPandas()

#get columns from booking table
msd101 = spark.table(f"{dbSchema}.msd101pregnancybooking")
msd101 = msd101.select('Person_ID_Mother','RPStartDate','OrgCodeProvider','UniqSubmissionID')
msd101 = msd101.filter(f"RPStartdate = '{RPBegindate}'")
msd101 = msd101.filter(f"AntenatalAppDate >= '{RPBegindate}'")
msd101 = msd101.filter(f"AntenatalAppDate <= '{RPEnddate}'").toPandas()

#join the above together on uniqsubmissionID
mothers = pd.merge(msd000,
                  msd101,
                  left_on='UniqSubmissionID',
                  right_on='UniqSubmissionID',
                  how='inner')

mothers = mothers[['Person_ID_Mother','RPStartDate_x','OrgCodeProvider_x','UniqSubmissionID']]
mothers.columns = ['Person_ID_Mother','RPStartDate','OrgCodeProvider','UniqSubmissionID']
mothers = mothers.copy()
mothers['Bookings'] = mothers.groupby(['RPStartDate','OrgCodeProvider'])['Person_ID_Mother'].transform('nunique').copy()

#get 401 table
msd401 = spark.table(f"{dbSchema}.msd401babydemographics")
msd401 = msd401.select('Person_ID_Baby','OrgCodeProvider','RPStartDate','UniqSubmissionID','LabourDeliveryID','PersonBirthDateBaby')
msd401 = msd401.filter(f"RPStartdate = '{RPBegindate}'")
msd401 = msd401.filter(f"PersonBirthDateBaby >='{RPBegindate}'")
msd401 = msd401.filter(f"PersonBirthDateBaby <='{RPEnddate}'").toPandas()
msd401['LabourDeliveryID'] = msd401['LabourDeliveryID'].astype(str)
msd401 = msd401[['Person_ID_Baby','OrgCodeProvider','RPStartDate','UniqSubmissionID','LabourDeliveryID']]
msd401['Births'] = msd401.groupby(['RPStartDate','OrgCodeProvider'])['Person_ID_Baby'].transform('nunique').copy()
msd401 = msd401[['OrgCodeProvider','RPStartDate','Births','UniqSubmissionID','LabourDeliveryID']].drop_duplicates(keep="first")
mothers = pd.merge(mothers,
                 msd401,
                 left_on=['OrgCodeProvider','RPStartDate'],
                 right_on=['OrgCodeProvider','RPStartDate'],
                 how='left')

#get 301 table
msd301 = spark.table(f"{dbSchema}.msd301labourdelivery")
msd301 = msd301.select('Person_ID_Mother','OrgCodeProvider','RPStartDate','UniqSubmissionID','LabourDeliveryID')
msd301 = msd301.filter(f"RPStartdate = '{RPBegindate}'").toPandas()
msd301['LabourDeliveryID'] = msd301['LabourDeliveryID'].astype(str)
msd301 = pd.merge(msd301,
                 msd401,
                 left_on=['LabourDeliveryID','UniqSubmissionID'],
                 right_on=['LabourDeliveryID','UniqSubmissionID'],                
                 how='inner')
msd301['Deliveries'] = msd301.groupby(['RPStartDate_x','OrgCodeProvider_x'])['Person_ID_Mother'].transform('nunique').copy()
msd301 = msd301[['OrgCodeProvider_x','RPStartDate_x','Deliveries']].drop_duplicates(keep="first")
msd301.columns = ['OrgCodeProvider','RPStartDate','Deliveries']

mothers = pd.merge(mothers,
                 msd301,
                 left_on=['OrgCodeProvider','RPStartDate'],
                 right_on=['OrgCodeProvider','RPStartDate'],
                 how='left')

mothers = mothers[['Person_ID_Mother','RPStartDate','OrgCodeProvider','Bookings','Births','Deliveries']]

# COMMAND ----------

#Get Tables
#get 001 table
msd001 = spark.table(f"{dbSchema}.msd001motherdemog")
msd001 = msd001.select("*")
msd001 = msd001.filter(f"RPStartdate = '{RPBegindate}'").toPandas()
msd001 = msd001.replace({'None': 'Missing Value'})
msd001 = msd001.replace({None: 'Missing Value'})

#get 101 table
msd101 = spark.table(f"{dbSchema}.msd101pregnancybooking")
msd101 = msd101.select("*")
msd101 = msd101.filter(f"RPStartdate == '{RPBegindate}'")
msd101 = msd101.filter(f"AntenatalAppDate >= '{RPBegindate}'")
msd101 = msd101.filter(f"AntenatalAppDate <= '{RPEnddate}'").toPandas()
msd101 = msd101.replace({'None': 'Missing Value'})
msd101 = msd101.replace({None: 'Missing Value'})
msd101['GestAgeBooking'] = msd101['GestAgeBooking'].replace({'Missing Value': -999})
msd101['PreviousCaesareanSections'] = msd101['PreviousCaesareanSections'].replace({'Missing Value': -999})
msd101['PreviousLiveBirths'] = msd101['PreviousLiveBirths'].replace({'Missing Value': -999})
msd101['PreviousStillBirths'] = msd101['PreviousStillBirths'].replace({'Missing Value': -999})


#get 401 table
msd401 = spark.table(f"{dbSchema}.msd401babydemographics")
msd401 = msd401.select('Person_ID_Mother','Person_ID_Baby','OrgCodeProvider','RPStartDate','UniqSubmissionID','PersonBirthDateBaby','DeliveryMethodCode','GestationLengthBirth','SkinToSkinContact1HourInd')
msd401 = msd401.filter(f"RPStartdate = '{RPBegindate}'")
msd401 = msd401.filter(f"PersonBirthDateBaby >= '{RPBegindate}'")
msd401 = msd401.filter(f"PersonBirthDateBaby <= '{RPEnddate}'").toPandas()
msd401['GestationLengthBirth'] = msd401['GestationLengthBirth'].replace({None: -999})
msd401['DeliveryMethodCode'] = msd401['DeliveryMethodCode'].replace({None: -999})
msd401['SkinToSkinContact1HourInd'] = msd401['SkinToSkinContact1HourInd'].replace({None: 'Missing Value'})

# COMMAND ----------

#get calculated values
#AgeAtBookingMotherGroup
msd101['AgeAtBookingMotherGroup']= np.where(msd101['AgeAtBookingMother'] == 'Missing Value', 'Missing Value', 
                                            np.where((msd101['AgeAtBookingMother'] >=11) & (msd101['AgeAtBookingMother'] <=19), '19 & under',
                                            np.where((msd101['AgeAtBookingMother'] >=20) & (msd101['AgeAtBookingMother'] <=24), '20 to 24',
                                            np.where((msd101['AgeAtBookingMother'] >=25) & (msd101['AgeAtBookingMother'] <=29), '25 to 29',
                                            np.where((msd101['AgeAtBookingMother'] >=30) & (msd101['AgeAtBookingMother'] <=34), '30 to 34',
                                            np.where((msd101['AgeAtBookingMother'] >=35) & (msd101['AgeAtBookingMother'] <=39), '35 to 39',
                                            np.where((msd101['AgeAtBookingMother'] >=40) & (msd101['AgeAtBookingMother'] <=44), '40 to 44',
                                            np.where((msd101['AgeAtBookingMother'] >=45) & (msd101['AgeAtBookingMother'] <=60), '45 or over', 
                                            np.where((msd101['AgeAtBookingMother'] <11) | (msd101['AgeAtBookingMother'] >60), 'Value outside reporting parameters', msd101['AgeAtBookingMother'] )))))))))

#BMI
# msd101['BMI'] =  np.where(msd101['PersonBMIBand'] == 'Missing Value', '5.  Missing', 
#                           np.where(msd101['PersonBMIBand'] == 'Less than 18.5', '1.  Underweight',
#                           np.where(msd101['PersonBMIBand'] == '18.5 - 24.9', '2.  Normal',
#                           np.where(msd101['PersonBMIBand'] == '25 - 29.9','3.  Overweight',
#                           np.where((msd101['PersonBMIBand'] == '30 - 39.9')|(msd101['PersonBMIBand'] == '40 or more'),'4.  Obese',
#                                    'Value outside reporting parameters')))))

#Complex Social Factors
msd101['ComplexSocialFactorsInd']= np.where(msd101['ComplexSocialFactorsInd'] == 'Missing Value','Missing',
                                             np.where(msd101['ComplexSocialFactorsInd'].str.upper() == 'Y', 'Yes',
                                             np.where(msd101['ComplexSocialFactorsInd'].str.upper() == 'N', 'No', 
                                                  'Outside parameters')))

#DeprivationDecile at booking
msd001['DeprivationDecileAtBooking'] = np.where(msd001['Rank_IMD_Decile_2015'] != 'None',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '01 - Most deprived','1st Decile (Most)',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '02','2nd Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '03','3rd Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '04','4th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '05','5th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '06','6th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '07','7th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '08','8th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '09','9th Decile',
                                                np.where(msd001['Rank_IMD_Decile_2015'] == '10 - Least deprived','10th Decile (Least)','Missing / outside parameters'
                                                        )))))))))),
										np.where((msd001['Rank_IMD_Decile_2015'] == 'Missing Value') & (msd001['LSOAMother2011'] != 'Missing Value'), 'Resident Elsewhere in UK',
										np.where((msd001['Rank_IMD_Decile_2015'] == 'Missing Value') & (msd001['PostcodeDistrictMother'] != 'Missing Value'), 'Pseudo Postcode','Value outside reporting parameters')))

#EthnicCategoryMotherGroup
msd001['EthnicCategoryMotherGroup'] = np.where((msd001['EthnicCategoryMother'].str.upper() == 'A') | 
                                               (msd001['EthnicCategoryMother'].str.upper() == 'B') | 
                                               (msd001['EthnicCategoryMother'].str.upper() == 'C'), 'White',
                                       np.where((msd001['EthnicCategoryMother'].str.upper() == 'D') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'E') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'F') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'G'), 'Mixed',
                                        np.where((msd001['EthnicCategoryMother'].str.upper() == 'H') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'J') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'K') | 
                                                 (msd001['EthnicCategoryMother'].str.upper() == 'L'), 'Asian or Asian British',
                                       np.where((msd001['EthnicCategoryMother'].str.upper() == 'M') | 
                                               (msd001['EthnicCategoryMother'].str.upper() == 'N') | 
                                               (msd001['EthnicCategoryMother'].str.upper() == 'P'), 'Black or Black British',
                                       np.where((msd001['EthnicCategoryMother'].str.upper() == 'R') | 
                                                (msd001['EthnicCategoryMother'].str.upper() == 'S'), 'Any other ethnic group',
                                       np.where((msd001['EthnicCategoryMother'].str.upper() == 'Z'), 'Not Stated',
                                       np.where((msd001['EthnicCategoryMother'].str.upper() == '99'), 
                                                'Not Known', 'Missing / outside parameters')))))))
                                                 
#GestAgeFormalAntenatalBookingGroup
msd101['GestAgeFormalAntenatalBookingGroup'] = np.where((msd101['GestAgeBooking'] >= 0) & (msd101['GestAgeBooking'] <=70), '1.  0 to 70 days',
                                                       np.where((msd101['GestAgeBooking'] >= 71) & (msd101['GestAgeBooking']<=90), '2.  71 to 90 days',
                                                       np.where((msd101['GestAgeBooking'] >= 91) & (msd101['GestAgeBooking']<=140), '3.  91 to 140 days',
                                                       np.where((msd101['GestAgeBooking'] >= 141) & (msd101['GestAgeBooking']<=999), '4.  141+ days',
                                                       'Missing / outside parameters'))))
#PreviousCaesaereanSectionsGroup
msd101['PreviousCaesareanSectionsGroup'] = np.where(msd101['PreviousCaesareanSections'] >= 1, 'At least one Caesarean Section',
                                                   np.where(((msd101['PreviousLiveBirths'] >0 ) | (msd101['PreviousStillBirths'] >0)) & (msd101['PreviousCaesareanSections'] == 0), 'At least one previous birth, zero caesareans',
                                                   np.where(
                                                     ((msd101['PreviousCaesareanSections'] == 'Missing Value') | (msd101['PreviousCaesareanSections'] == 0)) &
                                                     ((msd101['PreviousLiveBirths'] == 'Missing Value') | (msd101['PreviousLiveBirths'] == 0)) &
                                                     ((msd101['PreviousStillBirths'] == 'Missing Value') | (msd101['PreviousStillBirths'] == 0)),
                                                     'Zero Previous Births','Missing / outside parameters')))

       

#PreviousLiveBirthsGroup
msd101['PreviousLiveBirthsGroup'] = np.where((msd101['PreviousLiveBirths'] >= 1) & (msd101['PreviousLiveBirths'] <= 4), msd101['PreviousLiveBirths'],
                                             np.where(msd101['PreviousLiveBirths'] >= 5, 'More than 5',
                                             np.where(msd101['PreviousLiveBirths'] == 0, ' No Previous live births', 'Missing / outside parameters')))

msd401['MethodOfDelivery'] = np.where(((msd401['DeliveryMethodCode'] == '0') | 
                                      (msd401['DeliveryMethodCode'] == '1') | 
                                      (msd401['DeliveryMethodCode'] == '5')), 'Spontaneous',
                                    np.where((msd401['DeliveryMethodCode'] == '2') | 
                                      (msd401['DeliveryMethodCode'] == '3') | 
                                      (msd401['DeliveryMethodCode'] == '4') |
                                      (msd401['DeliveryMethodCode'] == '6'), 'Instrumental',
                                    np.where(msd401['DeliveryMethodCode'] == '7', 'Elective c-section',
                                    np.where(msd401['DeliveryMethodCode'] == '8', 'Emergency c-section', 
                                    np.where(msd401['DeliveryMethodCode'] == '9', 'Other', 
                                    'Missing / outside parameters')))))

msd401['AgeAtDelivery'] = np.where((msd401['GestationLengthBirth'] >= 141) & (msd401['GestationLengthBirth'] <= 195 ), "27 weeks and under",
                                    np.where((msd401['GestationLengthBirth'] >= 196) & (msd401['GestationLengthBirth'] <= 223 ), "28 to 31 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 224) & (msd401['GestationLengthBirth'] <= 237 ), "32 to 33 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 238) & (msd401['GestationLengthBirth'] <= 258 ), "34 to 36 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 259) & (msd401['GestationLengthBirth'] <= 265 ), "37 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 266) & (msd401['GestationLengthBirth'] <= 272 ), "38 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 273) & (msd401['GestationLengthBirth'] <= 279 ), "39 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 280) & (msd401['GestationLengthBirth'] <= 286 ), "40 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 287) & (msd401['GestationLengthBirth'] <= 293 ), "41 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 294) & (msd401['GestationLengthBirth'] <= 300 ), "42 weeks",
                                    np.where((msd401['GestationLengthBirth'] >= 301) & (msd401['GestationLengthBirth'] <= 315), "43 weeks and over",
                                   'Missing / outside parameters')))))))))))

msd401['SkintoSkin'] = np.where(
                        ((msd401['SkinToSkinContact1HourInd'].str.upper() == 'Y') & ((msd401['GestationLengthBirth'] >= 259) & (msd401['GestationLengthBirth'] <= 315 ))),'Yes',
                         np.where(
                        ((msd401['SkinToSkinContact1HourInd'].str.upper() == 'N') & ((msd401['GestationLengthBirth'] >= 259) & (msd401['GestationLengthBirth'] <= 315 ))) ,'No', 
                           'Missing / outside parameters'))

# COMMAND ----------


#select columns from both tables
msd001 = msd001[['CCGResidenceMother',
                   'EthnicCategoryMother',
                   'LAD_UAMother',
                   'LSOAMother2011',
                   'OrgCodeProvider',
                   'OrgIDLPID',
                   'OrgIDResidenceResp',
                   'Person_ID_Mother',
                   'Postcode',
                   'PostcodeDistrictMother',
                   'RPStartDate',
                   'Rank_IMD_Decile_2015',
                   #'UniqPregID',
                   'UniqSubmissionID',
                   'DeprivationDecileAtBooking',
                   'EthnicCategoryMotherGroup']]

msd101 = msd101[['AgeAtBookingMother',
                  'AlcoholUnitsBooking',
                  'AlcoholUnitsPerWeekBand',
                  'AntenatalAppDate',
                  'CigarettesPerDayBand',
                  'ComplexSocialFactorsInd',
                  'DisabilityIndMother',
                  'EmploymentStatusMother',
                  'EmploymentStatusPartner',
                  'FolicAcidSupplement',
                  'GestAgeBooking',
                  'MHPredictionDetectionIndMother',
                  'OrgCodeProvider',
#                   'PersonBMIBand',
                  'Person_ID_Mother',
                  'PregnancyID',
                  'PreviousCaesareanSections',
                  'PreviousLiveBirths',
                  'PreviousStillBirths',
                  'RPStartDate',
                  'SmokingStatusBooking',
                  'SmokingStatusDelivery',
                  'SmokingStatusDischarge',
                  'UniqPregID',
                  'UniqSubmissionID',
                  'AgeAtBookingMotherGroup',
#                   'BMI',
                  'GestAgeFormalAntenatalBookingGroup',
                  'PreviousCaesareanSectionsGroup',
                  'PreviousLiveBirthsGroup'
                  ]]

msd401 = msd401[['Person_ID_Mother',
                'Person_ID_Baby',
                'OrgCodeProvider',
                'RPStartDate',
                'UniqSubmissionID',
                'PersonBirthDateBaby',
                'MethodOfDelivery',
                'AgeAtDelivery',
                'SkintoSkin']]

#do the join
msd101001_base = pd.merge(msd101,msd001,
                     left_on=['UniqSubmissionID','Person_ID_Mother'],
                     right_on=['UniqSubmissionID','Person_ID_Mother'],
                     how='left')


msd101001 = msd101001_base[['Person_ID_Mother',
                            'OrgCodeProvider_x',
                            'RPStartDate_x',
                            'AgeAtBookingMotherGroup',
#                             'BMI',
                            'GestAgeFormalAntenatalBookingGroup',
                            'PreviousCaesareanSectionsGroup',
                            'PreviousLiveBirthsGroup',
                            'DeprivationDecileAtBooking',
                            'EthnicCategoryMotherGroup',
                            'ComplexSocialFactorsInd'
                           ]].drop_duplicates(keep="first")

# COMMAND ----------

#seperate for pivotting
msd_ageAtBooking = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','AgeAtBookingMotherGroup']].copy()
msd_ageAtBooking.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_ageAtBooking['Indicator'] = 'Age of mother at booking'
msd_ageAtBooking['Count_of'] = 'Women'

# msd_BMI = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','BMI']].copy()
# msd_BMI.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
# msd_BMI['Indicator'] = 'BMI of mother at booking'
# msd_BMI['Count_of'] = 'Women'

msd_comSoc = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','ComplexSocialFactorsInd']].copy()
msd_comSoc.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_comSoc['Indicator'] = 'Complex Social Factors'
msd_comSoc['Count_of'] = 'Women'

msd_ethCat = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','EthnicCategoryMotherGroup']].copy()
msd_ethCat.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_ethCat['Indicator'] = 'Ethnic category of mother'
msd_ethCat['Count_of'] = 'Women'

msd_gestAge = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','GestAgeFormalAntenatalBookingGroup']].copy()
msd_gestAge.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_gestAge['Indicator'] = 'Gestational Age at Booking'
msd_gestAge['Count_of'] = 'Women'

msd_depInd = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','DeprivationDecileAtBooking']].copy()
msd_depInd.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_depInd['Indicator'] = 'Index of deprivation of mother at booking'
msd_depInd['Count_of'] = 'Women'

msd_delMet = msd401[['Person_ID_Baby', 'OrgCodeProvider', 'RPStartDate', 'MethodOfDelivery']].copy()
msd_delMet.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_delMet['Indicator'] = 'Method of Delivery'
msd_delMet['Count_of'] = 'Babies'   

msd_ageDel = msd401[['Person_ID_Baby', 'OrgCodeProvider', 'RPStartDate', 'AgeAtDelivery']].copy()
msd_ageDel.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_ageDel['Indicator'] = 'Gestational age at Delivery'
msd_ageDel['Count_of'] = 'Babies' 

msd_sk2Sk = msd401[['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate', 'SkintoSkin']].copy()
msd_sk2Sk.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_sk2Sk['Indicator'] = 'Skin to skin contact at 1 hour'
msd_sk2Sk['Count_of'] = 'Babies' 
                  
#NEW MEASURES
msd_prevCaes = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','PreviousCaesareanSectionsGroup']].copy()
msd_prevCaes.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_prevCaes['Indicator'] = 'Previous Caesarean Sections Group'
msd_prevCaes['Count_of'] = 'Women'

msd_prevLive = msd101001[['Person_ID_Mother', 'OrgCodeProvider_x', 'RPStartDate_x','PreviousLiveBirthsGroup']].copy()
msd_prevLive.columns= ['Person_ID_Mother', 'OrgCodeProvider', 'RPStartDate','Measure']
msd_prevLive['Indicator'] = 'Previous live births Group'
msd_prevLive['Count_of'] = 'Women'


#Concatenate
msd_concat = pd.concat([msd_ageAtBooking,
#                           msd_BMI,
                          msd_gestAge,
                          msd_prevCaes,
                          msd_prevLive,
                          msd_depInd,
                          msd_ethCat,
                          msd_comSoc,
                          msd_delMet,
                          msd_ageDel,
                          msd_sk2Sk
                       ],
                      ignore_index=True, sort=True)

# COMMAND ----------

#get org denominator
denom = msd_concat.copy()
denom = denom.groupby([msd_concat.OrgCodeProvider, msd_concat.RPStartDate, msd_concat.Indicator])['Person_ID_Mother'].nunique()
#denom.drop_duplicates(keep="first", inplace=True)
denom = denom.reset_index()
denom.columns=['OrgCodeProvider','RPStartDate','Indicator','Denominator_Org']

#get org Numerator
numer = msd_concat.copy()
numer = numer.groupby([msd_concat.OrgCodeProvider, msd_concat.RPStartDate, msd_concat.Indicator, msd_concat.Measure])['Person_ID_Mother'].nunique()
#numer.drop_duplicates(keep="first", inplace=True)
numer = numer.reset_index()
numer.columns=['OrgCodeProvider','RPStartDate','Indicator','Measure','Numerator_Org']

#add to values df
msd_concat = msd_concat.merge(denom, on=['OrgCodeProvider','RPStartDate','Indicator'], how="left") 
msd_concat = msd_concat.merge(numer, on=['OrgCodeProvider','RPStartDate','Indicator','Measure'], how="left") 

#Add Geogs #THIS GEOGS VIEW/TABLE MUST ONLY BE USED FOR THE MONTH JUST RUN FOR MONTHLY PROCESSING - TO RUN INDEPENDENTLY NEED TO REBUILD VIEW
geog = spark.table(f'{outSchema}.geogtlrr').select('Trust_ORG','Trust','Mbrrace_Grouping','Mbrrace_Grouping_Short','STP_Code','STP_Name').toPandas()
msd_concat = pd.merge(msd_concat,geog,
                     left_on=['OrgCodeProvider'],
                     right_on=['Trust_ORG'],
                     how='left')

# COMMAND ----------

null_cols = msd_concat["Mbrrace_Grouping"].isnull()
check_pdf = msd_concat[null_cols]
# print(type(check_pdf))
check_pdf
assert check_pdf.empty, "some organisation does not have an Mbrrace_Grouping"

# COMMAND ----------

#get MBRRACE denominator
Mbrrace_denom = msd_concat.copy()
Mbrrace_denom = Mbrrace_denom.groupby([msd_concat.Mbrrace_Grouping, msd_concat.RPStartDate, msd_concat.Indicator])['Person_ID_Mother'].nunique()
Mbrrace_denom = Mbrrace_denom.reset_index()
Mbrrace_denom.columns=['Mbrrace_Grouping','RPStartDate','Indicator','Denominator_MBRRACE']
#get MBRRACE Numerator
Mbrrace_numer = msd_concat.copy()
Mbrrace_numer = Mbrrace_numer.groupby([msd_concat.Mbrrace_Grouping, msd_concat.RPStartDate, msd_concat.Indicator, msd_concat.Measure])['Person_ID_Mother'].nunique()
Mbrrace_numer = Mbrrace_numer.reset_index()
Mbrrace_numer.columns=['Mbrrace_Grouping','RPStartDate','Indicator','Measure','Numerator_MBRRACE']
#add to values df
msd_concat = msd_concat.merge(Mbrrace_denom, on=['Mbrrace_Grouping','RPStartDate','Indicator'], how="left") 
msd_concat = msd_concat.merge(Mbrrace_numer, on=['Mbrrace_Grouping','RPStartDate','Indicator','Measure'], how="left") 


#get STP denominator
STP_denom = msd_concat.copy()
STP_denom = STP_denom.groupby([msd_concat.STP_Code, msd_concat.RPStartDate, msd_concat.Indicator])['Person_ID_Mother'].nunique()
STP_denom = STP_denom.reset_index()
STP_denom.columns=['STP_Code','RPStartDate','Indicator','Denominator_STP']
#get STP Numerator
STP_numer = msd_concat.copy()
STP_numer = STP_numer.groupby([msd_concat.STP_Code, msd_concat.RPStartDate, msd_concat.Indicator, msd_concat.Measure])['Person_ID_Mother'].nunique()
STP_numer = STP_numer.reset_index()
STP_numer.columns=['STP_Code','RPStartDate','Indicator','Measure','Numerator_STP']
#add to values df
msd_concat = msd_concat.merge(STP_denom, on=['STP_Code','RPStartDate','Indicator'], how="left") 
msd_concat = msd_concat.merge(STP_numer, on=['STP_Code','RPStartDate','Indicator','Measure'], how="left") 


#get National denominator
National_denom = msd_concat.copy()
National_denom = National_denom.groupby([msd_concat.RPStartDate, msd_concat.Indicator])['Person_ID_Mother'].nunique()
National_denom = National_denom.reset_index()
National_denom.columns=['RPStartDate','Indicator','Denominator_National']
#get National Numerator
National_numer = msd_concat.copy()
National_numer = National_numer.groupby([msd_concat.RPStartDate, msd_concat.Indicator, msd_concat.Measure])['Person_ID_Mother'].nunique()
National_numer = National_numer.reset_index()
National_numer.columns=['RPStartDate','Indicator','Measure','Numerator_National']
#add to values df
msd_concat = msd_concat.merge(National_denom, on=['RPStartDate','Indicator'], how="left") 
msd_concat = msd_concat.merge(National_numer, on=['RPStartDate','Indicator','Measure'], how="left") 
#msd_concat = msd_concat.drop_duplicates(keep="first")

msd_values = msd_concat[['OrgCodeProvider', 
                         'RPStartDate', 
                         'Indicator', 
                         'Measure', 
                         'Count_of', 
                         'Denominator_Org', 
                         'Numerator_Org', 
                         'Trust', 
                         'Mbrrace_Grouping', 
                         'Mbrrace_Grouping_Short', 
                         'STP_Code', 
                         'STP_Name',
                         'Denominator_MBRRACE',
                         'Numerator_MBRRACE',
                         'Denominator_STP',
                         'Numerator_STP',
                         'Denominator_National',
                         'Numerator_National']].copy()

msd_values.drop_duplicates(keep="first",inplace=True)
msd_values.sort_values(['OrgCodeProvider','RPStartDate','Indicator','Measure'], inplace=True)
msd_values.reset_index(inplace=True,drop=True)

# COMMAND ----------

#suppress
#suppression defaults
lowest = 7 #round anything upto and including this number down to value in default_sup
nearest = 5 #round all numbers ave 'lowest' to the nearest
default_sup = 5 #lower suppression number: anything that is > 0 and <= 'lowest'
#ORG
msd_values['Denominator_Org_Suppressed'] = np.where((msd_values['Denominator_Org']<=lowest) & (msd_values['Denominator_Org']>0), default_sup, nearest * round(msd_values['Denominator_Org']/nearest))
msd_values['Numerator_Org_Suppressed'] = np.where((msd_values['Numerator_Org']<=lowest) & (msd_values['Numerator_Org']>0), default_sup, nearest * round(msd_values['Numerator_Org']/nearest))
msd_values['Raw_Percent_Org'] = round((msd_values.Numerator_Org/msd_values.Denominator_Org)*100)
msd_values['Supressed_Percent_Org'] = round((msd_values.Numerator_Org_Suppressed/msd_values.Denominator_Org_Suppressed)*100)
#MBRRACE
msd_values['Denominator_MBRRACE_Suppressed'] = np.where((msd_values['Denominator_MBRRACE']<=lowest) & (msd_values['Denominator_MBRRACE']>0), default_sup, nearest * round(msd_values['Denominator_MBRRACE']/nearest))
msd_values['Numerator_MBRRACE_Suppressed'] = np.where((msd_values['Numerator_MBRRACE']<=lowest) & (msd_values['Numerator_MBRRACE']>0), default_sup, nearest * round(msd_values['Numerator_MBRRACE']/nearest))
msd_values['Raw_Percent_MBRRACE'] = round((msd_values.Numerator_MBRRACE/msd_values.Denominator_MBRRACE)*100)
msd_values['Supressed_Percent_MBRRACE'] = round((msd_values.Numerator_MBRRACE_Suppressed/msd_values.Denominator_MBRRACE_Suppressed)*100)
#STP
msd_values['Denominator_STP_Suppressed'] = np.where((msd_values['Denominator_STP']<=lowest) & (msd_values['Denominator_STP']>0), default_sup, nearest * round(msd_values['Denominator_STP']/nearest))
msd_values['Numerator_STP_Suppressed'] = np.where((msd_values['Numerator_STP']<=lowest) & (msd_values['Numerator_STP']>0), 0, nearest * round(msd_values['Numerator_STP']/nearest))
msd_values['Raw_Percent_STP'] = round((msd_values.Numerator_STP/msd_values.Denominator_STP)*100)
msd_values['Supressed_Percent_STP'] = round((msd_values.Numerator_STP_Suppressed/msd_values.Denominator_STP_Suppressed)*100)
#NATIONAL
msd_values['Denominator_National_Suppressed'] = np.where((msd_values['Denominator_National']<=lowest) & (msd_values['Denominator_National']>0), default_sup, nearest * round(msd_values['Denominator_National']/nearest))
msd_values['Numerator_National_Suppressed'] = np.where((msd_values['Numerator_National']<=lowest) & (msd_values['Numerator_National']>0), 0, nearest * round(msd_values['Numerator_National']/nearest))
msd_values['Raw_Percent_National'] = round((msd_values.Numerator_National/msd_values.Denominator_National)*100)
msd_values['Supressed_Percent_National'] = round((msd_values.Numerator_National_Suppressed/msd_values.Denominator_National_Suppressed)*100)

#Add MBBRACE_abv
msd_values['MBRRACE_abv'] = 'MB' + msd_values['Mbrrace_Grouping'].str.extract('Group (\d)')
msd_values['RPStartDate'] = pd.to_datetime(msd_values['RPStartDate'])
msd_values['RPStartDate'] = msd_values['RPStartDate'].dt.strftime("%B %Y")

# COMMAND ----------

#print(msd_values['Mbrrace_Grouping'].value_counts())

# COMMAND ----------

#display(msd_values[msd_values['Mbrrace_Grouping'].isnull()])

# COMMAND ----------

# DBTITLE 1,BMI of mother at 15 weeks
bmi_wks = (
  spark.read.table(f'{outSchema}.measures_aggregated')
  .where(f'RPStartDate="{RPBegindate}" AND RPEndDate="{RPEnddate}"')
  .where("Indicator='BMI_SeverelyUnderweight' OR Indicator='BMI_Underweight' OR Indicator='BMI_Normal' OR Indicator='BMI_Overweight' OR Indicator='BMI_Obese' OR Indicator='BMI_SeverelyObese' OR Indicator='BMI_NotRecorded' ")
  .where("IndicatorFamily='BMI_14+1Wks'")
)

bmi_wks = (
  bmi_wks
  .replace('BMI_SeverelyUnderweight', 'Severely Underweight', subset='Indicator')
  .replace('BMI_Underweight', 'Underweight', subset='Indicator')
  .replace('BMI_Normal', 'Normal', subset='Indicator')
  .replace('BMI_Overweight', 'Overweight', subset='Indicator')
  .replace('BMI_Obese', 'Obese', subset='Indicator')
  .replace('BMI_SeverelyObese', 'Severely Obese', subset='Indicator')
  .replace('BMI_NotRecorded', 'Not Recorded', subset='Indicator')
)
 
# Renaming to make the columns same as the rest of the code
bmi_wks = (
  bmi_wks
  .select(
    F.col('RPStartDate').alias('Data_period'),
    F.col('OrgCodeProvider').alias('Org_Code'),
    F.lit('BMI of mother at 15 weeks gestation').alias('Indicator'),
    F.col('Indicator').alias('Measure'),
    F.lit('Women').alias('Count_Of'),
    F.col('Rounded_Numerator').alias('Numerator_Org'),
    F.col('Rounded_Denominator').alias('Denominator_Org'),
    F.col('Rounded_Rate').alias('Percent_Org'),
    F.col('OrgLevel')
  )
)
 
# Making it similar to existing field
bmi_wks = bmi_wks.withColumn('Data_period', F.date_format('Data_period', 'MMMM yyyy'))
 
# Converting to pandas as the existing code uses pandas
bmi_wks = bmi_wks.toPandas()
 
# For some reason conversion to pandas does not bring correct rounded figures
bmi_wks['Percent_Org'] = bmi_wks['Percent_Org'].astype(float).round(1)
 
# One last thing to make it similar to the existing code
bmi_wks['MBRRACE_abv'] = 'MB' + bmi_wks['Org_Code'].str.extract('Group (\d)')  # MBRRACE `Group 1 *` to `MB1`

# COMMAND ----------

org_base = msd_values[['RPStartDate', 
                       'OrgCodeProvider', 
                       'Indicator', 
                       'Measure', 
                       'Count_of', 
                       'Numerator_Org_Suppressed', 
                       'Denominator_Org_Suppressed', 
                       'Supressed_Percent_Org']].copy()
org_base.columns= ['Data_period', 'Org_Code', 'Indicator', 'Measure', 'Count_Of', 'Numerator_Org', 'Denominator_Org', 'Percent_Org']

# COMMAND ----------

# DBTITLE 1,Concat Provider level BMI of mother at 15 weeks measure to org_base
concat_bmi_wks_provider = bmi_wks.query("OrgLevel == 'Provider'").drop(columns=['OrgLevel', 'MBRRACE_abv'])
org_base = pd.concat([org_base, concat_bmi_wks_provider])

# COMMAND ----------

# DBTITLE 1,Sorting org_base
org_base['sortOrder'] = np.where(org_base['Measure'].str.upper() == 'YES', '1',
                                 np.where(org_base['Measure'].str.upper() == 'NO', '2',
                                 np.where(org_base['Measure'] is np.nan, '999',
                                 np.where(org_base['Measure'].str[:2].str[0].str.isnumeric(), org_base['Measure'].str[:2].astype(str),
                                 np.where(org_base['Measure'].str[0].str.isnumeric(), org_base['Measure'].str[0],'999')))))
org_base.sort_values(['Org_Code', 'Data_period', 'Indicator', 'sortOrder', 'Measure'], inplace=True)
display(org_base)

# COMMAND ----------

RefOrgStatBase = mothers[['OrgCodeProvider','RPStartDate','Bookings','Births','Deliveries']].drop_duplicates(keep="first").groupby([mothers.OrgCodeProvider,mothers.RPStartDate])[['Bookings','Births','Deliveries']].sum().reset_index()
RefOrgStatBase['Org_Count'] = RefOrgStatBase.groupby('RPStartDate')['OrgCodeProvider'].transform('nunique').copy()
RefOrgStatBase.columns= ['Org_Code', 'Data_period', 'Bookings', 'Births', 'Deliveries', 'Org_Count']
RefOrgStatBase['Data_period'] = pd.to_datetime(RefOrgStatBase['Data_period']) # PE added for consistency 
RefOrgStatBase['Data_period'] = RefOrgStatBase['Data_period'].dt.strftime("%B %Y") # PE added for consistency 
RefOrgStatBase = RefOrgStatBase.drop_duplicates(keep="first")
RefOrgStatBase.sort_values(['Org_Code', 'Data_period'], inplace=True)
#suppress
RefOrgStatBase['Bookings'] = np.where((RefOrgStatBase['Bookings']<=lowest) & (RefOrgStatBase['Bookings'] >0), default_sup, nearest * round(RefOrgStatBase['Bookings']/nearest))
RefOrgStatBase['Births'] = np.where((RefOrgStatBase['Births']<=lowest) & (RefOrgStatBase['Births'] >0), default_sup, nearest * round(RefOrgStatBase['Births']/nearest))
RefOrgStatBase['Deliveries'] = np.where((RefOrgStatBase['Deliveries']<=lowest) & (RefOrgStatBase['Deliveries'] >0), default_sup, nearest * round(RefOrgStatBase['Deliveries']/nearest))
RefOrgStatBase = RefOrgStatBase[['Data_period','Org_Code','Births','Bookings','Deliveries','Org_Count']]
 
display(RefOrgStatBase)

# COMMAND ----------

Ref_All_Stat = msd_values[['RPStartDate','Indicator','Measure','Numerator_National_Suppressed','Denominator_National_Suppressed','Supressed_Percent_National']].copy()
Ref_All_Stat.columns = ['Data_period','Indicator','Measure','Numerator_All','Denominator_All','Percent_All']
Ref_All_Stat = Ref_All_Stat.drop_duplicates(keep="first")
Ref_All_Stat.sort_values(['Data_period', 'Indicator', 'Measure'], inplace=True)
#print(Ref_All_Stat.to_csv(index=False))
display(Ref_All_Stat)

# COMMAND ----------

# DBTITLE 1,Concat National level BMI of mother at 15 weeks measure to org_base
concat_bmi_wks_national = bmi_wks.query("OrgLevel == 'National'").drop(columns=['OrgLevel', 'Org_Code', 'Count_Of', 'MBRRACE_abv']).rename(columns={'Numerator_Org': 'Numerator_All', 'Denominator_Org': 'Denominator_All', 'Percent_Org': 'Percent_All'})
Ref_All_Stat = pd.concat([Ref_All_Stat, concat_bmi_wks_national])

# COMMAND ----------

Ref_MBRRACE_Stat = msd_values[['RPStartDate','Indicator','Measure','MBRRACE_abv','Numerator_MBRRACE_Suppressed','Denominator_MBRRACE_Suppressed','Supressed_Percent_MBRRACE']].copy()
Ref_MBRRACE_Stat.columns = ['Data_period','Indicator','Measure','MBRRACE_abv','Numerator_MBRRACEAll','Denominator_MBRRACE','Percent_MBRRACE']
Ref_MBRRACE_Stat = Ref_MBRRACE_Stat.drop_duplicates(keep="first")
#Ref_MBRRACE_Stat["MBRRACE_abv"] = Ref_MBRRACE_Stat.MBRRACE_abv.fillna('')
Ref_MBRRACE_Stat.sort_values(['Data_period', 'Indicator', 'Measure', 'MBRRACE_abv'], inplace=True)
 
# print(type(Ref_MBRRACE_Stat))
# for item in Ref_MBRRACE_Stat.Numerator_MBRRACEAll.unique():
#   print(item)
#   print(type(item))
display(Ref_MBRRACE_Stat)

# COMMAND ----------

# DBTITLE 1,Concat MBRRACE level BMI of mother at 15 weeks measure to org_base
concat_bmi_wks_mbrrace = bmi_wks.query("OrgLevel == 'MBRRACE Grouping'").drop(columns=['OrgLevel', 'Org_Code', 'Count_Of']).rename(columns={'Numerator_Org': 'Numerator_MBRRACEAll', 'Denominator_Org': 'Denominator_MBRRACE', 'Percent_Org': 'Percent_MBRRACE'}).loc[:, Ref_MBRRACE_Stat.columns]
 
Ref_MBRRACE_Stat = pd.concat([Ref_MBRRACE_Stat, concat_bmi_wks_mbrrace])

# COMMAND ----------

from dsp.code_promotion.mesh_send import cp_mesh_send
import os

dfs_to_extract = {'org_base': org_base, 'RefOrgStatBase': RefOrgStatBase, 'Ref_All_Stat': Ref_All_Stat, 'Ref_MBRRACE_Stat': Ref_MBRRACE_Stat}

#Prod mail box id
mailbox_to = 'X26HC004'
workflow_id = 'GNASH_MAT_ANALYSIS'
# local_id = 'mat_analysis'


for dfname in dfs_to_extract:
  df_to_extract = spark.createDataFrame(dfs_to_extract[dfname])
  print(f"{dfname} rowcount", df_to_extract.count())
  if(os.environ.get('env') == 'prod'):
    dfname = f'{dfname}.csv'
    local_id = dfname
    # For supporting the LEAD MESH activity to rename the files appropriately with rename flag as option '1', local_id parameter should bear the value of file name.
    try:
      request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, dfname, local_id)
      print(f"{dfname} file have been pushed to MESH with request id {request_id}. \n")
    except Exception as ex:
      print(ex, 'MESH exception on SPARK 3 can be ignored, file would be delivered in the destined path')
  else:
    display(df_to_extract)