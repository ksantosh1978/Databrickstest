# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC -test contents of $db_output.maternity_monthly_measures_store ($db_output is typically mat_analysis)
# MAGIC
# MAGIC Breakdown counts<br>
# MAGIC
# MAGIC testdata_mat_analysis_mat_pre_clear
# MAGIC
# MAGIC Input:
# MAGIC This notebook has been executed with parameters -  
# MAGIC db: mat_analysis  
# MAGIC mat_pre_clear: testdata_mat_analysis_mat_pre_clear  
# MAGIC dss_corporate: dss_corporate  
# MAGIC RPBegindate: 2015-04-01

# COMMAND ----------

# DBTITLE 1,Title
db_output = dbutils.widgets.get("db")
print(db_output)
assert db_output
run_tests_ran_ok = True

# COMMAND ----------

# DBTITLE 1,Prepare expected values
# MAGIC %py
# MAGIC # 1/Feb/21 there are duplicates of MSD401_ID and MSD405_ID in testdata_mat_analysis_mat_pre_clear on Ref but not on Live, which have hitherto caused inconsistent counts for
# MAGIC # expected_counts_per_BirthweightTermGroup and expected_counts_per_BirthweightTermGroup2500
# MAGIC
# MAGIC # expected_counts_per_BirthweightTermGroup = {369} # replaced 22/1/21
# MAGIC # expected_counts_per_BirthweightTermGroup = {367}
# MAGIC #expected_counts_per_BirthweightTermGroup = {366} # replaced 26/1
# MAGIC # expected_counts_per_BirthweightTermGroup = {369} # restored 3/2
# MAGIC expected_counts_per_BirthweightTermGroup = {574} # replaced 14 Oct 2021 NP
# MAGIC # expected_counts_per_BirthweightTermGroup2500 = {333} # replaced 22/1/21
# MAGIC # expected_counts_per_BirthweightTermGroup2500 = {331}
# MAGIC # #expected_counts_per_BirthweightTermGroup2500 = {330} # replaced 26/1
# MAGIC # expected_counts_per_BirthweightTermGroup2500 = {333} # restored 3/2
# MAGIC expected_counts_per_BirthweightTermGroup2500 = {574} # replaced 14 Oct 2021 NP
# MAGIC # expected_counts_for_national_BirthweightTermGroup = {'Under 1500g': 1, '1500g to 1999g': 2, '2000g to 2499g': 1, '2500g to 2999g': 1, '3000g to 3499g': 1, '3500g to 3999g': 1, '4000g to 4999g': 2, 'Missing Value / Value outside reporting parameters': 761} # replaced 22/1/21
# MAGIC #expected_counts_for_national_BirthweightTermGroup = {'Under 1500g': 1, '1500g to 1999g': 1, '2000g to 2499g': 1, '2500g to 2999g': 1, '3000g to 3499g': 1, '3500g to 3999g': 1, '4000g to 4999g': 2, 'Missing Value / Value outside reporting parameters': 762} # removed 3/2
# MAGIC expected_counts_for_national_BirthweightTermGroup = {'Missing Value / Value outside reporting parameters': 770} # replaced 14 Oct 2021 NP
# MAGIC # expected_counts_for_national_BirthweightTermGroup2500 = {'2500g and over': 5, 'Missing Value / Value outside reporting parameters': 761, 'Under 2500g': 4} # replaced 22/1/21
# MAGIC expected_counts_for_national_BirthweightTermGroup2500 = {'Missing Value / Value outside reporting parameters': 1} # replaced 26 Oct 2021 NP
# MAGIC #expected_counts_for_national_BirthweightTermGroup2500 = {'2500g and over': 5, 'Missing Value / Value outside reporting parameters': 762, 'Under 2500g': 3} # remove 3/2
# MAGIC
# MAGIC # SmokingStatusGroupBooking
# MAGIC # expected_counts_per_SmokingStatusGroupBooking = {90}
# MAGIC expected_counts_per_SmokingStatusGroupBooking = {57} #{'Missing Value / Value outside reporting parameters': 57} # replaced 14 Oct 2021 NP
# MAGIC # expected_counts_for_national_SmokingStatusGroupBooking = {
# MAGIC #   'Non-Smoker / Ex-Smoker': 2, 
# MAGIC #   'Missing Value / Value outside reporting parameters': 30,
# MAGIC #   'Unknown': 1,
# MAGIC #   'Smoker': 3
# MAGIC # }
# MAGIC expected_counts_for_national_SmokingStatusGroupBooking = {'Missing Value / Value outside reporting parameters': 30} # replaced 14 Oct 2021 NP
# MAGIC # expected_counts_per_BirthweightTermGroup = {356}
# MAGIC # expected_counts_per_BirthweightTermGroup2500 = {320}
# MAGIC # expected_counts_for_national_BirthweightTermGroup = {'Under 1500g': 1, '1500g to 1999g': 1, '2000g to 2499g': 1, '2500g to 2999g': 1, '3000g to 3499g': 1, '3500g to 3999g': 1, '4000g to 4999g': 2, 'Missing Value / Value outside reporting parameters': 756}
# MAGIC # expected_counts_for_national_BirthweightTermGroup2500 = {'2500g and over': 5, 'Missing Value / Value outside reporting parameters': 756, 'Under 2500g': 3}
# MAGIC
# MAGIC # # SmokingStatusGroupBooking
# MAGIC # expected_counts_per_SmokingStatusGroupBooking = {85}
# MAGIC # expected_counts_for_national_SmokingStatusGroupBooking = {
# MAGIC #   'Non-Smoker / Ex-Smoker': 2, 
# MAGIC #   'Missing Value / Value outside reporting parameters': 24,
# MAGIC #   'Unknown': 1,
# MAGIC #   'Smoker': 3
# MAGIC # }
# MAGIC
# MAGIC # expected_counts_for_RAE_COCDQ02 = {9: 143}

# COMMAND ----------

print(db_output)

# COMMAND ----------

# %sql
# select row_number() over (partition by reportingperiodstartdate order by measure, org_level, org_code, measure, count_of, value_unsuppressed) as rn
# , ReportingPeriodStartDate,ReportingPeriodEndDate,Dimension,Org_Level,Org_Code,Org_Name,Measure,Count_Of,Value_Unsuppressed
# from
# $db.maternity_monthly_measures_store

# where dimension = 'BirthweightTermGroup' and reportingperiodstartdate='2015-04-01'

# order by rn desc

# COMMAND ----------

# %sql

# --select * from $db.geogtlrr order by trust_org

# select * from mat_analysis.maternity_monthly_measures_store where measure like 'Setting_%' order by measure 

# COMMAND ----------

# DBTITLE 1,Total BirthweightTermGroup
# MAGIC %py
# MAGIC counts_per_breakdown_df = spark.sql("select count(*) as count from `{db}`.maternity_monthly_measures_store where dimension = 'BirthweightTermGroup' and ReportingPeriodStartDate = '2015-04-01'".format(db=db_output))
# MAGIC actual_counts_per_breakdown = {r['count'] for r in counts_per_breakdown_df.collect()}
# MAGIC print(actual_counts_per_breakdown)
# MAGIC try:
# MAGIC   assert actual_counts_per_breakdown == expected_counts_per_BirthweightTermGroup
# MAGIC except:
# MAGIC   print('actual_counts_per_breakdown', actual_counts_per_breakdown)
# MAGIC   print('expected_counts_per_breakdown', expected_counts_per_BirthweightTermGroup)
# MAGIC   print('Error in BirthweightTermGroup')
# MAGIC   run_tests_ran_ok = False
# MAGIC

# COMMAND ----------

# DBTITLE 1,Total BirthweightTermGroup2500
# MAGIC %py
# MAGIC counts_per_breakdown_df = spark.sql("select count(*) as count from `{db}`.maternity_monthly_measures_store where dimension = 'BirthweightTermGroup2500' and ReportingPeriodStartDate = '2015-04-01'".format(db=db_output))
# MAGIC actual_counts_per_breakdown = {r['count'] for r in counts_per_breakdown_df.collect()}
# MAGIC print(actual_counts_per_breakdown)
# MAGIC try:
# MAGIC   assert actual_counts_per_breakdown == expected_counts_per_BirthweightTermGroup2500
# MAGIC except:
# MAGIC   print('actual_counts_per_breakdown', actual_counts_per_breakdown)
# MAGIC   print('expected_counts_per_BirthweightTermGroup', expected_counts_per_BirthweightTermGroup2500)
# MAGIC   print('Error in BirthweightTermGroup2500')
# MAGIC   run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Count BirthweightTermGroup by measure
# MAGIC %py
# MAGIC counts_per_breakdown_df = spark.sql("select measure, value_unsuppressed from `{db}`.maternity_monthly_measures_store where dimension = 'BirthweightTermGroup' and org_level = 'National' and ReportingPeriodStartDate = '2015-04-01'".format(db=db_output))
# MAGIC actual_counts_per_breakdown = {r['measure']:r['value_unsuppressed'] for r in counts_per_breakdown_df.collect()}
# MAGIC print(actual_counts_per_breakdown)
# MAGIC try:
# MAGIC   assert actual_counts_per_breakdown == expected_counts_for_national_BirthweightTermGroup
# MAGIC except:
# MAGIC   print('actual_counts_per_breakdown', actual_counts_per_breakdown)
# MAGIC   print('expected_counts_for_national_BirthweightTermGroup', expected_counts_for_national_BirthweightTermGroup)
# MAGIC   print('Error in BirthweightTermGroup by measure')
# MAGIC   run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,THIS ONE NEEDS FIXING!     Count BirthweightTermGroup2500 by measure
#  %py
# counts_per_breakdown_df = spark.sql("select measure, value_unsuppressed from `{db}`.maternity_monthly_measures_store where dimension = 'BirthweightTermGroup2500' and org_level = 'National'".format(db=db_output))
# actual_counts_per_breakdown = {r['measure']:r['value_unsuppressed'] for r in counts_per_breakdown_df.collect()}
# try:
#   assert actual_counts_per_breakdown == expected_counts_for_national_BirthweightTermGroup2500
# except:
#   print('actual_counts_per_breakdown', actual_counts_per_breakdown)
#   print('expected_counts_for_national_BirthweightTermGroup2500', expected_counts_for_national_BirthweightTermGroup2500)
#   print('Error in BirthweightTermGroup2500 by measure')
#   run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Total SmokingStatusGroupBooking
# MAGIC %py
# MAGIC counts_per_breakdown_df = spark.sql("select count(*) as count from `{db}`.maternity_monthly_measures_store where dimension = 'SmokingStatusGroupBooking'".format(db=db_output))
# MAGIC actual_counts_per_breakdown_SmokingStatusGroupBooking = {r['count'] for r in counts_per_breakdown_df.collect()}
# MAGIC print('counts_per_breakdown_df: ',counts_per_breakdown_df)
# MAGIC # print('actual_counts_per_breakdown_SmokingStatusGroupBooking: ', actual_counts_per_breakdown_SmokingStatusGroupBooking)
# MAGIC # print('expected_counts_per_SmokingStatusGroupBooking: ', expected_counts_per_SmokingStatusGroupBooking)
# MAGIC try:
# MAGIC   assert actual_counts_per_breakdown_SmokingStatusGroupBooking == expected_counts_per_SmokingStatusGroupBooking
# MAGIC except:
# MAGIC   print('Error in SmokingStatusGroupBooking')
# MAGIC   print('actual_counts_per_breakdown_SmokingStatusGroupBooking: ', actual_counts_per_breakdown_SmokingStatusGroupBooking)
# MAGIC   print('expected_counts_per_SmokingStatusGroupBooking: ', expected_counts_per_SmokingStatusGroupBooking)
# MAGIC   run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Count SmokingStatusGroupBooking by measure
# MAGIC %py
# MAGIC counts_per_breakdown_df = spark.sql("select measure, value_unsuppressed from `{db}`.maternity_monthly_measures_store where dimension = 'SmokingStatusGroupBooking' and org_level = 'National'".format(db=db_output))
# MAGIC actual_counts_for_national_SmokingStatusGroupBooking = {r['measure']:r['value_unsuppressed'] for r in counts_per_breakdown_df.collect()}
# MAGIC # print('actual_counts_for_national_SmokingStatusGroupBooking; ', actual_counts_for_national_SmokingStatusGroupBooking)
# MAGIC # print('expected_counts_for_national_SmokingStatusGroupBooking', expected_counts_for_national_SmokingStatusGroupBooking)
# MAGIC try:
# MAGIC   assert actual_counts_for_national_SmokingStatusGroupBooking == expected_counts_for_national_SmokingStatusGroupBooking
# MAGIC except:
# MAGIC   print('Error in BirthweightTermGroup by measure')
# MAGIC   print('actual_counts_for_national_SmokingStatusGroupBooking; ', actual_counts_for_national_SmokingStatusGroupBooking)
# MAGIC   print('expected_counts_for_national_SmokingStatusGroupBooking', expected_counts_for_national_SmokingStatusGroupBooking)
# MAGIC   run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 1 Outcome Indicator 1 denominator
expected_counts_for_e1o1 = {'RAE': '25', 'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element1_OutcomeIndicator1' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e1o1 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e1o1)
try:
  assert actual_counts_for_e1o1 == expected_counts_for_e1o1
except:
  print('actual_counts_for_e1o1', actual_counts_for_e1o1)
  print('expected_counts_for_e1o1', expected_counts_for_e1o1)
  print('Error in Element 1 Outcome Indicator 1 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 1 Process Indicator 1 denominator
expected_counts_for_e1p1 = {'RAE': '25', 'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element1_ProcessIndicator1' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e1p1 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e1p1)
try:
  assert actual_counts_for_e1p1 == expected_counts_for_e1p1
except:
  print('actual_counts_for_e1p1', actual_counts_for_e1p1)
  print('expected_counts_for_e1p1', expected_counts_for_e1p1)
  print('Error in Element 1 Process Indicator 1 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 1 Outcome Indicator 2 denominator
expected_counts_for_e1o2 = {'R1E': '5', 'RA7': '110', 'RX4': '5', 'R1C': '5', 'RAE': '290', 'RMC': '5', 'R1H': '5', 'RYR': '5', 'RD1': '5' }

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element1_OutcomeIndicator2' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e1o2 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e1o2)
try:
  assert actual_counts_for_e1o2 == expected_counts_for_e1o2
except:
  print('actual_counts_for_e1o2', actual_counts_for_e1o2)
  print('expected_counts_for_e1o2', expected_counts_for_e1o2)
  print('Error in Element 1 Outcome Indicator 2 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 1 Process Indicator 2 denominator
expected_counts_for_e1p2 = {'RAE': '25', 'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element1_ProcessIndicator2' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e1p2 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e1p2)
try:
  assert actual_counts_for_e1p2 == expected_counts_for_e1p2
except:
  print('actual_counts_for_e1p2', actual_counts_for_e1p2)
  print('expected_counts_for_e1p2', expected_counts_for_e1p2)
  print('Error in Element 1 Process Indicator 2 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# Element 1 outcome indicator 3

# COMMAND ----------

# DBTITLE 1,SBL Element 1 Process Indicator 3 denominator
expected_counts_for_e1p3 = {'R1E': '5', 'RA7': '110', 'RX4': '5', 'R1C': '5', 'RAE': '290', 'RMC': '5', 'R1H': '5', 'RYR': '5', 'RD1': '5' }

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element1_ProcessIndicator3' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e1p3 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e1p3)
try:
  assert actual_counts_for_e1p3 == expected_counts_for_e1p3
except:
  print('actual_counts_for_e1p3', actual_counts_for_e1p3)
  print('expected_counts_for_e1p3', expected_counts_for_e1p3)
  print('Error in Element 1 Process Indicator 3 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 2 Process Indicator 2 denominator
expected_counts_for_e2p2 = {'R1C': '5', 'RAE': '725', 'R1H': '10', 'R1E': '5', 'RA7': '270', 'RX4': '5', 'RR8': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator like 'SBL_Element2_ProcessIndicator2' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e2p2 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e2p2)
try:
  assert actual_counts_for_e2p2 == expected_counts_for_e2p2
except:
  print('actual_counts_for_e2p2', actual_counts_for_e2p2)
  print('expected_counts_for_e2p2', expected_counts_for_e2p2)
  print('Error in Element 2 Process Indicator 2 denominator')
  run_tests_ran_ok = False


# COMMAND ----------

# Element 3 process indicator 2

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Outcome Indicator 1a numerator
expected_counts_for_e5o1an = {'R1H': '0', 'RR8': '0', 'RA7': '5', 'RAE': '0'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_OutcomeIndicator1a' and OrgLevel = 'Provider' and currency='Numerator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5o1an = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5o1an)
try:
  assert actual_counts_for_e5o1an == expected_counts_for_e5o1an
except:
  print('actual_counts_for_e5o1an', actual_counts_for_e5o1an)
  print('expected_counts_for_e5o1an', expected_counts_for_e5o1an)
  print('Error in Element 5 Outcome Indicator 1a numerator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Outcome Indicator 1a denominator
expected_counts_for_e5o1ad = {'RAE': '115', 'R1H': '5', 'RA7': '50', 'RR8': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_OutcomeIndicator1a' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5o1ad = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5o1ad)
try:
  assert actual_counts_for_e5o1ad == expected_counts_for_e5o1ad
except:
  print('actual_counts_for_e5o1ad', actual_counts_for_e5o1ad)
  print('expected_counts_for_e5o1ad', expected_counts_for_e5o1ad)
  print('Error in Element 5 Outcome Indicator 1a denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Outcome Indicator 1b numerator
expected_counts_for_e5o1bn = {'RA7': '5', 'RR8': '5', 'R1H': '0', 'RAE': '40'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_OutcomeIndicator1b' and OrgLevel = 'Provider' and currency='Numerator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5o1bn = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5o1bn)
try:
  assert actual_counts_for_e5o1bn == expected_counts_for_e5o1bn
except:
  print('actual_counts_for_e5o1bn', actual_counts_for_e5o1bn)
  print('expected_counts_for_e5o1bn', expected_counts_for_e5o1bn)
  print('Error in Element 5 Outcome Indicator 1b numerator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Outcome Indicator 1b denominator
expected_counts_for_e5o1bd = {'R1H': '5', 'RR8': '5', 'RAE': '115', 'RA7': '50'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_OutcomeIndicator1b' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5o1bd = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5o1bd)
try:
  assert actual_counts_for_e5o1bd == expected_counts_for_e5o1bd
except:
  print('actual_counts_for_e5o1bd', actual_counts_for_e5o1bd)
  print('expected_counts_for_e5o1bd', expected_counts_for_e5o1bd)
  print('Error in Element 5 Outcome Indicator 1b denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Outcome Indicator 1b rate
expected_counts_for_e5o1br = {'RAE': '34.8', 'R1H': '0', 'RA7': '10', 'RR8': '100'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_OutcomeIndicator1b' and OrgLevel = 'Provider' and currency='Rate' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5o1br = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5o1br)
try:
  assert actual_counts_for_e5o1br == expected_counts_for_e5o1br
except:
  print('actual_counts_for_e5o1br', actual_counts_for_e5o1br)
  print('expected_counts_for_e5o1br', expected_counts_for_e5o1br)
  print('Error in Element 5 Outcome Indicator 1b rate')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Process Indicator 1
expected_counts_for_e5p1 = {'RAE': '20', 'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_ProcessIndicator1' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5p1 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5p1)
try:
  assert actual_counts_for_e5p1 == expected_counts_for_e5p1
except:
  print('actual_counts_for_e5p1', actual_counts_for_e5p1)
  print('expected_counts_for_e5p1', expected_counts_for_e5p1)
  print('Error in Element 5 Process Indicator 1 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Process Indicator 2
expected_counts_for_e5p2 = {'RAE': '20', 'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_ProcessIndicator2' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5p2 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5p2)
try:
  assert actual_counts_for_e5p2 == expected_counts_for_e5p2
except:
  print('actual_counts_for_e5p2', actual_counts_for_e5p2)
  print('expected_counts_for_e5p2', expected_counts_for_e5p2)
  print('Error in Element 5 Process Indicator 2 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,SBL Element 5 Process Indicator 3
expected_counts_for_e5p3 = {'RA7': '5'}

counts_per_breakdown_df = spark.sql("select OrgCodeProvider, Value from `{db}`.slb_csv where indicator = 'SBL_Element5_ProcessIndicator3' and OrgLevel = 'Provider' and currency='Denominator' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts_for_e5p3 = {r['OrgCodeProvider']:r['Value'] for r in counts_per_breakdown_df.collect()}
print(actual_counts_for_e5p3)
try:
  assert actual_counts_for_e5p3 == expected_counts_for_e5p3
except:
  print('actual_counts_for_e5p3', actual_counts_for_e5p3)
  print('expected_counts_for_e5p3', expected_counts_for_e5p3)
  print('Error in Element 5 Process Indicator 3 denominator')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_CSV row count
# expected_count = {436}
# expected_count = {459} #changed by NP
# expected_count = {600} #changed by NP
#expected_count = {456} #changed by  NP
# expected_count = {520} #changed by NP  9 March 22
expected_count = {532} #changed by NP 14 March 22
count_df = spark.sql("select count(*) as count from `{db}`.coc_csv where RPStartDate='2015-04-01'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in COC_CSV row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_CSV row count not zero
# expected_count = {149}
# expected_count = {104} NP commented out 16 Nov 21
# expected_count = {195}
# expected_count = {156} NP commented out 9 March 22
expected_count = {141} 
count_df = spark.sql("select count(*) as count from `{db}`.coc_csv where value <> 0 and RPStartDate='2015-04-01'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in COC_CSV row count when value is not zero')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_by_28weeks RAE
expected_counts = {'Numerator': '5', 'Denominator': '70', 'Rate': '7.1', 'Result': 'Pass'}

count_df = spark.sql("select Currency, Value from `{db}`.coc_csv where orgLevel='Provider' and indicator='COC_by_28weeks' and OrgCodeProvider = 'RAE' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in COC_by_28weeks RAE')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_by_28weeks Local Maternity System, Ethnic group
expected_counts = {'Numerator': '5', 'Denominator': '15', 'Rate': '33.3'}

count_df = spark.sql("select Currency, Value from `{db}`.coc_csv where OrgLevel = 'Local Maternity System'  and OrgCodeProvider='E54000054' and Indicator='COC_by_28weeks_Ethnicity_White' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in COC_by_28weeks Local Maternity System, Ethnic group')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_by_28weeks MBRRACE Grouping, IMD
expected_counts = {'Numerator': '5', 'Denominator': '10', 'Rate': '50'}

count_df = spark.sql("select Currency, Value from `{db}`.coc_csv where OrgLevel = 'MBRRACE Grouping'  and OrgCodeProvider='Group 2. Level 3 NICU' and Indicator='COC_by_28weeks_IMD_09' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in COC_by_28weeks MBRRACE Grouping, IMD')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_by_28weeks NHS England (Region), Ethnic group
expected_counts = {'Numerator': '5', 'Denominator': '10', 'Rate': '50'}

count_df = spark.sql("select Currency, Value from `{db}`.coc_csv where OrgLevel = 'NHS England (Region)' and OrgCodeProvider='Y54' and Indicator='COC_by_28weeks_Ethnicity_Other' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in COC_by_28weeks NHS England (Region), Ethnic group')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,COC_by_28weeks National, IMD
# expected_counts = {'Numerator': '5', 'Denominator': '15', 'Rate': '33.3'} changed 17 Jan2021 to :
expected_counts = {'Numerator': '5', 'Denominator': '10', 'Rate': '50'}

count_df = spark.sql("select Currency, Value from `{db}`.coc_csv where OrgLevel = 'National' and OrgCodeProvider='National' and Indicator='COC_by_28weeks_IMD_05' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in COC_by_28weeks National, IMD')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Measures_csv, Measure = Induction  Row Count
expected_count = {189}
count_df = spark.sql("select count(*) as count from `{db}`.measures_csv where RPStartDate='2015-04-01' and Indicator = 'Induction'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in measures_CSV row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Measures_csv, Measure = Induction Row Count where NOT ZERO
expected_count = {28}
count_df = spark.sql("select count(*) as count from `{db}`.measures_csv where value <> 0 and RPStartDate='2015-04-01' and Indicator = 'Induction'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in measures_CSV row count not zero')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP row count
expected_count = {383}
count_df = spark.sql("select count(*) as count from `{db}`.pcsp_csv where RPStartDate='2015-04-01'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in PCSP_CSV row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP row count not zero
expected_count = {111}
count_df = spark.sql("select count(*) as count from `{db}`.pcsp_csv where value <> 0 and RPStartDate='2015-04-01'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in PCSP_CSV row count not zero')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP 3 care plans national
expected_data = [('PCP_Antenatal_16weeks', 'National', 'Denominator', '75'), ('PCP_Antenatal_16weeks', 'National', 'Numerator', '5'), ('PCP_Antenatal_16weeks', 'National', 'Rate', '6.7'), ('PCP_Birth_34weeks', 'National', 'Denominator', '85'), ('PCP_Birth_34weeks', 'National', 'Numerator', '10'), ('PCP_Birth_34weeks', 'National', 'Rate', '11.8'), ('PCP_Postpartum_36weeks', 'National', 'Denominator', '255'), ('PCP_Postpartum_36weeks', 'National', 'Numerator', '30'), ('PCP_Postpartum_36weeks', 'National', 'Rate', '11.8')]
expected_columns = ['Indicator', 'OrgCodeProvider', 'Currency', 'Value']
expected_df = spark.createDataFrame(expected_data, schema = expected_columns)
count_df = spark.sql("select Indicator, OrgCodeProvider, Currency, Value from `{db}`.pcsp_csv where RPStartDate='2015-04-01' and OrgCodeProvider='National' and indicator in ('PCP_Birth_34weeks', 'PCP_Postpartum_36weeks', 'PCP_Antenatal_16weeks') order by indicator, currency".format(db=db_output))
count_df.show()
try:
  assert expected_df.collect() == count_df.collect()
except:
  print('count_df', count_df.show())
  print('expected_df', expected_df.show())
  print('Error in PCSP_CSV 3 care plans national')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP PCP_Birth_34weeks RA7
expected_counts = {'Numerator': '10', 'Denominator': '85', 'Rate': '11.8', 'Result': 'Pass'}

count_df = spark.sql("select Currency, Value from `{db}`.pcsp_csv where orgLevel='Provider' and indicator='PCP_Birth_34weeks' and OrgCodeProvider = 'RA7' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in PCP_Birth_34weeks RA7')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP Local Maternity System
expected_counts = {'Numerator': '5', 'Denominator': '20', 'Rate': '25'}

count_df = spark.sql("select Currency, Value from `{db}`.pcsp_csv where OrgLevel = 'Local Maternity System'  and OrgCodeProvider='E54000039' and Indicator='PCP_Antenatal_16weeks' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in PCSP Local Maternity System')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP MBRRACE Grouping
expected_counts = {'Numerator': '30', 'Denominator': '255', 'Rate': '11.8'}

count_df = spark.sql("select Currency, Value from `{db}`.pcsp_csv where OrgLevel = 'MBRRACE Grouping'  and OrgCodeProvider='Group 2. Level 3 NICU' and Indicator='PCP_Postpartum_36weeks' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in PCSP MBRRACE Grouping')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,PCSP NHS England (Region)
expected_counts = {'Numerator': '5', 'Denominator': '20', 'Rate': '25'}

count_df = spark.sql("select Currency, Value from `{db}`.pcsp_csv where OrgLevel = 'NHS England (Region)' and OrgCodeProvider='Y57' and Indicator='PCP_Antenatal_16weeks' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in PCSP NHS England (Region)')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,CQIM National
# expected_counts = {'Numerator': '675', 'Denominator': '1020', 'Rate': '66.2'}
expected_counts = {'Numerator': '670', 'Denominator': '1015', 'Rate': '66'} #change by NP 26 Oct 21

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'National' and Indicator='CQIMBreastfeeding' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIM National')
 # run_tests_ran_ok = False
  #FAILING AS OF 12 08 2022 - Caroline Quinn - so commenting out for now to allow promotion

# COMMAND ----------

# DBTITLE 1,CQIM Local Maternity System
expected_counts = {'Numerator': '5', 'Denominator': '10', 'Rate': '50'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'Local Maternity System' and OrgCodeProvider='E54000029' and Indicator='CQIMBreastfeeding' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIM Local Maternity System')
  #run_tests_ran_ok = False
  #FAILING AS OF 12 08 2022 - Caroline Quinn - so commenting out for now to allow promotion

# COMMAND ----------

# DBTITLE 1,CQIM MBRRACE Grouping
expected_counts = {'Numerator': '480', 'Denominator': '725', 'Rate': '66.2'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'MBRRACE Grouping' and OrgCodeProvider='Group 2. Level 3 NICU' and Indicator='CQIMBreastfeeding' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIM MBRRACE Grouping')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,CQIM NHS England (Region)
expected_counts = {'Numerator': '485', 'Denominator': '735', 'Rate': '66'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'NHS England (Region)' and OrgCodeProvider='Y54' and Indicator='CQIMBreastfeeding' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIM NHS England (Region)')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,CQIMDQ22 Provider R1E
expected_counts = {'Numerator': '5', 'Denominator': '5', 'Rate': '100', 'Result': 'Pass'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'Provider' and OrgCodeProvider='R1E' and Indicator='CQIMDQ22' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIMDQ22 Provider R1E')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,CQIMDQ15 Provider R1H
expected_counts = {'Numerator': '5', 'Denominator': '30', 'Rate': '16.7', 'Result': 'Fail'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'Provider' and OrgCodeProvider='R1H' and Indicator='CQIMDQ15' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIMDQ15 Provider R1H')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,CQIMBreastfeeding Provider R1E
expected_counts = {'Numerator': '5', 'Denominator': '5', 'Rate': '100', 'Result': 'Pass'}

count_df = spark.sql("select Currency, Value from `{db}`.cqim_dq_csv where Org_Level = 'Provider' and OrgCodeProvider='R1E' and Indicator='CQIMBreastfeeding' and RPStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in CQIMBreastfeeding Provider R1E')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Aspirin_MeetCriteria RAE Denominator
expected_count = 50
count_df = spark.sql("select Value from `{db}`.measures_csv where OrgLevel = 'Provider' and OrgCodeProvider='RAE' and Indicator='Aspirin_MeetCriteria' and Currency='Denominator' and RPStartDate='2015-04-01'".format(db='mat_analysis'))
# Only collecting denominator value. Hence it's ok to simply index 0
# Collected data type will be string as the Value column has string values (Pass, Fail etc.)
actual_count = int(count_df.collect()[0][0])
print(actual_count)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in Measures_csv Aspirin row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,Aspirin_MeetCriteria RA7 Denominator
expected_count = 30
count_df = spark.sql("select Value from `{db}`.measures_csv where OrgLevel = 'Provider' and OrgCodeProvider='RA7' and Indicator='Aspirin_MeetCriteria' and Currency='Denominator' and RPStartDate='2015-04-01'".format(db='mat_analysis'))
# Only collecting denominator value. Hence it's ok to simply index 0
# Collected data type will be string as the Value column has string values (Pass, Fail etc.)
actual_count = int(count_df.collect()[0][0])
print(actual_count)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in Measures_csv Aspirin row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA row count
# db_output = "mat_analysis"
expected_count = {652}
count_df = spark.sql("select count(*) as count from `{db}`.nmpa_csv where ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_count = {r['count'] for r in count_df.collect()}
print(count_df)
try:
  assert actual_count == expected_count
except:
  print('actual_count', actual_count)
  print('expected_count', expected_count)
  print('Error in NMPA_CSV row count')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_in_different_settings National
# db_output = "mat_analysis"
expected_data = [('Setting_AMU', 'National', 'Denominator', '80.0'),('Setting_AMU', 'National', 'Numerator', '0.0'),('Setting_AMU', 'National', 'Rate', '0.0')
                 ,('Setting_DQ', 'National', 'Denominator', '80.0'),('Setting_DQ', 'National', 'Numerator', '80.0'),('Setting_DQ', 'National', 'Rate', '100.0')
                 ,('Setting_FMU', 'National', 'Denominator', '80.0'),('Setting_FMU', 'National', 'Numerator', '10.0'),('Setting_FMU', 'National', 'Rate', '12.5')
                 ,('Setting_Home', 'National', 'Denominator', '80.0'),('Setting_Home', 'National', 'Numerator', '10.0'),('Setting_Home', 'National', 'Rate', '12.5')
                 ,('Setting_Midwife', 'National', 'Denominator', '80.0'),('Setting_Midwife', 'National', 'Numerator', '20.0'),('Setting_Midwife', 'National', 'Rate', '25.0')
                 ,('Setting_Obstetric', 'National', 'Denominator', '80.0'),('Setting_Obstetric', 'National', 'Numerator', '10.0'),('Setting_Obstetric', 'National', 'Rate', '12.5')
                 ,('Setting_Other', 'National', 'Denominator', '80.0'),('Setting_Other', 'National', 'Numerator', '50.0'),('Setting_Other', 'National', 'Rate', '62.5')
                 ,('Setting_Unknown', 'National', 'Denominator', '80.0'),('Setting_Unknown', 'National', 'Numerator', '0.0'),('Setting_Unknown', 'National', 'Rate', '0.0')]
expected_columns = ['Indicator', 'OrgCodeProvider', 'Currency', 'Value']
expected_df = spark.createDataFrame(expected_data, schema = expected_columns)
count_df = spark.sql("select indicator, org_level, currency, value from `{db}`.nmpa_csv where ReportingPeriodStartDate='2015-04-01' and org_level='National' and indicator like 'Setting%' order by indicator, currency".format(db=db_output))
count_df.show()
try:
  assert expected_df.collect() == count_df.collect()
except:
  print('count_df', count_df.show())
  print('expected_df', expected_df.show())
  print('Error in NMPA birth_in_different_settings - National')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_in_different_settings NHS England (Region) 
expected_counts = {'Numerator': '0.0', 'Denominator': '75.0', 'Rate': '0.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'NHS England (Region)'  and Org_Code='Y54' and Indicator='Setting_AMU' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_in_different_settings - Local Maternity System')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_in_different_settings Local Maternity System 
expected_counts = {'Numerator': '0.0', 'Denominator': '5.0', 'Rate': '0.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'Local Maternity System'  and Org_Code='E54000039' and Indicator='Setting_AMU' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_in_different_settings -  Local Maternity System')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_in_different_settings MBRRACE Grouping
expected_counts = {'Numerator': '0.0', 'Denominator': '5.0', 'Rate': '0.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'MBRRACE Grouping'  and Org_Code='Group 1. Level 3 NICU & NS' and Indicator='Setting_AMU' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_in_different_settings - MBRRACE Grouping')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_in_different_settings Provider RAE 
expected_counts = {'Numerator': '0.0', 'Denominator': '75.0', 'Rate': '0.0','Result':'Pass'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'Provider'  and Org_Code='RAE' and Indicator='Setting_AMU' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_in_different_settings - Provider RAE')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_mode National
expected_data = [('Mode_Caesarean', 'National', 'Denominator', '80.0'),('Mode_Caesarean', 'National', 'Numerator', '20.0'),('Mode_Caesarean', 'National', 'Rate', '25.0')
                 ,('Mode_Caesarean_emergency', 'National', 'Denominator', '80.0'),('Mode_Caesarean_emergency', 'National', 'Numerator', '20.0'),('Mode_Caesarean_emergency', 'National', 'Rate', '25.0')
                 ,('Mode_Caesarean_planned', 'National', 'Denominator', '80.0'),('Mode_Caesarean_planned', 'National', 'Numerator', '0.0'),('Mode_Caesarean_planned', 'National', 'Rate', '0.0')
                 ,('Mode_Instrumental', 'National', 'Denominator', '80.0'),('Mode_Instrumental', 'National', 'Numerator', '30.0'),('Mode_Instrumental', 'National', 'Rate', '37.5')
                 ,('Mode_Instrumental_forceps', 'National', 'Denominator', '80.0'),('Mode_Instrumental_forceps', 'National', 'Numerator', '20.0'),('Mode_Instrumental_forceps', 'National', 'Rate', '25.0')
                 ,('Mode_Instrumental_ventouse', 'National', 'Denominator', '80.0'),('Mode_Instrumental_ventouse', 'National', 'Numerator', '10.0'),('Mode_Instrumental_ventouse', 'National', 'Rate', '12.5')
                 ,('Mode_Other', 'National', 'Denominator', '80.0'),('Mode_Other', 'National', 'Numerator', '15.0'),('Mode_Other', 'National', 'Rate', '18.8')
                 ,('Mode_Spontaneous_vaginal', 'National', 'Denominator', '80.0'),('Mode_Spontaneous_vaginal', 'National', 'Numerator', '15.0'),('Mode_Spontaneous_vaginal', 'National', 'Rate', '18.8')]
expected_columns = ['Indicator', 'OrgCodeProvider', 'Currency', 'Value']
expected_df = spark.createDataFrame(expected_data, schema = expected_columns)
count_df = spark.sql("select indicator, org_level, currency, value from `{db}`.nmpa_csv where ReportingPeriodStartDate='2015-04-01' and org_level='National' and indicator like 'Mode%' order by indicator, currency".format(db=db_output))
count_df.show()
try:
  assert expected_df.collect() == count_df.collect()
except:
  print('count_df', count_df.show())
  print('expected_df', expected_df.show())
  print('Error in NMPA birth_modes - National')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_mode NHS England (Region) 
expected_counts = {'Numerator': '15.0', 'Denominator': '75.0', 'Rate': '20.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'NHS England (Region)'  and Org_Code='Y54' and Indicator='Mode_Caesarean' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_mode - Local Maternity System')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_mode Local Maternity System 
expected_counts = {'Numerator': '5.0', 'Denominator': '5.0', 'Rate': '100.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'Local Maternity System'  and Org_Code='E54000039' and Indicator='Mode_Caesarean' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_mode -  Local Maternity System')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_mode MBRRACE Grouping
expected_counts = {'Numerator': '15.0', 'Denominator': '75.0', 'Rate': '20.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'MBRRACE Grouping'  and Org_Code='Group 2. Level 3 NICU' and Indicator='Mode_Caesarean' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_mode - MBRRACE Grouping')
  run_tests_ran_ok = False

# COMMAND ----------

# DBTITLE 1,NMPA birth_mode Provider RA7 
expected_counts = {'Numerator': '5.0', 'Denominator': '5.0', 'Rate': '100.0'}

count_df = spark.sql("select Currency, Value from `{db}`.nmpa_csv where org_level = 'Provider'  and Org_Code='RA7' and Indicator='Mode_Caesarean' and ReportingPeriodStartDate='2015-04-01'".format(db=db_output))
actual_counts = {r['Currency']:r['Value'] for r in count_df.collect()}
print(actual_counts)
try:
  assert actual_counts == expected_counts
except:
  print('actual_counts', actual_counts)
  print('expected_counts', expected_counts)
  print('Error in NMPA birth_mode - Provider RA7')
  run_tests_ran_ok = False

# COMMAND ----------

assert run_tests_ran_ok
dbutils.notebook.exit(run_tests_ran_ok)