# Databricks notebook source
# MAGIC %md
# MAGIC #Monthly job execution

# COMMAND ----------

# DBTITLE 1,Import modules and Define Widgets
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


# dbutils.widgets.text("dbSchema", "mat_pre_clear")
# dbutils.widgets.text("outSchema", "mat_analysis", "Target database")
# dbutils.widgets.text("RPBegindate", "2023-09-01")
# dbutils.widgets.text("month_id", "1482", "month_id")
# dbutils.widgets.text("status", "Final", "status")
# dbutils.widgets.text("filename", "final", "filename")
# dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")

#startchoices = [str(r[0]) for r in spark.sql("select distinct RPStartDate from {DatabaseSchema}.msd000header order by RPStartDate".format(DatabaseSchema=DatabaseSchema)).collect()]
#dbutils.widgets.dropdown("RPBegindate", "2019-04-01", startchoices)

# COMMAND ----------

# DBTITLE 1,Get widget values
outputSchema = dbutils.widgets.get("outSchema")
DatabaseSchema = dbutils.widgets.get("dbSchema")
ReportingStartPeriod = dbutils.widgets.get("RPBegindate")
month_id = dbutils.widgets.get("month_id")
# Only passed to Continuity of Carer and Saving Babies Lives. Not currently used for output.
status = dbutils.widgets.get("status")
print(status)
assert(status)
filename = dbutils.widgets.get("filename")
print(filename)
assert(filename)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)

# Get the period end from the period start
startdateasdate = datetime.strptime(ReportingStartPeriod, "%Y-%m-%d")
ReportingEndPeriod = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
rp_startdate_rolling_two_months = (startdateasdate + relativedelta(months=-1, days=0)).strftime("%Y-%m-%d")
rp_startdate_rolling_six_months = (startdateasdate + relativedelta(months=-5, days=0)).strftime("%Y-%m-%d")

RunTime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

print("rp_startdate_rolling_six_months=", rp_startdate_rolling_six_months,", rp_startdate_rolling_two_months=", rp_startdate_rolling_two_months,", ReportingStartPeriod=", ReportingStartPeriod,", ReportingEndPeriod=", ReportingEndPeriod, ", DatabaseSchema=", DatabaseSchema,", outputSchema=", outputSchema,"RunTime=", RunTime)

# COMMAND ----------

# DBTITLE 1,Execute the Geographies notebook
geogsdict = { "RPEnddate" : ReportingEndPeriod
          ,"RPStartdate" : ReportingStartPeriod
          ,"outSchema" : outputSchema 
          ,"dbSchema" : DatabaseSchema 
          ,"dss_corporate" : dss_corporate
          ,"month_id" : month_id}


#Creates the geography views
print(dbutils.notebook.run("./01_Prepare/create_geography_tables", 0, geogsdict))

#Creates the updated base table views The merged organisations are given the correct code and name
print(dbutils.notebook.run("./01_Prepare/update_base_tables", 0, geogsdict))

#Creates a view giving the newly assigned code for any organisations which have merged
print(dbutils.notebook.run("./01_Prepare/create_merged_providers_view", 0, geogsdict))

# COMMAND ----------

# DBTITLE 1,Prepare steps of all the independent measures
params = { "RPEnddate" : ReportingEndPeriod
          ,"RPStartdate" : ReportingStartPeriod
          ,"outSchema" : outputSchema 
          ,"dbSchema" : DatabaseSchema 
          ,"dss_corporate" : dss_corporate
          ,"month_id" : month_id}


# Dependent on call to create_geography_tables
# Bringing all the Prep statements together and passing common parameters to the notebooks.

# Create CoC views, truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_CoC", 0, params))

# Create PCSP views
print(dbutils.notebook.run("./01_Prepare/Prepare_PCSP", 0, params))

# Create SLB views, truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_SLB", 0,  params))

# Create BMI views, truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_BMI", 0, params))

# CQIM truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_CQIM", 0, params))

# Create NMPA views, truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_NMPA", 0, params))

# Create Measures views, truncate intermediate tables and delete output of any execution of the same period
print(dbutils.notebook.run("./01_Prepare/Prepare_Measures", 0, params))

# Creates a view giving the most recent submission. Integrates the merged providers from the merged providers view. Creates a new Header table for use in reassigning org code everywhere in the code. 
print(dbutils.notebook.run("./01_Prepare/create_most_recent_subs", 0, params))

# Creates the views for the derivations:smokingstatusbooking,smokingstatus,deliverybirthweight
# derivation_params = {"RPStartdate" : ReportingStartPeriod, "RPEnddate" : ReportingEndPeriod, "dbSchema" : DatabaseSchema}
print(dbutils.notebook.run("./01_Prepare/create_views_for_derivations", 0, params))

# Creates the Base Table views, which bring geographies and measure definitions into some record-level views

print(dbutils.notebook.run("./01_Prepare/populate_base_tables", 0, params))

# COMMAND ----------

# DBTITLE 1,Clear out the monthly csv table
# MAGIC %sql
# MAGIC -- Better to delete current month?
# MAGIC TRUNCATE TABLE $outSchema.maternity_monthly_csv_sql

# COMMAND ----------

# DBTITLE 1,Execute the Aggregation of the main monthly measures
"""
aggregates at each geography and at national level
"""
#TODO: add a widget to pass in the monthly csv table name?

dbutils.notebook.run("./02_Aggregate/LoopThroughTablesMeasuresGeogs", 0, {"RPBegindate" : ReportingStartPeriod, "RPEnddate" : ReportingEndPeriod, "outSchema" : outputSchema, "month_id" : month_id})

# COMMAND ----------

# DBTITLE 1,NMPA
params = { "RPEnddate" : ReportingEndPeriod
          ,"RPStartdate" : ReportingStartPeriod
          ,"outSchema" : outputSchema 
          ,"dbSchema" : DatabaseSchema 
          ,"dss_corporate" : dss_corporate
          ,"month_id" : month_id}

print(dbutils.notebook.run("./02_Aggregate/NMPA/NMPA_Transfer_Home_Obstetric", 0, params))
print(dbutils.notebook.run("./02_Aggregate/NMPA/NMPA_Transfer_FMU_Obstetric", 0,params))
print(dbutils.notebook.run("./02_Aggregate/NMPA/NMPA_Transfer_AMU_Obstetric", 0, params))
print(dbutils.notebook.run("./02_Aggregate/NMPA/NMPA_Transfer_Midwife_Obstetric", 0, params))
# For Birth mode measure
print(dbutils.notebook.run("./02_Aggregate/NMPA/Birth_Modes", 0,params))
print(dbutils.notebook.run("./02_Aggregate/NMPA/Births_In_Different_Settings", 0, params))

# COMMAND ----------

# DBTITLE 1,NMPA InductionOfLabour Measure
params = { "RPEnddate" : ReportingEndPeriod
          ,"RPStartdate" : ReportingStartPeriod
          ,"outSchema" : outputSchema 
          ,"dbSchema" : DatabaseSchema 
          ,"dss_corporate" : dss_corporate
          ,"month_id" : month_id}

print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Indicator_Specific_Code/InductionOfLabour_MeetCriteria", 0, params))
#NP: the aggregate code for Induction must be done separately because of different logic to calculate the overDQThreshold
print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Indicator_Specific_Code/InductionOfLabour_Aggregate", 0, params))
#NP the code for induction geographies can be incorporated iton the measure geographies
# dbutils.notebook.run("./02_Aggregate/Measures/Induction_of_Labour/InductionOfLabour_Geographies", 0, params)
# dbutils.notebook.run("./02_Aggregate/Measures/Induction_of_Labour/InductionOfLabour_Final", 0, params)

# COMMAND ----------

# DBTITLE 1,BMI_14 weeks Measure
params = { 
   "outSchema" : outputSchema
  ,"source" : DatabaseSchema
  ,"dss_corporate" : dss_corporate
  ,"RPStartDate" : ReportingStartPeriod
  ,"RPEndDate" : ReportingEndPeriod
  ,"RunTime" : RunTime
}
dbutils.notebook.run("./02_Aggregate/BMI/BMI_14Weeks",0, params)

# COMMAND ----------

# DBTITLE 1,Aspirin Measure - MeetAspirinCritera
params = { 
   "outSchema" : outputSchema
  ,"source" : DatabaseSchema
  ,"dss_corporate" : dss_corporate
  ,"RPStartDate" : ReportingStartPeriod
  ,"RPEndDate" : ReportingEndPeriod
  ,"PatientFilterStartDate" : "2019-01-01"
  ,"RunTime" : RunTime
}

print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Indicator_Specific_Code/Aspirin_MeetCriteria", 0, params))
print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Indicator_Specific_Code/Aspirin_Aggregate", 0, params))
# dbutils.notebook.run("./02_Aggregate/Measures/Measures_Aggregate", 0, params)
# dbutils.notebook.run("./02_Aggregate/Measures/Measures_Geographies", 0, params)
# dbutils.notebook.run("./02_Aggregate/Measures/Measures_Final", 0, params)

# COMMAND ----------

# DBTITLE 1,Measures Generic code
params = { 
   "outSchema" : outputSchema
  ,"source" : DatabaseSchema
  ,"dss_corporate" : dss_corporate
  ,"RPStartDate" : ReportingStartPeriod
  ,"RPEndDate" : ReportingEndPeriod
  ,"PatientFilterStartDate" : "2019-01-01"
  ,"RunTime" : RunTime
}

#need to loop through the needed indicators before this can be used generically
# dbutils.notebook.run("./02_Aggregate/Measures/Measures_Generic_Code/Measures_Aggregate", 0, params)

#Aspirin_MeetCriteria,Induction
print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Generic_Code/Measures_Geographies", 0, params))

#move code from geographies to Final and uncomment and use as genric code (DONE)
print(dbutils.notebook.run("./02_Aggregate/Measures/Measures_Generic_Code/Measures_Final", 0, params))

# COMMAND ----------

# DBTITLE 1,Execute the CQIM DQs
# """
# Inserts the CQIM Measures and rates
# """
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPBegindate" : ReportingStartPeriod, "RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate}
dbutils.notebook.run("./02_Aggregate/CQIM/CQIM_Master", 0, params)

# COMMAND ----------

# DBTITLE 1,Execute the CQIM Rates suppression and aggregation
# """
# Inserts the CQIM Measures and rates
# """
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPBegindate" : ReportingStartPeriod, "RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate}
dbutils.notebook.run("./02_Aggregate/CQIM/CQIM_Suppression", 0, params)

# COMMAND ----------

# DBTITLE 1,Continuity of Carer (CoC) Measure COCDQ04, COCDQ05, COC_by_28weeks & COC_receiving_ongoing
# Status currently hardcoded 
params = { "dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPBegindate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "status" : status, "dss_corporate" : dss_corporate}

# print(params)

print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ04", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ05", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ06", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ07", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ08", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COCDQ09", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COC_by_28weeks", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COC_receiving_ongoing", 0, params))
print(dbutils.notebook.run("./02_Aggregate/COC/COC_Geographies", 0, params))

# COMMAND ----------

# DBTITLE 1,Saving Babies Lives (SLB)
#Status currently hardcoded 
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPBegindate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id}
print(params)

dbutils.notebook.run("./02_Aggregate/SLB", 0, params)

# COMMAND ----------

# DBTITLE 1,PCSP Measure - PCP_All_Pathways
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPBegindate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "status" : status, "dss_corporate" : dss_corporate, "rp_startdate_rolling_two_months" : rp_startdate_rolling_two_months, "rp_startdate_rolling_six_months" : rp_startdate_rolling_six_months, "RunTime" :RunTime }
print(params)

print(dbutils.notebook.run("./02_Aggregate/PCSP/PCP_All_Pathways", 0, params))
print(dbutils.notebook.run("./02_Aggregate/PCSP/PCP_3CarePlans", 0, params))
print(dbutils.notebook.run("./02_Aggregate/PCSP/PCSP_Geographies", 0, params))

# COMMAND ----------

# DBTITLE 1,Clear out the dq table
# MAGIC %sql
# MAGIC truncate table $outSchema.dq_csv

# COMMAND ----------

# DBTITLE 1,Execute populate dq_csv
"""
Creates dataframes to aggregate dq measures and insert final into the dq table
"""
#TODO: add a widget to pass in the dq table name?

print(dbutils.notebook.run("./02_Aggregate/calculate_dq", 0, {"RPBegindate" : ReportingStartPeriod, "RPEnddate" : ReportingEndPeriod, "dbSchema" : DatabaseSchema, "outSchema" : outputSchema}))

# COMMAND ----------

# DBTITLE 1,Check the CSV internally consistent
returned_table = dbutils.notebook.run("./csv_output_checking", 0, {"outSchema" : outputSchema})

print("*"*70+"CSV File Checking" + "*"*113)
print("")
for results in returned_table.split(','):
    print(results)
    print("")
print("*"*200)

#TODO: add breakpoint to process if result is unfavourable

# COMMAND ----------

# DBTITLE 1,Check DQ file internally consistent
pass

# COMMAND ----------

# DBTITLE 1,Execute DQMI
"""
Creates temp tables to summarise coverage and selected DQ analysis and drop then re-create permanent table for each
"""
#TODO: add a widget to pass in the dq table names?

dbutils.notebook.run("./02_Aggregate/DQMI", 0, {"RPBegindate" : ReportingStartPeriod, "outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Export and suppress the csvs
suppress = True
if suppress:
  pass #run the export function with suppression = "yes"
  print("Suppressed output(s) generated")
else:
  pass #run the export function with suppression = "no"
  print("Raw output(s) generated - caution with dissemination")

# COMMAND ----------

# DBTITLE 1,Ceph_Pres_Spon_Birth standalone measure
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "Birth", "Indicator" : "Spontaneous_birth_Cephalic_presentation", "OutputType" : "measure", "OutputCount" : "Women","outtable" : "ceph_spon_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Ceph_pres_Spon_birth_Publication_Code", 0, params)

# COMMAND ----------

# DBTITLE 1,LMUP 
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "Pregnancy", "Indicator" : "LMUP_score_recorded", "OutputType" : "measure", "OutputCount" : "Women","outtable" : "lmup_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_LMUP_Score_Recorded_Publication_Code", 0, params)


# COMMAND ----------

# DBTITLE 1,FNP
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "Pregnancy", "Indicator" : "FNP_support", "OutputType" : "measure", "OutputCount" : "Women","outtable" : "fnp_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Publication_FNP_Publication_Code", 0, params)

# COMMAND ----------

# DBTITLE 1,Smoke Free Pregnancy
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "Pregnancy", "Indicator" : "Smokefree_pregnancy", "OutputType" : "measure", "OutputCount" : "Women","outtable" : "smokefreepreg_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Smokefree_Pregnancy_NMPA_Publication_Code", 0, params)

# COMMAND ----------

# DBTITLE 1,SBL_E2_Od
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "SBL", "Indicator" : "SBL_Element2_OutcomeIndicator_d", "OutputType" : "measure", "OutputCount" : "Babies","outtable" : "sble2_o1_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Publication_SBL_El2_Outd", 0, params)

# COMMAND ----------

# DBTITLE 1,No longer used - SBL_E2_O2
# params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod,"RPEnddate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" # : "SBL", "Indicator" : "SBL_Element2_OutcomeIndicator2", "OutputType" : "measure", "OutputCount" : "Babies","outtable" : "sble2_o2_csv"}
# print(params)
# 
# dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Publication_SBL_El2_Out2", 0, params)

# COMMAND ----------

# DBTITLE 1,Safeguarding_1-4
params = {"dbSchema" : DatabaseSchema, "outSchema" : outputSchema, "RPStartDate" : ReportingStartPeriod, "RPEndDate" : ReportingEndPeriod, "dss_corporate" : dss_corporate, "month_id" : month_id, "IndicatorFamily" : "Pregnancy", "Indicator" : "SafeguardingConcern_", "OutputType" : "measure", "OutputCount" : "Babies", "outtable" : "safeguarding_csv"}
print(params)

dbutils.notebook.run("./02_Aggregate/standalone/MSDS_Safeguarding_1-4", 0, params)

# COMMAND ----------

# DBTITLE 1,Persist the aggregated data
#TODO: consider OPTIMIZE (see MH Round_output in aggregate) 
"""
Deletes old data runs and stores the new run data with a runtime stamp
"""
dbutils.notebook.run("./03_Extract/persist_data", 0, {"RPEnddate" : ReportingEndPeriod, "outSchema" : outputSchema, "runTimestamp" : RunTime})

# COMMAND ----------

# DBTITLE 1,Send output CSV extracts
#TODO: consider OPTIMIZE (see MH Round_output in aggregate) 
"""
Sends the monthly extracts and power BI reports via MESH
"""
params = {
  "RPBegindate" : ReportingStartPeriod,
  "RPEnddate" : ReportingEndPeriod,
  "dbSchema" : DatabaseSchema,
  "outSchema" : outputSchema,
  "month_id": month_id,
  "status" : status,
  "filename" : filename,
  "dss_corporate" : dss_corporate
}
dbutils.notebook.run("./03_Extract/000_Maternity_BI_csv", 0,params)
dbutils.notebook.run("./03_Extract/001_Maternity_Extracts", 0, params)
dbutils.notebook.run("./03_Extract/002_Maternity_Dashboard_Ref", 0, params)