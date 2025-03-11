# Databricks notebook source
# DBTITLE 1,MSDS_Suppress_Monthly_DQ.py - NB this will always pull out data for the most recently run month...  Now uses widgets
# this top section (commented out) might be useful if we ever get the extraction of data running as part of the monthly pipeline - it causes the code below to only pull out data for the most recent month - this is not always what we need at present so it is commented out and the values will be pulled from the widgets instead.

# # Databricks notebook source
# sql = "select max(ReportingPeriodStartDate) from mat_analysis.dq_csv"
# ReportingStartPeriod = spark.sql(sql).collect()[0][0]
# # ReportingStartPeriod = '2020-01-01'
# print(ReportingStartPeriod)

# # COMMAND ----------

# # derive Enddate from startdate
# from dateutil.relativedelta import relativedelta
from dsp.common.exports import create_csv_for_download
from dsp.code_promotion.mesh_send import cp_mesh_send
from dateutil.relativedelta import relativedelta
import os
from datetime import datetime, date
from pyspark.sql.types import StructType,StructField, DateType, IntegerType, StringType



#Prod mail box id
mailbox_to = 'X26HC004'
workflow_id = 'GNASH_MAT_ANALYSIS'
# local_id = 'mat_analysis'

# startdateasdate = datetime.strptime(ReportingStartPeriod, "%Y-%m-%d")
# RPEnddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")

# print(RPEnddate)
# use the widgets!

RPBegindate = dbutils.widgets.get("RPBegindate")
RPEnddate = dbutils.widgets.get("RPEnddate")
dbSchema = dbutils.widgets.get("dbSchema")
outSchema = dbutils.widgets.get("outSchema")
month_id = dbutils.widgets.get("month_id")
status = dbutils.widgets.get("status")
fname = dbutils.widgets.get("filename")

print('RPBegindate: ',RPBegindate)
print('RPEnddate: ',RPEnddate)

#import necessary packages
import pandas as pd

#convert sql table to pandas dataframe
pdf = spark.sql("select ReportingPeriodStartDate,ReportingPeriodEndDate,Org_Code,DataTable,UID,DataItem,Valid,Default,Invalid,Missing,Denominator from {outSchema}.dq_store where (right(dataItem, 10) <> 'Submission' and dataItem is not null) and ReportingPeriodEndDate = '{RPEnddate}'".format(RPEnddate=RPEnddate, outSchema=outSchema)).toPandas()


tabsubpdf = spark.sql("select ReportingPeriodStartDate,ReportingPeriodEndDate,Org_Code,DataTable,UID,DataItem,Valid,Default,Invalid,Missing,Denominator from {outSchema}.dq_store where (right(dataItem, 10) = 'Submission' or dataItem is null) and ReportingPeriodEndDate = '{RPEnddate}'".format(RPEnddate=RPEnddate, outSchema=outSchema)).toPandas()

pdf.count()

tabsubpdf.count()

#create suppression function (0 remains as 0, 1-7 are rounded to 5, all other values rounded to the nearest 5)
def suppress_value(valuein)->int:

    if valuein == 0:
        valueout = valuein
    if valuein < 0:
        raise ValueError("The input: {} is less than 0.".format(valuein))
    if 0 < valuein <= 7:
        valueout = 5
    if valuein >7:
        valueout = 5 * round(valuein/5)
    return valueout

  
#rounds valid, default, invalid, missing and denominator columns to: 0 = 0, 1-7 = 5, >7 rounded to nearest 5
pdf['Valid'] = pdf['Valid'].where(pdf["Valid"].notna()).apply(suppress_value)
pdf['Default'] = pdf['Default'].where(pdf["Default"].notna()).apply(suppress_value)
pdf['Invalid'] = pdf['Invalid'].where(pdf["Invalid"].notna()).apply(suppress_value)
pdf['Missing'] = pdf['Missing'].where(pdf["Missing"].notna()).apply(suppress_value)
pdf['Denominator'] = pdf['Denominator'].where(pdf["Denominator"].notna()).apply(suppress_value)
#print(pdf[["Valid", "Default", "Invalid", "Missing", "Denominator"]])
#pdf.head

##TODO: currently skips NaN values as the function wouldn't work otherwise but not sure if NaN values are likely?


frames = [pdf, tabsubpdf]
final_df = pd.concat(frames, sort=False)

final_df.count()

# display(final_df.sort_values(["Org_Code", "DataTable", "UID"]))

yyyy = RPBegindate[:4]
mname = datetime.strptime(RPBegindate, '%Y-%m-%d').strftime("%b").lower()

df_to_extract = final_df.sort_values(["Org_Code", "DataTable", "UID"])
df_to_extract = spark.createDataFrame(df_to_extract)
# RunTime = datetime.now().strftime("%Y%m%d%H%M%S")
if (fname =='Provisional'):
    filename = f"msds-{mname}{yyyy}{fname}-exp-dq.csv"
else:
    filename = f"msds-{mname}{yyyy}-exp-dq.csv"
local_id = filename
# For supporting the LEAD MESH activity to rename the files appropriately with rename flag as option '1', local_id parameter should bear the value of file name.

print(filename)
print(type(df_to_extract))
print(f"{df_to_extract.count()} records found in the given reporting period in msds-dq report.")

if(os.environ.get('env') == 'prod'):
    try:
      request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
      print(f"{filename} file have been pushed to MESH with request id {request_id}. \n")
    except Exception as ex:
      print(ex, 'MESH exception on SPARK 3 can be ignored, file would be delivered in the destined path')
else:
    display(df_to_extract)


# COMMAND ----------

# DBTITLE 1,MSDS_Suppress_Monthly_Measures.py - msds-mmmyyyy-exp-data.csv
sql = "select max(ReportingPeriodStartDate) from mat_analysis.maternity_monthly_csv_sql"

# NB there is only ever one month of data in this table!  The sql above is therefore somewhat redundant...


RPBegindate = dbutils.widgets.get("RPBegindate")
RPEnddate = dbutils.widgets.get("RPEnddate")
dbSchema = dbutils.widgets.get("dbSchema")
outSchema = dbutils.widgets.get("outSchema")


# ReportingStartPeriod = spark.sql(sql).collect()[0][0]
ReportingStartPeriod = RPBegindate
print(ReportingStartPeriod)

#import necessary packages
import pandas as pd

#convert sql table to pandas dataframe
pdf = spark.sql(f"""
select ReportingPeriodStartDate, ReportingPeriodEndDate, cast(Dimension as string)
     , cast(Org_Level as string), cast(Org_Code as string), cast(Org_Name as string)
     , cast(Measure as string), cast(Count_Of as string), cast(Value_Unsuppressed as float) 
     from {outSchema}.maternity_monthly_measures_store 
       where ReportingPeriodEndDate = '{RPEnddate}'""").toPandas()

#create suppression function (0 remains as 0, 1-7 are rounded to 5, all other values rounded to the nearest 5)
def suppress_value(valuein)->int:

    if valuein == 0:
        valueout = valuein
    if valuein < 0:
        raise ValueError("The input: {} is less than 0.".format(valuein))
    if 0 < valuein <= 7:
        valueout = 5
    if valuein >7:
        valueout = 5 * round(valuein/5)
    return valueout

  
#create dataframe for average values
Average_pdf = pdf.copy()[pdf['Count_Of'].str.contains("Average")]

#create dataframe for non-average values
NotAve_pdf =pdf.copy()[~pdf['Count_Of'].str.contains("Average")]

#create column in non-average dataframe with rounded values in
NotAve_pdf['Value_Suppressed'] = NotAve_pdf['Value_Unsuppressed'].apply(suppress_value).astype(float)
#drop unsuppressed columns leaving only suppressed/rounded values
NotAve_pdf = NotAve_pdf.drop("Value_Unsuppressed", axis = 1)


# display(Average_pdf)
# display(NotAve_pdf)

#concatenate unsuppressed national and suppressed non-national dataframes together
frames = [Average_pdf, NotAve_pdf]
final_df = pd.concat(frames, sort=False)


#create final column in combined dataframe with both unsuppressed average values and suppressed non average values
final_df['Final_value'] = final_df['Value_Suppressed']
final_df['Final_value'].update(final_df['Value_Unsuppressed'])

#drop unneccessary columns
final_df = final_df.drop("Value_Unsuppressed", axis = 1)
final_df = final_df.drop("Value_Suppressed", axis = 1)


#print(final_df)

## TODO: convert dataframe back into csv format so that it can be exported
#select distinct values of Org_level - check sorting errors if needed.

final_df["Org_Level"].unique().tolist()

yyyy = ReportingStartPeriod[:4]

mname = datetime.strptime(ReportingStartPeriod, '%Y-%m-%d').strftime("%b").lower()


df_to_extract = final_df.sort_values(["Org_Level", "Org_Code", "Dimension", "Measure"])#.head(100)
#empty dataframes couldn't infer schemas which is failing the run test with given reporting date. Hence declaring the schema
Schema = StructType([ StructField("ReportingPeriodStartDate", StringType(), True),
                      StructField("ReportingPeriodEndDate", StringType(), True),
                      StructField("Dimension", StringType(), True),
                      StructField("Org_Level", StringType(), True),
                      StructField("Org_Code", StringType(), True),
                     StructField("Org_Name", StringType(), True),
                     StructField("Measure", StringType(), True),
                     StructField("Count_Of", StringType(), True),
                      StructField("Final_value", StringType(), True)
                    ])
df_to_extract = spark.createDataFrame(df_to_extract,Schema)
import pyspark.sql.functions as F
df_to_extract = (
  df_to_extract
  .orderBy(
    F.when(F.col('Org_Level')=='National', 1)
    .when(F.col('Org_Level')=='NHS England (Region)', 2)
    .when(F.col('Org_Level')=='MBRRACE Grouping', 3)
    .when(F.col('Org_Level')=='Local Authority of Residence', 4)
    .when(F.col('Org_Level')=='Provider', 5)
    .when(F.col('Org_Level')=='Booking Site', 6)
    .when(F.col('Org_Level')=='Local Maternity System', 7)
    .when(F.col('Org_Level')=='SubICB of Responsibility' if int(month_id) > 1467 else 'CCG of Responsibility', 8)  # Since July, all CCG are referred as SubICB, so picking the corresponding field
    .when(F.col('Org_Level')=='Delivery Site', 9),
    F.when(F.col('Org_Code') != '', F.lower('Org_Code')).otherwise(None).asc_nulls_last(),  # we want null values at the end, but the code currently uses an empty string  instead of a NULL which makes it harder to query/sort the data
    F.col('Dimension').asc(),
    F.lower(F.col('Measure')).asc()
  )
)
# RunTime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if (fname =='Provisional'):
    filename = f"msds-{mname}{yyyy}{fname}-exp-data.csv"
else:
    filename = f"msds-{mname}{yyyy}-exp-data.csv"

local_id = filename
# For supporting the LEAD MESH activity to rename the files appropriately with rename flag as option '1', local_id parameter should bear the value of file name.

print(filename)
print(type(df_to_extract))
print(f"{df_to_extract.count()} records found in the given reporting period in msds-dq report.")

if(os.environ.get('env') == 'prod'):
    try:
      request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, filename, local_id)
      print(f"{filename} file have been pushed to MESH with request id {request_id}. \n")
    except Exception as ex:
      print(ex, 'MESH exception on SPARK 3 can be ignored, file would be delivered in the destined path')    
else:
    display(df_to_extract)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # See  Maternity Services Publication List Internal Tracker for inclusion/exclusion from publication output:
# MAGIC
# MAGIC https://hscic365.sharepoint.com/:x:/r/sites/GNASHCMHT/Shared%20Documents/General/Maternity%20Services%20Publications%20list_internal_Tracker.xlsx?d=wff2fc581f5234f1e82675420b0d012a1&csf=1&web=1&e=l82zcT   
# MAGIC
# MAGIC - All CofC data is provided as a separate output for checking - 02-03-2022 - actioned in this notebook
# MAGIC - All PCSP data currently in the pipeline is commented out until further notice - actioned in this notebook
# MAGIC - CQIM DQ35, 40, 41, 42 will be excluded from the Robson Group CQIMs but will otherwise remain in the pipeline outputs - DMS001-1155

# COMMAND ----------

# DBTITLE 1,msds-[mmmyyyy]-exp-measures
# MAGIC %py
# MAGIC
# MAGIC df_msds_exp_measures = spark.sql(f"""
# MAGIC select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level
# MAGIC      , RPStartDate, RPEndDate
# MAGIC      , IndicatorFamily, Indicator
# MAGIC      , Currency, Value 
# MAGIC      from {outSchema}.slb_csv where rpstartdate='{RPBegindate}'
# MAGIC union all
# MAGIC select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level
# MAGIC      , RPStartDate, RPEndDate
# MAGIC      , IndicatorFamily, Indicator
# MAGIC      , Currency, Value 
# MAGIC      from {outSchema}.coc_csv where rpstartdate='{RPBegindate}'
# MAGIC union all
# MAGIC select Org_code, Org_Name as Org_Name, Org_Level as Org_Level
# MAGIC      , reportingperiodstartdate, reportingperiodenddate
# MAGIC      , IndicatorFamily, Indicator
# MAGIC      , Currency, Value 
# MAGIC      from {outSchema}.nmpa_csv where reportingperiodstartdate='{RPBegindate}'
# MAGIC union all
# MAGIC select OrgCodeProvider as Org_Code, Org_Name, Org_Level
# MAGIC      , RPStartDate, RPEndDate
# MAGIC      , IndicatorFamily, Indicator
# MAGIC      , Currency, Value 
# MAGIC      from {outSchema}.cqim_dq_csv where rpstartdate='{RPBegindate}'
# MAGIC union all
# MAGIC select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level
# MAGIC      , RPStartDate, RPEndDate
# MAGIC      , IndicatorFamily, Indicator,
# MAGIC      Currency, Value 
# MAGIC      from {outSchema}.measures_csv where rpstartdate='{RPBegindate}'
# MAGIC
# MAGIC ORDER BY
# MAGIC         CASE WHEN Org_Level = 'National' THEN 1
# MAGIC              WHEN Org_Level = 'Local Maternity System' THEN 2
# MAGIC              WHEN Org_Level = 'MBRRACE Grouping' THEN 3
# MAGIC              WHEN Org_Level = 'NHS England (Region)' THEN 4
# MAGIC              WHEN Org_Level = 'Provider' THEN 5 END,
# MAGIC         Org_Code,
# MAGIC         LOWER(IndicatorFamily), --Spark prioritises upper cased letters when sorting ("BMI" would be the first in the output and then "Birth"); standardise to get the intended behaviour
# MAGIC         Indicator,
# MAGIC         Currency
# MAGIC """)
# MAGIC
# MAGIC print(f"{df_msds_exp_measures.count()} records found in the given reporting period in msds-exp-measures report.")
# MAGIC
# MAGIC if (fname =='Provisional'):
# MAGIC     filename = f"msds-{mname}{yyyy}{fname}-exp-measures.csv"
# MAGIC else:
# MAGIC     filename = f"msds-{mname}{yyyy}-exp-measures.csv"
# MAGIC
# MAGIC local_id = filename
# MAGIC # For supporting the LEAD MESH activity to rename the files appropriately with rename flag as option '1', local_id parameter should bear the value of file name.
# MAGIC if(os.environ.get('env') == 'prod'):
# MAGIC     try:
# MAGIC       request_id = cp_mesh_send(spark, df_msds_exp_measures, mailbox_to, workflow_id, filename, local_id)
# MAGIC       print(f"{filename} file have been pushed to MESH with request id {request_id}. \n")
# MAGIC     except Exception as ex:
# MAGIC       print(ex, 'MESH exception on SPARK 3 can be ignored, file would be delivered in the destined path')      
# MAGIC else:
# MAGIC     display(df_msds_exp_measures)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Measures - for review - InductionOfLabour (from msds-[mmmyyyy]-exp-measures)
# MAGIC %sql
# MAGIC
# MAGIC -- select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.measures_csv where rpstartdate='$RPBegindate' 
# MAGIC -- and Indicator = 'Induction'
# MAGIC -- -- union all
# MAGIC
# MAGIC
# MAGIC -- --to add to BAU when ready
# MAGIC -- union all
# MAGIC -- select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.measures_csv where rpstartdate='$RPBegindate' and Indicator = 'Induction'
# MAGIC -- --/Workspaces/mat_clear_collab/Maternity_Extracts
# MAGIC -- -- select * from mat_analysis.nmpa_induction_codes

# COMMAND ----------

# DBTITLE 1,Measures - for review - COC & PCSP (from msds-[mmmyyyy]-exp-measures) (COC Now in bau output)
# MAGIC %sql
# MAGIC
# MAGIC -- select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.coc_csv where rpstartdate='$RPBegindate'
# MAGIC -- union all
# MAGIC -- select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.pcsp_csv where rpstartdate='$RPBegindate'

# COMMAND ----------

# DBTITLE 1,Measures - for review - CQIM Now in bau output
# MAGIC %sql
# MAGIC -- Outputs are normally checked by the Turing team for 2 or 3 months before becomng bau
# MAGIC -- Post a message on GNASH teams to prompt these outputs to consider forwarding for review
# MAGIC -- Commented out 7/3 SH
# MAGIC --select OrgCodeProvider as Org_Code, Org_Name, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.cqim_dq_csv where rpstartdate='$RPBegindate'

# COMMAND ----------

# DBTITLE 1,Temp fix (to be done again): GBT: not needed all COC outputs from DMS to be excluded for November run
# MAGIC %sql
# MAGIC /* Due to the monthly run, the output was producing result for all CoC measures inclding COC_DQ04 and COC_DQ05 which has not been signed off yet. However, CoCDQ02 and CoCDQ03 will still be impacted as the DQ measures will depend on new values from under-development measures.
# MAGIC
# MAGIC Hence this is just a temp fix to extract only existing output */
# MAGIC
# MAGIC select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.coc_csv where rpstartdate='$RPBegindate'
# MAGIC -- and Indicator LIKE 'COC_by_28weeks%'
# MAGIC -- UNION
# MAGIC -- select OrgCodeProvider as Org_Code, OrgName as Org_Name, OrgLevel as Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator, Currency, Value from $outSchema.coc_csv where rpstartdate='$RPBegindate'
# MAGIC -- and Indicator LIKE 'COC_receiving_ongoing%'