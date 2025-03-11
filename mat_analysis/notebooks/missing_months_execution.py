# Databricks notebook source
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from shared.submissions.calendars.common import calculate_uniq_month_id_from_date

# COMMAND ----------

outputSchema = dbutils.widgets.get("outSchema")
DatabaseSchema = dbutils.widgets.get("dbSchema")
dss_corporate = dbutils.widgets.get("dss_corporate")
#month_id = dbutils.widgets.get("month_id")


# COMMAND ----------

# DBTITLE 1,BMI_14 weeks Time Series 
# this is a temp solution to load bmi for past 7 months. needs to be commented after one run
Date_to_insert = ["2022-01-01" , "2022-02-01" , "2022-03-01" , "2022-04-01" , "2022-05-01" , "2022-06-01" , "2022-07-01" , "2022-08-01" , "2022-09-01" , "2022-10-01" , "2023-01-01" , "2023-02-01"] 
 
for RPStartDate in Date_to_insert:
  startdateasdate = datetime.strptime(RPStartDate, "%Y-%m-%d")
  RPEndDate= (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
  month_id= calculate_uniq_month_id_from_date(startdateasdate)

  geogsdict = {"endperiod" : RPEndDate, "outSchema" : outputSchema, "dss_corporate" : dss_corporate, "month_id" : month_id}
  print(geogsdict)
  Return_Value = dbutils.notebook.run("./01_Prepare/create_geography_tables", 0, geogsdict)
  print(Return_Value)

  base_table_params = {"endperiod" : RPEndDate, "DatabaseSchema" : DatabaseSchema, "dss_corporate" : dss_corporate}
  Return_Value = dbutils.notebook.run("./01_Prepare/update_base_tables", 0, base_table_params)
  print(Return_Value)

  Return_Value = dbutils.notebook.run("./01_Prepare/create_merged_providers_view", 0, {"endperiod" : RPEndDate, "outSchema" : outputSchema, "dbSchema" : DatabaseSchema})
  print(Return_Value)

  params = { "outSchema" : outputSchema,"source" : DatabaseSchema,"dss_corporate" : dss_corporate
              ,"RPStartDate" : RPStartDate,"RPEndDate" : RPEndDate }
  print(params)
  dbutils.notebook.run("./01_Prepare/Prepare_BMI_T", 0, params)
  dbutils.notebook.run("./02_Aggregate/BMI/BMI_14Weeks",0, params)
  print("**************")
