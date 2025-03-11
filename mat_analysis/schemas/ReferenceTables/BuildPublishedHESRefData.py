# Databricks notebook source
# Data in these tables shouldn't be visible in DAE Ref environment, therefore DM-7696 created to remove data values from Ref.
# DAE Live will be left unchanged.
# Should the tables be dropped on Live, it's assumed they can be repopulated from HES - no assumption that they can be repopulated from mat_analysis can be made.
# See 01_Prepare/create_merged_providers_view for definition of HESAnnualBirths view.

# COMMAND ----------

# dbutils.widgets.text("outSchema", "mat_analysis", "Target database")
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)

# COMMAND ----------

import pandas as pd
from io import StringIO
from pandas.api.types import is_numeric_dtype, is_bool_dtype
from pyspark import SparkContext
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.annual_hes_input1718(
# MAGIC 	org_code string,
# MAGIC 	org_name string,
# MAGIC 	org_level string,
# MAGIC 	GestationAtBooking_0to4_weeks float,
# MAGIC 	GestationAtBooking_5to9_weeks float,
# MAGIC 	GestationAtBooking_10_weeks float,
# MAGIC 	GestationAtBooking_11_weeks float,
# MAGIC 	GestationAtBooking_12_weeks float,
# MAGIC 	GestationAtBooking_13_weeks float,
# MAGIC 	GestationAtBooking_14_weeks float,
# MAGIC 	GestationAtBooking_15to19_weeks float,
# MAGIC 	GestationAtBooking_20to24_weeks float,
# MAGIC 	GestationAtBooking_25to29_weeks float,
# MAGIC 	GestationAtBooking_30to34_weeks float,
# MAGIC 	GestationAtBooking_35to39_weeks float,
# MAGIC 	GestationAtBooking_40_weeks_or_more float,
# MAGIC 	GestationAtBooking_Unknown float,
# MAGIC 	GestationAtDelivery_22_weeks float,
# MAGIC 	GestationAtDelivery_23to25_weeks float,
# MAGIC 	GestationAtDelivery_26to28_weeks float,
# MAGIC 	GestationAtDelivery_29to31_weeks float,
# MAGIC 	GestationAtDelivery_32to34_weeks float,
# MAGIC 	GestationAtDelivery_35to37_weeks float,
# MAGIC 	GestationAtDelivery_38to40_weeks float,
# MAGIC 	GestationAtDelivery_41to43_weeks float,
# MAGIC 	GestationAtDelivery_44_weeks_or_more float,
# MAGIC 	GestationAtDelivery_Unknown float,
# MAGIC 	OnsetOfLabour_Spontaneous float,
# MAGIC 	OnsetOfLabour_Caesarean_Section float,
# MAGIC 	OnsetOfLabour_Surgical_Induction float,
# MAGIC 	OnsetOfLabour_Medical_Induction float,
# MAGIC 	OnsetOfLabour_Medical_and_Surgical_Induction float,
# MAGIC 	OnsetOfLabour_Unknown float,
# MAGIC 	MethodOfDelivery_Instrumental float,
# MAGIC 	MethodOfDelivery_Spontaneous float,
# MAGIC 	MethodOfDelivery_Elective_Caesarean_Section float,
# MAGIC 	MethodOfDelivery_Emergency_Caesarean_Section float,
# MAGIC 	MethodOfDelivery_Other float,
# MAGIC 	MethodOfDelivery_Unknown float,
# MAGIC 	TotalDeliveries_HES float
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (org_code);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.annual_hes_input1819(
# MAGIC 	org_code string,
# MAGIC 	org_name string,
# MAGIC 	org_level string,
# MAGIC 	GestationAtBooking_0to4_weeks float,
# MAGIC 	GestationAtBooking_5to9_weeks float,
# MAGIC 	GestationAtBooking_10_weeks float,
# MAGIC 	GestationAtBooking_11_weeks float,
# MAGIC 	GestationAtBooking_12_weeks float,
# MAGIC 	GestationAtBooking_13_weeks float,
# MAGIC 	GestationAtBooking_14_weeks float,
# MAGIC 	GestationAtBooking_15to19_weeks float,
# MAGIC 	GestationAtBooking_20to24_weeks float,
# MAGIC 	GestationAtBooking_25to29_weeks float,
# MAGIC 	GestationAtBooking_30to34_weeks float,
# MAGIC 	GestationAtBooking_35to39_weeks float,
# MAGIC 	GestationAtBooking_40_weeks_or_more float,
# MAGIC 	GestationAtBooking_Unknown float,
# MAGIC 	GestationAtDelivery_22_weeks float,
# MAGIC 	GestationAtDelivery_23to25_weeks float,
# MAGIC 	GestationAtDelivery_26to28_weeks float,
# MAGIC 	GestationAtDelivery_29to31_weeks float,
# MAGIC 	GestationAtDelivery_32to34_weeks float,
# MAGIC 	GestationAtDelivery_35to37_weeks float,
# MAGIC 	GestationAtDelivery_38to40_weeks float,
# MAGIC 	GestationAtDelivery_41to43_weeks float,
# MAGIC 	GestationAtDelivery_44_weeks_or_more float,
# MAGIC 	GestationAtDelivery_Unknown float,
# MAGIC 	OnsetOfLabour_Spontaneous float,
# MAGIC 	OnsetOfLabour_Caesarean_Section float,
# MAGIC 	OnsetOfLabour_Surgical_Induction float,
# MAGIC 	OnsetOfLabour_Medical_Induction float,
# MAGIC 	OnsetOfLabour_Medical_and_Surgical_Induction float,
# MAGIC 	OnsetOfLabour_Unknown float,
# MAGIC 	MethodOfDelivery_Instrumental float,
# MAGIC 	MethodOfDelivery_Spontaneous float,
# MAGIC 	MethodOfDelivery_Elective_Caesarean_Section float,
# MAGIC 	MethodOfDelivery_Emergency_Caesarean_Section float,
# MAGIC 	MethodOfDelivery_Other float,
# MAGIC 	MethodOfDelivery_Unknown float,
# MAGIC 	TotalDeliveries_HES float
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (org_code);

# COMMAND ----------

# DBTITLE 1,HES 1819
counts_df = spark.sql("select count(*) as count from `{outSchema}`.annual_hes_input1819".format(outSchema=outSchema))
RowCount = {r['count'] for r in counts_df.collect()}
r = list(RowCount)[0]
print(r)
IsEmpty = (r == 0)
print(RowCount)
print(IsEmpty)

if IsEmpty:
  spark.sql("INSERT INTO `{outSchema}`.annual_hes_input1819 VALUES ('All', 'ENGLAND', 'All Submitters', 1000, 999, 998, 997, 996, 995, 994, 993, 992, 991, 990, 989, 988, 10, 100, 200, 300, 400, 500, 600, 700, 150, 50, 8, 5000, 4000, 200, 150, 120, 80, 6500, 7500, 110, 130, 70, 30, 8000), ('Y56', 'LONDON COMMISSIONING REGION', 'Region', 200, 199, 198, 197, 196, 195, 194, 193, 192, 191, 190, 189, 188, 9, 20, 40, 60, 70, 80, 90, 21, 31, 11, 2, 1001, 801, 41, 31, 25, 17, 1601, 1701, 26, 27, 28, 9, 1601), ('Y55', 'MIDLANDS AND EAST OF ENGLAND COMMISSIONING REGION', 'Region', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('Y54', 'NORTH OF ENGLAND COMMISSIONING REGION', 'Region', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('Y57', 'SOUTH OF ENGLAND COMMISSIONING REGION', 'Region', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RCF', 'AIREDALE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTK', 'ASHFORD AND ST PETER''S HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RF4', 'BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RFF', 'BARNSLEY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('R1H', 'BARTS HEALTH NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RDD', 'BASILDON AND THURROCK UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RC1', 'BEDFORD HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RQ3', 'BIRMINGHAM WOMEN''S AND CHILDREN''S NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXL', 'BLACKPOOL TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RMC', 'BOLTON NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAE', 'BRADFORD TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXH', 'BRIGHTON AND SUSSEX UNIVERSITY HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXQ', 'BUCKINGHAMSHIRE HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJF', 'BURTON HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWY', 'CALDERDALE AND HUDDERSFIELD NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RGT', 'CAMBRIDGE UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RQM', 'CHELSEA AND WESTMINSTER HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RFS', 'CHESTERFIELD ROYAL HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RLN', 'CITY HOSPITALS SUNDERLAND NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RDE', 'COLCHESTER HOSPITAL UNIVERSITY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJR', 'COUNTESS OF CHESTER HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXP', 'COUNTY DURHAM AND DARLINGTON NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJ6', 'CROYDON HEALTH SERVICES NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RN7', 'DARTFORD AND GRAVESHAM NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTG', 'DERBY TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RP5', 'DONCASTER AND BASSETLAW TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBD', 'DORSET COUNTY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWH', 'EAST AND NORTH HERTFORDSHIRE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJN', 'EAST CHESHIRE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RVV', 'EAST KENT HOSPITALS UNIVERSITY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXR', 'EAST LANCASHIRE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXC', 'EAST SUSSEX HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RVR', 'EPSOM AND ST HELIER UNIVERSITY HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RDU', 'FRIMLEY HEALTH NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RR7', 'GATESHEAD HEALTH NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RLT', 'GEORGE ELIOT HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTE', 'GLOUCESTERSHIRE HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RN3', 'GREAT WESTERN HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJ1', 'GUY''S AND ST THOMAS'' NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RN5', 'HAMPSHIRE HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RCD', 'HARROGATE AND DISTRICT NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RR1', 'HEART OF ENGLAND NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RQX', 'HOMERTON UNIVERSITY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWA', 'HULL AND EAST YORKSHIRE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RYJ', 'IMPERIAL COLLEGE HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RGQ', 'IPSWICH HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('R1F', 'ISLE OF WIGHT NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RGP', 'JAMES PAGET UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RNQ', 'KETTERING GENERAL HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJZ', 'KING''S COLLEGE HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAX', 'KINGSTON HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXN', 'LANCASHIRE TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RR8', 'LEEDS TEACHING HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJ2', 'LEWISHAM AND GREENWICH NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('REP', 'LIVERPOOL WOMEN''S NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('R1K', 'LONDON NORTH WEST UNIVERSITY HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RC9', 'LUTON AND DUNSTABLE UNIVERSITY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWF', 'MAIDSTONE AND TUNBRIDGE WELLS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('R0A', 'MANCHESTER UNIVERSITY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RPA', 'MEDWAY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBT', 'MID CHESHIRE HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RQ8', 'MID ESSEX HOSPITAL SERVICES NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXF', 'MID YORKSHIRE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RD8', 'MILTON KEYNES UNIVERSITY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RM1', 'NORFOLK AND NORWICH UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RVJ', 'NORTH BRISTOL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RNL', 'NORTH CUMBRIA UNIVERSITY HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAP', 'NORTH MIDDLESEX UNIVERSITY HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RVW', 'NORTH TEES AND HARTLEPOOL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)".format(outSchema=outSchema))
  spark.sql("INSERT INTO `{outSchema}`.annual_hes_input1819 VALUES ('RGN', 'NORTH WEST ANGLIA NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RNS', 'NORTHAMPTON GENERAL HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBZ', 'NORTHERN DEVON HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJL', 'NORTHERN LINCOLNSHIRE AND GOOLE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTF', 'NORTHUMBRIA HEALTHCARE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RX1', 'NOTTINGHAM UNIVERSITY HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTH', 'OXFORD UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RW6', 'PENNINE ACUTE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RK9', 'PLYMOUTH HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RD3', 'POOLE HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RHU', 'PORTSMOUTH HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RHW', 'ROYAL BERKSHIRE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RT3', 'ROYAL BROMPTON & HAREFIELD NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('REF', 'ROYAL CORNWALL HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RH8', 'ROYAL DEVON AND EXETER NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAL', 'ROYAL FREE LONDON NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RA2', 'ROYAL SURREY COUNTY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RD1', 'ROYAL UNITED HOSPITALS BATH NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RNZ', 'SALISBURY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXK', 'SANDWELL AND WEST BIRMINGHAM HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RHQ', 'SHEFFIELD TEACHING HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RK5', 'SHERWOOD FOREST HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RXW', 'SHREWSBURY AND TELFORD HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTR', 'SOUTH TEES HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RE9', 'SOUTH TYNESIDE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJC', 'SOUTH WARWICKSHIRE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAJ', 'SOUTHEND UNIVERSITY HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RVY', 'SOUTHPORT AND ORMSKIRK HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJ7', 'ST GEORGE''S UNIVERSITY HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBN', 'ST HELENS AND KNOWSLEY HOSPITAL SERVICES NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWJ', 'STOCKPORT NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTP', 'SURREY AND SUSSEX HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RMP', 'TAMESIDE AND GLOSSOP INTEGRATED CARE NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBA', 'TAUNTON AND SOMERSET NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RNA', 'THE DUDLEY GROUP NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RAS', 'THE HILLINGDON HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTD', 'THE NEWCASTLE UPON TYNE HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RQW', 'THE PRINCESS ALEXANDRA HOSPITAL NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RCX', 'THE QUEEN ELIZABETH HOSPITAL, KING''S LYNN, NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RFR', 'THE ROTHERHAM NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RDZ', 'THE ROYAL BOURNEMOUTH AND CHRISTCHURCH HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RL4', 'THE ROYAL WOLVERHAMPTON NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RA9', 'TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWD', 'UNITED LINCOLNSHIRE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RRV', 'UNIVERSITY COLLEGE LONDON HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RHM', 'UNIVERSITY HOSPITAL SOUTHAMPTON NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RRK', 'UNIVERSITY HOSPITALS BIRMINGHAM NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RA7', 'UNIVERSITY HOSPITALS BRISTOL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RKB', 'UNIVERSITY HOSPITALS COVENTRY AND WARWICKSHIRE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWE', 'UNIVERSITY HOSPITALS OF LEICESTER NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RTX', 'UNIVERSITY HOSPITALS OF MORECAMBE BAY NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RJE', 'UNIVERSITY HOSPITALS OF NORTH MIDLANDS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBK', 'WALSALL HEALTHCARE NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWW', 'WARRINGTON AND HALTON HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWG', 'WEST HERTFORDSHIRE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RGR', 'WEST SUFFOLK NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RYR', 'WESTERN SUSSEX HOSPITALS NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RA3', 'WESTON AREA HEALTH NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RKE', 'WHITTINGTON HEALTH NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RBL', 'WIRRAL UNIVERSITY TEACHING HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RWP', 'WORCESTERSHIRE ACUTE HOSPITALS NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RRF', 'WRIGHTINGTON, WIGAN AND LEIGH NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RLQ', 'WYE VALLEY NHS TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RA4', 'YEOVIL DISTRICT HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ('RCB', 'YORK TEACHING HOSPITAL NHS FOUNDATION TRUST', 'Provider', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)".format(outSchema=outSchema))


# COMMAND ----------

# DBTITLE 1,HES 2122
# MAGIC %python
# MAGIC hes2122_df = pd.read_csv(StringIO("""org_code,org_name,org_level,GestationAtBooking_0to4_weeks,GestationAtBooking_5to9_weeks,GestationAtBooking_10_weeks,GestationAtBooking_11_weeks,GestationAtBooking_12_weeks,GestationAtBooking_13_weeks,GestationAtBooking_14_weeks,GestationAtBooking_15to19_weeks,GestationAtBooking_20to24_weeks,GestationAtBooking_25to29_weeks,GestationAtBooking_30to34_weeks,GestationAtBooking_35to39_weeks,GestationAtBooking_40_weeks_or_more,GestationAtBooking_Unknown,GestationAtDelivery_22_weeks,GestationAtDelivery_23to25_weeks,GestationAtDelivery_26to28_weeks,GestationAtDelivery_29to31_weeks,GestationAtDelivery_32to34_weeks,GestationAtDelivery_35to37_weeks,GestationAtDelivery_38to40_weeks,GestationAtDelivery_41to43_weeks,GestationAtDelivery_44_weeks_or_more,GestationAtDelivery_Unknown,OnsetOfLabour_Spontaneous,OnsetOfLabour_Caesarean_Section,OnsetOfLabour_Surgical_Induction,OnsetOfLabour_Medical_Induction,OnsetOfLabour_Medical_and_Surgical_Induction,OnsetOfLabour_Unknown,MethodOfDelivery_Caesarean,MethodOfDelivery_Instrumental,MethodOfDelivery_Spontaneous,MethodOfDelivery_Elective_Caesarean_Section,MethodOfDelivery_Emergency_Caesarean_Section,MethodOfDelivery_Breech_Extraction,MethodOfDelivery_Forceps_Low,MethodOfDelivery_Forceps_Other,MethodOfDelivery_Ventouse,MethodOfDelivery_Breech_Other,MethodOfDelivery_Spontaneous_Vertex,MethodOfDelivery_Spontaneous_Other,MethodOfDelivery_Other,MethodOfDelivery_Unknown,TotalDeliveries_HES
# MAGIC All,England,All Submitters,1585,176388,61389,33355,18662,10242,5241,11725,5957,6107,6211,14030,6460,220991,5442,993,1631,2625,7375,54866,302012,61330,50,142238,202384,87627,27088,87032,31067,143364,203764,67800,294174,89905,113859,116,19209,21514,26961,1543,290547,2084,18,12806,578562
# MAGIC Y56,LONDON COMMISSIONING REGION,Region,89,31038,12416,5709,3582,2299,1438,3476,1744,1694,1762,1681,172,47965,1959,214,311,462,1252,9807,52358,9835,4,39025,33472,13820,3037,13760,1898,49240,42731,14691,54611,17444,25287,23,4314,2635,7719,278,54160,173,1,3193,115227
# MAGIC Y58,SOUTH WEST COMMISSIONING REGION,Region,87,19323,7056,3538,1743,728,343,833,400,458,485,491,111,15350,44,88,117,265,744,5938,33943,8055,5,1752,20962,7511,2523,8952,3713,7290,17312,6294,25981,7667,9645,13,1614,2330,2337,108,25413,460,7,1357,50951
# MAGIC Y59,SOUTH EAST COMMISSIONING REGION,Region,68,21249,10161,6084,3037,1398,668,1376,756,963,948,1183,313,38526,305,126,188,304,872,5929,38155,9546,3,31320,34302,14009,4474,9843,4495,19625,31624,10661,43285,14231,17393,17,3047,3498,4099,218,42854,213,3,1175,86748
# MAGIC Y60,MIDLANDS COMMISSIONING REGION,Region,162,37277,12477,7576,4710,2854,1327,2741,1318,1201,1066,961,132,30181,111,211,363,604,1655,12019,64802,11900,30,12290,34979,16671,2707,20855,6437,22336,37197,11349,54561,15788,21409,13,3130,3915,4291,331,53690,540,2,876,103985
# MAGIC Y61,EAST OF ENGLAND COMMISSIONING REGION,Region,162,23316,7912,4424,2442,1290,584,1274,628,675,729,676,146,21859,46,87,164,218,726,6007,33974,7264,4,17652,24185,9864,2734,8889,4371,16099,23675,7319,33824,10651,13024,12,2566,2078,2663,171,33514,139,0,1324,66142
# MAGIC Y62,NORTH WEST COMMISSIONING REGION,Region,43,12377,4344,2561,1416,759,382,808,378,354,341,575,228,47018,47,86,172,284,702,5560,28763,5746,1,30227,24319,11292,4744,11990,5148,14095,24519,8254,34835,11692,12827,21,1860,3280,3093,205,34379,251,2,3978,71588
# MAGIC Y63,NORTH EAST AND YORKSHIRE COMMISSIONING REGION,Region,974,31808,7023,3463,1732,914,499,1217,733,762,880,8463,5358,20092,2930,181,316,488,1424,9606,50017,8984,3,9972,30165,14460,6869,12743,5005,14679,26706,9232,47077,12432,14274,17,2678,3778,2759,232,46537,308,3,903,83921
# MAGIC R0A,MANCHESTER UNIVERSITY NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,15924,0,0,0,0,0,0,0,0,0,15924,6425,1641,2650,1083,767,3358,4805,1569,6239,2680,2125,9,174,787,599,48,6184,7,0,3311,15924
# MAGIC R0B,SOUTH TYNESIDE AND SUNDERLAND NHS FOUNDATION TRUST,Provider,7,2036,460,233,109,59,28,78,51,42,24,32,1,481,3,8,16,33,78,618,2504,374,0,7,2,1,0,0,0,3638,949,396,2271,502,447,0,161,104,131,21,2247,3,0,25,3641
# MAGIC R0D,UNIVERSITY HOSPITALS DORSET NHS FOUNDATION TRUST,Provider,3,1408,800,538,446,212,71,116,43,48,51,41,12,284,4,3,9,22,43,463,2861,608,0,60,1600,909,275,796,318,175,1425,389,2047,630,795,0,79,151,159,5,2019,23,0,212,4073
# MAGIC R1F,ISLE OF WIGHT NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,964,0,0,0,0,0,0,0,0,0,964,251,123,13,185,19,373,351,101,504,152,199,0,10,56,35,0,501,3,0,8,964
# MAGIC R1H,BARTS HEALTH NHS TRUST,Provider,28,5720,2447,1301,832,620,349,808,364,263,264,329,40,1016,6,27,49,80,269,2529,9585,1821,0,164,1528,0,0,0,0,13002,4631,1771,7901,1697,2934,7,748,102,914,38,7844,19,0,227,14530
# MAGIC R1K,LONDON NORTH WEST UNIVERSITY HEALTHCARE NHS TRUST,Provider,1,1251,447,229,183,124,84,232,97,68,59,57,9,1098,0,1,1,2,26,425,2164,324,0,996,1604,659,220,188,272,996,1533,453,1929,604,929,0,227,5,221,7,1922,0,0,24,3939
# MAGIC RA2,ROYAL SURREY COUNTY HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,2867,0,0,0,0,0,0,0,0,0,2867,0,0,0,0,0,2867,1119,369,1379,556,563,0,91,112,166,3,1368,8,0,0,2867
# MAGIC RA4,YEOVIL DISTRICT HOSPITAL NHS FOUNDATION TRUST,Provider,1,565,313,151,32,26,10,30,14,22,9,20,10,36,0,0,1,1,22,139,903,173,0,0,444,266,103,330,96,0,433,170,635,188,245,1,90,19,60,1,633,1,0,1,1239
# MAGIC RA7,UNIVERSITY HOSPITALS BRISTOL AND WESTON NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4866,2,17,17,39,74,554,3255,831,0,77,2394,1058,219,844,274,77,1791,793,2239,757,1034,0,207,265,321,8,2205,26,0,43,4866
# MAGIC RA9,TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST,Provider,11,1421,235,81,32,19,12,22,10,18,16,16,4,76,0,3,2,5,15,255,1470,218,0,5,713,468,122,503,118,49,748,206,1018,344,404,1,73,63,69,8,989,21,0,1,1973
# MAGIC RAE,BRADFORD TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,5,17,7,4,0,0,0,1,0,2,0,0,0,4685,2841,51,54,40,38,38,33,3,0,1623,2404,722,201,862,428,104,1277,365,3047,475,802,2,119,158,86,19,3004,24,0,32,4721
# MAGIC RAJ,MID AND SOUTH ESSEX NHS FOUNDATION TRUST,Provider,4,3162,2227,1614,967,467,214,333,140,123,140,162,39,2407,11,6,27,48,139,1144,7200,1648,3,1773,4865,2388,189,2743,251,1563,4509,1112,6285,1909,2600,3,517,116,476,27,6219,39,0,93,11999
# MAGIC RAL,ROYAL FREE LONDON NHS FOUNDATION TRUST,Provider,3,1600,732,232,156,104,61,180,84,45,75,51,5,4615,1,5,7,21,54,427,2577,448,0,4403,0,0,0,0,0,7943,3254,784,3869,1285,1969,0,251,72,461,9,3848,12,0,36,7943
# MAGIC RAP,NORTH MIDDLESEX UNIVERSITY HOSPITAL NHS TRUST,Provider,1,403,161,105,85,47,27,104,50,43,35,49,9,2819,0,0,0,3,27,176,849,134,0,2749,534,273,11,369,10,2741,1566,304,2060,603,963,0,140,4,160,3,2057,0,0,8,3938
# MAGIC RAS,THE HILLINGDON HOSPITALS NHS FOUNDATION TRUST,Provider,2,1750,821,345,193,111,80,216,101,63,81,84,3,138,1,6,9,21,62,425,2918,507,0,39,1964,819,134,772,223,76,1494,625,1851,576,918,0,126,146,353,5,1844,2,0,18,3988
# MAGIC RAX,KINGSTON HOSPITAL NHS FOUNDATION TRUST,Provider,4,2606,640,333,208,89,52,114,78,98,101,74,7,412,3,6,6,22,68,525,3450,675,0,61,2177,1250,286,723,320,60,1824,708,2261,879,945,1,179,101,427,13,2233,15,0,23,4816
# MAGIC RBD,DORSET COUNTY HOSPITAL NHS FOUNDATION TRUST,Provider,0,830,239,112,55,18,7,29,21,22,28,29,7,25,0,2,2,4,17,199,996,202,0,0,549,365,110,121,240,37,580,122,708,276,304,0,9,49,64,2,705,1,0,12,1422
# MAGIC RBK,WALSALL HEALTHCARE NHS TRUST,Provider,5,1642,347,265,313,329,141,198,78,48,51,35,2,224,21,10,15,32,76,610,2542,333,0,39,1397,885,0,1342,0,54,1234,286,2057,516,718,0,104,51,131,8,2001,48,1,100,3678
# MAGIC RBL,WIRRAL UNIVERSITY TEACHING HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,2974,5,14,22,25,32,363,2026,427,0,60,1200,358,217,820,3,376,1023,364,1569,423,600,1,203,28,132,11,1555,3,0,18,2974
# MAGIC RBN,ST HELENS AND KNOWSLEY TEACHING HOSPITALS NHS TRUST,Provider,8,1833,654,393,219,120,68,135,65,80,73,81,27,130,2,2,14,18,56,505,2796,462,0,31,1533,831,47,1350,75,50,1461,390,2015,676,785,0,77,137,176,4,1995,16,0,20,3886
# MAGIC RBT,MID CHESHIRE HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3078,6,2,6,27,55,441,2035,453,0,53,0,0,0,0,0,3078,925,361,1747,555,370,0,92,139,130,14,1733,0,0,45,3078
# MAGIC RBZ,NORTHERN DEVON HEALTHCARE NHS TRUST,Provider,2,720,236,112,34,10,11,34,7,8,14,13,3,54,0,1,1,3,20,166,864,200,0,3,520,276,75,257,127,3,455,154,642,176,279,0,33,42,79,1,591,50,0,7,1258
# MAGIC RC9,BEDFORDSHIRE HOSPITALS NHS FOUNDATION TRUST,Provider,73,3949,914,637,458,279,147,339,143,160,146,113,16,808,10,10,34,25,96,1045,5508,1000,0,458,3857,1535,122,249,2392,31,2965,974,3575,1433,1532,2,248,388,336,28,3546,1,0,672,8186
# MAGIC RCB,YORK TEACHING HOSPITAL NHS FOUNDATION TRUST,Provider,16,3022,450,151,80,43,19,69,41,33,35,23,1,368,44,6,9,24,89,525,3018,529,0,107,1569,250,151,711,736,934,1246,485,2220,550,696,1,104,202,178,7,2205,8,1,399,4351
# MAGIC RCD,HARROGATE AND DISTRICT NHS FOUNDATION TRUST,Provider,2,974,275,130,63,26,7,40,17,34,27,44,3,107,0,0,3,7,32,157,1286,251,0,13,758,326,71,506,76,12,565,255,910,301,264,1,0,205,49,1,908,1,1,18,1749
# MAGIC RCF,AIREDALE NHS FOUNDATION TRUST,Provider,3,1027,350,176,64,29,11,33,17,19,22,29,12,66,6,0,3,3,30,200,1340,245,1,30,683,387,86,463,209,30,587,181,1078,299,288,1,105,4,71,6,1072,0,0,12,1858
# MAGIC RCX,"THE QUEEN ELIZABETH HOSPITAL, KING'S LYNN, NHS FOUNDATION TRUST",Provider,7,1041,392,196,106,51,18,32,28,21,18,23,14,14,2,0,3,9,28,267,1400,272,0,0,1014,388,2,577,0,0,597,153,1225,235,362,0,63,53,37,2,1221,2,0,6,1981
# MAGIC RD1,ROYAL UNITED HOSPITALS BATH NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4402,0,1,8,21,47,329,2709,1134,0,153,3099,49,275,783,154,42,1466,630,2223,647,819,1,213,206,210,8,2213,2,0,83,4402
# MAGIC RD8,MILTON KEYNES UNIVERSITY HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3588,0,0,0,0,0,0,0,0,0,3588,0,0,0,0,0,3588,1322,441,1810,649,673,1,100,170,170,3,1804,3,0,15,3588
# MAGIC RDE,EAST SUFFOLK AND NORTH ESSEX NHS FOUNDATION TRUST,Provider,16,3909,752,361,170,109,35,130,75,91,86,127,42,858,4,10,13,28,125,846,4383,901,0,451,2690,1424,123,1672,394,458,2225,811,3623,929,1296,0,463,69,279,16,3607,0,0,102,6761
# MAGIC RDU,FRIMLEY HEALTH NHS FOUNDATION TRUST,Provider,6,3545,2051,1190,561,256,126,267,133,174,169,166,12,482,7,12,16,46,102,909,6953,1014,1,78,3777,1877,494,2021,896,73,3312,1323,4490,1470,1842,2,327,375,619,12,4476,2,0,13,9138
# MAGIC REF,ROYAL CORNWALL HOSPITALS NHS TRUST,Provider,3,2040,656,271,116,40,14,56,24,39,33,31,7,704,2,2,7,18,63,459,2261,618,0,604,1568,567,188,635,464,612,984,489,2273,499,485,6,125,143,215,15,2096,162,0,288,4034
# MAGIC REM,LIVERPOOL UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,1,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1
# MAGIC REP,LIVERPOOL WOMEN'S NHS FOUNDATION TRUST,Provider,2,1359,275,186,102,59,31,43,14,6,0,0,0,5387,4,36,38,56,156,1034,5230,869,0,41,2456,1848,44,2745,5,366,2770,1014,3652,1313,1457,3,15,622,374,34,3497,121,0,28,7464
# MAGIC RF4,"BARKING, HAVERING AND REDBRIDGE UNIVERSITY HOSPITALS NHS TRUST",Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,7205,0,0,0,0,0,0,0,0,0,7205,3254,0,0,1886,0,2065,2556,754,3820,976,1580,3,128,240,383,27,3793,0,0,75,7205
# MAGIC RFF,BARNSLEY HOSPITAL NHS FOUNDATION TRUST,Provider,7,1610,333,200,90,53,26,71,35,42,20,35,18,246,5,2,7,19,61,335,1917,337,0,103,1123,533,239,504,224,163,912,349,1493,403,509,0,99,128,122,2,1491,0,0,32,2786
# MAGIC RFR,THE ROTHERHAM NHS FOUNDATION TRUST,Provider,14,1447,316,134,71,39,24,41,27,19,18,7,4,293,4,3,8,9,42,338,1739,249,0,62,864,540,0,914,0,136,790,273,1338,344,446,0,59,74,140,5,1200,133,0,53,2454
# MAGIC RFS,CHESTERFIELD ROYAL HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,2774,2,0,12,14,39,337,1979,322,0,69,0,0,0,0,0,2774,956,302,1508,395,561,0,20,181,101,9,1498,1,0,8,2774
# MAGIC RGM,ROYAL PAPWORTH HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,3,3,0,0,2,1,0,0,0,0,0,0,0,0,0,3
# MAGIC RGN,NORTH WEST ANGLIA NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,6209,0,0,0,0,0,0,0,0,0,6209,0,0,0,0,0,6209,2248,624,3323,966,1282,2,3,382,237,19,3300,4,0,14,6209
# MAGIC RGP,JAMES PAGET UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,1732,0,0,0,0,0,0,0,0,0,1732,0,0,0,0,0,1732,654,149,929,297,357,0,62,15,72,3,916,10,0,0,1732
# MAGIC RGR,WEST SUFFOLK NHS FOUNDATION TRUST,Provider,4,894,412,140,59,35,14,41,16,24,19,15,6,512,0,3,2,2,11,181,1205,399,0,388,957,94,88,580,86,386,664,273,1241,286,378,0,149,40,84,6,1232,3,0,13,2191
# MAGIC RGT,CAMBRIDGE UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,7,2070,367,191,103,56,22,61,45,51,69,38,1,2358,5,24,33,34,98,646,3642,858,0,99,2351,1166,212,1118,499,93,1840,697,2840,806,1034,1,157,304,235,28,2811,1,0,62,5439
# MAGIC RH5,SOMERSET NHS FOUNDATION TRUST,Provider,4,1728,511,221,88,34,38,47,26,32,41,39,9,153,4,2,6,17,59,336,1928,506,1,112,1340,590,109,606,198,128,915,302,1489,480,435,1,1,216,84,11,1476,2,0,265,2971
# MAGIC RH8,ROYAL DEVON AND EXETER NHS FOUNDATION TRUST,Provider,0,164,68,30,11,5,3,83,12,17,21,42,9,3489,4,4,6,21,49,461,2693,657,2,57,31,0,2,18,0,3903,1109,438,2151,557,552,0,157,127,154,3,2147,1,0,256,3954
# MAGIC RHM,UNIVERSITY HOSPITAL SOUTHAMPTON NHS FOUNDATION TRUST,Provider,0,285,339,243,78,21,12,20,20,25,18,15,1,4141,8,14,29,27,101,533,3362,846,0,298,2599,1116,48,1026,131,298,1814,676,2693,727,1087,3,130,271,272,22,2648,23,0,35,5218
# MAGIC RHQ,SHEFFIELD TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,14,58,142,3161,2318,172,4,18,34,56,107,645,3954,941,1,106,3124,1299,272,833,233,105,2195,645,2993,936,1259,0,85,284,276,7,2984,2,0,33,5866
# MAGIC RHU,PORTSMOUTH HOSPITALS UNIVERSITY NATIONAL HEALTH SERVICE TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4969,0,0,0,0,0,0,0,0,0,4969,2180,1355,819,123,356,136,1811,488,2670,816,995,3,157,96,232,22,2638,10,0,0,4969
# MAGIC RHW,ROYAL BERKSHIRE NHS FOUNDATION TRUST,Provider,10,2420,1112,364,143,62,37,104,51,77,71,83,45,57,2,5,9,21,55,387,2867,1276,0,14,2658,944,45,276,0,713,1760,699,2135,722,1038,0,405,92,202,8,2124,3,0,42,4636
# MAGIC RJ1,GUY'S AND ST THOMAS' NHS FOUNDATION TRUST,Provider,2,2264,807,514,331,240,152,372,176,208,212,224,14,823,9,34,35,50,91,774,4394,847,0,105,2690,2046,0,1498,0,105,2764,892,2652,1104,1660,1,313,146,432,23,2592,37,0,31,6339
# MAGIC RJ2,LEWISHAM AND GREENWICH NHS TRUST,Provider,14,4025,781,345,254,149,93,225,135,125,121,115,9,1139,3,10,12,47,110,761,4577,1062,1,947,3403,1752,355,1303,295,422,2881,920,3396,952,1929,1,407,111,401,17,3379,0,0,333,7530
# MAGIC RJ6,CROYDON HEALTH SERVICES NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3019,1925,34,41,36,42,49,17,3,0,872,1334,605,51,612,17,400,1106,407,1469,428,678,1,154,6,246,4,1465,0,0,37,3019
# MAGIC RJ7,ST GEORGE'S UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,4,2174,729,355,237,139,82,207,100,103,124,80,14,214,0,20,22,29,64,474,3189,693,2,69,2024,825,0,1641,0,72,1290,623,2590,653,637,4,294,18,307,25,2564,1,0,59,4562
# MAGIC RJC,SOUTH WARWICKSHIRE NHS FOUNDATION TRUST,Provider,7,1101,488,518,319,111,37,114,74,74,46,43,5,200,4,1,3,9,40,408,2248,416,0,8,1414,735,0,902,0,86,1143,332,1650,516,627,0,156,65,111,1,1607,42,0,12,3137
# MAGIC RJE,UNIVERSITY HOSPITALS OF NORTH MIDLANDS NHS TRUST,Provider,24,3370,970,585,245,115,57,170,86,101,73,54,2,162,12,20,26,56,113,1024,3974,740,0,49,1813,0,0,1959,0,2242,2179,533,3220,899,1280,0,23,271,239,27,3127,66,0,82,6014
# MAGIC RJL,NORTHERN LINCOLNSHIRE AND GOOLE NHS FOUNDATION TRUST,Provider,8,2346,468,238,105,44,27,83,39,46,50,61,11,136,1,5,26,32,89,586,2443,456,0,24,1532,618,1488,0,0,24,1079,325,2240,547,532,0,158,9,158,13,2226,1,0,18,3662
# MAGIC RJN,EAST CHESHIRE NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,1,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1
# MAGIC RJR,COUNTESS OF CHESTER HOSPITAL NHS FOUNDATION TRUST,Provider,0,427,117,68,30,8,6,14,17,9,5,15,0,1607,1,0,2,5,13,125,522,124,0,1532,686,183,31,449,126,849,903,249,1132,407,496,1,44,120,84,1,1129,2,0,40,2324
# MAGIC RJZ,KING'S COLLEGE HOSPITAL NHS FOUNDATION TRUST,Provider,1,1200,238,115,72,43,36,61,45,50,42,57,5,5903,5,11,14,23,46,297,1680,392,0,5400,1404,0,0,59,0,6405,3028,1115,3725,1318,1710,1,311,208,595,25,3680,20,0,0,7868
# MAGIC RK5,SHERWOOD FOREST HOSPITALS NHS FOUNDATION TRUST,Provider,4,202,68,35,19,9,4,14,10,3,2,4,0,2990,1,6,10,18,77,355,2395,444,0,58,1625,560,123,658,332,66,1114,281,1962,569,545,1,3,188,89,8,1897,57,0,7,3364
# MAGIC RK9,UNIVERSITY HOSPITALS PLYMOUTH NHS TRUST,Provider,17,1801,787,405,189,91,37,71,50,38,51,42,8,152,19,20,14,18,74,419,2711,423,0,46,1511,733,113,1041,243,103,1375,428,1882,520,855,0,180,62,186,6,1869,7,0,59,3744
# MAGIC RKB,UNIVERSITY HOSPITALS COVENTRY AND WARWICKSHIRE NHS TRUST,Provider,25,2079,706,437,263,156,64,189,84,82,60,57,3,1277,5,23,35,55,112,976,3826,447,1,3,1759,1934,572,493,429,296,1928,447,3105,758,1170,1,275,8,163,35,3067,3,0,3,5483
# MAGIC RKE,WHITTINGTON HEALTH NHS TRUST,Provider,9,1860,366,177,151,85,39,145,72,63,94,99,7,316,1,4,7,10,54,353,2363,524,0,167,1644,708,90,790,52,199,1318,567,1590,583,735,0,221,98,248,4,1586,0,0,8,3483
# MAGIC RL4,THE ROYAL WOLVERHAMPTON NHS TRUST,Provider,15,2716,570,330,272,152,73,176,97,99,103,101,7,200,8,27,25,30,93,607,3491,584,0,46,2209,1063,0,1593,0,46,1672,546,2681,659,1013,3,147,80,316,11,2636,34,0,12,4911
# MAGIC RLQ,WYE VALLEY NHS TRUST,Provider,0,548,196,90,51,20,15,60,32,27,22,24,3,583,2,1,4,5,24,176,798,147,0,514,609,498,0,475,0,89,670,229,745,294,376,0,87,31,111,4,732,9,0,27,1671
# MAGIC RLT,GEORGE ELIOT HOSPITAL NHS TRUST,Provider,2,867,276,168,187,208,55,55,28,30,28,29,10,82,0,2,4,10,18,261,1434,243,2,51,726,448,6,720,74,51,689,240,1095,309,380,0,63,89,88,2,1091,2,0,1,2025
# MAGIC RM1,NORFOLK AND NORWICH UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,8,2809,917,388,174,66,29,83,52,67,70,37,2,253,6,26,34,31,89,668,3369,652,0,81,2186,1177,310,676,530,77,1964,544,2374,857,1107,1,251,109,183,14,2353,7,0,74,4956
# MAGIC RM3,SALFORD ROYAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,13,0,0,0,0,0,0,0,0,0,13,0,0,0,0,0,13,7,2,4,2,5,0,1,1,0,0,4,0,0,0,13
# MAGIC RMC,BOLTON NHS FOUNDATION TRUST,Provider,2,964,598,413,208,118,58,108,42,50,36,45,15,3092,5,6,20,16,45,250,1747,620,0,3040,1297,392,125,557,340,3038,2008,701,2934,861,1147,0,54,366,281,21,2911,2,0,106,5749
# MAGIC RMP,TAMESIDE AND GLOSSOP INTEGRATED CARE NHS FOUNDATION TRUST,Provider,0,1135,350,191,107,71,21,76,43,40,37,34,0,53,2,1,8,16,40,317,1537,231,0,6,781,446,97,575,224,35,758,234,1156,356,402,0,0,131,103,5,1150,1,0,10,2158
# MAGIC RN3,GREAT WESTERN HOSPITALS NHS FOUNDATION TRUST,Provider,5,2076,630,359,151,82,35,96,57,49,62,60,8,71,1,4,3,33,63,504,2655,438,0,40,1547,818,226,915,182,53,1428,340,1945,553,875,0,0,247,93,5,1940,0,0,28,3741
# MAGIC RN5,HAMPSHIRE HOSPITALS NHS FOUNDATION TRUST,Provider,2,304,132,51,20,9,3,10,9,13,13,7,1,4416,0,0,1,3,7,70,382,120,1,4406,318,105,20,114,0,4433,1817,576,2469,895,922,3,291,72,210,8,2455,6,0,128,4990
# MAGIC RN7,DARTFORD AND GRAVESHAM NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4718,0,0,0,0,0,0,0,0,0,4718,1969,1141,373,638,387,210,1790,395,2506,871,919,0,10,203,182,11,2488,7,0,27,4718
# MAGIC RNA,THE DUDLEY GROUP NHS FOUNDATION TRUST,Provider,9,2223,618,346,224,98,69,119,49,52,36,20,2,159,1,4,12,14,50,367,2835,706,0,36,1417,1003,133,230,359,883,1451,366,2195,659,792,0,179,87,100,6,2188,1,0,13,4025
# MAGIC RNN,NORTH CUMBRIA INTEGRATED CARE NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,2625,0,0,0,0,0,0,0,0,0,2625,0,0,0,0,0,2625,938,305,1382,376,562,0,129,74,102,6,1352,24,0,0,2625
# MAGIC RNQ,KETTERING GENERAL HOSPITAL NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3274,4,5,6,19,58,388,2271,504,0,19,0,0,0,0,0,3274,1430,302,1530,756,674,0,71,128,103,1,1525,4,0,12,3274
# MAGIC RNS,NORTHAMPTON GENERAL HOSPITAL NHS TRUST,Provider,4,2005,705,393,194,97,57,121,67,55,44,57,10,287,2,5,16,18,58,462,2854,470,1,210,1811,905,83,814,253,230,1620,462,2005,676,944,0,45,249,168,7,1981,17,0,9,4096
# MAGIC RNZ,SALISBURY NHS FOUNDATION TRUST,Provider,7,1489,213,78,40,15,11,46,25,42,39,38,9,32,0,3,3,9,47,304,1386,329,0,3,928,337,201,233,298,87,615,272,1187,275,340,0,81,120,71,4,1163,20,0,10,2084
# MAGIC RP5,DONCASTER AND BASSETLAW TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,18,2301,691,387,197,102,58,108,52,50,43,39,16,272,0,2,12,20,78,617,3054,525,0,26,1711,570,367,236,1000,450,1605,363,2366,590,1015,3,5,160,195,3,2327,36,0,0,4334
# MAGIC RPA,MEDWAY NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4523,0,0,0,0,0,0,0,0,0,4523,2325,1013,0,0,1165,20,1849,364,2298,715,1134,1,132,103,128,12,2286,0,0,12,4523
# MAGIC RQ3,BIRMINGHAM WOMEN'S AND CHILDREN'S NHS FOUNDATION TRUST,Provider,7,1054,819,1012,902,584,233,254,59,34,66,26,1,1912,0,20,48,50,123,791,4203,666,0,1062,2446,1663,0,1688,0,1166,2551,975,3168,1050,1501,0,482,135,358,41,3126,1,0,269,6963
# MAGIC RQM,CHELSEA AND WESTMINSTER HOSPITAL NHS FOUNDATION TRUST,Provider,5,1876,706,323,171,118,83,187,88,75,77,135,29,8625,0,1,4,10,71,518,2930,586,0,8378,2132,936,1325,0,0,8105,4309,1514,4707,1891,2418,3,22,675,814,15,4689,3,0,1968,12498
# MAGIC RQW,THE PRINCESS ALEXANDRA HOSPITAL NHS TRUST,Provider,14,1952,674,354,180,101,42,84,45,51,64,59,9,136,0,4,10,24,62,438,2589,595,0,43,2758,0,0,968,0,39,1416,328,1924,686,730,1,153,86,88,1,1923,0,0,97,3765
# MAGIC RQX,HOMERTON UNIVERSITY HOSPITAL NHS FOUNDATION TRUST,Provider,4,1773,1285,448,295,216,148,303,150,115,126,133,16,543,3,18,40,27,64,563,3900,852,1,88,3035,1124,229,390,690,88,1927,836,2640,715,1212,0,135,272,429,32,2601,7,0,153,5556
# MAGIC RR7,GATESHEAD HEALTH NHS FOUNDATION TRUST,Provider,8,1158,227,85,55,31,14,40,23,20,22,13,2,107,1,4,4,2,28,228,1347,191,0,0,590,422,0,793,0,0,533,271,992,303,230,0,141,80,50,3,985,4,0,9,1805
# MAGIC RR8,LEEDS TEACHING HOSPITALS NHS TRUST,Provider,0,0,0,0,0,0,0,1,16,56,162,4774,2934,461,6,20,31,46,140,1017,5859,1059,0,226,3387,1768,254,1597,11,1387,2506,1228,4664,1301,1205,3,181,733,311,27,4628,9,0,6,8404
# MAGIC RRF,"WRIGHTINGTON, WIGAN AND LEIGH NHS FOUNDATION TRUST",Provider,13,754,189,117,54,26,17,33,18,9,18,21,11,744,1,0,1,6,12,198,982,166,0,659,920,422,195,206,261,21,660,238,1109,312,348,0,61,68,109,4,1040,65,0,18,2025
# MAGIC RRK,UNIVERSITY HOSPITALS BIRMINGHAM NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,8433,0,0,0,0,0,0,0,0,0,8433,0,0,0,0,0,8433,3239,852,4342,1221,2018,0,308,79,465,24,4278,40,0,0,8433
# MAGIC RRV,UNIVERSITY COLLEGE LONDON HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,5080,0,0,0,0,0,0,0,0,0,5080,0,0,0,0,0,5080,2191,811,2073,948,1243,0,161,285,365,9,2035,29,0,5,5080
# MAGIC RTD,THE NEWCASTLE UPON TYNE HOSPITALS NHS FOUNDATION TRUST,Provider,19,3590,874,406,190,88,64,137,90,84,60,50,1,282,1,17,25,40,124,992,3967,725,0,44,2032,1305,736,1095,705,62,1986,738,3204,1013,973,2,112,462,162,18,3183,3,0,7,5935
# MAGIC RTE,GLOUCESTERSHIRE HOSPITALS NHS FOUNDATION TRUST,Provider,31,2791,840,491,224,80,52,94,42,42,51,63,6,874,6,1,8,16,72,650,3442,948,2,536,2593,1075,245,946,286,536,1848,949,2884,864,984,3,131,438,377,26,2849,9,0,0,5681
# MAGIC RTF,NORTHUMBRIA HEALTHCARE NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,3151,0,0,0,0,0,0,0,0,0,3151,1010,630,0,1488,0,23,1002,443,1706,540,462,0,251,65,127,4,1701,1,0,0,3151
# MAGIC RTG,UNIVERSITY HOSPITALS OF DERBY AND BURTON NHS FOUNDATION TRUST,Provider,20,4855,1478,729,338,169,75,198,116,114,105,126,30,441,8,10,24,61,175,1093,6142,1071,1,209,3263,2043,397,1715,1167,209,3236,978,4442,1487,1749,1,77,504,396,27,4413,2,0,138,8794
# MAGIC RTH,OXFORD UNIVERSITY HOSPITALS NHS FOUNDATION TRUST,Provider,16,2791,1435,997,559,284,126,256,133,156,132,83,5,547,7,39,51,49,114,808,4703,1636,0,127,4145,627,707,223,0,1832,2428,1161,3904,949,1479,1,432,453,275,24,3857,23,0,41,7534
# MAGIC RTK,ASHFORD AND ST PETER'S HOSPITALS NHS FOUNDATION TRUST,Provider,1,1290,643,441,254,154,84,107,55,51,52,41,5,114,4,30,27,29,51,395,2231,489,0,36,1547,715,161,485,383,1,1189,485,1613,497,692,0,161,88,236,30,1540,43,0,5,3292
# MAGIC RTP,SURREY AND SUSSEX HEALTHCARE NHS TRUST,Provider,1,1276,1154,923,461,168,70,137,64,97,98,78,6,737,0,6,10,24,95,614,3270,685,0,566,2174,0,213,1002,268,1613,1754,561,2384,775,979,0,5,288,268,8,2366,10,0,571,5270
# MAGIC RTR,SOUTH TEES HOSPITALS NHS FOUNDATION TRUST,Provider,13,1696,509,275,126,72,38,105,59,47,50,49,11,1580,3,15,24,25,74,359,2303,274,0,1553,1621,808,2167,0,0,34,1383,375,2812,579,804,2,212,116,45,21,2784,7,0,60,4630
# MAGIC RTX,UNIVERSITY HOSPITALS OF MORECAMBE BAY NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,1,0,1,0,3,0,2730,5,2,7,13,37,325,1585,355,0,406,863,591,78,589,256,358,981,364,1318,421,560,0,165,68,131,4,1312,2,2,70,2735
# MAGIC RVJ,NORTH BRISTOL NHS TRUST,Provider,3,2290,1528,689,325,96,42,109,69,81,69,57,19,132,2,25,30,38,79,700,3809,770,0,56,2125,0,260,924,715,1485,2140,612,2658,901,1239,0,235,182,195,5,2518,135,7,92,5509
# MAGIC RVR,EPSOM AND ST HELIER UNIVERSITY HOSPITALS NHS TRUST,Provider,7,1608,1037,367,163,72,54,109,56,42,50,37,5,241,1,2,9,18,63,465,2913,313,0,64,1525,996,0,1238,0,89,1423,439,1913,539,884,0,198,29,212,6,1882,25,1,72,3848
# MAGIC RVV,EAST KENT HOSPITALS UNIVERSITY NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,6159,0,0,0,0,0,0,0,0,0,6159,0,0,0,0,0,6159,2368,747,3044,1012,1356,1,299,195,252,19,2982,43,0,0,6159
# MAGIC RVW,NORTH TEES AND HARTLEPOOL NHS FOUNDATION TRUST,Provider,7,1450,451,170,70,34,20,77,43,28,17,12,1,135,0,4,4,11,66,331,1702,318,1,80,872,461,43,760,300,81,781,204,1482,309,472,0,2,151,51,8,1472,2,0,50,2517
# MAGIC RVY,SOUTHPORT AND ORMSKIRK HOSPITAL NHS TRUST,Provider,1,1001,503,194,95,58,24,67,27,48,39,60,7,165,2,2,8,12,24,307,1504,297,0,133,746,441,103,709,148,142,847,266,1159,387,460,0,9,167,90,1,1158,0,0,17,2289
# MAGIC RW6,PENNINE ACUTE HOSPITALS NHS TRUST,Provider,4,1118,406,233,164,106,54,119,53,34,27,24,4,100,0,8,18,13,47,365,1618,320,1,56,1025,532,136,393,347,13,863,261,1301,357,506,2,95,90,74,14,1284,3,0,21,2446
# MAGIC RWA,HULL UNIVERSITY TEACHING HOSPITALS NHS TRUST,Provider,827,3406,89,61,41,33,23,77,65,69,51,37,14,75,2,10,21,32,107,667,3292,691,0,46,2011,1169,241,814,587,46,1651,425,2753,782,869,0,278,19,128,13,2739,1,0,39,4868
# MAGIC RWD,UNITED LINCOLNSHIRE HOSPITALS NHS TRUST,Provider,14,2692,653,314,126,98,41,97,54,71,51,45,13,191,4,5,10,29,76,563,3197,500,0,76,1640,797,91,1680,164,88,1486,469,2472,586,900,0,211,96,162,10,2450,12,1,32,4460
# MAGIC RWE,UNIVERSITY HOSPITALS OF LEICESTER NHS TRUST,Provider,7,4547,1484,754,382,195,120,258,141,106,133,101,8,1094,9,25,44,52,174,1031,6004,1263,0,728,3921,1903,561,649,1312,984,3576,1331,4360,1428,2148,6,546,404,375,30,4260,70,0,63,9330
# MAGIC RWF,MAIDSTONE AND TUNBRIDGE WELLS NHS TRUST,Provider,7,3364,893,425,186,67,45,123,91,89,89,62,4,235,2,1,6,21,92,536,3974,935,0,113,2713,1273,556,472,542,124,2083,585,2981,1045,1038,1,122,202,260,6,2970,5,0,31,5680
# MAGIC RWG,WEST HERTFORDSHIRE HOSPITALS NHS TRUST,Provider,26,1692,385,200,81,58,32,83,39,41,44,38,4,1367,6,3,4,13,48,315,2026,340,1,1334,1349,638,244,306,219,1334,1575,495,2009,725,850,1,20,205,269,8,1976,25,0,11,4090
# MAGIC RWH,EAST AND NORTH HERTFORDSHIRE NHS TRUST,Provider,3,1838,872,343,144,68,31,88,45,46,73,64,13,1614,2,1,4,4,30,457,2652,599,0,1493,2158,1054,1444,0,0,586,1693,718,2666,871,822,0,380,141,197,16,2606,44,0,165,5242
# MAGIC RWJ,STOCKPORT NHS FOUNDATION TRUST,Provider,3,1699,515,216,121,52,33,71,27,30,48,255,151,110,0,1,9,25,60,421,2342,464,0,10,1203,804,212,683,428,2,1373,351,1599,612,761,0,119,76,156,2,1597,0,0,9,3332
# MAGIC RWP,WORCESTERSHIRE ACUTE HOSPITALS NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,4784,4,4,10,28,96,803,3077,631,24,107,1100,1,69,2205,1408,1,1615,568,2600,820,795,0,243,146,179,10,2548,42,0,1,4784
# MAGIC RWW,WARRINGTON AND HALTON TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,0,0,0,0,0,0,0,0,0,1,0,0,4,2601,11,3,7,16,31,327,1753,369,0,89,896,635,111,495,380,89,899,287,1399,399,500,1,110,63,113,6,1376,17,0,21,2606
# MAGIC RWY,CALDERDALE AND HUDDERSFIELD NHS FOUNDATION TRUST,Provider,4,2686,612,271,157,97,64,107,63,50,67,44,2,286,2,5,14,23,73,527,3232,591,0,43,2234,1404,24,757,49,42,1417,458,2624,647,770,0,21,346,91,16,2599,9,0,11,4510
# MAGIC RX1,NOTTINGHAM UNIVERSITY HOSPITALS NHS TRUST,Provider,11,4163,1486,559,232,129,74,168,105,77,76,89,10,728,9,24,36,59,121,808,5397,1008,0,445,3344,1579,225,2173,458,128,2791,953,4104,1152,1639,0,2,565,386,33,4015,56,0,59,7907
# MAGIC RXC,EAST SUSSEX HEALTHCARE NHS TRUST,Provider,10,700,288,261,236,96,31,54,22,27,43,305,158,555,265,6,13,10,45,237,1609,345,0,256,1310,596,563,267,0,50,920,315,1500,436,484,2,22,142,149,6,1491,3,0,51,2786
# MAGIC RXF,MID YORKSHIRE HOSPITALS NHS TRUST,Provider,12,2982,891,532,311,164,76,147,79,62,70,53,8,291,7,3,16,44,94,827,3952,731,0,4,2638,1247,529,410,447,407,1935,618,3119,942,993,0,163,297,158,21,3058,40,0,6,5678
# MAGIC RXK,SANDWELL AND WEST BIRMINGHAM HOSPITALS NHS TRUST,Provider,6,1732,681,481,363,266,153,429,187,180,135,114,11,237,13,14,16,25,76,530,3482,817,0,2,2860,0,0,1005,0,1110,1514,506,2942,529,985,0,0,324,182,21,2913,8,0,13,4975
# MAGIC RXL,BLACKPOOL TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,7,1488,344,185,98,55,32,82,46,16,28,10,4,169,2,5,9,20,58,331,1780,248,0,111,986,535,4,656,269,114,897,353,1262,488,409,0,0,217,136,4,1246,12,0,52,2564
# MAGIC RXN,LANCASHIRE TEACHING HOSPITALS NHS FOUNDATION TRUST,Provider,3,599,393,365,218,86,38,59,26,30,30,27,5,2245,1,4,3,16,36,251,1306,341,0,2167,777,411,48,312,427,2150,1233,406,2319,608,625,0,161,110,135,16,2303,0,0,167,4125
# MAGIC RXP,COUNTY DURHAM AND DARLINGTON NHS FOUNDATION TRUST,Provider,4,60,20,10,3,0,0,2,2,1,0,0,1,4273,0,8,5,22,74,599,3075,494,0,99,0,0,0,0,0,4376,1369,530,2383,693,676,2,293,107,128,11,2372,0,1,93,4376
# MAGIC RXQ,BUCKINGHAMSHIRE HEALTHCARE NHS TRUST,Provider,7,1923,925,610,303,153,62,141,72,88,70,60,14,180,3,1,10,21,62,457,3200,703,0,154,2225,968,79,1046,103,190,1660,711,2186,719,941,0,39,495,177,9,2177,0,1,53,4611
# MAGIC RXR,EAST LANCASHIRE HOSPITALS NHS TRUST,Provider,0,0,0,0,0,0,0,0,0,0,0,0,0,5894,0,0,0,0,0,0,0,0,0,5894,2525,1222,646,368,1092,41,2104,844,2921,835,1269,4,480,90,270,16,2905,0,0,25,5894
# MAGIC RXW,THE SHREWSBURY AND TELFORD HOSPITAL NHS TRUST,Provider,2,1481,932,560,280,118,59,121,51,48,35,36,15,149,2,5,7,20,56,429,2653,588,1,126,1625,654,447,554,481,126,1103,391,2378,509,594,1,88,234,68,16,2337,25,0,15,3887
# MAGIC RYJ,IMPERIAL COLLEGE HEALTHCARE NHS TRUST,Provider,4,928,1219,520,251,142,98,213,148,333,301,157,0,4759,1,35,55,63,141,1046,4852,654,0,2238,3220,1827,336,2291,19,1392,3636,1168,4165,1693,1943,1,299,117,751,16,4146,3,0,116,9085
# MAGIC RYR,WESTERN SUSSEX HOSPITALS NHS FOUNDATION TRUST,Provider,8,3351,1189,579,236,128,72,157,106,166,193,283,62,2862,7,12,16,53,148,983,5604,1497,1,1072,4111,2156,383,1965,245,533,3599,1105,4529,1874,1725,0,414,255,436,18,4487,24,2,158,9393"""))

# COMMAND ----------

# MAGIC %python
# MAGIC StructSchema = []
# MAGIC for col in hes2122_df.columns:
# MAGIC   if is_bool_dtype(hes2122_df[col]):
# MAGIC     StructSchema.append(StructField(col, BooleanType(), True))    
# MAGIC   elif is_numeric_dtype(hes2122_df[col]):
# MAGIC     StructSchema.append(StructField(col, IntegerType(), True))    
# MAGIC   else:
# MAGIC     StructSchema.append(StructField(col, StringType(), True))
# MAGIC hesSchema = StructType(StructSchema)
# MAGIC hes2122_spdf = spark.createDataFrame(hes2122_df, schema=hesSchema)
# MAGIC # display(hes2122_spdf)
# MAGIC
# MAGIC hes2122_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.annual_hes_input2122')

# COMMAND ----------

# DBTITLE 1,HES 2223
hes2223_df = pd.read_csv(StringIO("""Fyear,org_level,org_code,TotalDeliveries_HES
2223,Provider,R0A,15652
2223,Provider,R0B,3522
2223,Provider,R0D,3832
2223,Provider,R1F,931
2223,Provider,R1H,13767
2223,Provider,R1K,3782
2223,Provider,RA2,2606
2223,Provider,RA4,1118
2223,Provider,RA7,4513
2223,Provider,RA9,177
2223,Provider,RAE,4769
2223,Provider,RAJ,11164
2223,Provider,RAL,7508
2223,Provider,RAP,3781
2223,Provider,RAS,3946
2223,Provider,RAX,4528
2223,Provider,RBD,1440
2223,Provider,RBK,3640
2223,Provider,RBL,2889
2223,Provider,RBN,3672
2223,Provider,RBT,2968
2223,Provider,RC9,8092
2223,Provider,RCB,4317
2223,Provider,RCD,1647
2223,Provider,RCF,1833
2223,Provider,RCX,1897
2223,Provider,RD1,4082
2223,Provider,RD8,3201
2223,Provider,RDE,6417
2223,Provider,RDU,1623
2223,Provider,REF,3783
2223,Provider,REP,7138
2223,Provider,RF4,7054
2223,Provider,RFF,2917
2223,Provider,RFR,2464
2223,Provider,RFS,2806
2223,Provider,RGN,5829
2223,Provider,RGP,1741
2223,Provider,RGR,2180
2223,Provider,RGT,5317
2223,Provider,RH5,2876
2223,Provider,RH8,6496
2223,Provider,RHM,5013
2223,Provider,RHQ,5467
2223,Provider,RHU,4531
2223,Provider,RHW,4639
2223,Provider,RJ1,6192
2223,Provider,RJ2,7006
2223,Provider,RJ6,3008
2223,Provider,RJ7,4295
2223,Provider,RJC,3069
2223,Provider,RJE,6061
2223,Provider,RJL,3616
2223,Provider,RJR,2151
2223,Provider,RJZ,7454
2223,Provider,RK5,3395
2223,Provider,RK9,3491
2223,Provider,RKB,5360
2223,Provider,RKE,2934
2223,Provider,RL4,5035
2223,Provider,RLQ,1596
2223,Provider,RLT,2112
2223,Provider,RM1,4818
2223,Provider,RM3,4306
2223,Provider,RMC,5524
2223,Provider,RMP,2092
2223,Provider,RN3,3607
2223,Provider,RN5,4703
2223,Provider,RN7,4597
2223,Provider,RNA,4056
2223,Provider,RNN,2485
2223,Provider,RNQ,3136
2223,Provider,RNS,4149
2223,Provider,RNZ,2050
2223,Provider,RP5,4403
2223,Provider,RPA,4431
2223,Provider,RQ3,7142
2223,Provider,RQM,12450
2223,Provider,RQW,1722
2223,Provider,RQX,5485
2223,Provider,RR7,1669
2223,Provider,RR8,8384
2223,Provider,RRF,1572
2223,Provider,RRK,8554
2223,Provider,RRV,5281
2223,Provider,RTD,5835
2223,Provider,RTE,5594
2223,Provider,RTF,3098
2223,Provider,RTG,8544
2223,Provider,RTH,7307
2223,Provider,RTK,2818
2223,Provider,RTP,5038
2223,Provider,RTR,4532
2223,Provider,RTX,2732
2223,Provider,RVJ,5247
2223,Provider,RVR,3668
2223,Provider,RVV,5927
2223,Provider,RVW,2471
2223,Provider,RVY,2159
2223,Provider,RWA,4718
2223,Provider,RWD,4410
2223,Provider,RWE,9317
2223,Provider,RWF,5669
2223,Provider,RWG,3769
2223,Provider,RWH,4833
2223,Provider,RWJ,3220
2223,Provider,RWP,4677
2223,Provider,RWW,2551
2223,Provider,RWY,4166
2223,Provider,RX1,7390
2223,Provider,RXC,2709
2223,Provider,RXF,5369
2223,Provider,RXK,5179
2223,Provider,RXL,2510
2223,Provider,RXN,4073
2223,Provider,RXP,4198
2223,Provider,RXQ,4439
2223,Provider,RXR,5609
2223,Provider,RXW,4077
2223,Provider,RYJ,9120
2223,Provider,RYR,8798"""))

# COMMAND ----------

# %sql
# drop table if exists $outSchema.annual_hes_input2223

# COMMAND ----------

# MAGIC %python
# MAGIC StructSchema = []
# MAGIC for col in hes2223_df.columns:
# MAGIC   if is_bool_dtype(hes2223_df[col]):
# MAGIC     StructSchema.append(StructField(col, BooleanType(), True))    
# MAGIC   elif is_numeric_dtype(hes2223_df[col]):
# MAGIC     StructSchema.append(StructField(col, IntegerType(), True))    
# MAGIC   else:
# MAGIC     StructSchema.append(StructField(col, StringType(), True))
# MAGIC hesSchema = StructType(StructSchema)
# MAGIC hes2223_spdf = spark.createDataFrame(hes2223_df, schema=hesSchema)
# MAGIC # display(hes2122_spdf)
# MAGIC
# MAGIC hes2223_spdf.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.annual_hes_input2223')

# COMMAND ----------

# %sql
# select a.org_code, a.org_name, a.TotalDeliveries_HES, b.org_code, b.org_name, b.TotalDeliveries_HES 
# from mat_analysis.annual_hes_input1819 a
# full outer join suchith_shetty1_102195.annual_hes_input2122 b
#   on a.org_code = b.org_code