# Databricks notebook source
# MAGIC %md
# MAGIC ### Latest month's (provisional) reference data is currently used in the dashboard.
# MAGIC ### When this notebook is run
# MAGIC   - ref_org file will be the same for every month as currently the whole reference data is used regardless of the input date.
# MAGIC   - ref_stp will get have the mapping of STPs in the input `reporting period`.
# MAGIC   - ref_MBRRACE_corp is same as ref_org where we just get the reference data regardless of the input.

# COMMAND ----------

import pyspark.sql.functions as F

from datetime import date
from pyspark.sql import Window

# COMMAND ----------

OUTSCHEMA: str = dbutils.widgets.get("outSchema")
DSS_CORPORATE: str = dbutils.widgets.get('dss_corporate')
FILENAME: str = dbutils.widgets.get("filename")

# COMMAND ----------

# DBTITLE 1,ref_org
NHS_TRUST = (F.col('ORG_TYPE_CODE') == 'TR')
INDEPENDENT_SECTOR_HEALTHCARE_PROVIDER = (F.col('ORG_TYPE_CODE') == 'PH')
DASHBOARD_PROVIDERS = (NHS_TRUST | INDEPENDENT_SECTOR_HEALTHCARE_PROVIDER)
most_recent_business_record = Window.partitionBy('ORG_CODE').orderBy(F.desc('BUSINESS_START_DATE'))

org_daily_spdf = spark.read.table(f'{DSS_CORPORATE}.org_daily')
ref_org = (
    org_daily_spdf
    .withColumn('recent_business_record', F.row_number().over(most_recent_business_record))
    .filter((F.col('recent_business_record') == 1) & (DASHBOARD_PROVIDERS))
    .select('ORG_CODE', 'NAME', 'COMMENTS', 'BUSINESS_START_DATE', 'BUSINESS_END_DATE')
    .orderBy('ORG_CODE')
)

# COMMAND ----------

# DBTITLE 1,ref_stp
geog = spark.table(f'{OUTSCHEMA}.geogtlrr')
ref_stp = (
    geog
    .select(F.col('Trust_ORG').alias('Org_ODS_Code'),
            F.col('Trust').alias('Org_Name'),
            F.col('LRegionORG').alias('STP_ODS_Code'),
            F.col('STP_Code').alias('STP_ONS_Code'),
            'STP_Name',
            F.col('RegionORG').alias('Region_ODS_Code'),
            F.regexp_replace(F.col('Region'), ' COMMISSIONING REGION', '').alias('Region_Name'))
)

# COMMAND ----------

# DBTITLE 1,ref_MBRRACE_corp
maternity_mbrrace_groups_spdf = spark.table('dss_corporate.maternity_mbrrace_groups')
ref_mbrrace_corp = maternity_mbrrace_groups_spdf.select('ORG_CODE', 'MBRRACE_GROUPING', 'MBRRACE_GROUPING_SHORT', 'MBRRACE_GROUPING_ABV', 'REL_OPEN_DATE', 'REL_CLOSE_DATE')

# COMMAND ----------

from dsp.code_promotion.mesh_send import cp_mesh_send
import os

dfs_to_extract = {'ref_org': ref_org, 'ref_stp': ref_stp, 'ref_MBRRACE_Corp': ref_mbrrace_corp}

#Prod mail box id
mailbox_to = 'X26HC004'
workflow_id = 'GNASH_MAT_ANALYSIS'
# local_id = 'mat_analysis'

for dfname in dfs_to_extract:
  df_to_extract = dfs_to_extract[dfname]
  print(f"{dfname} rowcount", df_to_extract.count())
  if (FILENAME=='Provisional'):
    file = dfname + ' ' + FILENAME
  else:
    file = dfname
  print(file)
  if(os.environ.get('env') == 'prod'):
    dfname = f'{file}.csv'
    local_id = dfname
    # For supporting the LEAD MESH activity to rename the files appropriately with rename flag as option '1', local_id parameter should bear the value of file name.
    try:
      request_id = cp_mesh_send(spark, df_to_extract, mailbox_to, workflow_id, dfname, local_id)
      print(f"{dfname} file have been pushed to MESH with request id {request_id}. \n")
    except Exception as ex:
      print(ex, 'MESH exception on SPARK 3 can be ignored, file would be delivered in the destined path')
  else:
    display(df_to_extract)