# Databricks notebook source
dbutils.widgets.text("db", "mat_analysis", "Target database")
db = dbutils.widgets.get("db")
assert db
dbutils.widgets.text("mat_pre_clear", "testdata_mat_analysis_mat_pre_clear", "Source database")
mat_pre_clear = dbutils.widgets.get("mat_pre_clear")
assert mat_pre_clear
dbutils.widgets.text("dss_corporate", "dss_corporate", "Reference database")
dss_corporate = dbutils.widgets.get("dss_corporate")
assert dss_corporate
RPBegindate = '2022-08-01'
assert RPBegindate
Pipeline_Run = 'Yes'
assert Pipeline_Run
# status not currently included in any output
params = {'RPBegindate' : RPBegindate, 'mat_pre_clear' : mat_pre_clear, 'db' : db, 'dss_corporate' : dss_corporate,'Pipeline_Run' : Pipeline_Run}

print(params)

# COMMAND ----------

# DBTITLE 1,Schema tests
dbutils.notebook.run("tests/check_tables_tests", 0, params)

# COMMAND ----------

# DBTITLE 1,Source Schema check with testdata database
from dsp.common.spark_helpers import vacuum_table, optimize_table, dataframes_equal, drop_table_or_view, datatypes_equal
import os


#Do these check only in ref. or Dev?

if(os.environ.get('env') == 'ref'):
  sourceTables = {sourceTable['tableName'] for sourceTable in spark.sql(f'SHOW TABLES IN mat_pre_clear').collect()}
  destTables = {destTable['tableName'] for destTable in spark.sql(f'SHOW TABLES IN {mat_pre_clear}').collect()}
  for sourceTable in sourceTables:
    df_source = spark.table(f'{mat_pre_clear}.{sourceTable}')
    df_test = spark.table(f'{mat_pre_clear}.{sourceTable}')
  #   print(datatypes_equal(df1.schema, df2.schema))
    if not(datatypes_equal(df_source.schema, df_test.schema)):
      print(f'{sourceTable} does not have matching schemas')
      assert False, f'{sourceTable}  does not have matching schema with testdata database. Please update the table schema either by dropping or altering the tables.'
  #     print(f'dropping {sourceTable}')
  # DPS function drop_table_or_view is not working, below function implementation was there in iapt_analysis project
  #     safely_drop_table_or_view('testdata_mat_analysis_mat_pre_clear', sourceTable)
  #     df_source.write.mode("overwrite").format('delta').saveAsTable(f'testdata_mat_analysis_mat_pre_clear.{sourceTable}')
  #     print(f'{sourceTable} delta table updated to match the source schema')
    else:
      print(f'{sourceTable} have matching schemas')

# COMMAND ----------

if(Pipeline_Run=='Yes'):
    dbutils.notebook.run('run_notebooks', 0, params)
else:
    print('Not running the pipeline from here')

# COMMAND ----------

# DBTITLE 1,Unit tests
# Leave uncommented when executing run_tests in Ref. Comment out in staging prior to a merge otherwise merge will fail
# Issue is likely a non-deterministic query combined with poor quality test data.
# There is intermittent investigation but this is a lengthy process as it involves adding debug to unit_tests notebook and promoting.
# As uncommenting breaks the promotion process, it's better to do this when there's no ongoing development of maternity (SH).

# dbutils.notebook.run("tests/unit_tests", 0, params)