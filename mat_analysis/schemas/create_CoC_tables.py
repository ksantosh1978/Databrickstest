# Databricks notebook source
# DBTITLE 1,Widget values
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert outSchema

# COMMAND ----------

# DBTITLE 1,CoC_Numerator_Raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.CoC_Numerator_Raw
# MAGIC (
# MAGIC        KeyValue        BIGINT,  -- e.g. MSD101_ID
# MAGIC        RPStartDate     DATE,
# MAGIC        RPEndDate       DATE,
# MAGIC        Status          STRING, -- not currently used
# MAGIC        Indicator       STRING,
# MAGIC        IndicatorFamily STRING,
# MAGIC        Person_ID       STRING,
# MAGIC        PregnancyID     STRING,
# MAGIC        CareConID       STRING,
# MAGIC        OrgCodeProvider STRING,
# MAGIC        Rank            INT, -- only consider when rank is 1
# MAGIC        OverDQThreshold INT, -- 0/1 at provider level. Only include in higher level aggregations when 1
# MAGIC        CreatedAt       TIMESTAMP,
# MAGIC        rank_imd_decile STRING,
# MAGIC        ethniccategory  STRING,
# MAGIC        ethnicgroup     STRING
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,CoC_Denominator_Raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.CoC_Denominator_Raw
# MAGIC (
# MAGIC        KeyValue        BIGINT,  -- e.g. MSD101_ID
# MAGIC        RPStartDate     DATE,
# MAGIC        RPEndDate       DATE,
# MAGIC        Status          STRING, -- not currently used
# MAGIC        Indicator       STRING,
# MAGIC        IndicatorFamily STRING,
# MAGIC        Person_ID       STRING,
# MAGIC        PregnancyID     STRING,
# MAGIC        CareConID       STRING,
# MAGIC        OrgCodeProvider STRING,
# MAGIC        Rank            INT, -- only consider when rank is 1
# MAGIC        OverDQThreshold INT, -- 0/1 at provider level. Only include in higher level aggregations when 1
# MAGIC        CreatedAt       TIMESTAMP,
# MAGIC        rank_imd_decile STRING,
# MAGIC        ethniccategory  STRING,
# MAGIC        ethnicgroup     STRING
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,CoC_Provider_Aggregated
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.CoC_Provider_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
# MAGIC        Status                STRING, -- not currently used
# MAGIC        Indicator             STRING,
# MAGIC        IndicatorFamily       STRING,
# MAGIC        OrgCodeProvider       STRING,
# MAGIC        OrgName               STRING,
# MAGIC        OrgLevel              STRING,
# MAGIC        Unrounded_Numerator   FLOAT,
# MAGIC        Unrounded_Denominator FLOAT,
# MAGIC        Unrounded_Rate        FLOAT,
# MAGIC        Rounded_Numerator     FLOAT,
# MAGIC        Rounded_Denominator   FLOAT,
# MAGIC        Rounded_Rate          FLOAT,
# MAGIC        OverDQThreshold       INT, -- 0/1 at provider level. Only include in higher level aggregations when 1
# MAGIC        CreatedAt             TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,CoC_Geography_Aggregated
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.CoC_Geography_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
# MAGIC        Status                STRING, -- not currently used
# MAGIC        Indicator             STRING,
# MAGIC        IndicatorFamily       STRING,
# MAGIC        OrgCodeProvider       STRING,
# MAGIC        OrgName               STRING,
# MAGIC        OrgLevel              STRING,
# MAGIC        Unrounded_Numerator   FLOAT,
# MAGIC        Unrounded_Denominator FLOAT,
# MAGIC        Unrounded_Rate        FLOAT,
# MAGIC        Rounded_Numerator     FLOAT,
# MAGIC        Rounded_Denominator   FLOAT,
# MAGIC        Rounded_Rate          FLOAT,
# MAGIC        CreatedAt             TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,CoC_CSV
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.CoC_CSV
# MAGIC (
# MAGIC        RPStartDate       DATE,
# MAGIC        RPEndDate         DATE,
# MAGIC        Status            STRING, -- not currently used
# MAGIC        Indicator         STRING,
# MAGIC        IndicatorFamily   STRING,
# MAGIC        OrgCodeProvider   STRING,
# MAGIC        OrgName           STRING,
# MAGIC        OrgLevel          STRING,
# MAGIC        Currency          STRING,
# MAGIC        Value             STRING,
# MAGIC        CreatedAt         TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,IsColumnInTable
# does datebase.table contain column? 1=yes, 0=no
def IsColumnInTable(database_name, table_name, column_name):
  try:
    df = spark.table(f"{database_name}.{table_name}")
    cols = df.columns
    if column_name in cols:
      return 1
    else:
      return 0
  except:
    return -1

# COMMAND ----------

# List of tables and column that needs adding to them
tableColumn = [{"table" : "coc_numerator_raw", "column" : "rank_imd_decile", "type" : "string"}, {"table" : "coc_numerator_raw", "column" : "ethniccategory", "type" : "string"}, {"table" : "coc_numerator_raw", "column" : "ethnicgroup", "type" : "string"}, {"table" : "coc_denominator_raw", "column" : "rank_imd_decile", "type" : "string"}, {"table" : "coc_denominator_raw", "column" : "ethniccategory", "type" : "string"}, {"table" : "coc_denominator_raw", "column" : "ethnicgroup", "type" : "string"}]

# COMMAND ----------

for item in tableColumn:
  table = item["table"]
  print(table)
  column = item["column"]
  print(column)
  columnType = item["type"]
  print(columnType)
  exists = IsColumnInTable(outSchema, table, column)
  if exists == 0:
    action = """ALTER TABLE {outSchema}.{table} ADD COLUMNS ({column} {columnType})""".format(outSchema=outSchema,table=table,column=column,columnType=columnType)
    print(action)
    spark.sql(action)