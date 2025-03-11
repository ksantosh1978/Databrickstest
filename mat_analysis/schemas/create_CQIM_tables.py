# Databricks notebook source
# DBTITLE 1,Capture Widgets Variables
# Capture the Widgets content - Database to be used for the query
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)

# COMMAND ----------

# DBTITLE 1,measures_and_rates - for all measures
# MAGIC %sql
# MAGIC -- Contains raw data
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.cqim_measures_and_rates
# MAGIC (
# MAGIC        OrgCodeProvider STRING,
# MAGIC        Org_Name        STRING,
# MAGIC        RPStartDate     DATE,
# MAGIC        RPEndDate       DATE,
# MAGIC        ADJStartDate    DATE,
# MAGIC        ADJEndDate      DATE,
# MAGIC        IndicatorFamily STRING,
# MAGIC        Indicator       STRING,
# MAGIC        Numerator       INT,
# MAGIC        Denominator     INT
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,cqim_output_rates - calculations for all rates
# MAGIC %sql
# MAGIC -- Contains unformatted output, unrounded and rounded, result (pass/fail), other data required for output (e.g. ADJStartDate)
# MAGIC -- Result/Rounded_Result is Pass/Fail. Better to use int 1/0?
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.cqim_output_rates
# MAGIC (
# MAGIC OrgCodeProvider	                    STRING,
# MAGIC Org_Name	                    STRING,
# MAGIC Org_Level	                STRING,
# MAGIC RPStartDate	                DATE,
# MAGIC RPEndDate	                DATE,
# MAGIC ADJStartDate	                DATE,
# MAGIC ADJEndDate	                DATE,
# MAGIC IndicatorFamily	            STRING,
# MAGIC Indicator	                STRING,
# MAGIC Unrounded_Numerator	        FLOAT,
# MAGIC Unrounded_Denominator	    FLOAT,
# MAGIC Unrounded_Rate               FLOAT,
# MAGIC Unrounded_RateperThousand    FLOAT,
# MAGIC Result                       STRING,
# MAGIC Rounded_Numerator	        FLOAT,
# MAGIC Rounded_Denominator	        FLOAT,
# MAGIC Rounded_Rate	                FLOAT,
# MAGIC Rounded_RateperThousand      FLOAT,
# MAGIC Rounded_Result              STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Create cqim_dq_csv - format of the CSV to extrtact
# MAGIC %sql
# MAGIC -- Contains data for output - no further formatting required
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.cqim_dq_csv
# MAGIC (
# MAGIC        OrgCodeProvider  STRING,
# MAGIC        Org_Name         STRING,
# MAGIC        Org_Level	    STRING,
# MAGIC        RPStartDate	    DATE,
# MAGIC        RPEndDate	    DATE,
# MAGIC        IndicatorFamily  STRING,
# MAGIC        Indicator        STRING,
# MAGIC        Currency  	    STRING,
# MAGIC        value     	    STRING
# MAGIC        
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MinRate, MaxRate: Threshold rate. To Pass, rate must be between MinRate and MaxRate 
# MAGIC -- Inclusive: If 1, MinRate and MaxRate are inclusive (>= and <=) ; If 0 MinRate and MaxRate are exclusive (> and <)
# MAGIC CREATE OR REPLACE VIEW $outSchema.CQIM_Suppression_Rule AS
# MAGIC SELECT "CQIMDQ02" as Indicator, 0.7 as MinRate, cast(null as float) as MaxRate, 1 as Inclusive
# MAGIC UNION(SELECT "CQIMDQ03", 0.5, null, 1)
# MAGIC UNION(SELECT "CQIMDQ04", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ05", 0.005, 0.5, 1)
# MAGIC UNION(SELECT "CQIMDQ06", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ07", 0.005, 0.5, 1)
# MAGIC UNION(SELECT "CQIMDQ08", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ09", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ10", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ11", null, 0.6, 1)
# MAGIC UNION(SELECT "CQIMDQ12", null, 0.2, 1)
# MAGIC UNION(SELECT "CQIMDQ13", 1, 1, 1)
# MAGIC UNION(SELECT "CQIMDQ14", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ15", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ16", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ18", 0.4, null, 0)
# MAGIC UNION(SELECT "CQIMDQ19", null, 0.5, 1)
# MAGIC UNION(SELECT "CQIMDQ20", null, 0.1, 1)
# MAGIC UNION(SELECT "CQIMDQ21", 1, 1, 1)
# MAGIC UNION(SELECT "CQIMDQ22", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ23", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ24", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ25", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ26", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ27", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ28", 0.2, 0.7, 1)
# MAGIC UNION(SELECT "CQIMDQ29", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ30", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ31", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ32", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ33", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ34", 0.4, null, 0)
# MAGIC UNION(SELECT "CQIMDQ35", 0.005, 0.4, 1)
# MAGIC UNION(SELECT "CQIMDQ36", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ37", 0.2, 0.7, 1)
# MAGIC UNION(SELECT "CQIMDQ38", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ39", 0.7, null, 1)
# MAGIC UNION(SELECT "CQIMDQ40", null, 0.3, 1)
# MAGIC UNION(SELECT "CQIMDQ41", 0.005, 0.6, 1)
# MAGIC UNION(SELECT "CQIMDQ42", 0.5, 0.95, 1)

# COMMAND ----------

# DBTITLE 1,IsColumnInTable
# MAGIC %py
# MAGIC # does datebase.table contain column? 1=yes, 0=no
# MAGIC def IsColumnInTable(database_name, table_name, column_name):
# MAGIC   try:
# MAGIC     df = spark.table(f"{database_name}.{table_name}")
# MAGIC     cols = df.columns
# MAGIC     if column_name in cols:
# MAGIC       return 1
# MAGIC     else:
# MAGIC       return 0
# MAGIC   except:
# MAGIC     return -1

# COMMAND ----------

# MAGIC %py
# MAGIC # List of tables and column that needs adding to them
# MAGIC tableColumn = {'cqim_output_rates': 'Rounded_Result'}

# COMMAND ----------

# DBTITLE 1,Add column to table if it doesn't exist
# MAGIC %py
# MAGIC for table, column in tableColumn.items():
# MAGIC   print(table)
# MAGIC   print(column)
# MAGIC   exists = IsColumnInTable(outSchema, table, column)
# MAGIC   if exists == 0:
# MAGIC     action = """ALTER TABLE {db_output}.{table} ADD COLUMNS ({column} STRING)""".format(db_output=outSchema,table=table,column=column)
# MAGIC     print(action)
# MAGIC     spark.sql(action)