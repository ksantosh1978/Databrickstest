# Databricks notebook source
outSchema = dbutils.widgets.get("outSchema")
assert outSchema
print(outSchema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW $outSchema.EthnicGroup AS
# MAGIC SELECT 'BAME' as Name
# MAGIC UNION (SELECT 'White')
# MAGIC UNION (SELECT 'Other')
# MAGIC UNION (SELECT 'Missing_Unknown')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If Rank_IMD_Decile_2015 is not one of 10 allowed values (expected value is null), then force it to 'Missing_Unknown' to join to this view
# MAGIC CREATE OR REPLACE VIEW $outSchema.IMD AS
# MAGIC SELECT 1 AS Rank_IMD_Decile, '01_Most_deprived' AS Description
# MAGIC UNION (SELECT 2, '02')
# MAGIC UNION (SELECT 3, '03')
# MAGIC UNION (SELECT 4, '04')
# MAGIC UNION (SELECT 5, '05')
# MAGIC UNION (SELECT 6, '06')
# MAGIC UNION (SELECT 7, '07')
# MAGIC UNION (SELECT 8, '08')
# MAGIC UNION (SELECT 9, '09')
# MAGIC UNION (SELECT 10, '10_Least_deprived')
# MAGIC UNION (SELECT null, 'Missing_Unknown')