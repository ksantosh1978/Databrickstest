# Databricks notebook source
dbutils.widgets.text("db", "", "Target database")
db = dbutils.widgets.get("db")
assert db

print(db)

# COMMAND ----------

sql = "select * from {db}.maternity_monthly_csv_sql".format(db=db)
df1 = spark.sql(sql)
df1.count()

# COMMAND ----------

sql = "select * from {db}.maternity_monthly_csv_sql".format(db=db)
df2 = spark.sql(sql)
df2.count()

# COMMAND ----------

# df1 = df1.union(df2)
# df1.count()

# COMMAND ----------

len(df1.subtract(df2).head(1)) == 0

# COMMAND ----------

len(df2.subtract(df1).head(1)) == 0

# COMMAND ----------

df1.count() == df2.count()