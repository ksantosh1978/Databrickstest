# Databricks notebook source
dbutils.widgets.text("db", "", "Target database")
db = dbutils.widgets.get("db")
print(db)
assert db
dbutils.widgets.text("mat_pre_clear", "mat_pre_clear", "Source database")
mat_pre_clear = dbutils.widgets.get("mat_pre_clear")
print(mat_pre_clear)
assert mat_pre_clear
dbutils.widgets.text("dss_corporate", "dss_corporate", "Reference database")
dss_corporate = dbutils.widgets.get("dss_corporate")

assert dss_corporate

params = {"outSchema" : db}

# #TODO:; rewrite to bring dss corporate variable into all notebooks
print(params)

# COMMAND ----------

dbutils.notebook.run('schemas/_init_', 0, params)