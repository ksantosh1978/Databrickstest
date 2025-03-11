# Databricks notebook source
#dbutils.widgets.text("outSchema", "mat_analysis")
#dbutils.widgets.removeAll()


# COMMAND ----------

outputSchema = dbutils.widgets.get("outSchema")
assert outputSchema
print(outputSchema)

# COMMAND ----------

# DBTITLE 1,Database created by other means


# COMMAND ----------

dbutils.notebook.run("./ReferenceTables/ALLSNOMEDforV2", 0, {"outSchema" : outputSchema})
dbutils.notebook.run("./ReferenceTables/Create_NPMA_Induction_Codes", 0, {"outSchema" : outputSchema})
dbutils.notebook.run("./ReferenceTables/BuildPublishedHESRefData", 0, {"outSchema" : outputSchema})
dbutils.notebook.run("./ReferenceTables/BuildAspirinSnomedLookup", 0, {"outSchema" : outputSchema})

# COMMAND ----------

dbutils.notebook.run("./create_CQIM_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

dbutils.notebook.run("./create_output_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

dbutils.notebook.run("./LoadTOS2", 0, {"outSchema" : outputSchema})

# COMMAND ----------

dbutils.notebook.run("./create_dq_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

dbutils.notebook.run("./create_intermediate_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Continuity of Carer (CoC)
dbutils.notebook.run("./ create_CoC_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Saving Babies Lives (SLB)
dbutils.notebook.run("./ create_SLB_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,PCSP
dbutils.notebook.run("./create_PCSP_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,NMPA tables
dbutils.notebook.run("./create_NMPA_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,BMI14+1 Weeks tables
dbutils.notebook.run("./create_BMI_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Measure Tables
dbutils.notebook.run("./create_Measure_tables", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Create views
dbutils.notebook.run("./create_views", 0, {"outSchema" : outputSchema})

# COMMAND ----------

# DBTITLE 1,Note that views are also created in notebooks\monthly_job_execution
