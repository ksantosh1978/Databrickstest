# Databricks notebook source
# MAGIC %md
# MAGIC # Code Promotion Job Configuration
# MAGIC ### This notebook sets some parameters for Databricks jobs wrapping the top-level entry point notebooks.
# MAGIC Only simple setting of variables is allowed in this notebook.

# COMMAND ----------

# DBTITLE 1,Global settings
# MAGIC %md
# MAGIC spark_version can be either "6.6.x-scala2.11" (spark 2) or "9.1.x-scala2.12" (spark 3).   
# MAGIC This applies to all jobs created

# COMMAND ----------

# spark_version = "6.6.x-scala2.11"
# spark_version = "9.1.x-scala2.12"
spark_version = "10.4.x-scala2.12"


# COMMAND ----------

# DBTITLE 1,init_schemas
# MAGIC %md
# MAGIC Currently, no parameter can be set for the job wrapping the *init_schemas* notebook.

# COMMAND ----------

# DBTITLE 1,run_notebooks
# MAGIC %md
# MAGIC Available parameters:
# MAGIC  - **concurrency**: Integer between 1 and 10. Allows you to run multiple *run_notebooks* jobs at the same time. 
# MAGIC  - **extra_parameters**: Dictionary(String, String) that maps *parameter names* to *default values*. These parameters are added to the list of parameters for the job.

# COMMAND ----------

# Example:
# run_notebooks = {
#   "concurrency": 5,
#   "extra_parameters": {
#     "month_id": "",
#     "end_of_year_report": "no",
#   },
# }
run_notebooks = {
  "concurrency": 1,
  "extra_parameters": {},
  "schedule": "0 0 19 1/1 * ? *"
}

# COMMAND ----------

# DBTITLE 1,tool_config
# MAGIC %md
# MAGIC Currently, no parameter can be set for the job wrapping the *tool_config* notebook.