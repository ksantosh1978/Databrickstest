# Databricks notebook source
# MAGIC %sql
# MAGIC -- populated with valid codes in /notebooks/common_objects/00_version_change_tables
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS $db_output.resp_comm_validcodes;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_induction_of_labour_codes
# MAGIC (
# MAGIC   Measure string,
# MAGIC   Type string,
# MAGIC   ValidValue string,
# MAGIC   FirstMonth int,
# MAGIC   LastMonth int
# MAGIC ) USING DELTA;