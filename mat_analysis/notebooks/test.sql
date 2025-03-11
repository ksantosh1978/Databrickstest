-- Databricks notebook source
-- MAGIC %sql
-- MAGIC
-- MAGIC create table  mat_analysis.testing_pivot as
-- MAGIC (
-- MAGIC inputstring='{"certificate_illegible":"please provide your qualification and supporting transcript as a PDF file or clear pictures/scans.","transcript_illegible":"Your documents have been provided as a word document, due to our verifying processes, please provide your qualification and supporting transcript as a PDF file or clear pictures/scans.", "payment_failed":"Your payment failed, please reprocess pay."}'
-- MAGIC )

-- COMMAND ----------

desc mat_analysis.measures_csv

-- COMMAND ----------

