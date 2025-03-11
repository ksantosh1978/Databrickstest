-- Databricks notebook source
-- MAGIC %python
-- MAGIC outputSchema = dbutils.widgets.get("outSchema")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ_Defaults
(
UniqSubmissionID STRING,
UID STRING,
Default INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ_StandardMissing
(
UniqSubmissionID STRING,
UID STRING,
Missing INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ_CustomMissingDenoms
(
UniqSubmissionID STRING,
Table STRING,
UID STRING,
Missing INT,
Denominator INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ_StandardDenoms
(
UniqSubmissionID INT,
Table STRING,
Denominator INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ_Invalid
(
UniqSubmissionID STRING,
UID STRING,
Invalid INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.zStaging_DQ
(
RPStartDate STRING,
RPEndDate STRING,
OrgCodeProvider STRING,
UniqSubmissionID STRING,
DataTable STRING,
UID STRING,
DataItem STRING,
Table_x STRING,
TableDenom STRING,
CustomMissing STRING,
CustomDenom STRING,
Missing STRING,
Default STRING,
Invalid STRING
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.DQMI_Coverage
(
PeriodFrom STRING,
PeriodTo STRING,
Dataset STRING,
DataProviderCode STRING,
DataProviderName STRING,
ExpectedToSubmit INT,
Submitted INT
)
USING DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS $outSchema.DQMI_Data
(
PeriodFrom STRING,
PeriodTo STRING,
Dataset STRING,
DataProviderCode STRING,
DataProviderName STRING,
DataItem STRING,
CompleteNumerator BIGINT,
CompleteDenominator BIGINT,
ValidNumerator BIGINT, 
DefaultNumerator BIGINT, 
ValidDenominator BIGINT
)
USING DELTA