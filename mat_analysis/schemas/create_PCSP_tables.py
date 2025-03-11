# Databricks notebook source
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)


# COMMAND ----------

# DBTITLE 1,Widget values
# dbutils.widgets.removeAll();
# outSchema = dbutils.widgets.get("outSchema")
# print(outSchema)
# assert outSchema

# COMMAND ----------

# DBTITLE 1,PCSP_Numerator_Raw
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.PCSP_Numerator_Raw
# MAGIC (
# MAGIC        KeyValue        BIGINT,  -- e.g. MSD101_ID, for audit only
# MAGIC        RPStartDate     DATE,
# MAGIC        RPEndDate       DATE,
# MAGIC        Status          STRING,
# MAGIC        Indicator       STRING,
# MAGIC        IndicatorFamily STRING,
# MAGIC        Person_ID       STRING,
# MAGIC        PregnancyID     STRING,
# MAGIC        OrgCodeProvider STRING,
# MAGIC        Rank            INT, -- only consider when rank is 1
# MAGIC        OverDQThreshold INT, -- 0/1 at provider level. Only include in higher level aggregations when 1
# MAGIC        CreatedAt       TIMESTAMP,
# MAGIC        Rank_IMD_Decile STRING,
# MAGIC        EthnicCategory  STRING,
# MAGIC        EthnicGroup     STRING
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,PCSP_Denominator_Raw
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.PCSP_Denominator_Raw
# MAGIC (
# MAGIC        KeyValue        BIGINT,  -- e.g. MSD101_ID, for audit only
# MAGIC        RPStartDate     DATE,
# MAGIC        RPEndDate       DATE,
# MAGIC        Status          STRING,
# MAGIC        Indicator       STRING,
# MAGIC        IndicatorFamily STRING,
# MAGIC        Person_ID       STRING,
# MAGIC        PregnancyID     STRING,
# MAGIC        OrgCodeProvider STRING,
# MAGIC        Rank            INT, -- only consider when rank is 1
# MAGIC        OverDQThreshold INT, -- 0/1 at provider level. Only include in higher level aggregations when 1
# MAGIC        CreatedAt       TIMESTAMP,
# MAGIC        Rank_IMD_Decile STRING,
# MAGIC        EthnicCategory  STRING,
# MAGIC        EthnicGroup     STRING
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,PCSP_Provider_Aggregated
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.PCSP_Provider_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
# MAGIC        Status                STRING,
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

# DBTITLE 1,PCSP_Geography_Aggregated
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.PCSP_Geography_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
# MAGIC        Status                STRING,
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

# DBTITLE 1,PCSP_CSV
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.PCSP_CSV
# MAGIC (
# MAGIC        RPStartDate       DATE,
# MAGIC        RPEndDate         DATE,
# MAGIC        Status            STRING,
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