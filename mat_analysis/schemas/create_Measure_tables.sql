-- Databricks notebook source
-- MAGIC %python
-- MAGIC # dbutils.widgets.text("outSchema", "mat_analysis", "Target database")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC outSchema = dbutils.widgets.get("outSchema")
-- MAGIC print(outSchema)
-- MAGIC assert(outSchema)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --DROP TABLE IF EXISTS $outSchema.Measures_raw;
-- MAGIC --DROP TABLE IF EXISTS $outSchema.Measures_Aggregated;
-- MAGIC --DROP TABLE IF EXISTS $outSchema.Measures_CSV; 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS $outSchema.Measures_raw
-- MAGIC (
-- MAGIC    KeyValue        BIGINT    -- e.g. MSD101_ID, for audit only
-- MAGIC   ,RPStartDate     DATE      -- Reporting period start date
-- MAGIC   ,RPEndDate       DATE      -- Reporting period end date
-- MAGIC   ,IndicatorFamily STRING    -- Type of measure (NMPC, CQC etc)
-- MAGIC   ,Indicator       STRING    -- Specific measure name
-- MAGIC   ,isNumerator     INT
-- MAGIC   ,isDenominator   INT
-- MAGIC   ,Person_ID       STRING    -- PersonID (MotherId, BabyId)
-- MAGIC   ,PregnancyID     STRING    -- Unqiue pregnancy id
-- MAGIC   ,OrgCodeProvider STRING    
-- MAGIC   ,Rank            INT       -- only consider when rank is 1
-- MAGIC   ,IsOverDQThreshold INT     -- 0/1 at provider level. Only include in higher level aggregations when 1
-- MAGIC   ,CreatedAt       TIMESTAMP 
-- MAGIC   ,Rank_IMD_Decile STRING
-- MAGIC   ,EthnicCategory  STRING
-- MAGIC   ,EthnicGroup     STRING
-- MAGIC )
-- MAGIC USING DELTA 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS $outSchema.Measures_Aggregated
-- MAGIC (
-- MAGIC    RPStartDate           DATE
-- MAGIC   ,RPEndDate             DATE
-- MAGIC   ,IndicatorFamily       STRING
-- MAGIC   ,Indicator             STRING
-- MAGIC   ,OrgCodeProvider       STRING 
-- MAGIC   ,OrgName               STRING
-- MAGIC   ,OrgLevel              STRING  -- Organisation level (RegionORG, MBRRACE_Grouping_Short, STP_Code etc)
-- MAGIC   ,Unrounded_Numerator   FLOAT
-- MAGIC   ,Unrounded_Denominator FLOAT
-- MAGIC   ,Unrounded_Rate        FLOAT
-- MAGIC   ,Rounded_Numerator     FLOAT
-- MAGIC   ,Rounded_Denominator   FLOAT
-- MAGIC   ,Rounded_Rate          FLOAT
-- MAGIC   ,IsOverDQThreshold     INT
-- MAGIC   ,CreatedAt             TIMESTAMP 
-- MAGIC )
-- MAGIC USING DELTA
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS $outSchema.Measures_CSV
-- MAGIC (
-- MAGIC    RPStartDate     DATE
-- MAGIC   ,RPEndDate       DATE
-- MAGIC   ,IndicatorFamily STRING
-- MAGIC   ,Indicator       STRING
-- MAGIC   ,OrgCodeProvider STRING  
-- MAGIC   ,OrgName         STRING
-- MAGIC   ,OrgLevel        STRING
-- MAGIC   ,Currency        STRING
-- MAGIC   ,Value           STRING
-- MAGIC   ,CreatedAt       TIMESTAMP
-- MAGIC ) 
-- MAGIC USING DELTA
-- MAGIC PARTITIONED  BY(RPStartDate);
-- MAGIC