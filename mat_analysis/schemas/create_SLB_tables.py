# Databricks notebook source
# DBTITLE 1,Widget values
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert outSchema

# COMMAND ----------

# DBTITLE 1,SLB_Numerator_Raw
# MAGIC %sql
# MAGIC -- LAD, booking site, CCG and delivery site columns were added and populated due to an incorrect specifiction.
# MAGIC -- They are not used for output but the logic is in place should the number of breakdowns be extended in future
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.SLB_Numerator_Raw
# MAGIC (
# MAGIC        KeyValue                  BIGINT,  -- e.g. MSD101_ID
# MAGIC        RPStartDate               DATE,
# MAGIC        RPEndDate                 DATE,
# MAGIC        Indicator                 STRING,
# MAGIC        IndicatorFamily           STRING,
# MAGIC        Person_ID_Mother          STRING,
# MAGIC        UniqPregID                STRING,
# MAGIC        Person_ID_Baby            STRING,
# MAGIC        RecordNumber              BIGINT, -- only used to derive fields that are not used for output
# MAGIC        OrgCodeProvider           STRING,
# MAGIC        LADCD                     STRING, -- Local authority code, not used for output
# MAGIC        LADNM                     STRING, -- Local authority name, not used for output
# MAGIC        OrgSiteIDBooking          STRING, -- Booking site org code, not used for output
# MAGIC        OrgSiteNameBooking        STRING, -- Booking site org name, not used for output
# MAGIC        CCGResponsibilityMother   STRING, -- CCG code of mother's GP, not used for output
# MAGIC        CCGName                   STRING, -- CCG name of mother's GP, not used for output
# MAGIC        OrgSiteIDActualDelivery   STRING, -- Delivery site org code, not used for output
# MAGIC        OrgSiteNameActualDelivery STRING, -- Delivery site org name, not used for output
# MAGIC        CreatedAt                 TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,SLB_Denominator_Raw
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.SLB_Denominator_Raw
# MAGIC (
# MAGIC        KeyValue                  BIGINT,  -- e.g. MSD101_ID
# MAGIC        RPStartDate               DATE,
# MAGIC        RPEndDate                 DATE,
# MAGIC        Indicator                 STRING,
# MAGIC        IndicatorFamily           STRING,
# MAGIC        Person_ID_Mother          STRING,
# MAGIC        UniqPregID                STRING,
# MAGIC        Person_ID_Baby            STRING,
# MAGIC        RecordNumber              BIGINT,-- only used to derive fields that are not used for output
# MAGIC        OrgCodeProvider           STRING,
# MAGIC        LADCD                     STRING, -- Local authority code, not used for output
# MAGIC        LADNM                     STRING, -- Local authority name, not used for output
# MAGIC        OrgSiteIDBooking          STRING, -- Booking site org code, not used for output
# MAGIC        OrgSiteNameBooking        STRING, -- Booking site org name, not used for output
# MAGIC        CCGResponsibilityMother   STRING, -- CCG code of mother's GP, not used for output
# MAGIC        CCGName                   STRING, -- CCG name of mother's GP, not used for output
# MAGIC        OrgSiteIDActualDelivery   STRING, -- Delivery site org code, not used for output
# MAGIC        OrgSiteNameActualDelivery STRING, -- Delivery site org name, not used for output
# MAGIC        CreatedAt                 TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,SLB_Provider_Aggregated
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.SLB_Provider_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
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

# DBTITLE 1,SLB_Geography_Aggregated
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.SLB_Geography_Aggregated
# MAGIC (
# MAGIC        RPStartDate           DATE,
# MAGIC        RPEndDate             DATE,
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

# DBTITLE 1,SLB_CSV
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  $outSchema.SLB_CSV
# MAGIC (
# MAGIC        RPStartDate       DATE,
# MAGIC        RPEndDate         DATE,
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