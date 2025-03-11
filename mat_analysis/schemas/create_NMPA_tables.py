# Databricks notebook source
# DBTITLE 1,Widget values
#outSchema = dbutils.widgets.get("outSchema")
#print(outSchema)
#assert outSchema

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer in birth raw table
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW
# MAGIC (
# MAGIC        UniqPregID                  STRING,
# MAGIC        Person_ID_Mother            STRING,
# MAGIC        RPStartDate                 DATE,
# MAGIC        RPEndDate                   DATE,
# MAGIC        RecordNumber                STRING,
# MAGIC        SettingIntraCare            STRING,
# MAGIC        LabourDeliveryID            STRING,
# MAGIC        OrgCodeProvider             STRING,
# MAGIC        PersonBirthDateBaby         STRING,
# MAGIC        SettingPlaceBirth           STRING,
# MAGIC        rowNum                      BIGINT,
# MAGIC        CreatedAt                   TIMESTAMP
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer in birth raw table joined with Geography
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC (
# MAGIC        UniqPregID                  STRING,
# MAGIC        Person_ID_Mother            STRING,
# MAGIC        RPStartDate                 DATE,
# MAGIC        RPEndDate                   DATE,
# MAGIC        RecordNumber                STRING,
# MAGIC        SettingIntraCare            STRING,
# MAGIC        LabourDeliveryID            STRING,
# MAGIC        OrgCodeProvider             STRING,
# MAGIC        PersonBirthDateBaby         STRING,
# MAGIC        SettingPlaceBirth           STRING,
# MAGIC        rowNum                      BIGINT,       
# MAGIC        Trust_ORG                   STRING,
# MAGIC        Trust                       STRING,
# MAGIC        RegionORG                   STRING,
# MAGIC        Region                      STRING,
# MAGIC        Mbrrace_Grouping_Short      STRING,
# MAGIC        Mbrrace_Grouping            STRING,
# MAGIC        STP_Code                    STRING,
# MAGIC        STP_Name                    STRING,  
# MAGIC        CreatedAt                   TIMESTAMP
# MAGIC )
# MAGIC using delta   
# MAGIC    

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Home_Obstetric metric - Denominator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Home_Obstetric_Denominator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_FMU_Obstetric metric - Denominator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_FMU_Obstetric_Denominator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_AMU_Obstetric metric - Denominator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_AMU_Obstetric_Denominator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Midwife_Obstetric metric - Denominator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Midwife_Obstetric_Denominator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Home_Obstetric metric - Numerator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Home_Obstetric_Numerator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_FMU_Obstetric metric - Numerator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_AMU_Obstetric metric - Numerator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_AMU_Obstetric_Numerator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Midwife_Obstetric metric - Numerator
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Midwife_Obstetric_Numerator
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       BIGINT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Home_Obstetric metric - RATE
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Home_Obstetric_RATE
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       FLOAT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_FMU_Obstetric metric - RATE
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_FMU_Obstetric_RATE
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       FLOAT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_AMU_Obstetric metric - RATE
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_AMU_Obstetric_RATE
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       FLOAT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,NMPA - Transfer_Midwife_Obstetric metric - RATE
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_Transfer_Midwife_Obstetric_RATE
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    DATE,
# MAGIC        ReportingPeriodEndDate      DATE,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       FLOAT
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create NMPA Numerator Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.nmpa_numerator_raw
# MAGIC (
# MAGIC        KeyValue        BIGINT  -- e.g. MSD101_ID, for audit only
# MAGIC        ,RPStartDate     DATE
# MAGIC        ,RPEndDate       DATE
# MAGIC        ,Indicator       STRING
# MAGIC        ,IndicatorFamily STRING
# MAGIC        ,Person_ID       STRING
# MAGIC        ,PregnancyID     STRING
# MAGIC        ,OrgCodeProvider STRING
# MAGIC        ,Rank            INT -- only consider when rank is 1
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,Create NMPA Denominator Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.nmpa_denominator_raw
# MAGIC (
# MAGIC        KeyValue        BIGINT  -- e.g. MSD101_ID, for audit only
# MAGIC        ,RPStartDate     DATE
# MAGIC        ,RPEndDate       DATE
# MAGIC        ,Person_ID       STRING
# MAGIC        ,PregnancyID     STRING
# MAGIC        ,OrgCodeProvider STRING
# MAGIC        ,Rank            INT -- only consider when rank is 1
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# DBTITLE 1,NMPA_CSV
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.NMPA_CSV
# MAGIC (
# MAGIC        Org_Code                    STRING,
# MAGIC        Org_Name                    STRING,
# MAGIC        Org_Level                   STRING,
# MAGIC        ReportingPeriodStartDate    STRING,
# MAGIC        ReportingPeriodEndDate      STRING,
# MAGIC        IndicatorFamily             STRING,
# MAGIC        Indicator                   STRING,
# MAGIC        Currency                    STRING,
# MAGIC        VALUE                       STRING,
# MAGIC        CreatedAt                   TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC