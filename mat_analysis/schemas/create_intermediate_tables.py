# Databricks notebook source
# dbutils.widgets.text("outSchema", "mat_analysis")

# COMMAND ----------

outputSchema = dbutils.widgets.get("outSchema")

# COMMAND ----------

# DBTITLE 1,code needed to clear up existing views on conversion to tables
tables = ["BookingsBaseTable", "BirthsBaseTable", "DeliveryBaseTable", "CarePlanBaseTable"] # RobsonGroup view should remain in place

for table in tables:
  try:
    sql="drop view if exists {outputSchema}.{table}".format(table=table, outputSchema=outputSchema)
    spark.sql(sql)
    print(sql)
  except:
    pass
  

# COMMAND ----------

# DBTITLE 1,Create MotherBabyGeogs table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $outSchema.motherbabybooking_geog(
# MAGIC   Record_Type STRING, 
# MAGIC   Sub_Type STRING,
# MAGIC   Person_ID_Mother STRING, 
# MAGIC   Person_ID_Baby STRING,
# MAGIC   UniqPregID STRING, 
# MAGIC   RecordNumber BIGINT,
# MAGIC   RPEndDate DATE, 
# MAGIC   valuecode STRING,
# MAGIC   valuename STRING,
# MAGIC   UniqSubmissionID BIGINT
# MAGIC        )
# MAGIC        USING DELTA

# COMMAND ----------

# DBTITLE 1,Create BookingsBaseTable
# MAGIC %sql
# MAGIC CREATE table if not exists $outSchema.BookingsBaseTable(
# MAGIC   ReportingPeriodEndDate STRING,
# MAGIC   Person_ID_Mother STRING,
# MAGIC   TotalBookings STRING, 
# MAGIC   AgeAtBookingMother STRING,
# MAGIC   AgeAtBookingMotherAvg STRING, 
# MAGIC   AgeAtBookingMotherGroup STRING,
# MAGIC   BMI STRING,
# MAGIC   ComplexSocialFactorsInd STRING,  
# MAGIC   DeprivationDecileAtBooking STRING,
# MAGIC   EthnicCategoryMotherGroup STRING,
# MAGIC   GestAgeFormalAntenatalBookingGroup STRING,
# MAGIC   CO_Concentration_Booking STRING,
# MAGIC   SmokingStatusGroupBooking STRING,
# MAGIC   SmokingStatusGroupDelivery STRING,
# MAGIC   FolicAcidSupplement STRING,
# MAGIC   AlcoholUnitsPerWeekBand STRING,
# MAGIC   PreviousCaesareanSectionsGroup STRING,
# MAGIC   PreviousLiveBirthsGroup STRING,
# MAGIC   Trust_ORG STRING,
# MAGIC   Trust_ORGTYPE STRING,
# MAGIC   Trust STRING,
# MAGIC   LRegionORG STRING,
# MAGIC   LRegionORGTYPE STRING,
# MAGIC   LRegion STRING,
# MAGIC   RegionORG STRING,
# MAGIC   RegionORGTYPE STRING,
# MAGIC   Region STRING,
# MAGIC   Mbrrace_Grouping STRING,
# MAGIC   Mbrrace_Grouping_Short STRING,
# MAGIC   STP_Code STRING,
# MAGIC   STP_Name STRING,
# MAGIC   BookingSiteCode STRING, 
# MAGIC   BookingSiteName STRING, 
# MAGIC   Mother_CCG_Code STRING, 
# MAGIC   Mother_CCG STRING, 
# MAGIC   Mother_LAD_Code STRING, 
# MAGIC   Mother_LAD STRING
# MAGIC     )
# MAGIC     USING DELTA
# MAGIC     PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# DBTITLE 1,Create BirthsBaseTable
# MAGIC %sql
# MAGIC CREATE table if not exists $outSchema.BirthsBaseTable(
# MAGIC   ReportingPeriodEndDate STRING,
# MAGIC   Person_ID_Baby STRING,
# MAGIC   TotalBabies STRING,  
# MAGIC   ApgarScore5TermGroup7 STRING,
# MAGIC   BabyFirstFeedBreastMilkStatus STRING,
# MAGIC   BirthweightTermGroup STRING,
# MAGIC   GestationLengthBirth STRING,
# MAGIC   GestationLengthBirthGroup37 STRING,
# MAGIC   PlaceTypeActualDeliveryMidwifery STRING,
# MAGIC   SkinToSkinContact1HourTerm STRING,
# MAGIC   RobsonGroup STRING,
# MAGIC   Trust_ORG STRING,
# MAGIC   Trust_ORGTYPE STRING,
# MAGIC   Trust STRING,
# MAGIC   LRegionORG STRING,
# MAGIC   LRegionORGTYPE STRING,
# MAGIC   LRegion STRING,
# MAGIC   RegionORG STRING,
# MAGIC   RegionORGTYPE STRING,
# MAGIC   Region STRING,
# MAGIC   Mbrrace_Grouping STRING,
# MAGIC   Mbrrace_Grouping_Short STRING,
# MAGIC   STP_Code STRING,
# MAGIC   STP_Name STRING,
# MAGIC   DeliverySiteCode STRING, 
# MAGIC   DeliverySiteName STRING, 
# MAGIC   Mother_CCG_Code STRING, 
# MAGIC   Mother_CCG STRING, 
# MAGIC   Mother_LAD_Code STRING, 
# MAGIC   Mother_LAD STRING,
# MAGIC   BirthWeightTermGroup2500 STRING,
# MAGIC   BirthWeightRank INT
# MAGIC   )
# MAGIC   USING DELTA
# MAGIC   PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# DBTITLE 1,Create DeliveryBaseTable
# MAGIC %sql
# MAGIC CREATE table if not exists $outSchema.DeliveryBaseTable(
# MAGIC   ReportingPeriodEndDate STRING,
# MAGIC   Person_ID_Mother STRING,
# MAGIC   TotalDeliveries STRING,
# MAGIC   DeliveryMethodBabyGroup STRING,
# MAGIC   GenitalTractTraumaticLesion STRING,
# MAGIC   GenitalTractTraumaticLesionGroup STRING,
# MAGIC   CO_Concentration_Delivery STRING,
# MAGIC   Trust_ORG STRING,
# MAGIC   Trust_ORGTYPE STRING,
# MAGIC   Trust STRING,
# MAGIC   LRegionORG STRING,
# MAGIC   LRegionORGTYPE STRING,
# MAGIC   LRegion STRING,
# MAGIC   RegionORG STRING,
# MAGIC   RegionORGTYPE STRING,
# MAGIC   Region STRING,
# MAGIC   Mbrrace_Grouping STRING,
# MAGIC   Mbrrace_Grouping_Short STRING,
# MAGIC   STP_Code STRING,
# MAGIC   STP_Name STRING,
# MAGIC   DeliverySiteCode STRING,
# MAGIC   DeliverySiteName STRING,
# MAGIC   Mother_CCG_Code STRING,
# MAGIC   Mother_CCG STRING,
# MAGIC   Mother_LAD_Code STRING,
# MAGIC   Mother_LAD STRING
# MAGIC   )
# MAGIC   USING DELTA
# MAGIC   PARTITIONED BY (ReportingPeriodEndDate);

# COMMAND ----------

# DBTITLE 1,Create CarePlanBaseTable
# MAGIC %sql
# MAGIC CREATE table if not exists $outSchema.CarePlanBaseTable(
# MAGIC   ReportingPeriodEndDate STRING,
# MAGIC   OrgCodeProvider STRING,
# MAGIC   CCP_Any_Pathways STRING,
# MAGIC   PCP_Any_Pathways STRING,
# MAGIC   CCP_Antenatal STRING,
# MAGIC   PCP_Antenatal STRING,
# MAGIC   CCP_Birth STRING,
# MAGIC   PCP_Birth STRING,
# MAGIC   CCP_Postpartum STRING,
# MAGIC   PCP_Postpartum STRING,
# MAGIC   CCP_Any_PathwaysCount STRING,
# MAGIC   PCP_Any_PathwaysCount STRING,
# MAGIC   CCP_AntenatalCount STRING,
# MAGIC   PCP_AntenatalCount STRING,
# MAGIC   CCP_BirthCount STRING,
# MAGIC   PCP_BirthCount STRING,
# MAGIC   CCP_PostpartumCount STRING,
# MAGIC   PCP_PostpartumCount STRING,
# MAGIC   Trust_ORG STRING,
# MAGIC   Trust_ORGTYPE STRING,
# MAGIC   Trust STRING,
# MAGIC   LRegionORG STRING,
# MAGIC   LRegionORGTYPE STRING,
# MAGIC   LRegion STRING,
# MAGIC   RegionORG STRING,
# MAGIC   RegionORGTYPE STRING,
# MAGIC   Region STRING,
# MAGIC   Mbrrace_Grouping STRING,
# MAGIC   Mbrrace_Grouping_Short STRING,
# MAGIC   STP_Code STRING,
# MAGIC   STP_Name STRING,
# MAGIC   BookingSiteCode STRING, 
# MAGIC   BookingSiteName STRING,
# MAGIC   Mother_CCG_Code STRING,
# MAGIC   Mother_CCG STRING,
# MAGIC   Mother_LAD_Code STRING,
# MAGIC   Mother_LAD STRING
# MAGIC  )
# MAGIC   USING DELTA
# MAGIC   PARTITIONED BY (ReportingPeriodEndDate);