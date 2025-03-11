# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Widget values
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)


# COMMAND ----------

# DBTITLE 1,Step 1: Create a list of raw data
# MAGIC %sql
# MAGIC /* 
# MAGIC Step 1: Create a list of unique pregnancy identifiers (single births / delivery)
# MAGIC Inner join MSD401 and MSD301 on RPStartDate, UniqPregID, LabourDeliveryID, Person_ID_Mother, RecordNumber, OrgCodeProvider
# MAGIC Keep records where PersonBirthDateBaby (MSD401) is between RPStartDate (MSD401) and RPEndDate (MSD401) AND BirthsPerLabAndDel (MSD301) is not null
# MAGIC Group records on UniqPregID, Person_ID_Mother
# MAGIC Create list of UniqPregID where the largest recorded value of BirthsPerLabAndDel for the pregnancy = 1 
# MAGIC */
# MAGIC
# MAGIC --CREATE OR REPLACE GLOBAL TEMPORARY VIEW NMPA_TRANSFER_IN_BIRTH_SETTING_RAW AS
# MAGIC TRUNCATE TABLE $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW;
# MAGIC
# MAGIC INSERT INTO $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW
# MAGIC SELECT MSD401.UniqPregID, MSD401.Person_ID_Mother, '$RPStartdate' AS RPStartDate, '$RPEnddate' AS RPEndDate, MSD401.RecordNumber
# MAGIC   , MSD301.SettingIntraCare, MSD401.LabourDeliveryID, MSD401.OrgCodeProvider, MSD401.PersonBirthDateBaby, MSD401.SettingPlaceBirth
# MAGIC   , ROW_NUMBER() OVER (PARTITION BY MSD401.Person_ID_Mother ORDER BY MSD401.RecordNumber DESC) rowNum
# MAGIC   , current_timestamp() AS CreatedAt
# MAGIC   
# MAGIC FROM $dbSchema.MSD301LabourDelivery MSD301
# MAGIC   INNER JOIN $dbSchema.MSD401BabyDemographics MSD401 ON MSD301.RPStartDate = MSD401.RPStartDate
# MAGIC         AND MSD301.RPEndDate = MSD401.RPEndDate
# MAGIC         AND MSD301.UniqPregID = MSD401.UniqPregID
# MAGIC         AND MSD301.LabourDeliveryID = MSD401.LabourDeliveryID
# MAGIC         AND MSD301.Person_ID_Mother = MSD401.Person_ID_Mother
# MAGIC         AND MSD301.RecordNumber = MSD401.RecordNumber
# MAGIC         AND MSD301.OrgCodeProvider = MSD401.OrgCodeProvider
# MAGIC         
# MAGIC WHERE MSD301.BirthsPerLabAndDel = 1
# MAGIC         AND MSD301.SettingIntraCare IN ('02', '03', '04', '05')
# MAGIC         AND MSD401.PersonBirthDateBaby BETWEEN MSD401.RPStartDate AND MSD401.RPEndDate 
# MAGIC         AND MSD401.RPStartDate = '$RPStartdate'
# MAGIC         AND MSD401.RPEndDate = '$RPEnddate' ;

# COMMAND ----------

# DBTITLE 1,Step 2: Join the RAW data with Geography and expand the data set
# MAGIC %sql
# MAGIC /* 
# MAGIC BS: This is the base RAW level data alongwith Geography amended to it. From here onwards, the 3 aggregation tables will be created. The aggregations will be created based on 5 different things which are based on below
# MAGIC
# MAGIC Local Maternity System	= STP_Code
# MAGIC Mbrrace	 	    = Mbrrace_Grouping_Short 
# MAGIC National 		= Everything
# MAGIC Provider 		= Trust_Org
# MAGIC Region			= RegionOrg
# MAGIC */
# MAGIC
# MAGIC --CREATE OR REPLACE GLOBAL TEMPORARY VIEW NMPA_PREG_RAW_GEOGRAPHY AS
# MAGIC TRUNCATE TABLE $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY;
# MAGIC
# MAGIC INSERT INTO $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY
# MAGIC SELECT A.UniqPregID, A.Person_ID_Mother, A.RPStartDate, A.RPEndDate, A.RecordNumber, A.SettingIntraCare
# MAGIC   , A.LabourDeliveryID, A.OrgCodeProvider, A.PersonBirthDateBaby, A.SettingPlaceBirth, A.rowNum
# MAGIC   , G.Trust_ORG, G.Trust
# MAGIC   , G.RegionORG, G.Region
# MAGIC   , G.Mbrrace_Grouping_Short, G.Mbrrace_Grouping
# MAGIC   , G.STP_Code, G.STP_Name
# MAGIC   , current_timestamp() AS CreatedAt
# MAGIC
# MAGIC FROM $outSchema.NMPA_TRANSFER_IN_BIRTH_SETTING_RAW A
# MAGIC    INNER JOIN $outSchema.geogtlrr G ON A.OrgCodeProvider = G.Trust_ORG
# MAGIC WHERE A.rowNum = 1 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table $outSchema.NMPA_Transfer_Home_Obstetric_Denominator;
# MAGIC truncate table $outSchema.NMPA_Transfer_Home_Obstetric_Numerator;
# MAGIC truncate table $outSchema.NMPA_Transfer_Home_Obstetric_RATE;
# MAGIC
# MAGIC truncate table $outSchema.NMPA_Transfer_AMU_Obstetric_Denominator;
# MAGIC truncate table $outSchema.NMPA_Transfer_AMU_Obstetric_Numerator;
# MAGIC truncate table $outSchema.NMPA_Transfer_AMU_Obstetric_RATE;
# MAGIC
# MAGIC truncate table $outSchema.NMPA_Transfer_FMU_Obstetric_Denominator;
# MAGIC truncate table $outSchema.NMPA_Transfer_FMU_Obstetric_Numerator;
# MAGIC truncate table $outSchema.NMPA_Transfer_FMU_Obstetric_RATE;
# MAGIC
# MAGIC truncate table $outSchema.NMPA_Transfer_Midwife_Obstetric_Denominator;
# MAGIC truncate table $outSchema.NMPA_Transfer_Midwife_Obstetric_Numerator;
# MAGIC truncate table $outSchema.NMPA_Transfer_Midwife_Obstetric_RATE;
# MAGIC
# MAGIC TRUNCATE TABLE $outSchema.nmpa_numerator_raw;
# MAGIC TRUNCATE TABLE $outSchema.nmpa_denominator_raw;
# MAGIC
# MAGIC delete from $outSchema.NMPA_CSV where ReportingPeriodStartDate = '$RPStartdate';

# COMMAND ----------

dbutils.notebook.exit("Notebook: Prepare_NMPA ran successfully")