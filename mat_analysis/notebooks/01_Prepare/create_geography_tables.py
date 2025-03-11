# Databricks notebook source
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# DBTITLE 1,Trust on hospital
# MAGIC %sql
# MAGIC --Temporary table for all hospital reference codes, hospital organisation type, hospital name and trust name
# MAGIC CREATE OR REPLACE VIEW $outSchema.trustonhospital
# MAGIC
# MAGIC AS SELECT
# MAGIC -- Fields populated from dss_corporate
# MAGIC
# MAGIC Hospital_Name.ORG_CODE AS Hospital_ORG,
# MAGIC Hospital_Name.ORG_TYPE_CODE AS Hospital_ORGTYPE,
# MAGIC Hospital_Name.NAME AS Hospital_Name,
# MAGIC Trust_Name.NAME AS Trust 
# MAGIC
# MAGIC FROM $dss_corporate.ORG_DAILY as Hospital_Name
# MAGIC LEFT JOIN $dss_corporate.org_relationship_daily AS Trust ON Hospital_Name.ORG_CODE = Trust.REL_FROM_ORG_CODE
# MAGIC LEFT JOIN $dss_corporate.org_daily AS Trust_Name ON Trust.REL_TO_ORG_CODE = Trust_Name.ORG_CODE
# MAGIC
# MAGIC --Only select hospitals and trusts that are active on the last day of the month as defined by widgets above
# MAGIC
# MAGIC WHERE
# MAGIC Hospital_Name.BUSINESS_START_DATE <= "$RPEnddate" 
# MAGIC AND
# MAGIC (Hospital_Name.BUSINESS_END_DATE >= "$RPEnddate" OR Hospital_Name.BUSINESS_END_DATE IS NULL)
# MAGIC AND
# MAGIC Hospital_Name.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (Hospital_Name.ORG_CLOSE_DATE IS NULL OR Hospital_Name.ORG_CLOSE_DATE>= "$RPEnddate")
# MAGIC AND
# MAGIC Hospital_Name.ORG_TYPE_CODE = 'TS'
# MAGIC AND
# MAGIC (Trust.REL_CLOSE_DATE IS NULL OR Trust.REL_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC Trust.REL_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC Trust.REL_TO_ORG_TYPE_CODE = 'TR'
# MAGIC AND
# MAGIC Trust_Name.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (Trust_Name.ORG_CLOSE_DATE IS NULL OR Trust_Name.ORG_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC Trust_Name.BUSINESS_START_DATE <= "$RPEnddate" 
# MAGIC AND
# MAGIC (Trust_Name.BUSINESS_END_DATE IS NULL OR Trust_Name.BUSINESS_END_DATE >= "$RPEnddate")
# MAGIC

# COMMAND ----------

# DBTITLE 1,Local region on trust
# MAGIC %sql
# MAGIC --Temporary table for all trust reference codes, trust organisation type, trust name and local region name
# MAGIC CREATE OR REPLACE VIEW $outSchema.lregionontrust
# MAGIC
# MAGIC AS SELECT
# MAGIC -- Fields populated from dss_corporate
# MAGIC trust2.ORG_CODE AS trust_ORG,
# MAGIC trust2.ORG_TYPE_CODE AS trust_ORGTYPE,
# MAGIC trust2.NAME AS trust2,
# MAGIC lregion_name.NAME AS lregion
# MAGIC
# MAGIC FROM $dss_corporate.ORG_DAILY AS trust2
# MAGIC LEFT JOIN $dss_corporate.org_relationship_daily AS lregion ON trust2.ORG_CODE = lregion.REL_FROM_ORG_CODE
# MAGIC LEFT JOIN $dss_corporate.org_daily AS lregion_name ON lregion.REL_TO_ORG_CODE = lregion_name.ORG_CODE
# MAGIC
# MAGIC --Only select trusts and local regions that are active on the last day of the month as defined by widgets above
# MAGIC
# MAGIC WHERE
# MAGIC trust2.BUSINESS_START_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (trust2.BUSINESS_END_DATE >= "$RPEnddate" OR trust2.BUSINESS_END_DATE IS NULL)
# MAGIC AND
# MAGIC trust2.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (trust2.ORG_CLOSE_DATE IS NULL OR trust2.ORG_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC trust2.ORG_TYPE_CODE = 'TR'
# MAGIC AND
# MAGIC (lregion.REL_CLOSE_DATE IS NULL OR lregion.REL_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC lregion.REL_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC lregion.REL_TO_ORG_TYPE_CODE = case when '$RPEnddate' <= '2020-03-31' then 'CF' else 'ST' end --takes account of switch to STP as local region
# MAGIC AND
# MAGIC lregion_Name.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (lregion_Name.ORG_CLOSE_DATE IS NULL OR lregion_Name.ORG_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC lregion_Name.BUSINESS_START_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (lregion_Name.BUSINESS_END_DATE IS NULL OR lregion_Name.BUSINESS_END_DATE >= "$RPEnddate")
# MAGIC

# COMMAND ----------

# DBTITLE 1,Region on local region
# MAGIC %sql
# MAGIC
# MAGIC --Temporary table for all local region reference codes, local region organisation type, local region name and region name
# MAGIC CREATE OR REPLACE VIEW $outSchema.regiononlregion
# MAGIC
# MAGIC AS SELECT
# MAGIC -- Fields populated from dss_corporate
# MAGIC LocalRegion2.ORG_CODE AS LRegionORG,
# MAGIC LocalRegion2.ORG_TYPE_CODE AS LRegionORGTYPE,
# MAGIC LocalRegion2.NAME AS LocalRegion2,
# MAGIC Region_Name.ORG_CODE AS RegionORG,
# MAGIC Region_Name.ORG_TYPE_CODE AS RegionORGTYPE,
# MAGIC Region_Name.NAME AS Region
# MAGIC  
# MAGIC FROM
# MAGIC $dss_corporate.ORG_DAILY AS LocalRegion2
# MAGIC LEFT JOIN $dss_corporate.org_relationship_daily AS Region ON LocalRegion2.ORG_CODE = Region.REL_FROM_ORG_CODE
# MAGIC LEFT JOIN $dss_corporate.org_daily AS Region_Name ON Region.REL_TO_ORG_CODE = Region_Name.ORG_CODE
# MAGIC
# MAGIC --Only select local regions and regions that are active on the last day of the month as defined by widgets above
# MAGIC WHERE
# MAGIC LocalRegion2.BUSINESS_START_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (LocalRegion2.BUSINESS_END_DATE >= "$RPEnddate" OR LocalRegion2.BUSINESS_END_DATE IS NULL)
# MAGIC AND
# MAGIC LocalRegion2.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (LocalRegion2.ORG_CLOSE_DATE IS NULL OR LocalRegion2.ORG_CLOSE_DATE>= "$RPEnddate")
# MAGIC AND
# MAGIC LocalRegion2.ORG_TYPE_CODE = case when '$RPEnddate' <= '2020-03-31' then 'CF' else 'ST' end --takes account of switch to STP as local region
# MAGIC AND
# MAGIC (Region.REL_CLOSE_DATE IS NULL OR Region.REL_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC Region.REL_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC Region.REL_TO_ORG_TYPE_CODE = 'CE'
# MAGIC AND
# MAGIC Region_Name.ORG_OPEN_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (Region_Name.ORG_CLOSE_DATE IS NULL OR Region_Name.ORG_CLOSE_DATE >= "$RPEnddate")
# MAGIC AND
# MAGIC Region_Name.BUSINESS_START_DATE <= "$RPEnddate"
# MAGIC AND
# MAGIC (Region_Name.BUSINESS_END_DATE IS NULL OR Region_Name.BUSINESS_END_DATE >= "$RPEnddate")

# COMMAND ----------

# DBTITLE 1,link hosp/trust/lreg/reg
# MAGIC %sql
# MAGIC --This creates a table which links hospital, trust, local region and region. All org codes and org type codes are shown
# MAGIC -- For this notebook to work all books above must be run to generate the tables required here.
# MAGIC CREATE OR REPLACE VIEW $outSchema.geoghtlrr
# MAGIC
# MAGIC --Select the fields from trustonhospital, lregionontrust and regiononlegion
# MAGIC AS SELECT 
# MAGIC Hospital_ORG,
# MAGIC Hospital_ORGTYPE,
# MAGIC Hospital_Name,
# MAGIC Trust_ORG,
# MAGIC Trust_ORGTYPE,
# MAGIC Trust,
# MAGIC LRegionORG,
# MAGIC LRegionORGTYPE,
# MAGIC LRegion,
# MAGIC RegionORG,
# MAGIC RegionORGTYPE,
# MAGIC Region
# MAGIC
# MAGIC FROM $outSchema.trustonhospital
# MAGIC LEFT JOIN $outSchema.lregionontrust ON trustonhospital.Trust = lregionontrust.Trust2
# MAGIC LEFT JOIN $outSchema.regiononlregion ON lregionontrust.LRegion = regiononlregion.LocalRegion2
# MAGIC
# MAGIC -- Trust that are not part of count are removed
# MAGIC WHERE Trust NOT IN ('PUBLIC HEALTH WALES NHS TRUST','VELINDRE NHS TRUST','WELSH AMBULANCE SERVICES NHS TRUST')
# MAGIC
# MAGIC ORDER BY region, lregion, trust

# COMMAND ----------

# DBTITLE 1,link trust/lreg/reg
# MAGIC %sql
# MAGIC -- This creates a table which links trust, local region and region. All org codes and org type codes are shown. 
# MAGIC -- For this notebook to work all books above must be run to generate the tables required here.
# MAGIC CREATE OR REPLACE VIEW $outSchema.geogtlrr
# MAGIC
# MAGIC -- select the fields from lregionontrust and regiononlegion and join in the MBBRACE groups
# MAGIC AS SELECT DISTINCT
# MAGIC Trust_ORG,
# MAGIC Trust_ORGTYPE,
# MAGIC Trust,
# MAGIC LRegionORG,
# MAGIC LRegionORGTYPE,
# MAGIC LRegion,
# MAGIC RegionORG,
# MAGIC RegionORGTYPE,
# MAGIC Region,
# MAGIC COALESCE(MB.Mbrrace_Grouping, 'Group 6. Unknown') as Mbrrace_Grouping,
# MAGIC COALESCE(MB.Mbrrace_Grouping_Short, 'Group 6. Unknown') as Mbrrace_Grouping_Short,
# MAGIC --Since July, all STP are referred as ICB, so picking the corresponding field. Maintaining the column name as geogtlrr have many instance of using the column names and to avoid any breaks.
# MAGIC  CASE
# MAGIC   WHEN $month_id <= 1467  then O.STP_Code
# MAGIC   ELSE LRegionORG
# MAGIC   END as STP_Code,
# MAGIC  CASE
# MAGIC   WHEN $month_id <= 1467  then O.STP_Name
# MAGIC   ELSE LRegion
# MAGIC   END as STP_Name
# MAGIC
# MAGIC From $outSchema.trustonhospital
# MAGIC LEFT JOIN $outSchema.lregionontrust on trustonhospital.Trust = lregionontrust.Trust2
# MAGIC LEFT JOIN $outSchema.regiononlregion on lregionontrust.LRegion = regiononlregion.LocalRegion2
# MAGIC LEFT JOIN 	(
# MAGIC 				SELECT
# MAGIC 				row_number() over (partition by org_code order by case when rel_close_date is null then 1 else 0 end desc, REL_CLOSE_DATE DESC) AS RN,
# MAGIC 				ORG_CODE,
# MAGIC 				Mbrrace_Grouping, 
# MAGIC 				Mbrrace_Grouping_Short
# MAGIC 				FROM $dss_corporate.maternity_mbrrace_groups
# MAGIC 				WHERE REL_OPEN_DATE <= "$RPEnddate"
# MAGIC 			) MB
# MAGIC 			ON lregionontrust.Trust_ORG = MB.ORG_CODE
# MAGIC 			AND MB.rn=1
# MAGIC 		
# MAGIC LEFT JOIN 	(
# MAGIC 				SELECT Y.CODE, Y.POSTCODE FROM
# MAGIC 					(SELECT
# MAGIC 						CODE, POSTCODE,
# MAGIC 						ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY DSS_RECORD_END_DATE) AS RN
# MAGIC 						FROM $dss_corporate.ods_nhs_trust
# MAGIC 						where dss_record_end_date is null or "$RPEnddate" between DSS_RECORD_START_DATE AND DSS_RECORD_END_DATE) AS Y
# MAGIC 				WHERE Y.RN=1 ) T ON T.CODE = lregionontrust.Trust_ORG
# MAGIC                 
# MAGIC LEFT JOIN 	(
# MAGIC 				SELECT DISTINCT	
# MAGIC 					A.PCDS 
# MAGIC 					,A.LSOA11
# MAGIC 					
# MAGIC 				FROM $dss_corporate.postcode A						
# MAGIC 					JOIN (	
# MAGIC 							SELECT 
# MAGIC 							MAX(SYSTEM_UPDATED_DATE) AS SYSTEM_UPDATED_DATE,
# MAGIC 							PCDS
# MAGIC 							FROM $dss_corporate.postcode
# MAGIC 							GROUP BY PCDS
# MAGIC 						 ) P
# MAGIC 					ON P.SYSTEM_UPDATED_DATE=A.SYSTEM_UPDATED_DATE AND P.PCDS=A.PCDS
# MAGIC 				) P 
# MAGIC 				ON P.PCDS=T.POSTCODE
# MAGIC LEFT JOIN	(
# MAGIC 				SELECT
# MAGIC 					y.lsoacd as LSOA,
# MAGIC 					y.stpcd  as STP_Code,
# MAGIC 					y.stpnm  as STP_Name
# MAGIC                    
# MAGIC 				FROM $dss_corporate.ONS_LSOA_CCG_STP_LAD_V01 Y
# MAGIC 					JOIN (
# MAGIC 							SELECT 
# MAGIC 							MAX(x.DSS_RECORD_START_DATE) AS DSS_RECORD_START_DATE,
# MAGIC 							X.LSOACD
# MAGIC 							FROM $dss_corporate.ONS_LSOA_CCG_STP_LAD_V01 x
# MAGIC 							GROUP BY x.LSOACD
# MAGIC 						  ) V
# MAGIC 						ON V.DSS_RECORD_START_DATE = Y.DSS_RECORD_START_DATE
# MAGIC 						AND V.LSOACD = Y.LSOACD
# MAGIC 			) O
# MAGIC 			ON O.LSOA = P.LSOA11
# MAGIC 			
# MAGIC WHERE Trust NOT IN ('PUBLIC HEALTH WALES NHS TRUST','VELINDRE NHS TRUST','WELSH AMBULANCE SERVICES NHS TRUST')
# MAGIC ORDER BY region, lregion, trust

# COMMAND ----------

# DBTITLE 1,Check Geographies at one-to-one with trust code
# GBT updated to remove pandas
outSchema = dbutils.widgets.get("outSchema")
dss_corporate = dbutils.widgets.get("dss_corporate")

SQLCODE = """select trust_org
from {outSchema}.geogtlrr
group by trust_org
having count(*) > 1""".format(outSchema=outSchema)

# GBT test non-blank scenario
# SQLCODE = """select trust_org
# from {outputSchema}.geogtlrr
# group by trust_org
# having count(*) = 1""".format(outputSchema=outputSchema)

pdf = spark.sql(SQLCODE)


#GBT
if pdf.count() > 0:
  checkIssues = "Error - Geogs not one-to-one at trust level"
  display(pdf)
  dbutils.notebook.exit("Notebook: Geographies issue: {checkIssues}".format(checkIssues=checkIssues))
else:
  dbutils.notebook.exit("Notebook: Geographies Ran Successfully")

##
# if not pdf.empty:
#   checkIssues = "Error - Geogs not one-to-one at trust level"
#   display(pdf)
#   dbutils.notebook.exit("Notebook: Geographies issue: {checkIssues}".format(checkIssues=checkIssues))
# else:
#   dbutils.notebook.exit("Notebook: Geographies Run Successfully")
