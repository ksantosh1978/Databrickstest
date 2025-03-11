# Databricks notebook source

expected_schema_json = {
  "birthsbasetable": [
    {"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Baby", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "TotalBabies", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ApgarScore5TermGroup7", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "BabyFirstFeedBreastMilkStatus", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "BirthweightTermGroup", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "GestationLengthBirth", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "GestationLengthBirthGroup37", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PlaceTypeActualDeliveryMidwifery", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "SkinToSkinContact1HourTerm", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RobsonGroup", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Trust_ORG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Trust_ORGTYPE", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Trust", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LRegionORG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LRegionORGTYPE", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LRegion", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RegionORG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RegionORGTYPE", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Region", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mbrrace_Grouping", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mbrrace_Grouping_Short", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "STP_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "STP_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "DeliverySiteCode", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "DeliverySiteName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mother_CCG_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mother_CCG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mother_LAD_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mother_LAD", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "BirthWeightTermGroup2500", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "BirthWeightRank", "nullable": True, "type": "integer"}
  ]
  ,"CoC_Numerator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PregnancyID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CareConID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Rank", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "rank_imd_decile", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ethniccategory", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ethnicgroup", "nullable": True, "type": "string"}
  ]
  ,"CoC_Denominator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PregnancyID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CareConID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Rank", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "rank_imd_decile", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ethniccategory", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ethnicgroup", "nullable": True, "type": "string"}
  ]
  ,"CoC_Provider_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
  ,"CoC_Geography_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
  ,"CoC_CSV": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Value", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
  ,"SLB_Numerator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Mother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "UniqPregID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Baby", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RecordNumber", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LADCD", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LADNM", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteIDBooking", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteNameBooking", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CCGResponsibilityMother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CCGName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteIDActualDelivery", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteNameActualDelivery", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
  ,"SLB_Denominator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Mother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "UniqPregID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Baby", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RecordNumber", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LADCD", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LADNM", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteIDBooking", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteNameBooking", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CCGResponsibilityMother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CCGName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteIDActualDelivery", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgSiteNameActualDelivery", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
  ,"SLB_Provider_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
  ,"SLB_Geography_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
  ,"SLB_CSV": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Value", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
}
expected_tables = expected_schema_json.keys();
print(expected_schema_json.keys())

# COMMAND ----------

db_output = dbutils.widgets.get("db")
source = dbutils.widgets.get("mat_pre_clear")
#db_output = "mat_analysis"

# COMMAND ----------

# DBTITLE 1,check if all tables are created
tables_meta_df = spark.sql("show tables in {db_output}".format(db_output=db_output))
actual_tables = [ r['tableName'] for r in tables_meta_df.collect()]
print(actual_tables)
print(expected_tables)
#for exp_table in expected_tables:
#  assert exp_table in actual_tables


# COMMAND ----------

# DBTITLE 1,check table with expected schema
import json

actual_schema = {}
error_list = []
for table_name in expected_tables:
  df = spark.table(f"{db_output}.{table_name}")
  # Save schema from the original DataFrame into json:
  schema_json = df.schema.json()
  actual_schema[table_name] = json.loads(schema_json).get("fields")
  try:
    print(table_name)
    print(actual_schema.get(table_name))
    print('------------------------------------------------------')
    assert actual_schema.get(table_name) == expected_schema_json.get(table_name), "schema validation failed for {}".format(table_name)
  except AssertionError as e:
    # add to error list to cehck all errors
    error_list.append(e)
    print(e)
    continue
assert not error_list

# COMMAND ----------

# DBTITLE 1,NMPA tables
# NMPA table schema
expected_schema_json_NMPA = {
  "NMPA_TRANSFER_IN_BIRTH_SETTING_RAW": [
    {"metadata": {}, "name": "UniqPregID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Mother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RecordNumber", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "SettingIntraCare", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LabourDeliveryID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PersonBirthDateBaby", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "SettingPlaceBirth", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "rowNum", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]

  ,"NMPA_TRANSFER_IN_BIRTH_SETTING_RAW_GEOGRAPHY": [
     {"metadata": {}, "name": "UniqPregID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Mother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RecordNumber", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "SettingIntraCare", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "LabourDeliveryID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PersonBirthDateBaby", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "SettingPlaceBirth", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "rowNum", "nullable": True, "type": "long"}    
    ,{"metadata": {}, "name": "Trust_ORG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Trust", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RegionORG", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Region", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mbrrace_Grouping_Short", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Mbrrace_Grouping", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "STP_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "STP_Name", "nullable": True, "type": "string"}    
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
    
  ,"NMPA_Transfer_Home_Obstetric_Denominator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]
  
 ,"NMPA_Transfer_FMU_Obstetric_Denominator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]
  
   ,"NMPA_Transfer_AMU_Obstetric_Denominator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]
   ,"NMPA_Transfer_Midwife_Obstetric_Denominator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]
  
  ,"NMPA_Transfer_Home_Obstetric_Numerator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]

    ,"NMPA_Transfer_FMU_Obstetric_Numerator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]

    ,"NMPA_Transfer_AMU_Obstetric_Numerator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]

    ,"NMPA_Transfer_Midwife_Obstetric_Numerator": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "long"}
  ]

  
  ,"NMPA_Transfer_Home_Obstetric_RATE": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "float"}
  ]
  
 ,"NMPA_Transfer_FMU_Obstetric_RATE": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "float"}
  ]
  
   ,"NMPA_Transfer_AMU_Obstetric_RATE": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "float"}
  ]
   ,"NMPA_Transfer_Midwife_Obstetric_RATE": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "float"}
  ]
  
  ,"NMPA_CSV": [
    {"metadata": {}, "name": "Org_Code", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Name", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Org_Level", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodStartDate", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "ReportingPeriodEndDate", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "VALUE", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]

  ,"aspirin_code_reference": [
    {"metadata": {}, "name": "Area", "nullable": False, "type": "string"}
    ,{"metadata": {}, "name": "Code", "nullable": False, "type": "string"}
    ,{"metadata": {}, "name": "Description", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Code_format", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "msds_table_to_query", "nullable": False, "type": "string"}
    ,{"metadata": {'comment': 'Some submitters are using variations of the ICD-10 codes such as O11X or O11X- or O11.0 when referring to O11. Indicates whether the code has this variation or not. Note: A side effect of this is that some icd10 code descriptions will not be valid, which does not matter that much since we are mainly interested in the codes'}, "name": "Typo", "nullable": True, "type": "boolean"}
  ]
  
  ,"nmpa_denominator_raw": [
    {'metadata': {}, 'name': 'KeyValue', 'nullable': True, 'type': 'long'}
    ,{'metadata': {}, 'name': 'RPStartDate', 'nullable': True, 'type': 'date'}
    ,{'metadata': {}, 'name': 'RPEndDate', 'nullable': True, 'type': 'date'}
    ,{'metadata': {}, 'name': 'Person_ID', 'nullable': True, 'type': 'string'}
    ,{'metadata': {}, 'name': 'PregnancyID', 'nullable': True, 'type': 'string'}
    ,{'metadata': {}, 'name': 'OrgCodeProvider', 'nullable': True, 'type': 'string'}
    ,{'metadata': {}, 'name': 'Rank', 'nullable': True, 'type': 'integer'}
  ]
}
expected_tables_NMPA = expected_schema_json_NMPA.keys();
print(expected_schema_json_NMPA.keys())

# COMMAND ----------

# DBTITLE 1,NMPA - check table with expected schema
import json

actual_schema = {}
error_list_NMPA = []
for table_name in expected_tables_NMPA:
  df = spark.table(f"{db_output}.{table_name}")
  # Save schema from the original DataFrame into json:
  schema_json = df.schema.json()
  actual_schema[table_name] = json.loads(schema_json).get("fields")
  try:
    print(table_name)
    print(actual_schema.get(table_name))
    print('------------------------------------------------------')
    assert actual_schema.get(table_name) == expected_schema_json_NMPA.get(table_name), "schema validation failed for {}".format(table_name)
  except AssertionError as e:
    # add to error list to cehck all errors
    error_list_NMPA.append(e)
    print(e)
    continue
assert not error_list_NMPA

# COMMAND ----------

# DBTITLE 1,Measures - check table with expected schema
# Measures table schema
expected_schema_json_Measures = {
   "measures_raw": [
     {'metadata': {}, 'name': 'KeyValue', 'nullable': True, 'type': 'long'}
     ,{'metadata': {}, 'name': 'RPStartDate', 'nullable': True, 'type': 'date'}
     ,{'metadata': {}, 'name': 'RPEndDate', 'nullable': True, 'type': 'date'}
     ,{'metadata': {}, 'name': 'IndicatorFamily', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'Indicator', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'isNumerator', 'nullable': True, 'type': 'integer'}
     ,{'metadata': {}, 'name': 'isDenominator', 'nullable': True, 'type': 'integer'}
     ,{'metadata': {}, 'name': 'Person_ID', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'PregnancyID', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'OrgCodeProvider', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'Rank', 'nullable': True, 'type': 'integer'}
     ,{'metadata': {}, 'name': 'IsOverDQThreshold', 'nullable': True, 'type': 'integer'}
     ,{'metadata': {}, 'name': 'CreatedAt', 'nullable': True, 'type': 'timestamp'}
     ,{'metadata': {}, 'name': 'Rank_IMD_Decile', 'nullable': True, 'type': 'string'}
     ,{'metadata': {}, 'name': 'EthnicCategory', 'nullable': True, 'type': 'string'}
     , {'metadata': {}, 'name': 'EthnicGroup', 'nullable': True, 'type': 'string'}
  ],
   "Measures_Aggregated": [
     {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}  
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}   
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}    
    ,{"metadata": {}, "name": "IsOverDQThreshold", "nullable": True, "type": "integer"}   
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}     
  ],
   "Measures_CSV": [
     {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Value", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"} 
  ]
}
expected_tables_Measures = expected_schema_json_Measures.keys();
print(expected_tables_Measures)

# COMMAND ----------

import json

actual_schema = {}
error_list_measures = []
for table_name in expected_tables_Measures:
  df = spark.table(f"{db_output}.{table_name}")
  # Save schema from the original DataFrame into json:
  schema_json = df.schema.json()
  actual_schema[table_name] = json.loads(schema_json).get("fields")
  try:
    print(table_name)
    print(actual_schema.get(table_name))
    print('hello-BS-----------------')
    print(expected_schema_json_Measures.get(table_name))
    print('------------------------------------------------------')
    assert actual_schema.get(table_name) == expected_schema_json_Measures.get(table_name), "schema validation failed for {}".format(table_name)
  except AssertionError as e:
    # add to error list to cehck all errors
    error_list_pcsp.append(e)
    print(e)
    continue
assert not error_list_measures

# COMMAND ----------

# DBTITLE 1,PCSP tables
# PCSP table schema
  
expected_schema_json_pcsp = {
 "PCSP_Numerator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PregnancyID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Rank", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "Rank_IMD_Decile", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "EthnicCategory", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "EthnicGroup", "nullable": True, "type": "string"}
  ]
  ,"PCSP_Denominator_Raw": [
    {"metadata": {}, "name": "KeyValue", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "PregnancyID", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Rank", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "Rank_IMD_Decile", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "EthnicCategory", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "EthnicGroup", "nullable": True, "type": "string"}
  ]
  ,"PCSP_Provider_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "OverDQThreshold", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}
  ]
  ,"PCSP_Geography_Aggregated": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Unrounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Unrounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Numerator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Denominator", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "Rounded_Rate", "nullable": True, "type": "float"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
  ,"PCSP_CSV": [
    {"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "Status", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Indicator", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "IndicatorFamily", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgName", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgLevel", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Currency", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Value", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "CreatedAt", "nullable": True, "type": "timestamp"}  
  ]
         
}
expected_tables_pcsp = expected_schema_json_pcsp.keys();
print(expected_schema_json_pcsp.keys())

# COMMAND ----------

# DBTITLE 1,PCSP - check table with expected schema
import json

actual_schema = {}
error_list_pcsp = []
for table_name in expected_tables_pcsp:
  df = spark.table(f"{db_output}.{table_name}")
  # Save schema from the original DataFrame into json:
  schema_json = df.schema.json()
  actual_schema[table_name] = json.loads(schema_json).get("fields")
  try:
    print(table_name)
    print(actual_schema.get(table_name))
    print('hello-BS-----------------')
    print(expected_schema_json_pcsp.get(table_name))
    print('------------------------------------------------------')
    assert actual_schema.get(table_name) == expected_schema_json_pcsp.get(table_name), "schema validation failed for {}".format(table_name)
  except AssertionError as e:
    # add to error list to cehck all errors
    error_list_pcsp.append(e)
    print(e)
    continue
assert not error_list_pcsp



# COMMAND ----------

expected_schema_json_preclear = {
 "msd002gp": [
    {"metadata": {}, "name": "EFFECTIVE_TO", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "CCGResponsibilityMother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Effective_From", "nullable": True, "type": "timestamp"}
    ,{"metadata": {}, "name": "EndDateGMPReg", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "LPIDMother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "MSD002_ID", "nullable": True, "type": "decimal(20,0)"}
    ,{"metadata": {}, "name": "OrgCodeGMPMother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgCodeProvider", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgIDGPPrac", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgIDICBGPPractice", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "OrgIDSubICBLocGP", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "Person_ID_Mother", "nullable": True, "type": "string"}
    ,{"metadata": {}, "name": "RPEndDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RPStartDate", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "RecordNumber", "nullable": True, "type": "long"}
    ,{"metadata": {}, "name": "RowNumber", "nullable": True, "type": "integer"}
    ,{"metadata": {}, "name": "StartDateGMPReg", "nullable": True, "type": "date"}
    ,{"metadata": {}, "name": "UniqSubmissionID", "nullable": True, "type": "long"}
  ]
}

expected_tables_preclear = expected_schema_json_preclear.keys();
print(expected_schema_json_preclear.keys())

# COMMAND ----------

import json

actual_schema = {}
error_list_preclear = []
for table_name in expected_tables_preclear:
  df = spark.table(f"{source}.{table_name}")
  # Save schema from the original DataFrame into json:
  schema_json = df.schema.json()
  actual_schema[table_name] = json.loads(schema_json).get("fields")
  try:
    print(table_name)
    print(actual_schema.get(table_name))
    print(expected_schema_json_preclear.get(table_name))
    print('------------------------------------------------------')
    assert actual_schema.get(table_name) == expected_schema_json_preclear.get(table_name), "schema validation failed for {}".format(table_name)
  except AssertionError as e:
    # add to error list to cehck all errors
    error_list_preclear.append(e)
    print(e)
    continue
assert not error_list_preclear
