# Databricks notebook source
from datetime import datetime
import copy

# COMMAND ----------

# DBTITLE 1,Get widget values
PeriodStart = dbutils.widgets.get("RPBegindate")
PeriodEnd = dbutils.widgets.get("RPEnddate")
outSchema = dbutils.widgets.get("outSchema")
month_id = dbutils.widgets.get("month_id")
print(PeriodEnd)
period_end_as_date =  datetime.strptime(PeriodEnd, "%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Define base geographies
#org level, org code column name, org name column name
# TODO: do we need Org_Geog_Code? it is in existing csv (e.g. October data)
# All tables have these columns
geogDictBase = {
  "'National'" : {"Org_Level":"'National'", "Org_Name_Column":"'All Submitters'"}
  ,"Trust_ORG" : {"Org_Level":"'Provider'", "Org_Name_Column":"Trust"} 
  ,"RegionORG" : {"Org_Level":"'NHS England (Region)'", "Org_Name_Column":"Region"}
  ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Org_Name_Column":"MBRRACE_Grouping"}
  ,"Mother_CCG_Code" : {"Org_Level": "'CCG of Responsibility'", "Org_Name_Column":"Mother_CCG"} 
  ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Org_Name_Column":"STP_Name"}
  ,"Mother_LAD_Code" : {"Org_Level": "'Local Authority of Residence'", "Org_Name_Column":"Mother_LAD"} 
}

# Since July, all CCG are referred as SubICB, so picking the corresponding field
if int(month_id) > 1467:
  geogDictBase['Mother_CCG_Code']['Org_Level'] = "'SubICB of Responsibility'"
  
# from 01/04/2020 Region Local Office is replaced by STP. MSDS already has STP from the LMS, so removed post 01/04/2020. Added back pre-01/04/2020 to allow re-runs of previous data.
if period_end_as_date < datetime.strptime('2020-04-01', "%Y-%m-%d"):
  add_lregion={"LRegionORG" : {"Org_Level":"'NHS England (Region, Local Office)'", "Org_Name_Column":"LRegion"}}
  geogDictBase.update(add_lregion)


# COMMAND ----------

print(geogDictBase)

# COMMAND ----------

# DBTITLE 1,Define dictionaries for Bookings and Deliveries from base dict
#only bookings has booking site name
geogDictBookings =  copy.deepcopy(geogDictBase)
a={"BookingSiteCode":{"Org_Level":"'Booking Site'", "Org_Name_Column":"BookingSiteName"}}
geogDictBookings.update(a)


#only babies/delivery related tables have delivery site
geogDictBabies =  copy.deepcopy(geogDictBase)
d={"DeliverySiteCode" : {"Org_Level":"'Delivery Site'", "Org_Name_Column":"DeliverySiteName"}}
geogDictBabies.update(d)


# COMMAND ----------

# DBTITLE 1,Define dictionary of all the tables and the measures with their aggregation
#TODO: confirm all measures are added to this
#TODO: populate/check "Count_of" metadata

dictTblsFldsAggs = {
  "{outSchema}.bookingsbasetable".format(outSchema=outSchema):{
    "TotalBookings":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"AgeAtBookingMotherAvg":{"Agg":"Avg", "AggField":"AgeAtBookingMother", "count_of": "Average over women", "geogs": geogDictBookings}
    ,"AgeAtBookingMotherGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    #,"BMI":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"ComplexSocialFactorsInd":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"DeprivationDecileAtBooking":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"EthnicCategoryMotherGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"GestAgeFormalAntenatalBookingGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    #,"CO_Concentration_Booking":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings} #confirm
    ,"SmokingStatusGroupBooking":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings} #confirm
#     ,"SmokingStatusGroupDelivery":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings} #confirm
    ,"FolicAcidSupplement":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings} #confirm
    #,"AlcoholUnitsPerWeekBand":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBase} #confirm
    ,"PreviousCaesareanSectionsGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    ,"PreviousLiveBirthsGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBookings}
    
  },
  "{outSchema}.birthsbasetable".format(outSchema=outSchema):{
    "TotalBabies":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"ApgarScore5TermGroup7":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"BabyFirstFeedBreastMilkStatus":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"BirthweightTermGroup":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies, "WhereClause": "BirthWeightRank = 1"}
    ,"BirthweightTermGroup2500":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies, "WhereClause": "BirthWeightRank = 1"}
    ,"GestationLengthBirth":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"GestationLengthBirthGroup37":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"PlaceTypeActualDeliveryMidwifery":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    #,"RobsonGroup":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
    ,"SkinToSkinContact1HourTerm":{"Agg":"Count", "AggField":"Person_ID_Baby", "count_of": "Babies", "geogs": geogDictBabies}
  },
  "{outSchema}.DeliveryBaseTable".format(outSchema=outSchema):{
    "TotalDeliveries":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBabies}
    ,"DeliveryMethodBabyGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBabies}
    ,"GenitalTractTraumaticLesion":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBabies, "WhereClause": "GenitalTractTraumaticLesion IS NOT NULL"}
    ,"GenitalTractTraumaticLesionGroup":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBabies, "WhereClause": "GenitalTractTraumaticLesionGroup IS NOT NULL"}
    ,"CO_Concentration_Delivery":{"Agg":"Count", "AggField":"Person_ID_Mother", "count_of": "Women", "geogs": geogDictBabies}
  }
  ,
  "{outSchema}.careplanbasetable".format(outSchema=outSchema):{
    "CCP_Any_Pathways":{"Agg":"Count", "AggField":"CCP_Any_PathwaysCount", "count_of": "Women", "geogs": geogDictBookings},
    "PCP_Any_Pathways":{"Agg":"Count", "AggField":"PCP_Any_PathwaysCount", "count_of": "Women", "geogs": geogDictBookings},
    "CCP_Antenatal":{"Agg":"Count", "AggField":"CCP_AntenatalCount", "count_of": "Women", "geogs": geogDictBookings},
    "PCP_Antenatal":{"Agg":"Count", "AggField":"PCP_AntenatalCount", "count_of": "Women", "geogs": geogDictBookings},
    "CCP_Birth":{"Agg":"Count", "AggField":"CCP_BirthCount", "count_of": "Women", "geogs": geogDictBookings},
    "PCP_Birth":{"Agg":"Count", "AggField":"PCP_BirthCount", "count_of": "Women", "geogs": geogDictBookings},
    "CCP_Postpartum":{"Agg":"Count", "AggField":"CCP_PostpartumCount", "count_of": "Women", "geogs": geogDictBookings},
    "PCP_Postpartum":{"Agg":"Count", "AggField":"PCP_PostpartumCount", "count_of": "Women", "geogs": geogDictBookings}
  }
  
}


# COMMAND ----------

# DBTITLE 1,SQL builder function
#TODO: measure descriptions, org name, org_level, period, count_of
#TODO: improvement - add a function to skip grouping where a measure doesn't require it. Currently handled with blank "group" in the base table.

def SQLBuilder(aggMethod: str, aggCol: str, measure: str, aggLevel: str, outTbl: str, inTbl: str, PeriodStart: str, PeriodEnd: str, Count_Of: str, Org_Name: str, Org_Level: str, whereClause: str)->str:
  """
  Build a SQL statement to insert rows to aggregate table with grouping if required.
  
  This function will output a SQL statement to insert aggregated information into a table. Aggregation is done at National level (i.e. no additional grouping) or by the column specified.
  Counts are automatically converted into Count(distinct ....).
  
  Parameters
  ----------
  aggMethod : str
    The aggregation method to pass into the statement. e.g. sum, count, avg, max, min.
  aggCol : str
    The column which will be operated on by the aggregatoin method.
  measure : str
    The name of the measure and the name of the column containing the main derived group in the base table.
  aggLevel : str
    The geography/grouping at which to also aggregate the measure. Can also be "National" to perform no grouping, i.e. return for all data.
  outTbl: str
    The destination table for the inserted data.
  inTbl: str
    The input base table where the data to be aggregated sits.
  PeriodStart: str
    A string representing the start date of the data period being run
  PeriodEnd: str
    A string representing the end date of the data period being run
  Count_Of: str
    Human-friendly meta-data describing what the measure is counting or how it's calculated
  Org_Name: str
    The name of the column in the data which holds the organisation's name data
  Org_Level: str
    Meta-data which describes the aggregation level which is produced. For example, 'Provider'
  whereClause: str
    Optional where clause
    
  Returns
  -------
  out : str
    The SQL select statement ready for execution
  
  Examples
  --------
  >>> SQLBuilder()
  
  """
    
    # ensure only distinct values of the countable field are counted
  if aggMethod == "Count": 
    aggMethod = "Count(distinct "
  else: 
    aggMethod = aggMethod + "("
    
  Dimension = measure
  Measure = measure
  Measure_Desc = 'Null' #TODO: is this required?
  Value_Unsuppressed = aggMethod + aggCol + ")"
  if whereClause != '':
    whereClause = ' where ' + whereClause
 
  # No additional grouping if National measure and set Org_Code for national level
  if aggLevel == "'National'":
    grpStmt = measure
    Org_Code = "'ALL'"
  else:
    grpStmt = "{measure}, {aggLevel}, {Org_Name}".format(measure=measure, aggLevel=aggLevel, Org_Name=Org_Name)
    Org_Code = aggLevel
  
  #TODO: add in PeriodEnd - needs change to csv table underlying
  
  selCols = "{Count_Of}, {PeriodStart}, {PeriodEnd}, '{Dimension}', {Org_Code}, UPPER({Org_Name}), {Org_Level}, {Measure}, {Measure_Desc}, {Value_Unsuppressed}".format(Count_Of=Count_Of, PeriodStart=PeriodStart, PeriodEnd=PeriodEnd, Dimension=Dimension, Org_Code=Org_Code, Org_Name=Org_Name, Org_Level=Org_Level, Measure=Measure, Measure_Desc=Measure_Desc, Value_Unsuppressed=Value_Unsuppressed)  
  sqlStmt = "select {selCols} from {inTbl}{where} group by {grpStmt} having {Value_Unsuppressed} > 0".format(selCols=selCols, inTbl=inTbl, where=whereClause, grpStmt=grpStmt, Value_Unsuppressed=Value_Unsuppressed) 
  return(sqlStmt)

# COMMAND ----------

# DBTITLE 1,Calculate expected number of SQL statements to execute
cnt = 0

for key, value in dictTblsFldsAggs.items():
  for k, v in value.items():
    numbergeogs = len(v["geogs"])
    cnt = cnt + numbergeogs  

  
expectedAggs = cnt

print("We expect to see {expectedAggs} calculations run".format(expectedAggs=expectedAggs))

# COMMAND ----------

# DBTITLE 1,Loop through the measures and geographies and execute the SQL
# running as "debug" in Prod to ensure some measures populate even in event of failure.

outTbl = "{outSchema}.maternity_monthly_csv_sql".format(outSchema=outSchema) 
debug = True
debugExec = True
bigSQL = ""
PeriodStart = "'{PeriodStart}'".format(PeriodStart=PeriodStart)
PeriodEnd = "'{PeriodEnd}'".format(PeriodEnd=PeriodEnd)
counter = 0

for Table, value in dictTblsFldsAggs.items():
  print("Table = {}".format(Table))
  print("*"*200)
  
  for key, value in value.items():
    measure = key
    AggField = value["AggField"]    
    Agg = value["Agg"]
    geogDict = value["geogs"]
    countof = value["count_of"] 
    countof = "'{countof}'".format(countof=countof)
    if "WhereClause" in value:
      whereclause = value["WhereClause"]
    else:
      whereclause = ''    
    
    
    for k, v in geogDict.items():
      orgname = v["Org_Name_Column"]
      orglevel = v["Org_Level"]
      
      sqlStmt = SQLBuilder(Agg, AggField, measure, k, outTbl, Table, PeriodStart, PeriodEnd, countof, orgname, orglevel, whereclause)
      if debug:
        counter = counter + 1
        print("{k} level. Executing {counter}/{expectedAggs}. Started: {now}".format(k=k, counter=counter, expectedAggs=expectedAggs, now=datetime.now()))
        print("--------------------------------")
        sqlStmt = "insert into {outTbl} {sqlStmt}".format(outTbl=outTbl, sqlStmt=sqlStmt)
        print(sqlStmt)
        if debugExec:
          spark.sql(sqlStmt)
        print("--------------------------------")
        print("{k} level. Ended: {now}".format(k=k, now=datetime.now()))
        print("-------------DONE---------------")
      else:
        if not bigSQL:
          bigSQL = "insert into {outTbl} ".format(outTbl=outTbl) + sqlStmt
        else:
          bigSQL = bigSQL + " UNION " + sqlStmt

if not debug:
  print(">>>>> not debugging")
  print("*"*200)
  print(bigSQL)
  spark.sql(bigSQL)       

# COMMAND ----------

# fail process if debugExec parameter is set to not execute code - used for debugging, but must not be promoted
assert debug, "Error - debug in LoopThroughTablesMeasuresGeogs should be set to True for end to end runs."
assert debugExec, "Error - debugExec in LoopThroughTablesMeasuresGeogs should be set to True for end to end runs."

# COMMAND ----------

SQL = "select count(*) as CheckCount from {outSchema}.maternity_monthly_csv_sql".format(outSchema=outSchema)
CheckCount = spark.sql(SQL).collect()[0][0]

# COMMAND ----------

dbutils.notebook.exit("Notebook: Run Successful and {CheckCount} rows available in {outTbl}".format(CheckCount=CheckCount, outTbl=outTbl)) # This will return a value with text specified