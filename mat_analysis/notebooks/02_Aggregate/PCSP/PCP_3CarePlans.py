# Databricks notebook source
RPBegindate = dbutils.widgets.get("RPBegindate")
print(RPBegindate)
assert(RPBegindate)
RPEnddate = dbutils.widgets.get("RPEnddate")
print(RPEnddate)
assert(RPEnddate)
dbSchema = dbutils.widgets.get("dbSchema")
print(dbSchema)
assert(dbSchema)
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)
status = dbutils.widgets.get("status")
print(status)
assert(status)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)

# COMMAND ----------

# DBTITLE 1,Create a view of denominator for each measure - Antenatal_16weeks 
# MAGIC %sql
# MAGIC -- msd101pregnancybooking_old_orgs_updated is msd101pregnancybooking with OrgCodeProvider updated with any organisation merge
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Denominator_Antenatal_16weeks AS 
# MAGIC
# MAGIC with cteAll as (
# MAGIC   select MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC     , rank() OVER (PARTITION BY Person_ID_Mother, UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS rank
# MAGIC     from global_temp.msd101pregnancybooking_old_orgs_updated
# MAGIC     where RPStartDate <= '$RPEnddate' AND GestAgeBooking > 0
# MAGIC )
# MAGIC SELECT MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC   from cteAll
# MAGIC   where rank = 1
# MAGIC   and (DischargeDateMatService is null or (DATEDIFF(DischargeDateMatService, AntenatalAppDate) + GestAgeBooking > 119))
# MAGIC   and (DATEDIFF('$RPBegindate', AntenatalAppDate) + GestAgeBooking <= 119)
# MAGIC   and (DATEDIFF('$RPEnddate', AntenatalAppDate) + GestAgeBooking >= 119)
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,... PCP_Birth_34weeks
# MAGIC %sql
# MAGIC -- msd101pregnancybooking_old_orgs_updated is msd101pregnancybooking with OrgCodeProvider updated with any organisation merge
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Denominator_PCP_Birth_34weeks AS 
# MAGIC
# MAGIC with cteAll as (
# MAGIC   select MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC     , rank() OVER (PARTITION BY Person_ID_Mother, UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS rank
# MAGIC     from global_temp.msd101pregnancybooking_old_orgs_updated
# MAGIC     where RPStartDate <= '$RPEnddate' AND GestAgeBooking > 0
# MAGIC )
# MAGIC SELECT MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC   from cteAll
# MAGIC   where rank = 1
# MAGIC   and (DischargeDateMatService is null or (DATEDIFF(DischargeDateMatService, AntenatalAppDate) + GestAgeBooking > 245))
# MAGIC   and (DATEDIFF('$RPBegindate', AntenatalAppDate) + GestAgeBooking <= 245)
# MAGIC   and (DATEDIFF('$RPEnddate', AntenatalAppDate) + GestAgeBooking >= 245)
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,... Postpartum_36weeks
# MAGIC %sql
# MAGIC -- msd101pregnancybooking_old_orgs_updated is msd101pregnancybooking with OrgCodeProvider updated with any organisation merge
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW PCSP_Denominator_PCP_Postpartum_36weeks AS 
# MAGIC
# MAGIC with cteAll as (
# MAGIC   select MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC     , rank() OVER (PARTITION BY Person_ID_Mother, UniqPregID ORDER BY AntenatalAppDate  DESC, RecordNumber  DESC) AS rank
# MAGIC     from global_temp.msd101pregnancybooking_old_orgs_updated
# MAGIC     where RPStartDate <= '$RPEnddate' AND GestAgeBooking > 0
# MAGIC )
# MAGIC SELECT MSD101_ID, Person_ID_Mother, UniqPregID, OrgCodeProvider, GestAgeBooking, AntenatalAppDate, DischargeDateMatService
# MAGIC   from cteAll
# MAGIC   where rank = 1
# MAGIC   and (DischargeDateMatService is null or (DATEDIFF(DischargeDateMatService, AntenatalAppDate) + GestAgeBooking > 259))
# MAGIC   and (DATEDIFF('$RPBegindate', AntenatalAppDate) + GestAgeBooking <= 259)
# MAGIC   and (DATEDIFF('$RPEnddate', AntenatalAppDate) + GestAgeBooking >= 259)
# MAGIC
# MAGIC

# COMMAND ----------

PCSPMeasures = {} # empty dictionary

# COMMAND ----------

# DBTITLE 1,Set parameters to use for each measure
PCSP16Weeks = {
    "Indicator" : "PCP_Antenatal_16weeks"
    ,"CarePlanType" : "05"
    ,"ViewName" : "PCSP_Denominator_Antenatal_16weeks"
    ,"AgeInDays" : "119"
}
PCSPMeasures['PCSP16Weeks'] = PCSP16Weeks
PCSPBirth34weeks = {
    "Indicator" : "PCP_Birth_34weeks"
    ,"CarePlanType" : "06"
    ,"ViewName" : "PCSP_Denominator_PCP_Birth_34weeks"
    ,"AgeInDays" : "245"
}
PCSPMeasures['PCSPBirth34weeks'] = PCSPBirth34weeks
PCPPostpartum36weeks = {
    "Indicator" : "PCP_Postpartum_36weeks"
    ,"CarePlanType" : "07"
    ,"ViewName" : "PCSP_Denominator_PCP_Postpartum_36weeks"
    ,"AgeInDays" : "259"
}
PCSPMeasures['PCPPostpartum36weeks'] = PCPPostpartum36weeks

# COMMAND ----------

# DBTITLE 1,Populate denominator, numerator, aggregated provider tables and determine if provider is over DQ threshold
for key, value in PCSPMeasures.items():
  Indicator = value["Indicator"]
  print(Indicator)
  CarePlanType = value["CarePlanType"]
  print(CarePlanType)
  ViewName = value["ViewName"]
  print(ViewName)
  AgeInDays = value["AgeInDays"]
  print(AgeInDays)

  sql = ("INSERT INTO {outSchema}.PCSP_Denominator_Raw \
        SELECT MSD101_ID as KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{status}' AS Status, '{Indicator}' AS Indicator, 'PCP' AS IndicatorFamily \
        , Person_ID_Mother as Person_ID, UniqPregID as PregnancyID, OrgCodeProvider, 1 as Rank \
        , 0 AS OverDQThreshold, current_timestamp() as CreatedAt \
        , null as Rank_IMD_Decile, null as EthnicCategory, null as EthnicGroup \
        from global_temp.{ViewName}").format(outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, status=status, ViewName=ViewName, Indicator=Indicator)
  print(sql)
  spark.sql(sql)

  # need to filter on rank = 1 on aggregation
  sql = ("INSERT INTO {outSchema}.PCSP_Numerator_Raw \
        SELECT plan.KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{status}' AS Status, '{Indicator}' AS Indicator, 'PCP' AS IndicatorFamily \
        , plan.Person_ID, plan.PregnancyID, plan.OrgCodeProvider, plan.Rank \
        , 0 AS OverDQThreshold, current_timestamp() as CreatedAt \
        , null as Rank_IMD_Decile, null as EthnicCategory, null as EthnicGroup \
        from (select daw.MSD101_ID as KeyValue, daw.Person_ID_Mother as Person_ID, daw.UniqPregID as PregnancyID, daw.OrgCodeProvider, 102mcp.MatPersCarePlanInd \
        , rank() OVER (PARTITION BY 102mcp.Person_ID_Mother, 102mcp.UniqPregID ORDER BY 102mcp.CarePlanDate  DESC, 102mcp.RecordNumber  DESC) AS Rank \
        from global_temp.{ViewName} as daw \
        inner join {DatabaseSchema}.MSD102MatCarePlan as 102mcp \
        on daw.Person_ID_Mother = 102mcp.Person_ID_Mother and daw.UniqPregID = 102mcp.UniqPregID \
        where 102mcp.RPStartDate < '{RPEnd}' \
        and 102mcp.CarePlanType = '{CarePlanType}' \
        and 102mcp.MatPersCarePlanInd is not null \
        and DATEDIFF(102mcp.CarePlanDate, daw.AntenatalAppDate) + GestAgeBooking <= {AgeInDays}) as plan \
        where plan.Rank = 1 and plan.MatPersCarePlanInd = 'Y'").format(DatabaseSchema=dbSchema, outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, status=status, ViewName=ViewName, Indicator=Indicator, CarePlanType=CarePlanType, AgeInDays=AgeInDays)
  print(sql)
  spark.sql(sql)
  
  # Aggregate by provider. cteclosed is cosmetic, a catch all should an organisation have closed
  # Unrounded_Rate should just be - round((COALESCE(num.Total, 0)/den.Total)*100, 1). Can also trap den.Total = 0 and set to 0 (but this SHOULD never happen)
  sql = ("with cteclosed as ( \
                            select ORG_CODE, NAME \
                            from {dss_corporate}.ORG_DAILY \
                            where BUSINESS_END_DATE is NULL \
                            and org_type_code='TR' \
                          ) \
        INSERT INTO {outSchema}.PCSP_Provider_Aggregated \
        SELECT den.RPStartDate, den.RPEndDate, den.Status, den.Indicator, den.IndicatorFamily, den.OrgCodeProvider \
        , COALESCE(den.Trust, c.NAME) AS OrgName, 'Provider' AS OrgLevel \
        , COALESCE(num.Total, 0) as Unrounded_Numerator \
        , den.Total as Unrounded_Denominator \
        , case when COALESCE(num.Total, 0) > 7 then round((COALESCE(num.Total, 0)/den.Total)*100, 1) else 0 end as Unrounded_Rate \
        , case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 end as Rounded_Numerator \
        , case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator \
        , round((case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 END / \
            case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end)*100, 1) as Rounded_Rate \
        , 0 AS OverDQThreshold \
        , current_timestamp() as CreatedAt \
        FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, count(*) as Total from {outSchema}.PCSP_Numerator_Geographies \
          WHERE Rank = 1 \
          AND Indicator = '{Indicator}' \
          group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num \
        RIGHT JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total \
        from {outSchema}.PCSP_Denominator_Geographies WHERE Rank = 1 AND Indicator = '{Indicator}' \
        group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) as den \
        ON num.OrgCodeProvider = den.OrgCodeProvider \
        LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE").format(outSchema=outSchema, dss_corporate=dss_corporate, Indicator=Indicator)
  print(sql)
  spark.sql(sql)
  
  # Identify if data from provider is over the DQ threshold (rounded_rate >= 5)
  sql = ("UPDATE {outSchema}.PCSP_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate >= 5 THEN 1 ELSE 0 END WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(sql)

# COMMAND ----------

dbutils.notebook.exit("Notebook: PCP_3CarePlans ran successfully")