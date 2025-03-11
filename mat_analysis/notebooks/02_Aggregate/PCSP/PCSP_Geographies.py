# Databricks notebook source
# DBTITLE 1,Widgets
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
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)
status = dbutils.widgets.get("status")
print(status)
assert(status)

# COMMAND ----------

PCSPAllMeasures = ["PCP_All_Pathways", "PCP_Antenatal_16weeks", "PCP_Birth_34weeks", "PCP_Postpartum_36weeks"]
PCSPIMDMeasures = ["PCP_All_Pathways"]

# COMMAND ----------

Geographies = {
  "RegionORG" : {"Org_Level":"'NHS England (Region)'", "Org_Name_Column":"Region", "Provider_Name_Column" : "RegionOrg"}
  ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Org_Name_Column":"MBRRACE_Grouping", "Provider_Name_Column" : "Mbrrace_Grouping_Short"}
  ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Org_Name_Column":"STP_Name", "Provider_Name_Column" : "STP_Code"}
}
print(Geographies)

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold flag in PCSP_Numerator_Raw
for Indicator in PCSPAllMeasures:
  sql = ("MERGE INTO {outSchema}.PCSP_Numerator_Raw as nr\
      USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.PCSP_Provider_Aggregated WHERE Indicator = '{Indicator}' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg \
      ON nr.OrgCodeProvider = agg.OrgCodeProvider AND nr.Indicator = '{Indicator}' \
      WHEN MATCHED THEN UPDATE SET nr.OverDQThreshold = agg.OverDQThreshold").format(outSchema=outSchema, Indicator=Indicator)
  print(sql)
  spark.sql(sql)
  

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold flag in PCSP_Denominator_Raw
for Indicator in PCSPAllMeasures:
  sql = ("MERGE INTO {outSchema}.PCSP_Denominator_Raw as dr \
      USING (select OrgCodeProvider, OverDQThreshold from {outSchema}.PCSP_Provider_Aggregated WHERE Indicator = '{Indicator}' group by OrgCodeProvider, OverDQThreshold HAVING COUNT(*) = 1) as agg \
      ON dr.OrgCodeProvider = agg.OrgCodeProvider AND dr.Indicator = '{Indicator}' \
      WHEN MATCHED THEN UPDATE SET dr.OverDQThreshold = agg.OverDQThreshold").format(outSchema=outSchema, Indicator=Indicator)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Set Ethnicity and IMD on numerator
# MAGIC %sql
# MAGIC with numerator as (
# MAGIC   -- Mothers of interest
# MAGIC   -- Assumes numerator is a subset of denominator
# MAGIC   select Person_ID, OrgCodeProvider from $outSchema.PCSP_Denominator_Raw group by Person_ID, OrgCodeProvider
# MAGIC )
# MAGIC , person as (
# MAGIC   select Person_ID from numerator group by Person_ID
# MAGIC ) 
# MAGIC , demographicsAll as (
# MAGIC   select 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) as OrgCodeProvider
# MAGIC     , 001md.EthnicCategoryMother
# MAGIC     , case when 001md.EthnicCategoryMother in ('D', 'E', 'F', 'H', 'J', 'K', 'M', 'L', 'N', 'P') then 'BAME'
# MAGIC       when 001md.EthnicCategoryMother in ('A', 'B', 'C') then 'White'
# MAGIC       when 001md.EthnicCategoryMother in ('G', 'R', 'S') then 'Other'
# MAGIC       else 'Missing_Unknown'
# MAGIC       end as EthnicGroup
# MAGIC     , eid.DECI_IMD
# MAGIC       -- using row_number rather than rank as there's duplicated data in DAE Ref 
# MAGIC     , row_number() over (partition by 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) order by 001md.RecordNumber desc) as rank
# MAGIC     from $dbSchema.msd001motherdemog as 001md
# MAGIC   inner join person as p on 001md.Person_ID_Mother = p.Person_ID -- filter to mothers identified in PCSP_Numerator_Raw
# MAGIC   LEFT JOIN $dss_corporate.org_relationship_daily as ord
# MAGIC     ON 001md.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR'
# MAGIC     and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate')
# MAGIC   LEFT JOIN $dss_corporate.english_indices_of_dep_v02 as eid on 001md.LSOAMother2011 = eid.LSOA_CODE_2011 and eid.IMD_YEAR = 2019
# MAGIC   where RPStartDate <= '$RPEnddate'
# MAGIC )
# MAGIC , demographics as (
# MAGIC   select da.Person_ID_Mother, da.OrgCodeProvider, coalesce(i.Description, 'Missing_Unknown') as Rank_IMD_Decile, da.EthnicCategoryMother, da.EthnicGroup from demographicsAll as da
# MAGIC   left join $outSchema.IMD as i on da.DECI_IMD = i.Rank_IMD_Decile
# MAGIC   where da.rank = 1
# MAGIC )
# MAGIC MERGE INTO $outSchema.PCSP_Numerator_Raw as nr
# MAGIC USING (select Person_ID_Mother, OrgCodeProvider, Rank_IMD_Decile, EthnicCategoryMother, EthnicGroup from demographics) as demo
# MAGIC ON nr.Person_ID = demo.Person_ID_Mother and nr.OrgCodeProvider = demo.OrgCodeProvider
# MAGIC WHEN MATCHED THEN UPDATE SET nr.Rank_IMD_Decile = demo.Rank_IMD_Decile, nr.EthnicCategory = demo.EthnicCategoryMother, nr.EthnicGroup = demo.EthnicGroup
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set Ethnicity and IMD on denominator
# MAGIC %sql
# MAGIC with denominator as (
# MAGIC   select Person_ID, OrgCodeProvider from $outSchema.PCSP_Denominator_Raw group by Person_ID, OrgCodeProvider
# MAGIC )
# MAGIC , person as (
# MAGIC   select Person_ID from denominator group by Person_ID
# MAGIC ) 
# MAGIC , demographicsAll as (
# MAGIC   -- identify ethnic group and IMD 2019 using latest demographic data of mother
# MAGIC   select 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) as OrgCodeProvider
# MAGIC     , 001md.EthnicCategoryMother
# MAGIC     , case when 001md.EthnicCategoryMother in ('D', 'E', 'F', 'H', 'J', 'K', 'M', 'L', 'N', 'P') then 'BAME'
# MAGIC       when 001md.EthnicCategoryMother in ('A', 'B', 'C') then 'White'
# MAGIC       when 001md.EthnicCategoryMother in ('G', 'R', 'S') then 'Other'
# MAGIC       else 'Missing_Unknown'
# MAGIC       end as EthnicGroup
# MAGIC     , eid.DECI_IMD
# MAGIC     , row_number() over (partition by 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) order by 001md.RecordNumber desc) as rank
# MAGIC     from $dbSchema.msd001motherdemog as 001md
# MAGIC   inner join person as p on 001md.Person_ID_Mother = p.Person_ID -- filter to mothers identified in PCSP_Denominator_Raw
# MAGIC   LEFT JOIN $dss_corporate.org_relationship_daily as ord
# MAGIC     ON 001md.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR'
# MAGIC     and ord.REL_OPEN_DATE <= '$RPEnddate'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate')
# MAGIC   LEFT JOIN $dss_corporate.english_indices_of_dep_v02 as eid on 001md.LSOAMother2011 = eid.LSOA_CODE_2011 and eid.IMD_YEAR = 2019
# MAGIC   where RPStartDate <= '$RPEnddate'
# MAGIC )
# MAGIC , demographics as (
# MAGIC   select da.Person_ID_Mother, da.OrgCodeProvider, coalesce(i.Description, 'Missing_Unknown') as Rank_IMD_Decile, da.EthnicCategoryMother, da.EthnicGroup from demographicsAll as da
# MAGIC   left join $outSchema.IMD as i on da.DECI_IMD = i.Rank_IMD_Decile
# MAGIC   where da.rank = 1
# MAGIC )
# MAGIC MERGE INTO $outSchema.PCSP_Denominator_Raw as dr
# MAGIC USING (select Person_ID_Mother, OrgCodeProvider, Rank_IMD_Decile, EthnicCategoryMother, EthnicGroup from demographics) as demo
# MAGIC ON dr.Person_ID = demo.Person_ID_Mother and dr.OrgCodeProvider = demo.OrgCodeProvider
# MAGIC WHEN MATCHED THEN UPDATE SET dr.Rank_IMD_Decile = demo.Rank_IMD_Decile, dr.EthnicCategory = demo.EthnicCategoryMother, dr.EthnicGroup = demo.EthnicGroup
# MAGIC

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator, provider and ethnic group
# Aggregate for each indicator, for each ethnic group, for each of the providers for which data has been aggregated in the month
# cross join/left join results in a row being output where there is no data
for Indicator in PCSPIMDMeasures:
  sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, orgs.Status \
, concat('{Indicator}_Ethnicity_', orgs.EthnicGroup) as Indicator \
, COALESCE(orgs.IndicatorFamily, 'PCP') as IndicatorFamily \
, orgs.OrgCodeProvider AS OrgCodeProvider \
, orgs.OrgName AS OrgName \
, 'Provider' AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end \
  as Rounded_Denominator \
, round((case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end / \
    case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) \
  as Rounded_Rate \
, current_timestamp() as CreatedAt \
  FROM \
  (select o.RPStartDate, o.RPEndDate, o.Status, o.Indicator, o.IndicatorFamily, o.OrgCodeProvider, o.OrgName, e.Name as EthnicGroup \
    FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, OrgName from {outSchema}.PCSP_Provider_Aggregated WHERE Indicator = '{Indicator}' \
    group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, OrgName) AS o \
    cross join {outSchema}.EthnicGroup as e \
  ) AS orgs \
  left join (select den.OrgCodeProvider, den.EthnicGroup, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
    from (select OrgCodeProvider, EthnicGroup, count(*) as Total from {outSchema}.PCSP_Numerator_Geographies \
    WHERE Rank = 1 AND Indicator = '{Indicator}' \
    group by OrgCodeProvider, EthnicGroup) AS num \
  right JOIN (select OrgCodeProvider, EthnicGroup, count(*) as Total from {outSchema}.PCSP_Denominator_Geographies \
    WHERE Rank = 1 AND Indicator = '{Indicator}' \
    group by OrgCodeProvider, EthnicGroup) as den \
  ON num.OrgCodeProvider = den.OrgCodeProvider and num.EthnicGroup = den.EthnicGroup  \
  ) as totals \
  on orgs.OrgCodeProvider = totals.OrgCodeProvider and orgs.EthnicGroup = totals.EthnicGroup").format(outSchema=outSchema,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator, provider and IMD (index of multiple deprivation)
# Aggregate for each indicator, for each IMD, for each of the providers for which data has been aggregated in the month
# cross join/left join results in a row being output where there is no data
for Indicator in PCSPIMDMeasures:
  sql = (" \
  INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, orgs.Status \
, concat('{Indicator}_IMD_', orgs.IMDDescription) as Indicator \
, COALESCE(orgs.IndicatorFamily, 'PCP') as IndicatorFamily \
, orgs.OrgCodeProvider AS OrgCodeProvider \
, orgs.OrgName AS OrgName \
, 'Provider' AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end \
  as Rounded_Denominator \
, round((case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end / \
    case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) \
  as Rounded_Rate \
, current_timestamp() as CreatedAt \
  FROM (select o.RPStartDate, o.RPEndDate, o.Status, o.Indicator, o.IndicatorFamily, o.OrgCodeProvider, o.OrgName, i.Description as IMDDescription \
         from (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, OrgName from {outSchema}.PCSP_Provider_Aggregated WHERE Indicator = '{Indicator}') as o \
         cross join (select Description from {outSchema}.IMD) as i \
         ) AS orgs \
    left join (select den.OrgCodeProvider, den.Rank_IMD_Decile, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
    from (select OrgCodeProvider, Rank_IMD_Decile, count(*) as Total \
  from {outSchema}.PCSP_Numerator_Geographies \
    WHERE Rank = 1 AND Indicator = '{Indicator}' \
    group by OrgCodeProvider, Rank_IMD_Decile) AS num \
    right JOIN (select OrgCodeProvider, Rank_IMD_Decile, count(*) as Total from {outSchema}.PCSP_Denominator_Geographies \
    WHERE Rank = 1 AND Indicator = '{Indicator}' \
    group by OrgCodeProvider, Rank_IMD_Decile) as den \
  ON num.OrgCodeProvider = den.OrgCodeProvider and num.Rank_IMD_Decile = den.Rank_IMD_Decile \
  ) as totals \
  on orgs.OrgCodeProvider = totals.OrgCodeProvider and orgs.IMDDescription = totals.Rank_IMD_Decile").format(outSchema=outSchema,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator and geography
# Aggregate for each indicator, each geography
for Indicator in PCSPAllMeasures:
  for key, value in Geographies.items():
    print(key, value)
    Geog_Org_Level = value["Org_Level"]
    Geog_Org_Name_Column = value["Org_Name_Column"]
    Geog_Provider_Name_Column = value["Provider_Name_Column"]
    sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, den.Status \
, den.Indicator \
, COALESCE(den.IndicatorFamily, 'PCP') as IndicatorFamily \
, den.{Geog_Provider_Name} AS OrgCodeProvider \
, den.{Geog_Org_Name_Column} AS OrgName \
, {Geog_Org_Level} AS OrgLevel \
, num.Total as Unrounded_Numerator \
, den.Total as Unrounded_Denominator \
, case when num.Total > 7 then round((num.Total/den.Total)*100, 1) else 0 end as Unrounded_Rate \
, case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end as Rounded_Numerator \
, case when den.Total = 0 then 0 when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator \
, round((case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end / \
    case when den.Total = 0 then 0 when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end)*100, 1) as Rounded_Rate \
, current_timestamp() as CreatedAt \
FROM (select {Geog_Org_Name_Column}, {Geog_Provider_Name}, count(*) as Total from {outSchema}.PCSP_Numerator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by {Geog_Org_Name_Column}, {Geog_Provider_Name}) AS num \
  right JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name}, count(*) as Total from {outSchema}.PCSP_Denominator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name}) as den \
  ON num.{Geog_Org_Name_Column} = den.{Geog_Org_Name_Column}"
).format(outSchema=outSchema,Geog_Org_Level=Geog_Org_Level,Geog_Org_Name_Column=Geog_Org_Name_Column,Geog_Provider_Name=Geog_Provider_Name_Column,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
    print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator, geography and ethnic group
# Aggregate for each indicator, for each geography, for each of the providers for which data has been aggregated in the month
# cross join/left join results in a row being output where there is no data
for Indicator in PCSPIMDMeasures:
  for key, value in Geographies.items():
    print(key, value)
    Geog_Org_Level = value["Org_Level"]
    Geog_Org_Name_Column = value["Org_Name_Column"]
    Geog_Provider_Name_Column = value["Provider_Name_Column"]
    sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, orgs.Status \
, concat('{Indicator}_Ethnicity_', orgs.EthnicGroup) as Indicator \
, COALESCE(orgs.IndicatorFamily, 'PCP') as IndicatorFamily \
, orgs.OrgCodeProvider AS OrgCodeProvider \
, orgs.OrgName AS OrgName \
, {Geog_Org_Level} AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end \
  as Rounded_Denominator \
, round((case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end / \
    case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) \
  as Rounded_Rate \
, current_timestamp() as CreatedAt \
  FROM \
    (select o.RPStartDate, o.RPEndDate, o.Status, o.Indicator, o.IndicatorFamily, o.{Geog_Org_Name_Column} AS OrgName, o.{Geog_Provider_Name} AS OrgCodeProvider, e.Name as EthnicGroup \
    FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name} \
      from {outSchema}.PCSP_Denominator_Geographies \
      WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
      group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name}) AS o \
    cross join (select Name from {outSchema}.EthnicGroup) as e \
  ) AS orgs \
  left join (select den.{Geog_Provider_Name} AS OrgCodeProvider, den.EthnicGroup, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
    from (select {Geog_Provider_Name}, EthnicGroup, count(*) as Total from {outSchema}.PCSP_Numerator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by {Geog_Provider_Name}, EthnicGroup) AS num \
  right JOIN (select {Geog_Provider_Name}, EthnicGroup, count(*) as Total from {outSchema}.PCSP_Denominator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by {Geog_Provider_Name}, EthnicGroup) as den \
  ON num.{Geog_Provider_Name} = den.{Geog_Provider_Name} and num.EthnicGroup = den.EthnicGroup \
  ) as totals \
  on orgs.OrgCodeProvider = totals.OrgCodeProvider and orgs.EthnicGroup = totals.EthnicGroup").format(outSchema=outSchema,Geog_Org_Level=Geog_Org_Level,Geog_Org_Name_Column=Geog_Org_Name_Column,Geog_Provider_Name=Geog_Provider_Name_Column,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
    print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Aggregate for each indicator, geography and IMD (index of multiple deprivation)
# Aggregate for each indicator, for each geography, for each IMD, for each of the providers for which data has been aggregated in the month
# cross join/left join results in a row being output where there is no data
for Indicator in PCSPIMDMeasures:
  for key, value in Geographies.items():
    print(key, value)
    Geog_Org_Level = value["Org_Level"]
    Geog_Org_Name_Column = value["Org_Name_Column"]
    Geog_Provider_Name_Column = value["Provider_Name_Column"]
    sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, orgs.Status \
, concat('{Indicator}_IMD_', orgs.IMDDescription) as Indicator \
, COALESCE(orgs.IndicatorFamily, 'PCP') as IndicatorFamily \
, orgs.OrgCodeProvider AS OrgCodeProvider \
, orgs.OrgName AS OrgName \
, {Geog_Org_Level} AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end \
  as Rounded_Denominator \
, round((case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end / \
    case when totals.Unrounded_Denominator = 0 then 0 when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) \
  as Rounded_Rate \
, current_timestamp() as CreatedAt \
  FROM (select o.{Geog_Org_Name_Column}, o.RPStartDate, o.RPEndDate, o.Status, o.Indicator, o.IndicatorFamily, o.{Geog_Org_Name_Column} AS OrgName, o.{Geog_Provider_Name} AS OrgCodeProvider \
  , i.Description as IMDDescription \
  from (select {Geog_Org_Name_Column}, RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name} from {outSchema}.PCSP_Denominator_Geographies  \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, {Geog_Org_Name_Column}, {Geog_Provider_Name}) as o \
    cross join (select Description from {outSchema}.IMD) as i \
  ) AS orgs \
    left join (select den.{Geog_Provider_Name} AS OrgCodeProvider, den.Rank_IMD_Decile, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
    from (select {Geog_Provider_Name}, Rank_IMD_Decile, count(*) as Total \
  from {outSchema}.PCSP_Numerator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by {Geog_Provider_Name}, Rank_IMD_Decile) AS num \
    right JOIN (select {Geog_Provider_Name}, Rank_IMD_Decile, count(*) as Total from {outSchema}.PCSP_Denominator_Geographies \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by {Geog_Provider_Name}, Rank_IMD_Decile) as den \
  ON num.{Geog_Provider_Name} = den.{Geog_Provider_Name} and num.Rank_IMD_Decile = den.Rank_IMD_Decile \
  ) as totals \
  on orgs.OrgCodeProvider = totals.OrgCodeProvider and orgs.IMDDescription = totals.Rank_IMD_Decile").format(outSchema=outSchema,Geog_Org_Level=Geog_Org_Level,Geog_Org_Name_Column=Geog_Org_Name_Column,Geog_Provider_Name=Geog_Provider_Name_Column,Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate)
    print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,National aggregation per indicator
# use cross join to avoid 'detected implicit cartesian product' message
for Indicator in PCSPAllMeasures:
  sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, coalesce(den.Status, '{status}') \
, den.Indicator \
, COALESCE(den.IndicatorFamily, 'PCP') as IndicatorFamily \
, 'National' AS OrgCodeProvider \
, 'All Submitters' AS OrgName \
, 'National' AS OrgLevel \
, num.Total as Unrounded_Numerator \
, den.Total as Unrounded_Denominator \
, case when num.Total > 7 and den.total > 0 then round((num.Total/den.Total)*100, 1) else 0 end as Unrounded_Rate \
, case when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end as Rounded_Numerator \
, case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator \
, case when den.Total = 0 then 0 else \
round((case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 else round(num.Total/5,0)*5 end) / \
(case when den.Total between 1 and 7 then 5 else round(den.Total/5,0)*5 end)*100, 1) end as Rounded_Rate \
, current_timestamp() as CreatedAt \
FROM (select count(*) as Total from {outSchema}.PCSP_Numerator_Raw \
  WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
  group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily) AS num \
cross JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, count(*) as Total from {outSchema}.PCSP_Denominator_Raw \
  WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
  group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily) as den").format(outSchema=outSchema, Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate,status=status)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,National aggregation per indicator and ethnic group
for Indicator in PCSPIMDMeasures:
  sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, coalesce(totals.Status, '{status}') \
, concat('{Indicator}_Ethnicity_', orgs.EthnicGroup) as Indicator \
, COALESCE(totals.IndicatorFamily, 'PCP') as IndicatorFamily \
, 'National' AS OrgCodeProvider \
, 'All Submitters' AS OrgName \
, 'National' AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 and totals.Unrounded_Denominator > 0 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end as Rounded_Denominator \
, case when totals.Unrounded_Denominator = 0 then 0 else \
round((case when totals.Unrounded_Numerator = 0 then 0 when totals.Unrounded_Numerator between 1 and 7 then 5 else round(totals.Unrounded_Numerator/5,0)*5 end) / \
(case when totals.Unrounded_Denominator between 1 and 7 then 5 else round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) end as Rounded_Rate \
, current_timestamp() as CreatedAt \
FROM (select Name as EthnicGroup from {outSchema}.EthnicGroup) as orgs \
  left join \
  (select den.RPStartDate, den.RPEndDate, den.Status, den.Indicator, den.IndicatorFamily, den.EthnicGroup, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
  FROM \
    (select EthnicGroup, count(*) as Total from {outSchema}.PCSP_Numerator_Raw \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by EthnicGroup) AS num \
  RIGHT JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, EthnicGroup, count(*) as Total from {outSchema}.PCSP_Denominator_Raw \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, EthnicGroup) as den \
    on num.EthnicGroup = den.EthnicGroup \
  ) as totals on orgs.EthnicGroup = totals.EthnicGroup").format(outSchema=outSchema, Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate,status=status)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,National aggregation per indicator and IMD (index of multiple deprivation)
# Currently only outputs Decile = 01
for Indicator in PCSPIMDMeasures:
  sql = ("INSERT INTO {outSchema}.PCSP_Geography_Aggregated \
SELECT '{RPBegindate}' as RPStartDate \
, '{RPEnddate}' as RPEndDate \
, coalesce(totals.Status, '{status}') \
, concat('{Indicator}_IMD_', orgs.Description) as Indicator \
, COALESCE(totals.IndicatorFamily, 'PCP') as IndicatorFamily \
, 'National' AS OrgCodeProvider \
, 'All Submitters' AS OrgName \
, 'National' AS OrgLevel \
, totals.Unrounded_Numerator \
, totals.Unrounded_Denominator \
, case when totals.Unrounded_Numerator > 7 and totals.Unrounded_Denominator > 0 then round((totals.Unrounded_Numerator/totals.Unrounded_Denominator)*100, 1) else 0 end as Unrounded_Rate \
, case when totals.Unrounded_Numerator between 1 and 7 then 5 when totals.Unrounded_Numerator > 7 then round(totals.Unrounded_Numerator/5,0)*5 end as Rounded_Numerator \
, case when totals.Unrounded_Denominator between 1 and 7 then 5 when totals.Unrounded_Denominator > 7 then round(totals.Unrounded_Denominator/5,0)*5 end as Rounded_Denominator \
, case when totals.Unrounded_Denominator = 0 then 0 else \
round((case when totals.Unrounded_Numerator between 1 and 7 then 5 else round(totals.Unrounded_Numerator/5,0)*5 end) / \
(case when totals.Unrounded_Denominator between 1 and 7 then 5 else round(totals.Unrounded_Denominator/5,0)*5 end)*100, 1) end as Rounded_Rate \
, current_timestamp() as CreatedAt \
FROM {outSchema}.IMD as orgs \
  left join \
  (select den.RPStartDate, den.RPEndDate, den.Status, den.Indicator, den.IndicatorFamily, den.Rank_IMD_Decile, num.Total as Unrounded_Numerator, den.Total as Unrounded_Denominator \
   from \
    (select Rank_IMD_Decile, count(*) as Total from {outSchema}.PCSP_Numerator_Raw \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by Rank_IMD_Decile) AS num \
  right JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, Rank_IMD_Decile, count(*) as Total from {outSchema}.PCSP_Denominator_Raw \
    WHERE Rank = 1 AND OverDQThreshold = 1 AND Indicator = '{Indicator}' \
    group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, Rank_IMD_Decile) as den \
  ON num.Rank_IMD_Decile = den.Rank_IMD_Decile \
  ) as totals \
  on orgs.Description = totals.Rank_IMD_Decile").format(outSchema=outSchema, Indicator=Indicator,RPBegindate=RPBegindate,RPEnddate=RPEnddate, status=status)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Populate PCSP_CSV
  # 7 statements to populate CSV output table
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Denominator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Denominator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Provider_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Numerator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Numerator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Provider_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Rate' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Rate, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Provider_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Result' AS Currency, \
    CASE WHEN (Rounded_Numerator = 0) OR (OverDQThreshold = 0) THEN 'Fail' ELSE 'Pass' END AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Provider_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Denominator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Denominator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Geography_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Numerator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Numerator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Geography_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.PCSP_CSV \
    SELECT RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Rate' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Rate, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.PCSP_Geography_Aggregated").format(outSchema=outSchema)
  spark.sql(csvStatement)

# COMMAND ----------

dbutils.notebook.exit("Notebook: PCSP_Geographies ran successfully")