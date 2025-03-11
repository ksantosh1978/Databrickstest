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
status = dbutils.widgets.get("status")
print(status)
assert(status)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)

# COMMAND ----------

# DBTITLE 1,Identify numerator rows
# Identify denomnator rows, then apply further filters
sql = ("INSERT INTO {outSchema}.CoC_Numerator_Raw  \
      SELECT ranked.MSD101_ID as KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{status}' AS Status, 'COC_by_28weeks' AS Indicator, 'COC_Rate' AS IndicatorFamily \
      , ranked.Person_ID_Mother as Person_ID, ranked.UniqPregID as PregnancyID, null as CareConID, ranked.OrgCodeProvider, ranked.Rank \
      , 0 AS OverDQThreshold, current_timestamp() as CreatedAt \
      , null as Rank_IMD_Decile, null as EthnicCategory, null as EthnicGroup \
      FROM (SELECT plans.MSD101_ID, plans.Person_ID_Mother, plans.UniqPregID, plans.OrgCodeProvider \
      , row_number() OVER (PARTITION BY plans.Person_ID_Mother, plans.UniqPregID ORDER BY plans.CarePlanDate DESC, plans.RecordNumber desc) as Rank \
      FROM \
      (SELECT denom.MSD101_ID, 102mcp.Person_ID_Mother, 102mcp.UniqPregID, denom.GestAgeBooking, denom.AntenatalAppDate, denom.DischargeDateMatService, denom.OrgCodeProvider \
      , 102mcp.CarePlanDate, 102mcp.RecordNumber, 102mcp.ContCarePathInd, 102mcp.CareProfLID, 102mcp.TeamLocalID \
      , row_number() over (partition by 102mcp.Person_ID_Mother, 102mcp.UniqPregID order by 102mcp.CarePlanDate desc, 102mcp.RecordNumber desc) as CarePlanRank \
      FROM \
      (SELECT tmp.MSD101_ID, tmp.Person_ID_Mother, tmp.UniqPregID, tmp.GestAgeBooking, tmp.AntenatalAppDate, tmp.DischargeDateMatService, tmp.OrgCodeProvider \
      FROM \
      (SELECT 101pb.MSD101_ID, 101pb.Person_ID_Mother, 101pb.UniqPregID \
      , 101pb.GestAgeBooking, 101pb.AntenatalAppDate, 101pb.DischargeDateMatService, COALESCE(ord.REL_FROM_ORG_CODE, 101pb.OrgCodeProvider) AS OrgCodeProvider \
      , row_number() OVER (PARTITION BY 101pb.Person_ID_Mother, 101pb.UniqPregID ORDER BY 101pb.AntenatalAppDate DESC, 101pb.RecordNumber DESC) as DenominatorRank \
      FROM {DatabaseSchema}.msd101pregnancybooking as 101pb \
      LEFT JOIN {dss_corporate}.org_relationship_daily as ord \
      ON 101pb.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' \
      and ord.REL_OPEN_DATE <= '{RPEnd}'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '{RPEnd}') \
      WHERE 101pb.RPStartDate <= '{RPStart}' AND 101pb.GestAgeBooking > 0 \
      ) as tmp \
      WHERE tmp.DenominatorRank = 1 \
      AND (tmp.GestAgeBooking + DATEDIFF('{RPStart}', tmp.AntenatalAppDate) <= 203) \
      AND (tmp.GestAgeBooking + DATEDIFF('{RPEnd}', tmp.AntenatalAppDate) >= 203) \
      AND (tmp.DischargeDateMatService is null or (tmp.GestAgeBooking + DATEDIFF(tmp.DischargeDateMatService, tmp.AntenatalAppDate) > 203))) as denom \
      inner join {DatabaseSchema}.msd102matcareplan as 102mcp \
      on denom.Person_ID_Mother = 102mcp.Person_ID_Mother and denom.UniqPregID = 102mcp.UniqPregID \
      where 102mcp.RPStartDate <= '{RPStart}' \
      and (denom.GestAgeBooking + DATEDIFF(102mcp.CarePlanDate, denom.AntenatalAppDate) <= 203) \
      and 102mcp.CarePlanType = '05' \
      and 102mcp.ContCarePathInd is not null \
      ) as plans \
      where plans.CarePlanRank = 1 AND plans.ContCarePathInd = 'Y' AND plans.CareProfLID IS NOT NULL AND plans.TeamLocalID IS NOT NULL) as ranked").format(DatabaseSchema=dbSchema, outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, status=status, dss_corporate=dss_corporate)
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Identify denominator rows
sql = ("INSERT INTO {outSchema}.CoC_Denominator_Raw \
      SELECT tmp.KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{status}' AS Status, 'COC_by_28weeks' AS Indicator, 'COC_Rate' AS IndicatorFamily \
      , tmp.Person_ID, tmp.PregnancyID, null as CareConID, tmp.OrgCodeProvider, tmp.Rank \
      , 0 AS OverDQThreshold, current_timestamp() as CreatedAt \
      , null as Rank_IMD_Decile, null as EthnicCategory, null as EthnicGroup \
      FROM ( \
      SELECT 101pb.MSD101_ID AS KeyValue, 101pb.Person_ID_Mother AS Person_ID, 101pb.UniqPregID AS PregnancyID , COALESCE(ord.REL_FROM_ORG_CODE, 101pb.OrgCodeProvider) AS OrgCodeProvider \
      , 101pb.GestAgeBooking, 101pb.AntenatalAppDate, 101pb.DischargeDateMatService \
      , row_number() OVER (PARTITION BY 101pb.Person_ID_Mother, 101pb.UniqPregID ORDER BY 101pb.AntenatalAppDate DESC, 101pb.RecordNumber DESC) as Rank \
      FROM {DatabaseSchema}.msd101pregnancybooking as 101pb \
      LEFT JOIN {dss_corporate}.org_relationship_daily as ord \
      ON 101pb.OrgCodeProvider = ord.REL_TO_ORG_CODE AND ord.REL_IS_CURRENT = 1 and ord.REL_TYPE_CODE = 'P' and ord.REL_FROM_ORG_TYPE_CODE = 'TR' \
      and ord.REL_OPEN_DATE <= '{RPEnd}'  and (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '{RPEnd}') \
      WHERE 101pb.RPStartDate <= '{RPStart}' AND 101pb.GestAgeBooking > 0 \
      ) as tmp \
      WHERE tmp.Rank = 1 \
      AND (tmp.GestAgeBooking + DATEDIFF('{RPStart}', tmp.AntenatalAppDate) <= 203) \
      AND (tmp.GestAgeBooking + DATEDIFF('{RPEnd}', tmp.AntenatalAppDate) >= 203) \
      AND (tmp.DischargeDateMatService is null or (tmp.GestAgeBooking + DATEDIFF(tmp.DischargeDateMatService, tmp.AntenatalAppDate) > 203))").format(DatabaseSchema=dbSchema, outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, status=status, dss_corporate=dss_corporate)
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Aggregate by provider
# Use cteclosed to identify name of any organisation that thier name unresolved (i.e. closed org)
sql = ("with cteclosed as ( \
                            select ORG_CODE, NAME \
                            from {dss_corporate}.ORG_DAILY \
                            where BUSINESS_END_DATE is NULL \
                            and org_type_code='TR' \
                          ) INSERT INTO {outSchema}.CoC_Provider_Aggregated SELECT den.RPStartDate, den.RPEndDate, den.Status, den.Indicator, den.IndicatorFamily, den.OrgCodeProvider \
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
      FROM (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, count(*) as Total from {outSchema}.CoC_Numerator_Geographies \
        WHERE Rank = 1 \
        AND Indicator = 'COC_by_28weeks' \
        group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider) AS num \
      RIGHT JOIN (select RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(*) as Total \
      from {outSchema}.CoC_Denominator_Geographies WHERE Rank = 1 AND Indicator = 'COC_by_28weeks' \
      group by RPStartDate, RPEndDate, Status, Indicator, IndicatorFamily, OrgCodeProvider, Trust) as den \
      ON num.OrgCodeProvider = den.OrgCodeProvider \
      LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE").format(outSchema=outSchema, dss_corporate=dss_corporate)
spark.sql(sql)

# COMMAND ----------

# Identify if data from provider is over the DQ threshold (rounded_rate >= 5)
# sql = ("UPDATE {outSchema}.CoC_Provider_Aggregated SET OverDQThreshold = CASE WHEN Rounded_Rate >= 5 THEN 1 ELSE 0 END WHERE Indicator = 'COC_by_28weeks'").format(outSchema=outSchema)
# spark.sql(sql)


# COMMAND ----------

# # Identify if data from provider is over the DQ threshold (rounded_rate >= 5)
sql = ("UPDATE {outSchema}.CoC_Provider_Aggregated A SET OverDQThreshold = 1   WHERE Indicator = 'COC_by_28weeks' AND (EXISTS(SELECT * FROM {outSchema}.CoC_Provider_Aggregated B WHERE Indicator = 'COC_DQ04' AND A.OrgCodeProvider = B.OrgCodeProvider ) AND EXISTS(SELECT * FROM {outSchema}.CoC_Provider_Aggregated C WHERE Indicator = 'COC_DQ05' AND A.OrgCodeProvider = C.OrgCodeProvider )) AND NOT EXISTS(SELECT * FROM {outSchema}.CoC_Provider_Aggregated D WHERE Indicator IN ('COC_DQ04', 'COC_DQ05') AND A.OrgCodeProvider = D.OrgCodeProvider  AND D.OverDQThreshold = 0)").format(outSchema=outSchema)
spark.sql(sql)

# COMMAND ----------

dbutils.notebook.exit("Notebook: COC_by_28weeks ran successfully")