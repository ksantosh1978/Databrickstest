# Databricks notebook source
# DBTITLE 1,Import Functions
# MAGIC %run ./../MaternityFunctions

# COMMAND ----------

Aggregates = {
  "Induction" : {"IndicatorFamily":"Birth", "Indicator":"Induction", "OverDQ":"5"}
}
print(Aggregates)

# COMMAND ----------

outSchema = dbutils.widgets.get("outSchema")
assert outSchema
RPStartdate = dbutils.widgets.get("RPStartdate")
assert RPStartdate

# COMMAND ----------

# DBTITLE 1,Populate aggregated provider 
# MAGIC %sql
# MAGIC WITH Numerator AS
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total
# MAGIC   FROM  $outSchema.Measures_Geographies
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsNumerator = 1   
# MAGIC     AND Indicator = 'Induction'
# MAGIC     AND RPStartDate = '$RPStartdate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust
# MAGIC ), 
# MAGIC Denominator AS 
# MAGIC (
# MAGIC   SELECT RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust, COUNT(1) AS Total
# MAGIC   FROM $outSchema.Measures_Geographies
# MAGIC   WHERE Rank = 1
# MAGIC     AND IsDenominator = 1   
# MAGIC     AND Indicator = 'Induction'
# MAGIC     AND RPStartDate = '$RPStartdate'
# MAGIC   GROUP BY RPStartDate, RPEndDate, IndicatorFamily, Indicator, OrgCodeProvider, Trust
# MAGIC ),
# MAGIC cteclosed AS 
# MAGIC (
# MAGIC   SELECT  ORG_CODE
# MAGIC          ,NAME 
# MAGIC   FROM $dss_corporate.ORG_DAILY
# MAGIC   WHERE BUSINESS_END_DATE IS NULL
# MAGIC   AND org_type_code='TR' 
# MAGIC )
# MAGIC INSERT INTO $outSchema.Measures_Aggregated 
# MAGIC SELECT  '$RPStartdate' AS RPStartDate
# MAGIC        ,'$RPEnddate' AS RPEndDate
# MAGIC        ,'Birth' AS IndicatorFamily
# MAGIC        ,'Induction' AS Indicator
# MAGIC        ,Denominator.OrgCodeProvider AS OrgCodeProvider
# MAGIC        ,COALESCE(Denominator.Trust, c.NAME) AS OrgName
# MAGIC        ,'Provider' AS OrgLevel 
# MAGIC        ,COALESCE(numerator.Total, 0) AS UnroundedNumerator
# MAGIC        ,Denominator.Total AS UnroundedDenominator
# MAGIC        ,MaternityRate(COALESCE(Numerator.Total,0), Denominator.Total, NULL) AS UnroundedRate -- Default Rate
# MAGIC        ,MaternityRounding(COALESCE(Numerator.Total,0))   AS RoundedNumerator      -- Maternity Rounding
# MAGIC        ,MaternityRounding(Denominator.Total)  AS RoundedDenominator    -- Maternity Rounding
# MAGIC        ,MaternityRate(MaternityRounding(COALESCE(Numerator.Total, 0)), MaternityRounding(Denominator.Total), 0) AS RoundedRate -- Maternity Rate & Maternity Rounding
# MAGIC        ,0 AS OverDQThreshold 
# MAGIC        ,current_timestamp() as CreatedAt 
# MAGIC FROM Denominator
# MAGIC       LEFT OUTER JOIN Numerator ON Numerator.OrgCodeProvider = Denominator.OrgCodeProvider
# MAGIC       LEFT OUTER JOIN cteclosed AS c on Denominator.OrgCodeProvider = c.ORG_CODE   

# COMMAND ----------

# DBTITLE 1,Not Needed for Induction -   Set Ethnicity and IMD on raw data
# %sql
# WITH denominator AS 
# (
#   -- Mothers of interest
#   -- Assumes numerator is a subset of denominator
#   SELECT  raw.Person_ID
#          ,raw.OrgCodeProvider 
#   FROM $outSchema.Measures_raw raw
#   WHERE raw.IndicatorFamily = 'Birth'
#     AND raw.Indicator = 'Induction'
#     AND raw.IsDenominator = 1
#   GROUP BY  raw.Person_ID
#            ,raw.OrgCodeProvider
# )
# ,person AS 
# (
#   SELECT Person_ID 
#   FROM denominator 
#   GROUP BY Person_ID
# ) 
# ,demographicsAll AS 
# (
#   SELECT  001md.Person_ID_Mother
#          ,COALESCE(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) AS OrgCodeProvider
#          ,001md.EthnicCategoryMother
#          ,CASE WHEN 001md.EthnicCategoryMother IN ('D', 'E', 'F', 'H', 'J', 'K', 'M', 'L', 'N', 'P') THEN 'BAME'
#                WHEN 001md.EthnicCategoryMother IN ('A', 'B', 'C') THEN 'White'
#                WHEN 001md.EthnicCategoryMother IN ('G', 'R', 'S') THEN 'Other'
#                ELSE 'Missing_Unknown'
#           END AS EthnicGroup
#          ,eid.DECI_IMD
#          ,row_number() OVER (
#                               PARTITION BY 001md.Person_ID_Mother, coalesce(ord.REL_FROM_ORG_CODE, 001md.OrgCodeProvider) 
#                               ORDER BY 001md.RecordNumber DESC
#                             ) AS rank
#   FROM $source.msd001motherdemog AS 001md
#         INNER JOIN person AS p on 001md.Person_ID_Mother = p.Person_ID -- filter to mothers identified in PCSP_Numerator_Raw
#         LEFT JOIN $dss_corporate.org_relationship_daily AS ord ON 001md.OrgCodeProvider = ord.REL_TO_ORG_CODE AND 
#                                                                   ord.REL_IS_CURRENT = 1 AND 
#                                                                   ord.REL_TYPE_CODE = 'P' AND 
#                                                                   ord.REL_FROM_ORG_TYPE_CODE = 'TR' AND 
#                                                                   ord.REL_OPEN_DATE <= '$RPEnddate'  AND 
#                                                                   (ord.REL_CLOSE_DATE is null or ord.REL_CLOSE_DATE > '$RPEnddate')
#         LEFT JOIN $dss_corporate.english_indices_of_dep_v02 AS eid ON 001md.LSOAMother2011 = eid.LSOA_CODE_2011 AND 
#                                                                       eid.IMD_YEAR = 2019
#   WHERE RPStartDate <= '$RPEnddate' 
# )
# ,demographics AS 
# (
#   SELECT  da.Person_ID_Mother
#          ,da.OrgCodeProvider
#          ,COALESCE(i.Description, 'Missing_Unknown') AS Rank_IMD_Decile
#          ,da.EthnicCategoryMother, da.EthnicGroup 
#   FROM demographicsAll AS da
#         LEFT JOIN $outSchema.IMD AS i ON da.DECI_IMD = i.Rank_IMD_Decile
#   WHERE da.rank = 1
# )
# MERGE 
# INTO $outSchema.Measures_raw AS raw
# USING (
#         SELECT  Person_ID_Mother
#                ,OrgCodeProvider
#                ,Rank_IMD_Decile
#                ,EthnicCategoryMother
#                ,EthnicGroup 
#         FROM demographics
# ) AS demo
# ON raw.Person_ID = demo.Person_ID_Mother AND 
#    raw.OrgCodeProvider = demo.OrgCodeProvider AND
#    raw.Indicator = 'Induction' AND
#    raw.IndicatorFamily = 'Birth' 
# WHEN MATCHED 
#   THEN UPDATE SET  raw.Rank_IMD_Decile = demo.Rank_IMD_Decile
#                   ,raw.EthnicCategory = demo.EthnicCategoryMother
#                   ,raw.EthnicGroup = demo.EthnicGroup;


# COMMAND ----------

# DBTITLE 1,Determine if provider is over DQ threshold - from Measures_Aggregate
for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = ("\
      UPDATE {outSchema}.Measures_Aggregated \
      SET IsOverDQThreshold =  CASE \
                                 WHEN Rounded_Rate > {OverDQ} \
                                   THEN 1 \
                                   ELSE 0 \
                               END \
      WHERE Indicator = '{Indicator}' \
      AND OrgLevel = 'Provider'"
    ).format(outSchema=outSchema, Indicator=Indicator, OverDQ=OverDQ)
    print(sql)
    spark.sql(sql)           
;

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold on measures_raw based on org code provider in aggregate table
for key, value in Aggregates.items():
    
    IndicatorFamily = value["IndicatorFamily"]
    Indicator = value["Indicator"]
    OverDQ = value["OverDQ"]
    
    
    print(IndicatorFamily,Indicator,OverDQ)
    
    sql = ("\
      WITH ProviderOverDQThreshold \
      AS \
      ( \
        SELECT OrgCodeProvider \
        FROM {outSchema}.Measures_Aggregated \
        WHERE Indicator = '{Indicator}' \
          AND RPStartDate = '{RPStartdate}' \
          AND IsOverDQThreshold = 1 \
        GROUP BY OrgCodeProvider \
      ) \
      UPDATE {outSchema}.Measures_raw raw \
      SET IsOverDQThreshold = 1 \
      WHERE EXISTS ( SELECT 1 \
                     FROM ProviderOverDQThreshold overDQ \
                     WHERE raw.OrgCodeProvider = overDQ.OrgCodeProvider \
                   ) \
        AND raw.Indicator = '{Indicator}' \
        AND raw.RPStartDate = '{RPStartdate}'"
    ).format(outSchema=outSchema, Indicator=Indicator, OverDQ=OverDQ, RPStartdate=RPStartdate)
    print(sql)
    spark.sql(sql)           
;

# COMMAND ----------

# DBTITLE 1,Determine if provider is over DQ threshold - SQL
# %sql  

# -- Identify if data from provider is over the DQ threshold (rounded_rate >= 5%)

# UPDATE $outSchema.Measures_Aggregated 
# SET IsOverDQThreshold = CASE 
#                         WHEN Rounded_Rate > 5  
#                           THEN 1 
#                           ELSE 0 
#                        END 
# WHERE Indicator = 'Induction'
# ;

# COMMAND ----------

# DBTITLE 1,Set OverDQThreshold on measures_raw based on org code provider in aggregate table
# %sql
# WITH ProviderOverDQThreshold 
# AS 
# (
#   SELECT OrgCodeProvider
#   FROM $outSchema.Measures_Aggregated
#   WHERE Indicator = 'Induction'
#     AND RPStartDate = '$RPStartdate'
#   GROUP BY OrgCodeProvider
# )
# UPDATE $outSchema.Measures_raw raw
# SET IsOverDQThreshold = 1 
# WHERE EXISTS ( SELECT 1 
#                FROM ProviderOverDQThreshold overDQ
#                WHERE raw.OrgCodeProvider = overDQ.OrgCodeProvider 
#              )
#   AND raw.Indicator = 'Induction' 
#   AND raw.RPStartDate = '$RPStartdate'


# COMMAND ----------

dbutils.notebook.exit("Notebook: InductionOfLabour_Aggregate ran successfully")