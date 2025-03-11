# Databricks notebook source
# MAGIC %md
# MAGIC ##MHA-4645 - Exploratory measure builds for Safeguarding##
# MAGIC
# MAGIC The measure builds in this notebook are based on requirements detailed in initial investigations undertaken in 2022 -
# MAGIC
# MAGIC ###MHA-1804 - Newer measure builds###
# MAGIC Following the original investigations, it was agreed with OHID stakeholders, and notified to the ERG (now the MSDS Data Quality Steering Group) without concerns being raised, that instead of looking in MSDS for any potential safeguarding causes we will instead move forward with building four measures looking in MSDS for two specific SNOMED codes to indicate unborn child or adult safeguarding concerns being raised:
# MAGIC
# MAGIC 1. Pregnancies where an adult safeguarding concern was identified -  
# MAGIC 766561000000109 - Adult safeguarding concern (finding) has been recorded&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;*(SafeguardingConcern_Adult - Measure name assigned in this notebook)*
# MAGIC 2. Pregnancies where an unborn child safeguarding concern was identified-   
# MAGIC 878111000000109 - Unborn child is cause for safeguarding concern (finding) has been recorded&emsp;&emsp;&emsp;&emsp;&emsp;*(SafeguardingConcern_UnbornChild - Measure name assigned in this notebook)*  
# MAGIC 3. Pregnancies where an adult or unborn child safeguarding concern was identified  
# MAGIC Either 766561000000109 **OR** 878111000000109 have been recorded&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;*(SafeguardingConcern_Adult_or_UnbornChild - Measure name assigned in this notebook)*   
# MAGIC 4. Pregnancies where an adult and unborn child safeguarding concern was identified    
# MAGIC Both 766561000000109 **AND** 878111000000109 have been recorded;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;*(SafeguardingConcern_Adult_and_UnbornChild- Measure name assigned in this notebook)*  
# MAGIC
# MAGIC This base cohort for these measures will be all deliveries (not just live births) that took place in the reporting month, then looking back for each over the pregnancy activity up to (but not after) the day of delivery to see if the above SNOMED codes were recorded at any time.
# MAGIC
# MAGIC These are to be reported as four separate measures, although as mentioned they will share the above cohort as a denominator  
# MAGIC
# MAGIC **MSD302CareActivityLabDel included in code search as a result of this confirmation -**   
# MAGIC On 21/08/2023, the customers (primarily Helen Duncan at OHID) confirmed that "Yes we would definitely want to include safeguarding concerns identified at the delivery itself as well as those identified at any time during the pregnancy up to that point. Some of the highest risk women book very late or not at all for pregnancy and can literally turn up to deliver. In such cases the booking information is completed during the delivery episode, as much as is feasibly possible. A failure to engage with antenatal care is, in most cases, a safeguarding concern in its own right."

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import row_number, count, when, lit, col, round, concat, upper, max, sum,expr
# MAGIC from pyspark.sql.types import DoubleType
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %py
# MAGIC ##Uncomment to update choices
# MAGIC # dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %py
# MAGIC #Uncomment to update choices 
# MAGIC # startchoices = [str(r[0]) for r in spark.sql("select distinct RPStartDate from mat_pre_clear.msd000header order by RPStartDate desc").collect()]
# MAGIC # endchoices = [str(r[0]) for r in spark.sql("select distinct RPEndDate from mat_pre_clear.msd000header order by RPEndDate desc").collect()]
# MAGIC
# MAGIC # dbutils.widgets.dropdown("RPStartDate", "2023-01-01", startchoices)
# MAGIC # dbutils.widgets.dropdown("RPEndDate", "2023-01-31", endchoices)
# MAGIC # dbutils.widgets.text("DatabaseSchema","mat_pre_clear")
# MAGIC # dbutils.widgets.text("GeogSchema","mat_analysis")
# MAGIC # dbutils.widgets.text("IndicatorFamily", "Pregnancy")
# MAGIC # dbutils.widgets.text("outschema_geog","john_middlewick1_101573")

# COMMAND ----------

# %sql
# -- use $DatabaseSchema

# COMMAND ----------

# MAGIC %py
# MAGIC #Collect widget values
# MAGIC StartDate = dbutils.widgets.get("RPStartDate")
# MAGIC EndDate = dbutils.widgets.get("RPEndDate")
# MAGIC DatabaseSchema=dbutils.widgets.get("dbSchema")
# MAGIC outSchema = dbutils.widgets.get("outSchema")
# MAGIC geog_schema = dbutils.widgets.get("outSchema")
# MAGIC dss_corporate = dbutils.widgets.get("dss_corporate")
# MAGIC IndicatorFamily = dbutils.widgets.get("IndicatorFamily")
# MAGIC outtable = dbutils.widgets.get("outtable")

# COMMAND ----------

# DBTITLE 1,Denominator
#Selects mothers delivery within the reporting month and reported in the same month

#msd401babydemographics to be used to indentifty babies born in month 
msd401 = table(f"{DatabaseSchema}.msd401babydemographics")
#Select required fields
cohort = (msd401.select(msd401.RPStartDate,msd401.RPEndDate,msd401.UniqPregID
                        ,msd401.Person_ID_Mother,msd401.PersonBirthDateBaby
                        ,msd401.OrgCodeProvider,msd401.RecordNumber,msd401.MSD401_ID ))
#Filter for deliveries within the reporting month and reported in the same month
cohort = (cohort
          .filter(cohort.RPStartDate == StartDate)
          .filter(cohort.PersonBirthDateBaby.between(StartDate,EndDate))
         )
cohort = (cohort
          .withColumn("ranking"
                      ,row_number().over(Window
                                        .partitionBy(cohort.Person_ID_Mother)
                                        .orderBy(cohort.RecordNumber.desc(),cohort.MSD401_ID.desc(),cohort.PersonBirthDateBaby.asc())
                                         )
                     )
         )
cohort = cohort.filter(cohort.ranking  == '1')
#Use latest or code reflecting org changes since booking
# cohort = cohort.alias("c").join(org_rel_daily,(cohort.OrgCodeProvider==org_rel_daily.Pred_Provider),"left").select(["c.*",org_rel_daily.Succ_Provider]) 
# cohort = cohort.withColumn("OrgCodeCurrent", when(col("Succ_Provider").isNull(),cohort.OrgCodeProvider).otherwise(cohort.Succ_Provider))  #Identify succesor code if exists
# cols=("OrgCodeProvider","Succ_Provider")
# cohort = cohort.drop(*cols).withColumnRenamed("OrgCodeCurrent","OrgCodeProvider")
# cohort.display()
denominator=(cohort
             .groupby(cohort.RPStartDate, cohort.RPEndDate, cohort.OrgCodeProvider)
             .count()
             .withColumnRenamed("count","Denominator")
            )

denominator.createOrReplaceTempView("denominator")
XX= spark.sql("SELECT SUM(denominator) FROM denominator")
XX.display()

# COMMAND ----------

#Define safeguarding codes to be used to search MSD109, MSD202 and MSD302
#"Adult safeguarding concern" codes
AdultCodes=['766561000000109']#,'XaXP4'
#"Unborn child safeguarding concern" Codes 
UnbornChildCodes=['878111000000109']#,'XaaNV'

# COMMAND ----------

# DBTITLE 1,Indentify safeguarding concerns recorded in MSD109FindingObsMother
#Find and flag where safeguarding codes found in MSD109FindingObsMother
msd109 = table(f"{DatabaseSchema}.MSD109FindingObsMother")
#Link Cohort to MSD109 and flag safeguarding codes
msd109 = (cohort
          .join(msd109
                , (cohort.Person_ID_Mother == msd109.Person_ID_Mother)&(cohort.UniqPregID == msd109.UniqPregID)
                , "inner")
          .select([cohort.RPStartDate, cohort.RPEndDate, cohort.UniqPregID, cohort.Person_ID_Mother, cohort.OrgCodeProvider, cohort.PersonBirthDateBaby, msd109.FindingCode, msd109.FindingDate])
         )
#Filter for findings before or on birth date or null
msd109=msd109.filter((msd109.FindingDate.isNull()== 'true')|(msd109.FindingDate<=msd109.PersonBirthDateBaby))
#Add flag to pregnancies with adult safeguarding concern 
msd109 = msd109.withColumn("ASCFlag", when(msd109.FindingCode.isin(AdultCodes),1).otherwise(0))
#Add flag to pregnancies with unborn child safeguarding concern
msd109 = msd109.withColumn("UBCFlag", when(msd109.FindingCode.isin(UnbornChildCodes),1).otherwise(0))
msd109.display()
# #Check
# Checkmsd109=msd109.filter(msd109.OrgCodeProvider=="R0A").filter(msd109.ASCFlag+msd109.UBCFlag>0)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select ObsCode from MSD109FindingObsMother
# MAGIC -- where ObsCode in ('766561000000109','878111000000109')
# MAGIC -- union all
# MAGIC -- select ObsCode from MSD202CareActivityPreg
# MAGIC -- where ObsCode in ('766561000000109','878111000000109')
# MAGIC -- union all
# MAGIC -- select ObsCode from MSD302CareActivityLabDel
# MAGIC -- where ObsCode in ('766561000000109','878111000000109')
# MAGIC
# MAGIC --select * from MSD109FindingObsMother where upper(MasterSnomedCTObsTerm) like "%SAFE%"
# MAGIC ---select * from MSD202CareActivityPreg where upper(MasterSnomedCTObsTerm) like "%SAFE%"
# MAGIC ---select * from MSD302CareActivityLabDel where upper(MasterSnomedCTObsTerm) like "%SAFE%"

# COMMAND ----------

# DBTITLE 1,Indentify safeguarding concerns recorded in MSD202CareActivityPreg
msd201 = table(f"{DatabaseSchema}.MSD201CareContactPreg")
msd202 = table(f"{DatabaseSchema}.MSD202CareActivityPreg")
#Link Cohort to MSD201 to enable link to MSD202CareActivityPreg
msd201 = (cohort
          .join(msd201
                ,(cohort.Person_ID_Mother == msd201.Person_ID_Mother)&(cohort.UniqPregID == msd201.UniqPregID)
                ,"inner" )
          .select([cohort.RPStartDate, cohort.PersonBirthDateBaby, cohort.RPEndDate, cohort.UniqPregID, cohort.Person_ID_Mother, cohort.OrgCodeProvider, msd201.CareConID, msd201.CContactDate, msd201.RPStartDate.alias("RPStartDate201"), msd201.OrgCodeProvider.alias("OrgCodeProvider201")])
         )
#Link to MSD202 for  finding codes
msd202 = (msd201
          .join(msd202
                ,(msd201.Person_ID_Mother == msd202.Person_ID_Mother)
                &(msd201.UniqPregID == msd202.UniqPregID)
                &(msd201.RPStartDate201 == msd202.RPStartDate)
                &(msd202.OrgCodeProvider == msd201.OrgCodeProvider201)
                &(msd201.CareConID==msd202.CareConID)
                ,"left")
          .select([msd201.RPStartDate, msd201.RPEndDate, msd201.UniqPregID, msd201.Person_ID_Mother, msd201.OrgCodeProvider, msd202.FindingCode, msd201.CContactDate, msd201.PersonBirthDateBaby])
         )
#Filter for findings before or on birth date or null
msd202=msd202.filter((msd202.CContactDate.isNull()=='true')|(msd202.CContactDate<=msd202.PersonBirthDateBaby))
#Add flag to pregnancies with adult safeguarding concern 
msd202 = msd202.withColumn("ASCFlag", when(msd202.FindingCode.isin(AdultCodes),1).otherwise(0))
#Add flag to pregnancies with unborn child safeguarding concern
msd202 = msd202.withColumn("UBCFlag", when(msd202.FindingCode.isin(UnbornChildCodes),1).otherwise(0))
msd202.display()
#Check
# Checkmsd202=msd202.filter(msd202.OrgCodeProvider=="R0A").filter(msd202.ASCFlag+msd202.UBCFlag>0)


# COMMAND ----------

# DBTITLE 1,Indentify safeguarding concerns recorded in MSD302CareActivityLabDel
#Concerns idebtified at delivery
msd301 = table(f"{DatabaseSchema}.MSD301LabourDelivery")
msd302 = table(f"{DatabaseSchema}.MSD302CareActivityLabDel")

#Link Cohort to MSD201 to enable link to MSD302
msd301 = (cohort
          .join(msd301
                ,(cohort.Person_ID_Mother == msd301.Person_ID_Mother)
                &(cohort.UniqPregID == msd301.UniqPregID)
                , "inner" )
          .select([cohort.RPStartDate, cohort.RPEndDate, cohort.UniqPregID, cohort.Person_ID_Mother, cohort.OrgCodeProvider, cohort.PersonBirthDateBaby, msd301.LabourDeliveryID, msd301.RPStartDate.alias("RPStartDate301"), msd301.OrgCodeProvider.alias("OrgCodeProvider301")])
         )
msd302 = (msd301
          .join(msd302
                , (msd301.Person_ID_Mother == msd302.Person_ID_Mother)
                &(msd301.UniqPregID == msd302.UniqPregID)
                &(msd301.RPStartDate301 == msd302.RPStartDate)
                &(msd302.OrgCodeProvider == msd301.OrgCodeProvider301)
                &(msd301.LabourDeliveryID==msd302.LabourDeliveryID)
                ,"left")
          .select([msd301.RPStartDate, msd301.RPEndDate, msd301.UniqPregID, msd301.Person_ID_Mother, msd301.OrgCodeProvider, msd301.PersonBirthDateBaby, msd302.ClinInterDateMother, msd302.FindingCode])
         )
#Filter for findings before or on birth date or null
msd302=msd302.filter((msd302.ClinInterDateMother.isNull()=='true')|(msd302.ClinInterDateMother<=msd302.PersonBirthDateBaby))
#Add flag pregnancies with adult safeguarding concern 
msd302 = msd302.withColumn("ASCFlag", when(msd302.FindingCode.isin(AdultCodes),1).otherwise(0))
#Add flag pregnancies with unborn child safeguarding concern
msd302 = msd302.withColumn("UBCFlag", when(msd302.FindingCode.isin(UnbornChildCodes),1).otherwise(0))
msd302.display()

# COMMAND ----------

# DBTITLE 1,Numerator - Combine tables findings, select distinct and aggregate
#Union of findings from msd109, msd202 and msd302
combined = msd109.union(msd202).union(msd302)
#Aggregate combined findings for distinct selection

combined=(combined
         .groupBy(combined.RPStartDate,combined.RPEndDate,combined.UniqPregID,combined.Person_ID_Mother,combined.OrgCodeProvider)
         .agg(max(combined.ASCFlag).alias("ASCFlag")
             ,max(combined.UBCFlag).alias("UBCFlag"))
         )

#Assign measure combinations to distinct pregnancies  
combined=(combined
          .withColumn("AdultSafeguardingConcern_Adult_or_UnbornChild", when(combined.ASCFlag==1,1)
                      .otherwise(0))
          .withColumn("SafeguardingConcern_UnbornChild", when(combined.UBCFlag==1,1)
                      .otherwise(0))
          .withColumn("SafeguardingConcern_Adult_or_UnbornChild", when((combined.UBCFlag==1) | (combined.ASCFlag==1),1)
                      .otherwise(0))
          .withColumn("SafeguardingConcern_Adult_and_UnbornChild", when((combined.UBCFlag==1) & (combined.ASCFlag==1),1)
                      .otherwise(0))
         )

#count of distinct pregnancies for each measure
combined=(combined
          .groupby(combined.RPStartDate,combined.RPEndDate,combined.OrgCodeProvider)
          .agg(sum(combined.AdultSafeguardingConcern_Adult_or_UnbornChild).alias("SafeguardingConcern_Adult")
               ,sum(combined.SafeguardingConcern_UnbornChild).alias("SafeguardingConcern_UnbornChild")
               ,sum(combined.SafeguardingConcern_Adult_or_UnbornChild).alias("SafeguardingConcern_Adult_or_UnbornChild")
               ,sum(combined.SafeguardingConcern_Adult_and_UnbornChild).alias("SafeguardingConcern_Adult_and_UnbornChild"))
         )
#Tranpose individual measure columns into a measure column
StackExpr = "Stack(4, 'SafeguardingConcern_Adult', SafeguardingConcern_Adult, 'SafeguardingConcern_UnbornChild', SafeguardingConcern_UnbornChild, 'SafeguardingConcern_Adult_or_UnbornChild', SafeguardingConcern_Adult_or_UnbornChild,'SafeguardingConcern_Adult_and_UnbornChild', SafeguardingConcern_Adult_and_UnbornChild) as (Indicator, Numerator)"
numerator = combined.select("RPStartDate","RPEndDate","OrgCodeProvider", expr(StackExpr))

# COMMAND ----------

#Create look up between predecessor / successor org -  provider trusts

org_rel_daily = table(f"{dss_corporate}.org_relationship_daily")
org_rel_daily = (org_rel_daily
                 .filter(org_rel_daily.REL_IS_CURRENT == '1')
                 .filter(org_rel_daily.REL_TYPE_CODE == 'P')
                 .filter(org_rel_daily.REL_FROM_ORG_TYPE_CODE == 'TR')
                 .filter(org_rel_daily.REL_OPEN_DATE <= EndDate)
                )
org_rel_daily = org_rel_daily.filter((org_rel_daily.REL_CLOSE_DATE > EndDate) | (org_rel_daily.REL_CLOSE_DATE.isNull() == 'true'))

org_rel_daily = (org_rel_daily
                 .select(org_rel_daily.REL_FROM_ORG_CODE, org_rel_daily.REL_TO_ORG_CODE)
                 .withColumnRenamed("REL_FROM_ORG_CODE", "Succ_Provider")
                 .withColumnRenamed("REL_TO_ORG_CODE", "Pred_Provider")
                )

#Some predecessor organisations map to multiple successors (which one is correct?)
#Exclude those orgs (RW6 is most recent affected predecesor)
dup_pred_to_succ_exc = (org_rel_daily
                        .groupBy("Pred_Provider")
                        .agg(count(org_rel_daily.Pred_Provider).alias("count"))
                        .where("count == 1")
                       )

org_rel_daily = (org_rel_daily.alias("g")
                 .join(dup_pred_to_succ_exc
                       , dup_pred_to_succ_exc.Pred_Provider == org_rel_daily.Pred_Provider
                       , "inner")
                 .select("g.Pred_Provider", "g.Succ_Provider")
                )

#Sometimes the successor organisation isn't the latest / current org 
#Loop around to find the current successor
count_provider = org_rel_daily.count()
loop_count = 0

while count_provider > 0:
  org_rel_daily_inter = (org_rel_daily.alias('u')
                         .join(org_rel_daily.alias('v')
                               , col("v.Pred_Provider") == col("u.Succ_Provider")
                               , "inner")
                         .withColumn("New_Provider", when(col("v.Succ_Provider").isNull() == 'true', col("u.Succ_Provider"))
                                     .otherwise(col("v.Succ_Provider")))
                         .select("u.Pred_Provider","New_Provider")
                         .withColumnRenamed("Pred_Provider","Old_Provider")
                        )
  org_rel_daily = (org_rel_daily
                   .join(org_rel_daily_inter
                         , org_rel_daily.Pred_Provider == org_rel_daily_inter.Old_Provider
                         , "left")
                   .withColumn("Succ_Provider_2", when(org_rel_daily_inter.New_Provider.isNull() == 'true', org_rel_daily.Succ_Provider)
                               .otherwise(org_rel_daily_inter.New_Provider))
                   .select(org_rel_daily.Pred_Provider, "Succ_Provider_2")
                   .withColumnRenamed("Succ_Provider_2","Succ_Provider")
                  )
  loop_count = loop_count + 1
  count_provider = org_rel_daily_inter.count()



# COMMAND ----------

# DBTITLE 1,Combine Numerator and Denominator
msd_final=(denominator
           .join(numerator
                 ,(denominator.OrgCodeProvider==numerator.OrgCodeProvider)
                 &(denominator.RPStartDate==numerator.RPStartDate)
                 ,"left")
           .select([denominator.RPStartDate, denominator.RPEndDate, denominator.OrgCodeProvider, numerator.Indicator, denominator.Denominator, numerator.Numerator])
          )
#Use latest or code reflecting org changes since booking
msd_final = (msd_final.alias("c")
             .join(org_rel_daily
                   ,(msd_final.OrgCodeProvider==org_rel_daily.Pred_Provider)
                   ,"left")
             .select(["c.*",org_rel_daily.Succ_Provider]) 
            )
msd_final = (msd_final
             .withColumn("OrgCodeCurrent", when(col("Succ_Provider").isNull(),msd_final.OrgCodeProvider)
                         .otherwise(msd_final.Succ_Provider))  #Identify succesor code if exists
            )
msd_final = (msd_final
             .select([msd_final.RPStartDate, msd_final.RPEndDate, msd_final.OrgCodeCurrent, msd_final.Indicator, msd_final.Denominator, msd_final.Numerator, msd_final.Succ_Provider])
            )
msd_final = msd_final.withColumnRenamed("OrgCodeCurrent","OrgCodeProvider")


# COMMAND ----------

#%run /data_analysts_collaboration/CMH_TURING_Team(Create)/code_sharing/geogtlrr/geogtlrr_table_creation $endperiod=$RPEndDate $outSchema=$outschema_geog

# COMMAND ----------

#Bring in the current geography table
geog_add = geog_schema + ".geogtlrr"
geography = table(geog_add)

geography = (geography
             .withColumnRenamed("Trust","Trust_Name")
             .withColumnRenamed("LRegion","LRegion_Name")
             .withColumnRenamed("Region","Region_Name")
            )


geog_flat = (geography.select(geography.Trust_ORG.alias("Org1"), geography.Trust_ORG, geography.Trust_Name, lit("Provider"))
             .union(geography.select(geography.Trust_ORG, geography.STP_Code, geography.STP_Name, lit("Local Maternity System")))
             .union(geography.select(geography.Trust_ORG, geography.RegionORG, geography.Region_Name, lit("NHS England (Region)")))
             .union(geography.select(geography.Trust_ORG, geography.Mbrrace_Grouping_Short, geography.Mbrrace_Grouping, lit('MBRRACE Grouping')))
            )

geog_flat = geog_flat.withColumnRenamed("Provider","OrgGrouping").distinct()


# COMMAND ----------

# DBTITLE 1,Assign geographies
#Assign each record a geography breakdown (provider, LMS, MBR, Region, National)
#Provider breakdown
msd_final_prov = (msd_final
                  .join(geography
                        , geography.Trust_ORG == msd_final.OrgCodeProvider
                        , "left")
                  .select(msd_final.RPStartDate,msd_final.RPEndDate,msd_final.Indicator,msd_final.Denominator,msd_final.Numerator, geography.Trust_ORG, geography.Trust_Name)
                  .withColumnRenamed("Trust_Name", "Org_Name")
                  .withColumnRenamed("Trust_ORG", "Org_Code")
                  .withColumn("Org_Level",lit("Provider"))
                 )

#Mbrrace breakdown
msd_final_mbr = (msd_final
                 .join(geography
                       , geography.Trust_ORG == msd_final.OrgCodeProvider
                       , "left")
                 .select(msd_final.RPStartDate,msd_final.RPEndDate,msd_final.Indicator,msd_final.Denominator,msd_final.Numerator, geography.Mbrrace_Grouping_Short, geography.Mbrrace_Grouping)
                 .withColumnRenamed("Mbrrace_Grouping", "Org_Name")
                 .withColumnRenamed("Mbrrace_Grouping_Short", "Org_Code")
                 .withColumn("Org_Level",lit("MBRRACE Grouping"))
                )

#Local Maternity service breakdown
msd_final_lms = (msd_final
                 .join(geography
                       , geography.Trust_ORG == msd_final.OrgCodeProvider
                       , "left")
                 .select(msd_final.RPStartDate,msd_final.RPEndDate,msd_final.Indicator,msd_final.Denominator,msd_final.Numerator, geography.LRegionORG, geography.LRegion_Name)
                 .withColumnRenamed("LRegion_Name", "Org_Name")
                 .withColumnRenamed("LRegionORG", "Org_Code")
                 .withColumn("Org_Level",lit("Local Maternity System"))
                )

#Region breakdown
msd_final_reg = (msd_final
                 .join(geography
                       , geography.Trust_ORG == msd_final.OrgCodeProvider
                       , "left")
                 .select(msd_final.RPStartDate,msd_final.RPEndDate,msd_final.Indicator,msd_final.Denominator,msd_final.Numerator, geography.RegionORG, geography.Region_Name)
                 .withColumnRenamed("Region_ORG", "Org_Code")
                 .withColumn("Org_Level",lit("NHS England (Region)"))
                )

#National breakdown
msd_final_nat = (msd_final
                 .select(msd_final.RPStartDate, msd_final.RPEndDate, msd_final_prov.Indicator, msd_final.Denominator, msd_final.Numerator)
                 .withColumn("Org_Code",lit("National"))
                 .withColumn("Org_Name",lit("All Submitters"))
                 .withColumn("Org_Level",lit("National"))
                )

#Union of all geographies
msd_final_combined = (msd_final_prov
                      .union(msd_final_mbr)
                      .union(msd_final_lms)
                      .union(msd_final_reg)
                      .union(msd_final_nat)
                      .withColumn("IndicatorFamily",lit(IndicatorFamily))
                     )

#Aggregate combined geographies
msd_final_combined=(msd_final_combined
                    .groupby(msd_final_combined.Org_Code, msd_final_combined.Org_Name, msd_final_combined.Org_Level, msd_final_combined.RPStartDate, msd_final_combined.RPEndDate, msd_final_combined.IndicatorFamily, msd_final_combined.Indicator, msd_final_combined.Indicator)
                    .agg(sum(msd_final_combined.Denominator).alias("Denominator")
                        ,sum(msd_final_combined.Numerator).alias("Numerator"))
                   )
msd_final_combined


# COMMAND ----------

# DBTITLE 1,Rate Calculation Suppression & Output
#Output suppressed
#Apply suppresion and rounding
output=(msd_final_combined
        .select(msd_final_combined.Org_Code, msd_final_combined.Org_Name, msd_final_combined.Org_Level, msd_final_combined.RPStartDate, msd_final_combined.RPEndDate, msd_final_combined.IndicatorFamily, msd_final_combined.Indicator, msd_final_combined.Denominator, msd_final_combined.Numerator)
        .withColumn("Denominator", when(col("Denominator").between(1,7), 5)
                    .otherwise(5 * round(col("Denominator") / 5)))
        .withColumn("Numerator", when(col("Numerator").between(1,7), 5)
                    .otherwise(5 * round(col("Numerator") / 5)))
       )
#Caluclate and add rate 
output=output.withColumn("Rate",round(output.Numerator/output.Denominator,1))
                        
#Transpose to standardised output format
StackExpr = "Stack(3, 'Denominator', Denominator, 'Numerator', Numerator, 'Rate', Rate) as (Currency, Value)"
output = output.select("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate","IndicatorFamily","Indicator", expr(StackExpr))
# output = output.sort(col("Org_Level").asc(),col("Org_Code").asc(),col("Indicator").asc())
#output = output.sort(col("Org_Level").asc(),col("Org_Code").asc(),col("Indicator").asc()).filter(output.Org_Code=="RCF").display()
output = output.sort(col("Org_Level").asc(),col("Org_Code").asc(),col("Indicator").asc())

# COMMAND ----------

#Ouput unsuppressed
outputUS=(msd_final_combined
          .select(msd_final_combined.Org_Code, msd_final_combined.Org_Name, msd_final_combined.Org_Level, msd_final_combined.RPStartDate, msd_final_combined.RPEndDate, msd_final_combined.IndicatorFamily, msd_final_combined.Indicator, msd_final_combined.Denominator, msd_final_combined.Numerator)
          .withColumn("Denominator", col("Denominator").cast("double"))
          .withColumn("Numerator", col("Numerator").cast("double"))
         )
#Caluclate and add rate 
outputUS=outputUS.withColumn("Rate",round(outputUS.Numerator/outputUS.Denominator,1))
             
#Transpose to standardised output format
StackExpr = "Stack(3, 'Denominator', Denominator, 'Numerator', Numerator, 'Rate', Rate) as (Currency, Value)"
outputUS = outputUS.select("Org_Code","Org_Name","Org_Level","RPStartDate","RPEndDate","IndicatorFamily","Indicator", expr(StackExpr))
outputUS = outputUS.sort(col("Org_Level").asc(),col("Org_Code").asc(),col("Indicator").asc())
# outputUS.sort(col("Org_Level").asc(),col("Org_Code").asc(),col("Indicator").asc()).filter(outputUS.Org_Code=="RN3").display()

# RAJ
# RCF
# RKE
# 
# RN3


# COMMAND ----------

output.write.format('delta').mode('overwrite').saveAsTable(f'{outSchema}.{outtable}')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $outSchema.measures_csv
# MAGIC SELECT
# MAGIC RPStartDate,
# MAGIC RPEndDate,
# MAGIC IndicatorFamily,
# MAGIC Indicator,
# MAGIC Org_Code as OrgCodeProvider,
# MAGIC Org_Name as OrgName,
# MAGIC Org_Level as OrgLevel,
# MAGIC Currency,
# MAGIC Value,
# MAGIC current_timestamp() AS CreatedAt 
# MAGIC
# MAGIC from $outSchema.$outtable where RPStartDate = '$RPStartDate'