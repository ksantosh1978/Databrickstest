# Databricks notebook source
# DBTITLE 1,Birth Modes - Denominator
# MAGIC %sql
# MAGIC
# MAGIC insert into $outSchema.nmpa_denominator_raw
# MAGIC
# MAGIC select 
# MAGIC   id.msd101_id as KeyValue
# MAGIC   ,'$RPStartdate' as RPStartDate
# MAGIC   ,'$RPEnddate'as RPEndDate
# MAGIC   ,ld.Person_ID_Mother as Person_ID
# MAGIC   ,ld.UniqPregID as PregnancyID
# MAGIC   ,p.OrgCodeProvider 
# MAGIC   ,rank() OVER (PARTITION BY p.Person_ID_Mother ORDER BY p.RecordNumber DESC) as rank
# MAGIC from 
# MAGIC   $dbSchema.msd301LabourDelivery ld 
# MAGIC inner join 
# MAGIC   $dbSchema.msd401BabyDemographics p 
# MAGIC on
# MAGIC   ld.Person_ID_Mother = p.Person_ID_Mother and ld.UniqPregID = p.UniqPregID and ld.LabourDeliveryID = p.LabourDeliveryID and ld.RecordNumber = p.RecordNumber
# MAGIC left join 
# MAGIC   $dbSchema.msd101pregnancybooking id 
# MAGIC on
# MAGIC   ld.Person_ID_Mother = id.Person_ID_Mother and ld.UniqPregID = id.UniqPregID and ld.RecordNumber = id.RecordNumber
# MAGIC where ld.BirthsPerLabandDel = 1
# MAGIC and p.PregOutcome = 1
# MAGIC and p.GestationLengthBirth >= 259 and p.GestationLengthBirth <= 315
# MAGIC and ld.RPStartDate = '$RPStartdate' and p.RPStartDate = '$RPStartdate'
# MAGIC and p.PersonBirthDateBaby >= '$RPStartdate' and p.PersonBirthDateBaby <= '$RPEnddate'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view denominatorMeasures
# MAGIC as
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_DQ'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_Home'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_FMU'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_AMU'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_Obstetric'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_Other'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_Midwife'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Setting_Unknown'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view settings_numerator_staging
# MAGIC
# MAGIC as
# MAGIC
# MAGIC select 
# MAGIC   id.msd101_id as KeyValue
# MAGIC   ,'$RPStartdate' as RPStartDate
# MAGIC   ,'$RPEnddate' as RPEndDate
# MAGIC   ,'Birth' as IndicatorFamily
# MAGIC   ,ld.Person_ID_Mother as Person_ID
# MAGIC   ,ld.UniqPregID as PregnancyID
# MAGIC   ,p.OrgCodeProvider 
# MAGIC   ,rank() OVER (PARTITION BY p.Person_ID_Mother ORDER BY p.RecordNumber DESC) as rank
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('01','02','03','04','05','06','07','08','09','10','11','12','98')
# MAGIC     then 1 end as Setting_DQ
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('04','05')
# MAGIC     then 1 end as Setting_Home
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('03')
# MAGIC     then 1 end as Setting_FMU
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('02')
# MAGIC     then 1 end as Setting_AMU
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('01')
# MAGIC     then 1 end as Setting_Obstetric
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('06','07','08','09','10','11','12','98')
# MAGIC     then 1 end as Setting_Other
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth in ('02','03','04','05')
# MAGIC     then 1 end as Setting_Midwife
# MAGIC   ,case when
# MAGIC       p.SettingPlaceBirth not in ('01','02','03','04','05','06','07','08','09','10','11','12','98') or SettingPlaceBirth is null or SettingPlaceBirth = '99'
# MAGIC     then 1 end as Setting_Unknown
# MAGIC from 
# MAGIC   $dbSchema.msd301LabourDelivery ld 
# MAGIC inner join 
# MAGIC   $dbSchema.msd401BabyDemographics p 
# MAGIC on
# MAGIC   ld.Person_ID_Mother = p.Person_ID_Mother and ld.UniqPregID = p.UniqPregID and ld.LabourDeliveryID = p.LabourDeliveryID and ld.RecordNumber = p.RecordNumber
# MAGIC left join 
# MAGIC   $dbSchema.msd101pregnancybooking id 
# MAGIC on
# MAGIC   ld.Person_ID_Mother = id.Person_ID_Mother and ld.UniqPregID = id.UniqPregID and ld.RecordNumber = id.RecordNumber
# MAGIC where ld.BirthsPerLabandDel = 1
# MAGIC and p.PregOutcome = 1
# MAGIC and p.GestationLengthBirth >= 259 and p.GestationLengthBirth <= 315
# MAGIC and ld.RPStartDate = '$RPStartdate' and p.RPStartDate = '$RPStartdate'
# MAGIC and p.PersonBirthDateBaby >= '$RPStartdate' and p.PersonBirthDateBaby <= '$RPEnddate'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Numerator
# sqlContext is deprecated
from datetime import datetime
measures = ['Setting_DQ','Setting_Home','Setting_FMU','Setting_AMU','Setting_Obstetric','Setting_Other','Setting_Midwife','Setting_Unknown']

for a in measures:
   sqlContext.sql("""
insert into """ + dbutils.widgets.get("outSchema") + """.nmpa_numerator_raw

select 
  KeyValue
  ,RPStartDate
  ,RPEndDate
  ,'""" + a + """' as Indicator
  ,'Birth' as IndicatorFamily
  ,Person_ID
  ,PregnancyID
  ,OrgCodeProvider 
  ,rank
from 
  global_temp.settings_numerator_staging 
where """ + a + """ = 1""")


# COMMAND ----------

# DBTITLE 1,Denominator view
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW AggDenominatorGeogView AS
# MAGIC select 
# MAGIC   d.OrgCodeProvider
# MAGIC   ,g.Trust as Org_Name
# MAGIC   ,case when g.regionOrg is null then '' else g.regionOrg end as regionOrg
# MAGIC   ,g.region
# MAGIC   ,case when g.Mbrrace_Grouping is null then '' else g.Mbrrace_Grouping end as Mbrrace_Grouping 
# MAGIC   ,case when g.Mbrrace_Grouping_Short is null then '' else g.Mbrrace_Grouping_short end as Mbrrace_Grouping_Short
# MAGIC   ,case when g.stp_code is null then '' else g.stp_code end as stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,d.RPStartDate as ReportingPeriodStartDate
# MAGIC   ,d.RPEndDate as ReportingPeriodEndDate
# MAGIC   ,dm.Indicator
# MAGIC   , 'Birth' as IndicatorFamily
# MAGIC   ,'Denominator' as Currency
# MAGIC   ,count(distinct Person_ID) as Value
# MAGIC from
# MAGIC   $outSchema.nmpa_denominator_raw d
# MAGIC left join
# MAGIC   $outSchema.geogtlrr g
# MAGIC on
# MAGIC   d.OrgCodeProvider = g.Trust_ORG
# MAGIC left join
# MAGIC   global_temp.denominatorMeasures dm
# MAGIC on d.OrgCodeProvider = dm.OrgCodeProvider
# MAGIC where 
# MAGIC   rank = 1
# MAGIC and 
# MAGIC   dm.Indicator in ('Setting_DQ','Setting_Home','Setting_FMU','Setting_AMU','Setting_Obstetric','Setting_Other','Setting_Midwife','Setting_Unknown')
# MAGIC group by
# MAGIC   d.OrgCodeProvider
# MAGIC   ,g.Trust
# MAGIC   ,g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,g.Mbrrace_Grouping
# MAGIC   ,g.Mbrrace_Grouping_Short
# MAGIC   ,'Denominator'
# MAGIC   ,d.RPStartDate
# MAGIC   ,d.RPEndDate 
# MAGIC   ,dm.Indicator
# MAGIC   ,IndicatorFamily

# COMMAND ----------

# DBTITLE 1,Numerator view
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW AggNumeratorGeogView AS
# MAGIC with cte as (
# MAGIC select 
# MAGIC   n.OrgCodeProvider
# MAGIC   ,g.Trust as Org_Name
# MAGIC   ,case when g.regionOrg is null then '' else g.regionOrg end as regionOrg
# MAGIC   ,g.region
# MAGIC   ,case when g.Mbrrace_Grouping is null then '' else g.Mbrrace_Grouping end as Mbrrace_Grouping
# MAGIC   ,case when g.Mbrrace_Grouping_Short is null then '' else g.Mbrrace_Grouping_Short end as Mbrrace_Grouping_Short
# MAGIC   ,case when g.stp_code is null then '' else g.stp_code end as stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,n.RPStartDate as ReportingPeriodStartDate
# MAGIC   ,n.RPEndDate as ReportingPeriodEndDate
# MAGIC   ,n.Indicator
# MAGIC   ,n.IndicatorFamily
# MAGIC   ,'Numerator' as Currency
# MAGIC   ,count(distinct Person_ID) as Value
# MAGIC from
# MAGIC   $outSchema.nmpa_numerator_raw n
# MAGIC left join
# MAGIC   $outSchema.geogtlrr g
# MAGIC on
# MAGIC   n.OrgCodeProvider = g.Trust_Org
# MAGIC where 
# MAGIC   n.rank = 1
# MAGIC and 
# MAGIC n.indicator in ('Setting_DQ','Setting_Home','Setting_FMU','Setting_AMU','Setting_Obstetric','Setting_Other','Setting_Midwife','Setting_Unknown')
# MAGIC group by
# MAGIC   n.OrgCodeProvider
# MAGIC   ,g.Trust
# MAGIC   ,g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.Mbrrace_Grouping
# MAGIC   ,g.Mbrrace_Grouping_Short
# MAGIC   ,g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,'Numerator'
# MAGIC   ,n.RPStartDate
# MAGIC   ,n.RPEndDate 
# MAGIC   ,n.Indicator
# MAGIC   ,n.IndicatorFamily
# MAGIC )
# MAGIC ,cte2 as (
# MAGIC select 
# MAGIC   d.OrgCodeProvider
# MAGIC   ,d.Org_Name
# MAGIC   ,case when d.Mbrrace_Grouping is null then '' else d.Mbrrace_Grouping end as Mbrrace_Grouping
# MAGIC   ,case when d.Mbrrace_Grouping_Short is null then '' else d.Mbrrace_Grouping_Short end as Mbrrace_Grouping_Short
# MAGIC   ,case when d.stp_code is null then '' else d.stp_code end as stp_code
# MAGIC   ,d.stp_name
# MAGIC   ,case when d.regionOrg is null then '' else d.regionOrg end as regionOrg
# MAGIC   ,d.region
# MAGIC   ,d.ReportingPeriodStartDate
# MAGIC   ,d.ReportingPeriodEndDate
# MAGIC   ,d.Indicator
# MAGIC   ,'Birth' as IndicatorFamily
# MAGIC   ,'Numerator' as Currency
# MAGIC   ,case when cte.Value is not null then cte.Value else 0 end as Value 
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView d
# MAGIC left join
# MAGIC   cte
# MAGIC on
# MAGIC   cte.OrgCodeProvider = d.OrgCodeProvider
# MAGIC   and
# MAGIC     cte.Indicator = d.indicator
# MAGIC where d.indicator in ('Setting_DQ','Setting_Home','Setting_FMU','Setting_AMU','Setting_Obstetric','Setting_Other','Setting_Midwife','Setting_Unknown')
# MAGIC )
# MAGIC select * from cte2
# MAGIC
# MAGIC order by orgcodeprovider

# COMMAND ----------

# DBTITLE 1,Org Result View
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW dqCountsView 
# MAGIC AS
# MAGIC select 
# MAGIC   OrgCodeProvider
# MAGIC   ,indicator
# MAGIC   ,count(distinct Person_ID) as qty
# MAGIC from 
# MAGIC   $outSchema.nmpa_numerator_raw 
# MAGIC where indicator = 'Setting_DQ' and rank =1
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,indicator;
# MAGIC   
# MAGIC
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ResultView AS
# MAGIC select distinct
# MAGIC   d.OrgCodeProvider
# MAGIC   ,d.Org_Name
# MAGIC   ,d.regionOrg
# MAGIC   ,d.region
# MAGIC   ,d.Mbrrace_Grouping 
# MAGIC   ,d.Mbrrace_Grouping_Short
# MAGIC   ,d.stp_code
# MAGIC   ,d.stp_name
# MAGIC   ,d.ReportingPeriodStartDate
# MAGIC   ,d.ReportingPeriodEndDate
# MAGIC   ,d.Indicator
# MAGIC   ,d.IndicatorFamily
# MAGIC   ,'Result' as Currency
# MAGIC   ,case when dq.qty is null then 0 else dq.qty end as qty
# MAGIC   ,d.Value
# MAGIC from
# MAGIC   global_temp.AggDenominatorGeogView d
# MAGIC left join
# MAGIC   global_temp.dqCountsView dq
# MAGIC on
# MAGIC   dq.OrgCodeProvider = d.OrgCodeProvider
# MAGIC
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Org Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW orgResults AS
# MAGIC select 
# MAGIC   r.OrgCodeProvider
# MAGIC   ,r.Org_Name
# MAGIC   ,r.ReportingPeriodStartDate
# MAGIC   ,r.ReportingPeriodEndDate
# MAGIC   ,r.Indicator
# MAGIC   ,r.IndicatorFamily
# MAGIC   ,r.Currency
# MAGIC   ,Case when sum(r.qty)/sum(r.Value) > 0.8 then 'Pass'
# MAGIC     else 'Fail' end as Value
# MAGIC from 
# MAGIC   global_temp.ResultView r
# MAGIC group by
# MAGIC   r.OrgCodeProvider
# MAGIC   ,r.Org_Name
# MAGIC   ,r.ReportingPeriodStartDate
# MAGIC   ,r.ReportingPeriodEndDate
# MAGIC   ,r.Indicator
# MAGIC   ,r.IndicatorFamily
# MAGIC   ,r.Currency;
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW orgPassFail AS
# MAGIC select 
# MAGIC   distinct 
# MAGIC     OrgCodeprovider
# MAGIC     ,Value 
# MAGIC from 
# MAGIC   global_temp.OrgResults

# COMMAND ----------

# DBTITLE 1,CSV Output - Organisation Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvOrgAggregation as
# MAGIC
# MAGIC with den as (
# MAGIC
# MAGIC select 
# MAGIC   d.OrgCodeProvider
# MAGIC   ,d.Org_Name
# MAGIC   ,d.ReportingPeriodStartDate
# MAGIC   ,d.ReportingPeriodEndDate
# MAGIC   ,d.Indicator
# MAGIC   ,d.IndicatorFamily
# MAGIC   ,d.Currency
# MAGIC   ,Case when d.Value = 0 then 0
# MAGIC         when d.Value >= 1 and d.Value <=7 then 5
# MAGIC         when d.Value > 7 then round(d.Value/5,0)*5
# MAGIC    end as Value
# MAGIC   
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView d),
# MAGIC  
# MAGIC num as (
# MAGIC  
# MAGIC select 
# MAGIC   n.OrgCodeProvider
# MAGIC   ,n.Org_Name
# MAGIC   ,n.ReportingPeriodStartDate
# MAGIC   ,n.ReportingPeriodEndDate
# MAGIC   ,n.Indicator
# MAGIC   ,n.IndicatorFamily
# MAGIC   ,n.Currency
# MAGIC   ,Case when n.Value = 0 then 0
# MAGIC         when n.Value >= 1 and n.Value <=7 then 5
# MAGIC         when n.Value > 7 then round(n.Value/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView n)
# MAGIC   
# MAGIC select 
# MAGIC   den.OrgCodeProvider
# MAGIC   ,den.Org_Name
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,case when den.indicator = 'Setting_DQ' then round((num.Value/den.Value)*100, 1)
# MAGIC     else case when res.Value = 'Fail' then 'Low DQ'
# MAGIC           else round((num.Value/den.Value)*100, 1)
# MAGIC          end
# MAGIC    end as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.OrgCodeProvider = num.OrgCodeProvider
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC left join 
# MAGIC   (select distinct OrgCodeProvider, Value from global_temp.orgResults) res
# MAGIC on
# MAGIC   res.OrgCodeProvider = den.OrgCodeProvider
# MAGIC   
# MAGIC union all
# MAGIC  
# MAGIC select * from den
# MAGIC  
# MAGIC union all
# MAGIC  
# MAGIC select * from num
# MAGIC  
# MAGIC union all 
# MAGIC
# MAGIC select * from global_temp.orgResults
# MAGIC  
# MAGIC Order by OrgCodeProvider, Indicator, Currency
# MAGIC

# COMMAND ----------

# DBTITLE 1,CSV Output - STP Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvSTPAggregation as
# MAGIC
# MAGIC with den as (
# MAGIC select 
# MAGIC   g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC )
# MAGIC ,num as 
# MAGIC (select 
# MAGIC   g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.stp_code
# MAGIC   ,g.stp_name
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   den.stp_code
# MAGIC   ,den.stp_name
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,round((sum(num.Value)/sum(den.Value))*100, 1) as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.stp_code = num.stp_code
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC group by
# MAGIC   den.stp_code
# MAGIC   ,den.stp_name
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,den.Currency
# MAGIC   
# MAGIC union all
# MAGIC  
# MAGIC select * from den
# MAGIC  
# MAGIC union all
# MAGIC  
# MAGIC select * from num
# MAGIC
# MAGIC
# MAGIC Order by stp_code, Indicator, Currency

# COMMAND ----------

# DBTITLE 1,CSV Output - Region Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvRegAggregation as
# MAGIC
# MAGIC
# MAGIC with den as (
# MAGIC select 
# MAGIC   g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC ,num as (
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.regionOrg
# MAGIC   ,g.region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC  
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   den.regionOrg
# MAGIC   ,den.region
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,round((sum(num.Value)/sum(den.Value))*100, 1) as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.regionOrg = num.regionOrg
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC group by
# MAGIC   den.regionOrg
# MAGIC   ,den.region
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,den.Currency
# MAGIC   
# MAGIC union all
# MAGIC  
# MAGIC select * from den
# MAGIC union all
# MAGIC select * from num
# MAGIC  
# MAGIC Order by regionOrg, Indicator, Currency
# MAGIC

# COMMAND ----------

# DBTITLE 1,CSV Output - Mbrrace Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvMbrraceAggregation as
# MAGIC with den as (
# MAGIC  
# MAGIC select 
# MAGIC   g.Mbrrace_Grouping_Short as regionOrg
# MAGIC   ,g.Mbrrace_Grouping as region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.Mbrrace_Grouping
# MAGIC   ,g.Mbrrace_Grouping_Short
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC , num as (
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   g.Mbrrace_Grouping_Short as regionOrg
# MAGIC   ,g.Mbrrace_Grouping as region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   g.Mbrrace_Grouping
# MAGIC   ,g.Mbrrace_Grouping_Short
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   den.RegionOrg
# MAGIC   ,den.region
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,round((sum(num.Value)/sum(den.Value))*100, 1) as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.regionOrg = num.regionOrg
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC group by
# MAGIC   den.regionOrg
# MAGIC   ,den.region
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,den.Currency
# MAGIC   
# MAGIC union all
# MAGIC select * from den
# MAGIC union all
# MAGIC select * from num
# MAGIC Order by regionOrg, Indicator, Currency
# MAGIC

# COMMAND ----------

# DBTITLE 1,CSV Output - National Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvNatAggregation as
# MAGIC with den as (
# MAGIC select 
# MAGIC   'National' as regionOrg
# MAGIC   ,'All Submitters' as region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   'National'
# MAGIC   ,'All Submitters'
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC  
# MAGIC ,num as 
# MAGIC (
# MAGIC select 
# MAGIC   distinct
# MAGIC   'National' as regionOrg
# MAGIC   ,'All Submitters' as region
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency
# MAGIC   ,case when sum(g.Value) = 0 then 0
# MAGIC         when sum(g.Value) >=1 and sum(g.Value) <= 7 then 5
# MAGIC         when sum(g.Value) > 7 then round(sum(g.Value)/5,0)*5
# MAGIC     end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView g
# MAGIC inner join
# MAGIC   global_temp.orgPassFail pf
# MAGIC on 
# MAGIC   g.OrgCodeProvider = pf.OrgCodeProvider
# MAGIC where 
# MAGIC   pf.Value = 'Pass'
# MAGIC group by
# MAGIC   'National'
# MAGIC   ,'All Submitters'
# MAGIC   ,g.ReportingPeriodStartDate
# MAGIC   ,g.ReportingPeriodEndDate
# MAGIC   ,g.Indicator
# MAGIC   ,g.IndicatorFamily
# MAGIC   ,g.Currency)
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   'National' as RegionOrg
# MAGIC   ,'All Submitters' as region
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,'Rate' as Currency
# MAGIC   ,round((sum(num.Value)/sum(den.Value))*100, 1) as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.regionOrg = num.regionOrg
# MAGIC and
# MAGIC   den.indicator = num.indicator
# MAGIC group by
# MAGIC   'National'
# MAGIC   ,'All Submitters'
# MAGIC   ,den.ReportingPeriodStartDate
# MAGIC   ,den.ReportingPeriodEndDate
# MAGIC   ,den.Indicator
# MAGIC   ,den.IndicatorFamily
# MAGIC   ,den.Currency
# MAGIC   
# MAGIC union all
# MAGIC select * from den
# MAGIC union all
# MAGIC select * from num
# MAGIC  
# MAGIC Order by regionOrg, Indicator, Currency
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insert into CSV table
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO $outSchema.nmpa_CSV
# MAGIC SELECT
# MAGIC   OrgCodeProvider
# MAGIC   ,Org_Name as OrgName
# MAGIC   ,'Provider' as OrgLevel
# MAGIC   ,ReportingPeriodStartDate as RPStartDate
# MAGIC   ,ReportingPeriodEndDate as RPEndDate
# MAGIC   ,IndicatorFamily
# MAGIC   ,Indicator
# MAGIC   ,Currency
# MAGIC   ,Value
# MAGIC   ,current_timestamp() AS CreatedAt
# MAGIC FROM 
# MAGIC   global_temp.csvOrgAggregation
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   stp_code as OrgCodeProvider
# MAGIC   ,stp_name as OrgName
# MAGIC   ,'Local Maternity System' as OrgLevel
# MAGIC   ,ReportingPeriodStartDate as RPStartDate
# MAGIC   ,ReportingPeriodEndDate as RPEndDate
# MAGIC   ,IndicatorFamily
# MAGIC   ,Indicator
# MAGIC   ,Currency
# MAGIC   ,Value
# MAGIC   ,current_timestamp() AS CreatedAt
# MAGIC FROM 
# MAGIC   global_temp.csvSTPAggregation
# MAGIC
# MAGIC UNION all
# MAGIC SELECT
# MAGIC   regionOrg as OrgCodeProvider
# MAGIC   ,region as OrgName
# MAGIC   ,'NHS England (Region)' as OrgLevel
# MAGIC   ,ReportingPeriodStartDate as RPStartDate
# MAGIC   ,ReportingPeriodEndDate as RPEndDate
# MAGIC   ,IndicatorFamily
# MAGIC   ,Indicator
# MAGIC   ,Currency
# MAGIC   ,Value
# MAGIC   ,current_timestamp() AS CreatedAt
# MAGIC FROM 
# MAGIC   global_temp.csvRegAggregation
# MAGIC   
# MAGIC   
# MAGIC UNION all
# MAGIC SELECT
# MAGIC   regionOrg as OrgCodeProvider
# MAGIC   ,region as OrgName
# MAGIC   ,'National' as OrgLevel
# MAGIC   ,ReportingPeriodStartDate as RPStartDate
# MAGIC   ,ReportingPeriodEndDate as RPEndDate
# MAGIC   ,IndicatorFamily
# MAGIC   ,Indicator
# MAGIC   ,Currency
# MAGIC   ,Value
# MAGIC   ,current_timestamp() AS CreatedAt
# MAGIC FROM 
# MAGIC   global_temp.csvNatAggregation
# MAGIC   
# MAGIC UNION all
# MAGIC SELECT
# MAGIC   regionOrg as OrgCodeProvider
# MAGIC   ,region as OrgName
# MAGIC   ,'MBRRACE Grouping' as OrgLevel
# MAGIC   ,ReportingPeriodStartDate as RPStartDate
# MAGIC   ,ReportingPeriodEndDate as RPEndDate
# MAGIC   ,IndicatorFamily
# MAGIC   ,Indicator
# MAGIC   ,Currency
# MAGIC   ,Value
# MAGIC   ,current_timestamp() AS CreatedAt
# MAGIC FROM 
# MAGIC   global_temp.csvMbrraceAggregation 
# MAGIC   
# MAGIC order by OrgLevel, OrgCodeProvider, Indicator, Currency

# COMMAND ----------

dbutils.notebook.exit("Notebook: Births_In_Different_Settings ran successfully")