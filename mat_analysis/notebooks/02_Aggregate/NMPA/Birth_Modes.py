# Databricks notebook source
# DBTITLE 1,Birth Modes - Denominator
# MAGIC %sql
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
# MAGIC and p.PersonBirthDateBaby >= '$RPStartdate' and p.PersonBirthDateBaby <= '$RPEnddate';

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view denominatorMeasures
# MAGIC as
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Spontaneous_vaginal'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Instrumental'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Instrumental_forceps'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Instrumental_ventouse'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Caesarean'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Caesarean_planned'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Caesarean_emergency'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator
# MAGIC union all
# MAGIC select 
# MAGIC   distinct orgCodeProvider
# MAGIC   ,'Mode_Other'as indicator
# MAGIC   ,count(*) 
# MAGIC from 
# MAGIC   $outSchema.nmpa_denominator_raw 
# MAGIC group by 
# MAGIC   OrgCodeProvider
# MAGIC   ,Indicator

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view nmpa_numerator_staging
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
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (0,1)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Spontaneous_Vaginal
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (2,3,4)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Instrumental
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (2,3)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Instrumental_Forceps
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (4)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Instrumental_Ventouse
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (7,8)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Caesarean
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (7)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Caesarean_Planned
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode in (8)
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Caesarean_Emergency
# MAGIC   ,case
# MAGIC     when
# MAGIC       p.deliveryMethodCode not in (0,1,2,3,4,7,8) 
# MAGIC       or 
# MAGIC         p.deliveryMethodCode is null
# MAGIC     then
# MAGIC       1
# MAGIC   end as Mode_Other
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
from datetime import datetime
measures = ['Mode_Spontaneous_vaginal','Mode_Instrumental','Mode_Instrumental_forceps','Mode_Instrumental_ventouse','Mode_Caesarean','Mode_Caesarean_planned','Mode_Caesarean_emergency','Mode_Other']

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
  global_temp.nmpa_numerator_staging 
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
# MAGIC   ,case when g.Mbrrace_Grouping_Short is null then '' else g.Mbrrace_Grouping_Short end as Mbrrace_Grouping_Short 
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
# MAGIC   dm.indicator in ('Mode_Spontaneous_vaginal','Mode_Instrumental','Mode_Instrumental_forceps','Mode_Instrumental_ventouse','Mode_Caesarean','Mode_Caesarean_planned','Mode_Caesarean_emergency','Mode_Other')
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
# MAGIC   n.Indicator in ('Mode_Spontaneous_vaginal','Mode_Instrumental','Mode_Instrumental_forceps','Mode_Instrumental_ventouse','Mode_Caesarean','Mode_Caesarean_planned','Mode_Caesarean_emergency','Mode_Other')
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
# MAGIC   ,case
# MAGIC       when 
# MAGIC         cte.Value is not null 
# MAGIC       then 
# MAGIC         cte.Value
# MAGIC       else 
# MAGIC         0
# MAGIC   end as Value 
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView d
# MAGIC left join
# MAGIC   cte
# MAGIC on
# MAGIC   cte.OrgCodeProvider = d.OrgCodeProvider
# MAGIC   and
# MAGIC     cte.Indicator = d.indicator
# MAGIC )
# MAGIC select * from cte2
# MAGIC
# MAGIC order by orgcodeprovider

# COMMAND ----------

# DBTITLE 1,CSV Output - Organisation Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvOrgAggregation as
# MAGIC
# MAGIC with den as (
# MAGIC  
# MAGIC  
# MAGIC select 
# MAGIC   d.OrgCodeProvider
# MAGIC   ,d.Org_Name
# MAGIC   ,d.ReportingPeriodStartDate
# MAGIC   ,d.ReportingPeriodEndDate
# MAGIC   ,d.Indicator
# MAGIC   ,d.IndicatorFamily
# MAGIC   ,d.Currency
# MAGIC   ,Case 
# MAGIC     when
# MAGIC       d.Value = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       d.Value >= 1 and d.Value <=7
# MAGIC     then 
# MAGIC       5
# MAGIC     when 
# MAGIC       d.Value > 7 
# MAGIC     then
# MAGIC       round(d.Value/5,0)*5
# MAGIC end as 
# MAGIC   Value
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
# MAGIC   ,Case 
# MAGIC     when
# MAGIC       n.Value = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       n.Value >= 1 and n.Value <=7
# MAGIC     then 
# MAGIC       5
# MAGIC     when 
# MAGIC       n.Value > 7 
# MAGIC     then
# MAGIC       round(n.Value/5,0)*5
# MAGIC end as 
# MAGIC   Value
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
# MAGIC   ,round((num.Value/den.Value)*100, 1) as Value
# MAGIC from
# MAGIC   den
# MAGIC left join 
# MAGIC   num
# MAGIC on 
# MAGIC   den.OrgCodeProvider = num.OrgCodeProvider
# MAGIC and
# MAGIC   den.indicator = num.indicator
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
# MAGIC Order by OrgCodeProvider, Indicator, Currency
# MAGIC

# COMMAND ----------

# DBTITLE 1,CSV Output - STP Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvSTPAggregation as
# MAGIC
# MAGIC with den as (
# MAGIC select 
# MAGIC   stp_code
# MAGIC   ,stp_name
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView 
# MAGIC group by
# MAGIC   stp_code
# MAGIC   ,stp_name
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC )
# MAGIC ,num as 
# MAGIC (select 
# MAGIC   stp_code
# MAGIC   ,stp_name
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView
# MAGIC group by
# MAGIC   stp_code
# MAGIC   ,stp_name
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
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
# MAGIC Order by stp_code, Indicator, Currency

# COMMAND ----------

# DBTITLE 1,CSV Output - Region Aggregation
# MAGIC %sql
# MAGIC create or replace global temp view csvRegAggregation as
# MAGIC
# MAGIC with den as (
# MAGIC select 
# MAGIC   regionOrg
# MAGIC   ,region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView 
# MAGIC group by
# MAGIC   regionOrg
# MAGIC   ,region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
# MAGIC ,num as (
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   regionOrg
# MAGIC   ,region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView
# MAGIC group by
# MAGIC   regionOrg
# MAGIC   ,region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
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
# MAGIC   Mbrrace_Grouping_Short as regionOrg
# MAGIC   ,Mbrrace_Grouping as region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView 
# MAGIC group by
# MAGIC   Mbrrace_Grouping_Short
# MAGIC   ,Mbrrace_Grouping
# MAGIC   --,'Mbrrace'
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
# MAGIC , num as (
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   Mbrrace_Grouping_Short as regionOrg
# MAGIC   ,Mbrrace_Grouping as region
# MAGIC  -- ,'Region' as region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView
# MAGIC group by
# MAGIC   Mbrrace_Grouping_Short
# MAGIC   ,Mbrrace_Grouping
# MAGIC --  ,'Mbrrace'
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
# MAGIC  
# MAGIC select 
# MAGIC   distinct
# MAGIC   den.RegionOrg
# MAGIC   ,den.region
# MAGIC   --,'NHS England (Region)' as region
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
# MAGIC --  ,'Mbrrace'
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
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggDenominatorGeogView 
# MAGIC group by
# MAGIC   'National'
# MAGIC   ,'All Submitters'
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
# MAGIC  
# MAGIC ,num as 
# MAGIC (
# MAGIC select 
# MAGIC   distinct
# MAGIC   'National' as regionOrg
# MAGIC   ,'All Submitters' as region
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency
# MAGIC   ,case 
# MAGIC     when 
# MAGIC       sum(Value) = 0 
# MAGIC     then
# MAGIC       0
# MAGIC     when 
# MAGIC       sum(Value) >=1 and sum(Value) <= 7
# MAGIC     then
# MAGIC       5
# MAGIC     when 
# MAGIC       sum(Value) > 7
# MAGIC     then
# MAGIC       round(sum(Value)/5,0)*5
# MAGIC   end as Value
# MAGIC from 
# MAGIC   global_temp.AggNumeratorGeogView
# MAGIC group by
# MAGIC   'National'
# MAGIC   ,'All Submitters'
# MAGIC   ,ReportingPeriodStartDate
# MAGIC   ,ReportingPeriodEndDate
# MAGIC   ,Indicator
# MAGIC   ,IndicatorFamily
# MAGIC   ,Currency)
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
# MAGIC   ,'All Submitters' as OrgName
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
# MAGIC order by OrgCodeProvider, Indicator, Currency

# COMMAND ----------

dbutils.notebook.exit("Notebook: Birth_Modes ran successfully")