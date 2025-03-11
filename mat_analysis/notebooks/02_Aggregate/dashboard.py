# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table mat_analysis.cqim_output_rates_store

# COMMAND ----------

db = "mat_analysis"
table = "cqim_output_rates_store"

import pandas as pd

create = "CREATE TABLE IF NOT EXISTS `" + db + "`.`" + table + "` ("

df = spark.sql("describe mat_analysis.cqim_output_rates").toPandas()

cols = df.col_name.tolist()

table_dict = {}

for a in cols:
  table_dict[a] = df.loc[df.col_name == a].data_type.to_list()[0].upper()

for a in cols:
  create += a + " " + table_dict[a] + ","
  
create = create[:-1]

create += """)	
USING DELTA	"""

print(create)
spark.sql(create)

spark.sql("insert into " + db + "." + table + " select * from " + db + ".cqim_output_rates")

# COMMAND ----------

import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
 
def fyear(p):
  if int(p[5:7]) >= 4:
    q = p[0:5] + '04-01'
    return(q)
  else:
    q = str(int(p[:4])-1) + '-04-01'
    return(q)

dateVar = fyear(dbutils.widgets.get("RPStartDate"))
 
print("Start date: " + dateVar)
 
def fyearEnd(q):
  if int(q[:4]) == datetime.datetime.now().year :
    return(str(int(q[:4])) + "-" + str(datetime.datetime.now().month-2).zfill(2) + "-01")
  else:
    return(str(int(q[:4])+1) + "-04-01")
 
    
    
endDateVar = fyearEnd(dateVar)
 
print("End date: " + endDateVar)
 
rollingStart = datetime.date(int(endDateVar[:4]),int(endDateVar[5:7]),int(endDateVar[8:])) + relativedelta(months=-6)
 
print("Rolling rate start date: " + str(rollingStart))
  
  
  
indicators = ['CQIMApgar', 'CQIMTears', 'CQIMPreterm', 'CQIMPPH', 'CQIMRobson02', 'CQIMVBAC', 'CQIMRobson01', 'CQIMSmokingDelivery', 'CQIMRobson05', 'CQIMBreastfeeding', 'CQIMSmokingBooking']
descs = ['Number of single babies born with an APGAR score between 0 and 6', 'Women who had a 3rd or 4th degree tear at delivery', 'Babies who were born preterm', 'Women who had a PPH more than 1500ml', 'Number of women in Robson Group 2 having a caesarean section with no previous births', 'Women who gave birth to a second baby following a caesarean section for their first baby', 'Number of women in Robson Group 1 having a caesarean section with no previous births', 'Women who were current smokers at delivery', 'Number of women in Robson Group 5 having a caesarean section with at least one previous birth', 'Babies with a first feed of breast milk', 'Women who were current smokers at booking appointment']
descsShort = ['Babies with an APGAR score between 0 and 6', '3rd or 4th degree tears', 'Preterm Births', 'PPH more than 1500ml', 'Robson Group 2', 'Vaginal birth following a caesarean section', 'Robson Group 1', 'Smokers at delivery', 'Robson Group 5', 'Babies with a first feed of breast milk', 'Smokers at booking']
 

indDescs = pd.DataFrame(list(zip(indicators, descs, descsShort)),columns =['Indicator', 'Indicator_Desc_Full', 'Indicator_Desc_Abv'])

# COMMAND ----------

measuresAndRates = {}
 
measuresAndRates['CQIMApgar'] = ['CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ24']
measuresAndRates['CQIMTears'] = ['CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ18','CQIMDQ19','CQIMDQ20','CQIMDQ21']
measuresAndRates['CQIMPreterm'] = ['CQIMDQ09','CQIMDQ22','CQIMDQ23']
measuresAndRates['CQIMPPH'] = ['CQIMDQ10','CQIMDQ11','CQIMDQ12','CQIMDQ13']
measuresAndRates['CQIMRobson02'] = ['CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ41']
measuresAndRates['CQIMVBAC'] = ['CQIMDQ14','CQIMDQ15','CQIMDQ16','CQIMDQ18','CQIMDQ19','CQIMDQ26','CQIMDQ27','CQIMDQ28']
measuresAndRates['CQIMRobson01'] = ['CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ40']
measuresAndRates['CQIMSmokingDelivery'] = ['CQIMDQ02','CQIMDQ06','CQIMDQ07']
measuresAndRates['CQIMRobson05'] = ['CQIMDQ30','CQIMDQ31','CQIMDQ32','CQIMDQ33','CQIMDQ34','CQIMDQ35','CQIMDQ36','CQIMDQ37','CQIMDQ38','CQIMDQ39','CQIMDQ42']
measuresAndRates['CQIMBreastfeeding'] = ['CQIMDQ08','CQIMDQ09']
measuresAndRates['CQIMSmokingBooking'] = ['CQIMDQ03','CQIMDQ04','CQIMDQ05']
 
inClauses = {}
for a in measuresAndRates:
  where = "("
  for i in range(len(measuresAndRates[a])):
    where += "'" + measuresAndRates[a][i] + "',"
  where = where[:-1]
  where += ')'
  inClauses[a] = where

# COMMAND ----------

ratesDf = ""
for a in inClauses:
  ratesDf += """select 
  '""" + a + """' as Indicator
  ,RPStartDate
  ,OrgCodeProvider
  ,min(Rounded_Rate)
from 
  """ + db + """.""" + table + """
where 
  Indicator in """ + inClauses[a] + """ and RPStartDate >= '""" + dateVar + """'
group by
  '""" + a + """'
  ,RPStartDate
  ,OrgCodeProvider
 
union all
 
"""
  
ratesDf = ratesDf[:-12]
 
actuallyRatesDf = sqlContext.sql(ratesDf)
actuallyRatesDf = actuallyRatesDf.toPandas()

# COMMAND ----------

df = sqlContext.sql("""
with cte as 
(select distinct OrgCodeProvider, Indicator, RPStartDate, Value from mat_analysis.cqim_dq_csv where value = 'DNS')
 
select 
  cqim.Indicator
  ,cqim.RPStartDate
  ,cqim.OrgCodeProvider
  ,g.Mbrrace_Grouping
  ,cqim.Unrounded_Numerator
  ,cqim.Unrounded_Denominator
  ,cqim.Unrounded_Rate
  ,cqim.Unrounded_RateperThousand
  ,cqim.Result
  ,cqim.Rounded_Numerator
  ,cqim.Rounded_Denominator
  ,cqim.Rounded_Rate
  ,cqim.Rounded_RateperThousand
  ,cqim.Rounded_Result
  ,csv.Value
from 
  """ + db + """.""" + table + """ cqim 
inner join
  """ + db + """.geogtlrr g
on
  cqim.OrgCodeProvider = g.Trust_ORG
left join
  cte csv
on
  csv.OrgCodeProvider = cqim.OrgCodeProvider
and
  csv.Indicator = cqim.Indicator
and
  csv.RPStartDate = cqim.RPStartDate
where 
  cqim.indicator in ('CQIMApgar', 'CQIMBreastfeeding', 'CQIMPPH', 'CQIMPreterm', 'CQIMRobson01', 'CQIMRobson02', 'CQIMRobson05', 'CQIMSmokingBooking', 'CQIMSmokingDelivery', 'CQIMTears', 'CQIMVBAC')
and 
  cqim.RPStartDate >= '""" + dateVar + """'
and 
  cqim.RPStartDate <= '""" + endDateVar + "'")

# COMMAND ----------

df = df.toPandas()
display(df)

# COMMAND ----------

indDict = {}
for a in set(df.Indicator.tolist()):
  indDict[a] = {}
  for b in set(df.RPStartDate.tolist()):
    if len(df.loc[(df.Indicator == a) & (df.RPStartDate == b) & (df.Unrounded_Numerator>0)]) > 0:
      indDict[a][b] = {"Submitted_all":str(len(df.loc[(df.Indicator == a) & (df.RPStartDate == b) & ((df.Rounded_Result == "Pass") | (df['Unrounded_Numerator'] > 0) & (df['Unrounded_Numerator'] < 8))])) + " of " + str(len(df.loc[(df.Indicator == a) & (df.RPStartDate == b)])),"Numerator_all":df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Numerator.sum(),"Denominator_all":df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Denominator.sum(),"Rate_all":100*(df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Numerator.sum()/df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Denominator.sum()),"Stat_Mean":df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Numerator.sum()/len(df.loc[(df.Indicator == a) & (df.RPStartDate == b) & (df.Unrounded_Numerator>0)]),"Stat_Min":df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Numerator.min(),"Stat_Max":df.loc[(df.Indicator == a) & (df.RPStartDate == b)].Unrounded_Numerator.max(),"Stat_Median":df.loc[(df.Indicator == a) & (df.RPStartDate == b) & (df.Unrounded_Numerator>0)].Unrounded_Numerator.median(),"Stat_Q1":np.percentile(df.loc[(df.Indicator == a) & (df.RPStartDate == b) & (df.Unrounded_Numerator>0)].Unrounded_Numerator, 25),"Stat_Q3":np.percentile(df.loc[(df.Indicator == a) & (df.RPStartDate == b) & (df.Unrounded_Numerator>0)].Unrounded_Numerator, 75)}
    else:
      indDict[a][b] = {"Stat_Mean":-1,"Stat_Min":-1,"Stat_Max":-1,"Stat_Median":-1,"Stat_Q1":-1,"Stat_Q3":-1,"Numerator_all":0,"Denominator_all":0,"Rate_all":0,"Submitted_all":0}
 
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Mean'] = indDict[a][b]['Stat_Mean']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Min'] = indDict[a][b]['Stat_Min']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Max'] = indDict[a][b]['Stat_Max']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Median'] = indDict[a][b]['Stat_Median']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Q1'] = indDict[a][b]['Stat_Q1']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Stat_Q3'] = indDict[a][b]['Stat_Q3']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Numerator_all'] = indDict[a][b]['Numerator_all']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Denominator_all'] = indDict[a][b]['Denominator_all']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Rate_all'] = indDict[a][b]['Rate_all']
    df.loc[(df.Indicator == a) & (df.RPStartDate == b), 'Submitted_all'] = indDict[a][b]['Submitted_all']

# COMMAND ----------

indMbrraceDict = {}
for a in set(df.Indicator.tolist()):
  indMbrraceDict[a] = {}
  for b in set(df.Mbrrace_Grouping.tolist()):
    indMbrraceDict[a][b] = {}
    for c in set(df.RPStartDate.tolist()):
      if len(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & (df.Unrounded_Numerator>0)]) > 0:
        indMbrraceDict[a][b][c] = {"Submitted_MBRRACE":str(len(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & ((df.Rounded_Result == "Pass") | (df['Unrounded_Numerator'] > 0) & (df['Unrounded_Numerator'] < 8))])) + " of " + str(len(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)])),"Stat_MBRRACE_Denominator":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Denominator.sum(),"Stat_MBRRACE_Numerator":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Numerator.sum(),"Rate_MBRRACE":100*(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Numerator.sum()/df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Denominator.sum()),"Stat_MBRRACE_Mean":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Numerator.sum()/len(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & (df.Unrounded_Numerator>0)]),"Stat_MBRRACE_Min":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Numerator.min(),"Stat_MBRRACE_Max":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c)].Unrounded_Numerator.max(),"Stat_MBRRACE_Median":df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & (df.Unrounded_Numerator>0)].Unrounded_Numerator.median(),"Stat_MBRRACE_Q1":np.percentile(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & (df.Unrounded_Numerator>0)].Unrounded_Numerator, 25),"Stat_MBRRACE_Q3":np.percentile(df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c) & (df.Unrounded_Numerator>0)].Unrounded_Numerator, 75)}
      else:
        indMbrraceDict[a][b][c] = {"Stat_MBRRACE_Mean":-1,"Stat_MBRRACE_Min":-1,"Stat_MBRRACE_Max":-1,"Stat_MBRRACE_Median":-1,"Stat_MBRRACE_Q1":-1,"Stat_MBRRACE_Q3":-1, "Stat_MBRRACE_Numerator":-1,"Stat_MBRRACE_Denominator":-1, "Rate_MBRRACE":0, "Submitted_MBRRACE":0}
      
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Numerator'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Numerator']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Denominator'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Denominator']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Rate_MBRRACE'] = indMbrraceDict[a][b][c]['Rate_MBRRACE']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Mean'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Mean']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Min'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Min']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Max'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Max']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Median'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Median']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Q1'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Q1']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Stat_MBRRACE_Q3'] = indMbrraceDict[a][b][c]['Stat_MBRRACE_Q3']
      df.loc[(df.Indicator == a) & (df.Mbrrace_Grouping == b) & (df.RPStartDate == c), 'Submitted_MBRRACE'] = indMbrraceDict[a][b][c]['Submitted_MBRRACE']
df = df.merge(actuallyRatesDf, on=["Indicator", "OrgCodeProvider", "RPStartDate"], how="left")
df['RPStartDate']= pd.to_datetime(df['RPStartDate'])
df

# COMMAND ----------

indDict = {}
for a in set(df.Indicator.tolist()):
  indDict[a] = {}
  for b in set(df.OrgCodeProvider.tolist()):
    indDict[a][b] = {}
    for c in set(df.RPStartDate.tolist()):
      if len(df.loc[(df.Indicator == a) & (df.OrgCodeProvider == b) & (df.RPStartDate <= c)])>0:
        indDict[a][b][c] = {"rollingTrust":100*(df.loc[(df.Indicator == a) & (df.OrgCodeProvider == b) & (df.RPStartDate <= c) & (df.RPStartDate >= str(rollingStart))].Unrounded_Numerator.sum()/df.loc[(df.Indicator == a) & (df.OrgCodeProvider == b) & (df.RPStartDate <= c) & (df.RPStartDate >= str(rollingStart))].Unrounded_Denominator.sum())}
    
 
        df.loc[(df.Indicator == a) & (df.OrgCodeProvider == b) & (df.RPStartDate == c), 'rollingTrust'] = indDict[a][b][c]['rollingTrust']

# COMMAND ----------

df['DQ_Flag'] = df['Result'].copy(deep=True)
df.loc[df['Value'] == 'DNS', 'DQ_Flag'] = 'DNS'
 
df['Numerator_Org'] = df['Unrounded_Numerator'].copy(deep=True)
df.loc[df['DQ_Flag'] == 'DNS', 'Numerator_Org'] = -2
df.loc[df['DQ_Flag'] == 'Fail', 'Numerator_Org'] = 0
df.loc[(df['Unrounded_Numerator'] > 0) & (df['Unrounded_Numerator'] < 8), 'Numerator_Org'] = -1

 
df['Denominator_Org'] = df['Unrounded_Denominator'].copy(deep=True)
df.loc[df['DQ_Flag'] == 'DNS', 'Denominator_Org'] = -2
df.loc[df['DQ_Flag'] == 'Fail', 'Denominator_Org'] = 0

# COMMAND ----------

merged = df.merge(indDescs, on='Indicator', how='left')
merged.rename(columns={'OrgCodeProvider':'Org_Code', 'Mbrrace_Grouping':'MBRRACE Group', "Stat_MBRRACE_Numerator":"Numerator_MBRRACE", "Stat_MBRRACE_Denominator":"Denominator_MBRRACE", "Rate_all":"Rates_all","Stat_Mean":"Stat_Org_Mean", "Stat_Min":"Stat_Org_Min", "Stat_Max":"Stat_Org_Max", "Stat_Q1":"Stat_Org_Q1", "Stat_Median":"Stat_Org_Median", "Stat_Q3":"Stat_Org_Q3", "min(Rounded_Rate)":"RAG_DQ"}, inplace=True)

# COMMAND ----------

merged.loc[merged['DQ_Flag'] == 'DNS', 'RAG_DQ'] = -2
merged.loc[merged['DQ_Flag'] == 'Fail', 'RAG_DQ'] = -3
merged.loc[merged['Numerator_Org'] == -1, 'DQ_Flag'] = "Suppressed"
merged.loc[merged['DQ_Flag'] == "Suppressed", 'RAG_DQ'] = -1

# COMMAND ----------

final = merged[["Indicator", "Indicator_Desc_Full", "Indicator_Desc_Abv", "RPStartDate", "Org_Code", "MBRRACE Group", "Stat_MBRRACE_Mean", "Stat_MBRRACE_Min", "Stat_MBRRACE_Max", "Stat_MBRRACE_Q1", "Stat_MBRRACE_Median", "Stat_MBRRACE_Q3", "Numerator_MBRRACE", "Denominator_MBRRACE", "Rate_MBRRACE", "Submitted_MBRRACE", "Numerator_all", "Denominator_all", "Rates_all", "Submitted_all", "Numerator_Org", "Denominator_Org", "Unrounded_Rate", "Stat_Org_Mean", "Stat_Org_Min", "Stat_Org_Max", "Stat_Org_Q1", "Stat_Org_Median", "Stat_Org_Q3", "RAG_DQ", "DQ_Flag", "rollingTrust"]].copy(deep=True)

# COMMAND ----------

from pyspark.sql.types import *
 
# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)
  
sparkFinal = pandas_to_spark(final)
 
display(sparkFinal)

# COMMAND ----------

