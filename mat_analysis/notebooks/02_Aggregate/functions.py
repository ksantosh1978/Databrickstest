# Databricks notebook source
# DBTITLE 1,get_dq_measures function 
# loop through all the DQ Measures

def get_dq_measures(DQ_Measure_List):
  pdf_collect_all_DQ_measures = pd.DataFrame() # Empty Dataframe

  dcount = 0

  for dqm in DQ_Measure_List:
    dq_measure = dqm
       #   loop through all the DQ measures
    get_measure = f'''SELECT OrgCodeProvider, ifnull(Org_name,'') Org_Name, Org_Level, RPStartDate, RPEndDate, Unrounded_Numerator as Numerator, Unrounded_Denominator as Denominator, Unrounded_Rate as Percentage, Result from {mydbtbl} where (Indicator = '{dq_measure}') and (RPStartDate between '{startPeriod}' and '{endPeriod}')'''

    sdf_get_measure = spark.sql(get_measure)

   # Empty the DataFrame
    pdf_collect_measure = pd.DataFrame() 

       # Convert spark data frame to a pandas data frame
    pdf_collect_measure = sdf_get_measure.toPandas()

    pdf_collect_measure = pdf_collect_measure.fillna(0)

    pdf_collect_measure.rename(columns={'Numerator':dq_measure + ' Numerator'}, inplace=True)
    pdf_collect_measure.rename(columns={'Denominator':dq_measure + ' Denominator'}, inplace=True)
    pdf_collect_measure.rename(columns={'Percentage':dq_measure + ' Percentage'}, inplace=True)
    pdf_collect_measure.rename(columns={'Result':dq_measure + ' Result'}, inplace=True)


      # if first count then just copy the merged DataFrame else concatenate the rest of the measure
    if dcount == 0:
        pdf_collect_all_DQ_measures = pdf_collect_measure
        dcount = dcount + 1
    else:
        pdf_collect_all_DQ_measures = pd.merge(pdf_collect_all_DQ_measures, pdf_collect_measure, how='left', on=['OrgCodeProvider', 'Org_Name', 'Org_Level',  'RPStartDate', 'RPEndDate'])
    
  return(pdf_collect_all_DQ_measures)

# COMMAND ----------

# DBTITLE 1,Get_the_rate function
def get_the_rate(rate):
 
  get_rate = f'''SELECT OrgCodeProvider, ifnull(Org_name,'') Or, Org_Level, RPStartDate, RPEndDate, IndicatorFamily, Indicator,  Unrounded_Numerator as Numerator, Unrounded_Denominator as Denominator, Unrounded_Rate as Percentage from {mydbtbl} where (Indicator = '{Rate}') and (RPStartDate between '{startPeriod}' and '{endPeriod}') '''

  sdf_get_rate = spark.sql(get_rate)
  pdf_collect_rate = sdf_get_rate.toPandas()
  pdf_collect_rate = pdf_collect_rate.fillna(0)
#   sdf_get_CQIMPPH = spark.sql(get_CQIMPPH)
#   pdf_collect_CQIMPPH = sdf_get_CQIMPPH.toPandas()
#   pdf_collect_CQIMPPH = pdf_collect_CQIMPPH.fillna(0)
  # Rate per 1000 for UnRounded
  pdf_collect_rate['Rate per Thousand'] = round((pdf_collect_rate['Numerator'] / (pdf_collect_rate['Denominator']/1000)),0)
  #pdf_collect_CQIMPPH['Rate per Thousand'] = round((pdf_collect_CQIMPPH['Numerator'] / (pdf_collect_CQIMPPH['Denominator']/1000)),0)
  
  ## SUPPRESSION
  # if value is 1 thru 7 then the count is 5
  pdf_collect_rate['Rounded Numerator'] = pdf_collect_rate.loc[(pdf_collect_rate['Numerator'] >= 1 ) & (pdf_collect_rate['Numerator'] <= 7 ), ['Rounded Numerator']] = 5
  pdf_collect_rate['Rounded Denominator'] = pdf_collect_rate.loc[(pdf_collect_rate['Denominator'] >= 1 ) & (pdf_collect_rate['Denominator'] <= 7), ['Rounded Denominator']] = 5
  
  # if value is 0
  pdf_collect_rate.loc[(pdf_collect_rate['Numerator'] == 0 ), ['Rounded Numerator']] = 0
  pdf_collect_rate.loc[(pdf_collect_rate['Denominator'] == 0 ), ['Rounded Denominator']] = 0
  
  # Rounded Rates to the nearest 5 for values that are greater than 7 for numerator and denominator
  Nx = pdf_collect_rate['Numerator']
  Dx = pdf_collect_rate['Denominator']

  pdf_collect_rate.loc[(pdf_collect_rate['Numerator'] > 7 ), ['Rounded Numerator']] = (base * round(Nx/base))
  pdf_collect_rate.loc[(pdf_collect_rate['Denominator'] > 7 ), ['Rounded Denominator']] = (base * round(Dx/base))
  
  # Rounded Percentagepdf_collect_
  pdf_collect_rate['Rounded Percentage'] = round((pdf_collect_rate['Rounded Numerator'] / pdf_collect_rate['Rounded Denominator'])*100,1)

  # Rate per 1000
  pdf_collect_rate['Rounded Rate per Thousand'] = round((pdf_collect_rate['Rounded Numerator'] / (pdf_collect_rate['Rounded Denominator']/1000)),0)
  
  # Rename the colums
  pdf_collect_rate.rename(columns={'Numerator':Rate + ' Numerator'}, inplace=True)
  pdf_collect_rate.rename(columns={'Denominator':Rate + ' Denominator'}, inplace=True)
  pdf_collect_rate.rename(columns={'Percentage':Rate + ' Percentage'}, inplace=True)
  pdf_collect_rate.rename(columns={'Rate per Thousand':Rate + ' Rate per Thousand'}, inplace=True)
  
  return(pdf_collect_rate)
  # pdf_collect_CQIMPPH.head(20)