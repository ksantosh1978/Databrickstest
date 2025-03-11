# Databricks notebook source
# DBTITLE 1,Add these Libraries to your calling program for these functions to work
#import pandas as pd
#import calendar
#import datetime
#from dateutil.relativedelta import relativedelta
#from calendar import monthrange
#from datetime import timedelta

## Add the following command to your notebook, to run this notebook - you can then call any of the functions in this notebook
# %run "/Users/mohammed.khaliq2@nhs.net/Sample Notebooks/CQIMDQ_DATE_Functions"

# COMMAND ----------

def CQIMDQDates(ReportingStartPeriod,ReportingEndPeriod,adjstrtperiod,adjustendperiod):

  # convert string time format to a datetime.date format to use in calculations
  startperiod = pd.to_datetime(ReportingStartPeriod).date()
  endperiod = pd.to_datetime(ReportingEndPeriod).date()

###############################
  # use this to get the start and end dates relevant for this query with an adjustment of x months
  strtpXMonthago = startperiod - relativedelta(months=int(adjstrtperiod))
  endpXlastMonth = endperiod - relativedelta(months=int(adjustendperiod))
###############################
  
  # Get the month number to get the month name
  rpstartmonth = int(startperiod.strftime('%m'))
  rpsendtmonth = int(endperiod.strftime('%m'))
  startmonth = int(strtpXMonthago.strftime('%m'))

  # This makes sure that the last month is reflected accurately with the correct end date
  endday = endpXlastMonth.strftime('%d')
  endmonth = endpXlastMonth.strftime('%m')
  endyear = endpXlastMonth.strftime('%Y')

# get the days in the month using the calendar library functionality
  monthandday = monthrange(int(endyear),int(endmonth))
  enddayofweek = monthandday[0]
  numberofdayinmonth = monthandday[1]
  
  # Get the Month Names
  rpStartmonthname = calendar.month_name[int(rpstartmonth)]
  rpEndmonthname = calendar.month_name[int(rpsendtmonth)]
  AdjStartmonthname = calendar.month_name[int(startmonth)]
  AdjEndmonthname = calendar.month_name[int(endmonth)]

# This looks at the previous months last day and makes any adjustment required is made
  dateadjustment = numberofdayinmonth - int(endday)
  endpXlastMonth = endpXlastMonth + timedelta(days=int(dateadjustment))

# convert datetime.date format to a date string value ready to be passed to the sql query
  Adjstrtp = str(strtpXMonthago)
  Adjendp = str(endpXlastMonth)
  # print("strtp = ", Adjstrtp, type(Adjstrtp))
  # print("endp = ", Adjendp, type(Adjendp))

# Get the number of days between start and end dates for the HES calculation
  def days_between(d1, d2):
      d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
      d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
      return abs((d2 - d1).days)

# Calculates the days in the reporting period
  rpdaysdiff = days_between(ReportingStartPeriod,ReportingEndPeriod)
  rpdaysdiff = rpdaysdiff + 1
  
# Calculates the days in the adjusted period with reference taken from the reporting period
  daysdiff = days_between(Adjstrtp,Adjendp)
  daysdiff = daysdiff + 1
  # print(daysdiff)
  return (Adjstrtp,Adjendp,daysdiff,AdjStartmonthname,AdjEndmonthname,rpStartmonthname,rpEndmonthname, rpdaysdiff)

# COMMAND ----------

#import pandas as pd
#import calendar
#import datetime
#from dateutil.relativedelta import relativedelta
#from calendar import monthrange
#from datetime import timedelta

#date1='2019-04-01'
#date2='2019-04-30'
#date1toadjust='0'
#date2toadjust='0'

#dates = CQIMDQDates(date1,date2,date1toadjust,date2toadjust)
#print(dates)

# COMMAND ----------

# DBTITLE 1,Add Days
 def Add_days(date,number_of_days_to_add):

    date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    calculated_add_date = date + datetime.timedelta(days=int(number_of_days_to_add))
    
    added_new_date = calculated_add_date.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    add_new_date = str(added_new_date)
    return add_new_date


# COMMAND ----------

# DBTITLE 1,Subtract Days
def Subtract_days(date,number_of_days_to_subtract):
        
    date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to subtract days
    
    calculated_subtract_date = date - datetime.timedelta(days=int(number_of_days_to_subtract))
    
    subtract_new_date = calculated_subtract_date.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    subtract_new_date = str(subtract_new_date)
    return subtract_new_date

# COMMAND ----------

# DBTITLE 1,Add Months
def Add_months(date,months_to_add):

    start_date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    calculated_add_months = start_date + relativedelta(months=(int(months_to_add)))
    
    added_new_months_date = calculated_add_months.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    added_months_new_date = str(added_new_months_date)
    return added_months_new_date

# COMMAND ----------

# DBTITLE 1,Subtract Months
def Subtract_months(date,months_to_subtract):

    start_date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
   
    calculated_add_months = start_date - relativedelta(months=(int(months_to_subtract)))
    
    subtracted_new_months_date = calculated_add_months.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    subtrcated_months_new_date = str(subtracted_new_months_date)
    return subtrcated_months_new_date

# COMMAND ----------

# DBTITLE 1,Add Years
def Add_years(date,years_to_add):

    start_date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    calculated_add_years = start_date + relativedelta(years=(int(years_to_add)))
    
    added_new_years_date = calculated_add_years.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    added_years_new_date = str(added_new_years_date)
    return added_years_new_date

# COMMAND ----------

# DBTITLE 1,Subtract Years
def Subtract_years(date,years_to_subtract):

    start_date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    calculated_subtract_years = start_date - relativedelta(years=(int(years_to_add)))
    
    subtracted_new_years_date = calculated_subtract_years.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    subtracted_years_new_date = str(subtracted_new_years_date)
    return subtracted_years_new_date

# COMMAND ----------

# import datetime
# from dateutil.relativedelta import relativedelta
 
# add_days = datetime.datetime.today() + relativedelta(days=+6)
# add_months = datetime.datetime.today() + relativedelta(months=6)
# add_years = datetime.datetime.today() + relativedelta(years=+6)
 
# add_hours = datetime.datetime.today() + relativedelta(hours=+6)
# add_mins = datetime.datetime.today() + relativedelta(minutes=+6)
# add_seconds = datetime.datetime.today() + relativedelta(seconds=+6)
 
#print("Current Date Time:", datetime.datetime.today())
#print("Add 6 days:", add_days,'type', type(add_days))
#print("Add 6 months:", add_months)
#print("Add 6 years:", add_years)
#print("Add 6 hours:", add_hours)
#print("Add 6 mins:", add_mins)
#print("Add 6 seconds:", add_seconds)

# COMMAND ----------

def Add_time_period(date, number_to_add:str = 0, unitoftime = "M"):

    date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    hourstoadd = 0
    daystoadd = 0
    weekstoadd = 0
    monthstoadd = 0
    yearstoadd = 0
    
    if(unitoftime == "H"):
      hourstoadd = int(number_to_add)
    elif(unitoftime == "D"):
      daystoadd = int(number_to_add)
    elif(unitoftime == "W"):
      weekstoadd = int(number_to_add)
    elif(unitoftime == "M"):
      monthstoadd = int(number_to_add)
    elif(unitoftime == "Y"):
      yearstoadd = int(number_to_add)
    else:
      return "Unknown time unit"
        
    calculated_add_date = date + relativedelta(years= yearstoadd, months=monthstoadd,  weeks=weekstoadd, days=daystoadd, hours=hourstoadd )
    
    added_new_date = calculated_add_date.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    add_new_date = str(added_new_date)
    return add_new_date

# COMMAND ----------

# Add_time_period("2016-01-10", 48, "H")

# COMMAND ----------

def Subtract_time_period(date, number_to_add:str = 0, unitoftime = "M"):

    date = datetime.datetime.strptime(date, "%Y-%m-%d") # convert the string date value passed  to a datetime type, this will make it easy to add days
    
    hourstoadd = 0
    daystoadd = 0
    weekstoadd = 0
    monthstoadd = 0
    yearstoadd = 0
    
    if(unitoftime == "H"):
      hourstoadd = int(number_to_add)
    elif(unitoftime == "D"):
      daystoadd = int(number_to_add)
    elif(unitoftime == "W"):
      weekstoadd = int(number_to_add)
    elif(unitoftime == "M"):
      monthstoadd = int(number_to_add)
    elif(unitoftime == "Y"):
      yearstoadd = int(number_to_add)
    else:
      return "Unknown time unit"
        
    calculated_add_date = date - relativedelta(years= yearstoadd, months=monthstoadd,  weeks=weekstoadd, days=daystoadd, hours=hourstoadd )
    
    added_new_date = calculated_add_date.strftime('%Y-%m-%d') # convert the new date back to a string and return back to calling program
    add_new_date = str(added_new_date)
    return add_new_date

# COMMAND ----------

# Subtract_time_period("2016-01-10", 48, "H")

# COMMAND ----------

# from datetime import date, timedelta
# import calendar
# start_date = date(2014, 5, 25)
# days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
#print('start_date',start_date, type(start_date))
# print('days_in_month',days_in_month,'type',type(days_in_month))
#print('start_date.year',start_date.year)
#print('start_date.month',start_date.month)
#print('start_date.year',start_date.year)
#print(start_date + timedelta(days=days_in_month))