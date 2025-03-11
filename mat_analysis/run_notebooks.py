# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text("RPBegindate", "2023-07-01")
#dbutils.widgets.text("dss_corporate", "dss_corporate", "Reference database")
#dbutils.widgets.text("mat_pre_clear", "mat_pre_clear", "Source database")
#dbutils.widgets.text("db", "mat_analysis", "Target database")
#dbutils.widgets.text("month_id", "", "month_id")

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC '''
# MAGIC In Prod if the scheduler runs the job, at that time only default params which were already configured will be passed. 'RPBegindate' is manual entry for parameters during custom job run, these params are passed as widget entry or json values but won't be there during automatic run. So, we need to check presence of these params to determine if this is an Automatic run
# MAGIC '''
# MAGIC
# MAGIC is_rp_start_avail = True
# MAGIC
# MAGIC # Toggle the comments if you want to simulate the run in Ref
# MAGIC #if(os.environ.get('env') == 'ref'):
# MAGIC if(os.environ.get('env') == 'prod'):
# MAGIC   try:
# MAGIC     dbutils.widgets.get("RPBegindate")
# MAGIC   except Exception as ex:
# MAGIC     is_rp_start_avail = False
# MAGIC
# MAGIC print(f'RPBegindate parameter availability: {is_rp_start_avail}')
# MAGIC
# MAGIC # It can  be auto run if RPBegindate fields are not available as in automatic run (means those widgets are not present)
# MAGIC auto_prov_check = False if (is_rp_start_avail) else True
# MAGIC print(f'Provisional check for automatic run: {auto_prov_check}')

# COMMAND ----------

# DBTITLE 1,Setting Parameter fields and defining functions
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from shared.submissions.calendars.common import calculate_uniq_month_id_from_date  

db = dbutils.widgets.get("db")
assert db
mat_pre_clear = dbutils.widgets.get("mat_pre_clear")
assert mat_pre_clear
dss_corporate = dbutils.widgets.get("dss_corporate")
assert dss_corporate
print(dss_corporate)

params = {
  'RPBegindate' : '',
  'RPEnddate': '',
  'dbSchema' : mat_pre_clear, 
  'outSchema' : db, 
  'status' : '', 
  'dss_corporate' : dss_corporate,
  'month_id' : '', 
  'custom_run': False,  #Defaulting to false for job run
  'automatic_run' : False # Defaulting to false
}

'''
we have two boolean parameters because they are not complimentary:
Custom - true , means obviously we fed the rp_startdate and status to run
automatic - true, custom - false, means there is data available at source for provisional and there is no job ran recently.
automatic - false, custom - false, there is nothing to do, if we kept complimentary this case would be off

'''

print('Basic parameters: {}'.format(json.dumps(params,
                                              indent = 4)))
'''
Function to calculate month_id for the custom run, we can use the submission calendar too but its date range is limited and this function block is already there to calculate month id for custom run, so been using it.
'''

def calculateUniqMonthID(RPStartDate: datetime)-> int:
  date_for_month = RPStartDate
  print(f'Reporting period start date: {RPStartDate}')
  start_date = datetime(1900, 4, 1)
  time_diff = relativedelta(date_for_month, start_date)
  return time_diff.years * 12 + time_diff.months + 1

def get_prov_final_flag(rp_start_date):
  # Convert the datetime to date type as the source table columns is of date date type, facilitates the comparison without error
  run_date =  datetime.strptime(rp_start_date, "%Y-%m-%d").date()
  source_month_start = spark.sql(f"SELECT MAX(RPStartDate) AS RPStartDate FROM {mat_pre_clear}.msd000header").collect()[0]['RPStartDate']; 
  #source_month_start =source_month_start + relativedelta(months=1)  # for testing july 2023 run
  if run_date == source_month_start:
    return 'Provisional'
  elif run_date < source_month_start:
    return 'Final'
  else:
    assert False, 'Future date has been passed to parameter'

# COMMAND ----------

# MAGIC %python
# MAGIC from shared.constants import DS
# MAGIC from shared.submissions.calendars import msds, submission_calendar
# MAGIC from datetime import datetime, date
# MAGIC   
# MAGIC     
# MAGIC if(auto_prov_check):
# MAGIC
# MAGIC   # As the provisional check is passed, we need to get the month id, reporting periods for the Refresh months of current date
# MAGIC   # With the change to double window process, 'Provisional and Final' are the names adopted. Interpret the previous terminology accordingly (Final = Refresh, Provisional = Primary)
# MAGIC   _msds_ytd_calendar = submission_calendar(DS.MSDS, {})
# MAGIC
# MAGIC
# MAGIC   today_date = datetime.today()
# MAGIC   #today_date = datetime.today()+ relativedelta(days=5)  # for testing july 2023 run
# MAGIC   print(today_date)
# MAGIC   print('Validity check for the date: {0}\n'.format( today_date))
# MAGIC   submission_window = _msds_ytd_calendar.find_last_closed_submission_window(
# MAGIC       today_date, fake_historic_windows=False,
# MAGIC   )
# MAGIC  
# MAGIC   idx_current_report_month = len(submission_window.reporting_periods) - 1
# MAGIC   refresh_month_id = submission_window.reporting_periods[idx_current_report_month].unique_month_id
# MAGIC
# MAGIC   print(f"\
# MAGIC           length of period tuple from Submission Calendar: {len(submission_window.reporting_periods)}\n\
# MAGIC           Submission window opens: {submission_window.opens}\n\
# MAGIC           Submission window closes: {submission_window.closes}\n\
# MAGIC           Refresh period start: {submission_window.reporting_periods[idx_current_report_month].start}\n\
# MAGIC           Refresh period end: {submission_window.reporting_periods[idx_current_report_month].end}\n\
# MAGIC           Refresh period month id: {refresh_month_id}\n\
# MAGIC           ")
# MAGIC   #### CHECK THE RECENT MONTH ID OF SUCCESSFUL RUN IN audit_menh_dq TABLE
# MAGIC   audit_month_id = spark.sql("SELECT MAX(MONTH_ID) AS auditMonthId FROM {0}.audit_mat_analysis WHERE RUN_END IS NOT NULL".format(params['outSchema'])).collect()[0]['auditMonthId']; 
# MAGIC   print(f'Audit month ID from recent job runs: {audit_month_id}')
# MAGIC #   audit_month_id = 1461
# MAGIC   source_month_start = spark.sql("SELECT MAX(RPStartDate) AS RPStartDate FROM {0}.msd000header".format(params['dbSchema'])).collect()[0]['RPStartDate']; 
# MAGIC  
# MAGIC   source_month_id = calculate_uniq_month_id_from_date(source_month_start)
# MAGIC   #source_month_id =1478 # for testing july 2023 run
# MAGIC   print(f'Recent month id available at source database: {source_month_id}')
# MAGIC   ### CONDITION TO CHECK WHETHER THERE IS SUCCESSFUL RUN FOR PROVISIONAL MONTH AND DATA AVAILABLE FOR PROVISIONAL IN SOURCE DB
# MAGIC #   creating new variable as the calendar is not storing the provisional values.
# MAGIC   prov_month_id = refresh_month_id + 1
# MAGIC   if((prov_month_id > audit_month_id) and (prov_month_id == source_month_id )):
# MAGIC #   if(True):
# MAGIC     params['automatic_run'] = True 
# MAGIC     final_rpbegindate = submission_window.reporting_periods[idx_current_report_month].start
# MAGIC     params['final_RPBegindate'] = str(final_rpbegindate)
# MAGIC     params['final_RPEnddate'] = str(submission_window.reporting_periods[idx_current_report_month].end)
# MAGIC     params['final_month_id'] = refresh_month_id
# MAGIC     final_status_flag = get_prov_final_flag(params['final_RPBegindate'])
# MAGIC     params['final_status'] = f'Auto - {final_status_flag}'
# MAGIC     params['final_filename'] = ''
# MAGIC     
# MAGIC     # Adding a month to get the Provisional month date related parameters
# MAGIC     prov_rpbegindate = final_rpbegindate + relativedelta(months=1)
# MAGIC     params['RPBegindate'] = str(prov_rpbegindate)
# MAGIC     params['RPEnddate'] = (prov_rpbegindate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
# MAGIC     params['month_id'] = calculate_uniq_month_id_from_date(prov_rpbegindate)
# MAGIC     prov_status_flag = get_prov_final_flag(params['RPBegindate'])
# MAGIC     params['status'] = f'Auto - {prov_status_flag}'
# MAGIC     params['filename'] = 'Provisional'
# MAGIC     print('Provisional parameters for Eligible automatic run: {}'.format(json.dumps(params,
# MAGIC                                                          indent = 4)))
# MAGIC     
# MAGIC   else:
# MAGIC     params['automatic_run'] = False # We don't need to run the automatic job
# MAGIC     print('Provisional parameters for Failsafe Automatic run: {}'.format(json.dumps(params,
# MAGIC                                                                                    indent = 4)))
# MAGIC else:
# MAGIC   # Few assertions need to done as rp_startdate and status are passed only in the custom job run
# MAGIC   RPBegindate = dbutils.widgets.get("RPBegindate")
# MAGIC   assert RPBegindate
# MAGIC   startdateasdate = datetime.strptime(RPBegindate, "%Y-%m-%d")
# MAGIC   rp_enddate = (startdateasdate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
# MAGIC   UniqMonthID = calculate_uniq_month_id_from_date(startdateasdate)
# MAGIC   params['RPEnddate'] = rp_enddate
# MAGIC   # assign the calculated parameters that are needed for the cutom job run 
# MAGIC   params['custom_run'] = True
# MAGIC   params['month_id'] = UniqMonthID
# MAGIC   params['RPBegindate'] = RPBegindate
# MAGIC   params['status'] = 'Refresh - Provisional'
# MAGIC   params['filename'] = 'Provisional'
# MAGIC   # Setting up final month parameters
# MAGIC   final_rpbegindate = startdateasdate + relativedelta(months=-1)
# MAGIC   params['final_RPBegindate'] = str((final_rpbegindate.strftime("%Y-%m-%d")))
# MAGIC   params['final_RPEnddate'] = (final_rpbegindate + relativedelta(months=1, days=-1)).strftime("%Y-%m-%d")
# MAGIC  
# MAGIC   params['final_month_id'] = UniqMonthID-1
# MAGIC   params['final_status'] = 'Refresh'
# MAGIC   params['final_filename'] = 'Refresh_Final'
# MAGIC   print('Custom job parameters: {}'.format(json.dumps(params,
# MAGIC                                                       indent = 4)))

# COMMAND ----------

# DBTITLE 1,This function logs the Job run params to the audit_mat_analysis table
def auditInsertLog(params, runStart, runEnd):
  try:
    
    spark.sql("INSERT INTO TABLE {db_output}.audit_mat_analysis VALUES('{month_id}','{status}','{rp_startdate}','{rp_enddate}','{dbm}',FROM_UNIXTIME({runStart}),FROM_UNIXTIME({runEnd}))"
              .format(db_output = params['outSchema'],
                      month_id = params['month_id'],
                      status = params['status'],
                      rp_startdate = params['RPBegindate'],
                      rp_enddate = params['RPEnddate'],
                      dbm = params['dbSchema'],
                      runStart = runStart,
                      runEnd = runEnd))
  except Exception as ex:
    print(ex)

# COMMAND ----------

# DBTITLE 1,To Determine No.of runs need during auto run
# MAGIC %python
# MAGIC jobs = [params['status']] # defaultly adding one one job as it is passed from the status. Example 'Provisional' for automatic run, sometimes 'Final' for custom run
# MAGIC if(params['automatic_run'] or params['custom_run']):
# MAGIC   jobs.append('Final') # Only for automatic run we need to add this
# MAGIC   print(jobs)
# MAGIC   print('We are going for Automatic run now which runs First for Provisional and then Final')

# COMMAND ----------

try:
  
  #run the job only if one of the case is true
  if(params['automatic_run'] or params['custom_run']):
    
    for run in jobs:
      now = datetime.now()
      runStart = datetime.timestamp(now)
      print(f'The current run is: {run}')
      print(f'Started the job at: {datetime.time(now)}')
      if(run == 'Final'):
        if(params['automatic_run']):
        #This if condition helps in tailoring the Final paramters during the auto run process and then run the job process.
            params['RPBegindate'] = params['final_RPBegindate']
            params['RPEnddate'] = params['final_RPEnddate']
            status_flag = get_prov_final_flag(params['final_RPBegindate'])
            params['status'] = f'Auto - {status_flag}'
            params['filename'] = 'final'
            params['month_id'] = params['final_month_id']
        else:
            params['RPBegindate'] = params['final_RPBegindate']
            params['RPEnddate'] = params['final_RPEnddate']
            params['status'] = params['final_status']
            params['filename'] = 'final'
            params['month_id'] = params['final_month_id']
      dbutils.notebook.run('./notebooks/monthly_job_execution', 0, params)
      print('Job parameters: {}'.format(json.dumps(params,
                                                      indent = 4)))
      now = datetime.now()
      runEnd = datetime.timestamp(now)
      print(f'Ended the job at: {datetime.time(now)}')
      print('logging the successful run')
      auditInsertLog(params,runStart,runEnd)
      print('Inserted into Audit Log: {}'.format(json.dumps(params,
                                                      indent = 4)))

  else:
    print(f'Neither Custom run nor Automatic run conditions are met and the job could not run')
  

except Exception as ex:
  print(ex, ' Job run failed and logged to audit_mat_analysis.')
#     calling the log function as the job failed with empty end date which will record as null in table
  auditInsertLog(params,runStart,'NULL')
  # using exception block escapes the show of'Failure' tag for Code Promotion notebooks even if there is an error in the notebooks.(Instead it shows as 'Success' even in case of errors and very misleading). So, need to raise the error here after logging to audit table as it is the entry point
  ## DONOT USE TRY-EXCEPt BLOCKS ANYWHERE INSIDE THE CALLING NOTEBOOKS
  assert False

# COMMAND ----------

# DBTITLE 1,Quick glance at the audit table for runs
# MAGIC %python
# MAGIC audit_table = spark.sql(f"SELECT * FROM {params['outSchema']}.audit_mat_analysis ORDER BY RUN_START DESC LIMIT 5")
# MAGIC display(audit_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use this for testing automatic run
# MAGIC --delete  from mat_analysis.audit_mat_analysis where month_id in (1476,1477,1478,1479)