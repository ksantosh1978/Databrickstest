-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC #dbutils.widgets.text("outSchema", "mat_analysis", "outSchema")
-- MAGIC #dbutils.widgets.text("RPStartDate", "2022-02-01", "RPStartDate")
-- MAGIC #dbutils.widgets.text("RPEndDate", "2022-02-28", "RPEndDate")
-- MAGIC #dbutils.widgets.text("dss_corporate", "dss_corporate", "dss_corporate")
-- MAGIC #dbutils.widgets.text("source", "mat_pre_clear", "source")
-- MAGIC
-- MAGIC outSchema = dbutils.widgets.get("outSchema")
-- MAGIC assert outSchema
-- MAGIC  
-- MAGIC dss_corporate = dbutils.widgets.get("dss_corporate")
-- MAGIC assert dss_corporate
-- MAGIC  
-- MAGIC RPStartDate = dbutils.widgets.get("RPStartDate")
-- MAGIC assert RPStartDate
-- MAGIC  
-- MAGIC RPEndDate = dbutils.widgets.get("RPEndDate")
-- MAGIC assert RPEndDate
-- MAGIC  
-- MAGIC source = dbutils.widgets.get("source")
-- MAGIC assert source
-- MAGIC
-- MAGIC params = {"Target database" : outSchema, "Source database" : source, "Reference database" : dss_corporate, "Start Date" : RPStartDate, "End Date" : RPEndDate}
-- MAGIC print(params)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import datetime
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC from pyspark.sql import DataFrame as SparkDataFrame
-- MAGIC from pyspark.sql.session import SparkSession
-- MAGIC from pyspark.sql.types import TimestampType

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark = SparkSession.getActiveSession()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC def bmi_test_data(spark: SparkSession = spark) -> SparkDataFrame:
-- MAGIC     data = [
-- MAGIC         [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Overweight', 'RA7', None, 'Provider', 30.0,
-- MAGIC         40.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Obese', 'RA7', None, 'Provider', 340.0, 40.0,
-- MAGIC         0.0, 670.0, 50.0, 50.0, 0,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyObese', 'RA7', None, 'Provider',
-- MAGIC         450.0, 60.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyUnderweight', 'RA7', None,
-- MAGIC         'Provider', 30.0, 200.0, 150.0, 270.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyObese', 'RA7', None, 'Provider',
-- MAGIC         450.0, 60.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Underweight', 'RA7', None, 'Provider', 40.0,
-- MAGIC         560.0, 50.0, 40.0, 442.0, 4330.0, 430,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Underweight', 'RA7', None, 'Provider', 40.0,
-- MAGIC         560.0, 50.0, 40.0, 442.0, 4330.0, 430,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Overweight', 'RA7', None, 'Provider', 30.0,
-- MAGIC         40.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Normal', 'RA7', None, 'Provider', 10.0,
-- MAGIC         540.0, 42.0, 430.0, 86760.0, 230.0, 230,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Normal', 'RA7', None, 'Provider', 10.0,
-- MAGIC         540.0, 42.0, 430.0, 86760.0, 230.0, 230,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyUnderweight', 'RA7', None,
-- MAGIC         'Provider', 30.0, 200.0, 150.0, 270.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_NotRecorded', 'RA7', None, 'Provider', 0.0,
-- MAGIC         0.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Underweight', 'RA7', None, 'Provider', 40.0,
-- MAGIC         560.0, 50.0, 40.0, 442.0, 4330.0, 430,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Normal', 'RA7', None, 'Provider', 10.0,
-- MAGIC         540.0, 42.0, 430.0, 86760.0, 230.0, 230,
-- MAGIC         '2023-01-31 16:15:10.173000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyUnderweight', 'RA7', None,
-- MAGIC         'Provider', 30.0, 200.0, 150.0, 270.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_SeverelyObese', 'RA7', None, 'Provider',
-- MAGIC         450.0, 60.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_NotRecorded', 'RA7', None, 'Provider', 0.0,
-- MAGIC         0.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_NotRecorded', 'RA7', None, 'Provider', 0.0,
-- MAGIC         0.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:09:31.437000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Overweight', 'RA7', None, 'Provider', 30.0,
-- MAGIC         40.0, 0.0, 0.0, 0.0, 0.0, 0,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Obese', 'RA7', None, 'Provider', 340.0, 40.0,
-- MAGIC         0.0, 670.0, 50.0, 50.0, 0,
-- MAGIC         '2023-01-31 16:01:08.580000'],
-- MAGIC        [datetime.date(2022, 9, 1), datetime.date(2022, 9, 30),
-- MAGIC         'BMI_14+1Wks', 'BMI_Obese', 'RA7', None, 'Provider', 340.0, 40.0,
-- MAGIC         0.0, 670.0, 50.0, 50.0, 0,
-- MAGIC         '2023-01-31 16:09:31.437000']
-- MAGIC     ]
-- MAGIC     schema = 'RPStartDate: date, RPEndDate: date, IndicatorFamily: string, Indicator: string, OrgCodeProvider: string, OrgName: string, OrgLevel: string, Unrounded_Numerator: float, Unrounded_Denominator: float, Unrounded_Rate: float, Rounded_Numerator: float, Rounded_Denominator: float, Rounded_Rate: float, IsOverDQThreshold: int, CreatedAt: string'
-- MAGIC     spdf = spark.createDataFrame(data, schema)
-- MAGIC     spdf = spdf.withColumn('CreatedAt', F.col('CreatedAt').cast(TimestampType()))
-- MAGIC     return spdf

-- COMMAND ----------

-- MAGIC %py
-- MAGIC bmi_test_data().display()

-- COMMAND ----------

-- MAGIC %py
-- MAGIC bmi_test_data().createOrReplaceTempView('bmi_test_data')

-- COMMAND ----------

SELECT * FROM bmi_test_data;


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC insert into $outSchema.measures_aggregated 
-- MAGIC SELECT * FROM bmi_test_data

-- COMMAND ----------

select * from $outSchema.measures_aggregated