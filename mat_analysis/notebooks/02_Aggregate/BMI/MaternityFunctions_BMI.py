# Databricks notebook source
# Mimic the rounding of sql, so that e.g. 1.249 rounds to 1.2, 1.25 rounds to 1.3
# Taken from https://stackoverflow.com/questions/33019698/how-to-properly-round-up-half-float-numbers
from decimal import Decimal, getcontext, ROUND_HALF_UP
def round_half_up(x: float, num_decimals: int) -> float:
    """Use explicit ROUND HALF UP. See references, for an explanation.

    This is the proper way to round, as taught in school.

    Args:
        x:
        num_decimals:

    Returns:
            https://stackoverflow.com/questions/33019698/how-to-properly-round-up-half-float-numbers-in-python

    """

    if num_decimals < 0:
        raise ValueError("Num decimals needs to be at least 0.")
    target_precision = "1." + "0" * num_decimals
    rounded_x = float(Decimal(x).quantize(Decimal(target_precision), ROUND_HALF_UP))
    return rounded_x


# COMMAND ----------

# DBTITLE 1,Calculate Rate
# MAGIC %python
# MAGIC def MaternityRate(numerator, denominator, rateType=0):
# MAGIC   
# MAGIC   # rateType=0: Default rate - if numerator or denominator = 0 then 0 else (numerator/denominator) * 100
# MAGIC   # rateType=1: PCSP rate    - if numerator or denominator = 0 then 0, if numerator <=7 then 0 else (numerator/denominator) * 100
# MAGIC   
# MAGIC   if numerator is None or denominator is None:
# MAGIC     return None
# MAGIC   
# MAGIC   if rateType == 0:
# MAGIC     
# MAGIC     if (numerator == 0):
# MAGIC       retval = float(0)    
# MAGIC     if float(denominator) == 0:
# MAGIC       retval = float(0)
# MAGIC     else:
# MAGIC       retval = round_half_up(float(((float(numerator) / float(denominator)) * 100)),1)
# MAGIC
# MAGIC   elif rateType == 1:
# MAGIC     
# MAGIC     if (numerator == 0):
# MAGIC       retval = float(0)    
# MAGIC     if float(denominator) == 0:
# MAGIC       retval = float(0)
# MAGIC     if float(numerator) > 7:
# MAGIC       retval = round_half_up(((float(numerator) / float(denominator)) * 100))
# MAGIC     else:
# MAGIC       retval = 0
# MAGIC       
# MAGIC   else:
# MAGIC     
# MAGIC     retval = float(((float(numerator) / float(denominator)) * 100))
# MAGIC     
# MAGIC   return(round(retval,1))
# MAGIC
# MAGIC spark.udf.register("MaternityRate", MaternityRate)

# COMMAND ----------

# DBTITLE 1,Round Value based on maternity rounding rules
def MaternityRounding(value,base=5):
  if value is None:
    return None
  if value >=  8:
    return int(base * round(float(value)/base))
  elif value > 0 and value < 8:
    return(5)
  else:
        retval = float(5 * round(value/5))
  return(0)

spark.udf.register("MaternityRounding", MaternityRounding)