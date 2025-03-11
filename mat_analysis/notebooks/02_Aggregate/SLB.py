# Databricks notebook source
RPBegindate = dbutils.widgets.get("RPBegindate")
print(RPBegindate)
assert(RPBegindate)
RPEnddate = dbutils.widgets.get("RPEnddate")
print(RPEnddate)
assert(RPEnddate)
dbSchema = dbutils.widgets.get("dbSchema")
print(dbSchema)
assert(dbSchema)
outSchema = dbutils.widgets.get("outSchema")
print(outSchema)
assert(outSchema)
dss_corporate = dbutils.widgets.get("dss_corporate")
print(dss_corporate)
assert(dss_corporate)
month_id = dbutils.widgets.get("month_id")
print(month_id)
assert(month_id)

# COMMAND ----------

# Data contains multiple rows in MSD202CareActivityPreg for the same activity (e.g. ProcedureCode = '226571000000100') that will match 1 row of MSD101pregnancybooking
# Similarly MSD401BabyDemographics where recordnumber=335893000000931

# Some fields contain both null value and empty string. I have grouped these separately. Is this correct? Should they be grouped together under ‘Not Known’?
# Select * from mat_pre_clear.msd001motherdemog where person_id_mother=4341909231 and RecordNumber=328314000003837 and LAD_UAMother=’’
# Select * from mat_pre_clear.msd001motherdemog where person_id_mother=7037581747 and RecordNumber=336891000001555 and LAD_UAMother is null
# Althira - I think both should be grouped under ‘Not Known’ . I looked into monthly publication csv file for a few reporting months and I could find only valid values and ‘LOCAL AUTHORITY UNKNOWN’.
# No work done on this as LAD removed from output

# COMMAND ----------

SLBMeasures = {} # empty dictionary

# COMMAND ----------

# Commented out geographies were initially included in some of the specifications
# As of 2/3/21, uncommenting here is sufficient for them to be included  in the output
Geographies = {
#      "LRegionORG" : {"Org_Level":"'NHS England (Region, Local Office)'", "Name_Column":"LRegion", "Code_Column" : "LRegionOrg"}, 
      "RegionORG" : {"Org_Level":"'NHS England (Region)'", "Name_Column":"Region", "Code_Column" : "RegionOrg"}
      ,"MBRRACE_Grouping_Short" : {"Org_Level": "'MBRRACE Grouping'", "Name_Column":"MBRRACE_Grouping", "Code_Column" : "Mbrrace_Grouping_Short"}
      ,"STP_Code" : {"Org_Level": "'Local Maternity System'", "Name_Column":"STP_Name", "Code_Column" : "STP_Code"}
#      ,"LocalAuthority" : {"Org_Level": "'Local Authority of Residence'", "Name_Column":"LADNM", "Code_Column" : "LADCD"}
#      ,"BookingSite" : {"Org_Level": "'Booking Site'", "Name_Column":"OrgSiteNameBooking", "Code_Column" : "OrgSiteIDBooking"}
#      ,"CCG" : {"Org_Level": "'CCG of Responsibility'", "Name_Column":"CCGName", "Code_Column" : "CCGResponsibilityMother"}
#      ,"DeliverySite" : {"Org_Level": "'Delivery Site'", "Name_Column":"OrgSiteNameActualDelivery", "Code_Column" : "OrgSiteIDActualDelivery"}
    }
print(Geographies)

# COMMAND ----------

IndicatorFamily = "SBL"

# COMMAND ----------

# DBTITLE 1,No longer used - Element 1 Outcome Indicator 1
# # Percentage of women with a CO measurement >=4ppm at booking.
# SLB1_1 = {
#     "Indicator" : "SBL_Element1_OutcomeIndicator1"
#     ,"CountOf" : "UniqPregID"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
#         on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
#         on 201ccp.UniqPregID = 202cap.UniqPregID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother and 201ccp.CareConID = 202cap.CareConID and 201ccp.RPStartDate = 202cap.RPStartDate \
#       where \
#         101pb.RPStartDate between '{RPStart}' and '{RPEnd}' \
#         and 101pb.AntenatalAppDate between '{RPStart}' and '{RPEnd}' \
#         and 201ccp.RPStartDate <= '{RPStart}' \
#         and datediff(201ccp.CContactDate, 101pb.AntenatalAppDate)  <= 3 \
#         and 202cap.COMonReading >= 4"
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , Person_ID_Mother, UniqPregID, '' AS Person_ID_Baby, RecordNumber, OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking \
#       where RPStartDate between '{RPStart}' and '{RPEnd}' \
#       and AntenatalAppDate between '{RPStart}' and '{RPEnd}'"
# }
# SLBMeasures['SLB1.1'] = SLB1_1

# COMMAND ----------

# DBTITLE 1,No longer used - Element 1 Process Indicator 1
# # Recording of CO reading for each pregnant woman on Maternity Information System (MIS) and inclusion of this data in the providers' Maternity Services Dataset (MSDS) submission to NHS Digital.
# SLB1_P1 = {
#     "Indicator" : "SBL_Element1_ProcessIndicator1"
#     ,"CountOf" : "UniqPregID"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
#         on 101pb.UniqPregID = 202cap.UniqPregID and 101pb.Person_ID_Mother = 202cap.Person_ID_Mother \
#       where \
#         101pb.RPStartDate between '{RPStart}' and '{RPEnd}' \
#         and 101pb.AntenatalAppDate between '{RPStart}' and '{RPEnd}' \
#         and 202cap.RPStartDate <= '{RPStart}' \
#         and (202cap.ProcedureCode = '226571000000100' or 202cap.COMonReading >= 0)"
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , Person_ID_Mother, UniqPregID, '' AS Person_ID_Baby, RecordNumber, OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking \
#       where RPStartDate between '{RPStart}' and '{RPEnd}' \
#       and AntenatalAppDate between '{RPStart}' and '{RPEnd}'"
# }
# SLBMeasures['SLB1.P1'] = SLB1_P1
# 

# COMMAND ----------

# DBTITLE 1,No longer used - Element 1 Outcome Indicator 2
# # Percentage of women with a CO measurement >=4ppm at 36 weeks.
# # Join on RecordNumber isn't in specification but leaving it in as it's been signed off
# SLB1_2 = {
#     "Indicator" : "SBL_Element1_OutcomeIndicator2"
#     ,"CountOf" : "UniqPregID"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
#         on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother and 101pb.RecordNumber = 201ccp.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
#         on 201ccp.UniqPregID = 202cap.UniqPregID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother and 201ccp.CareConID = 202cap.CareConID \
#       where \
#         201ccp.RPStartDate between '{RPStart}' and '{RPEnd}' \
#         and 201ccp.CContactDate between '{RPStart}' and '{RPEnd}' \
#         and 201ccp.GestAgeCContactDate between 245 and 258 \
#         and 202cap.RPStartDate between '{RPStart}' and '{RPEnd}' \
#         and 202cap.COMonReading >= 4"
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT 101pb.MSD101_ID, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#       inner join {DatabaseSchema}.msd201carecontactpreg as 201ccp \
#       on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother and 101pb.RecordNumber = 201ccp.RecordNumber \
#       where 201ccp.CContactDate between '{RPStart}' and '{RPEnd}' \
#       and 201ccp.GestAgeCContactDate between 245 and 258"
# }
# SLBMeasures['SLB1.2'] = SLB1_2

# COMMAND ----------

# DBTITLE 1,Element 1 Process Indicator a.i - formerly Element 1 Process Indicator 2
# Percentage of women where CO measurement at booking is recorded.
SLB1_P2 = {
    "Indicator" : "SBL_Element1_ProcessIndicator_a.i"
    ,"CountOf" : "UniqPregID"
    # Capture numerator rows in the reporting period.
    ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
        on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother \
        Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
        on 101pb.UniqPregID = 202cap.UniqPregID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother and 201ccp.CareConID = 202cap.CareConID and 201ccp.RPStartDate = 202cap.RPStartDate \
      where \
        101pb.RPStartDate between '{RPStart}' and '{RPEnd}' \
        and 101pb.AntenatalAppDate between '{RPStart}' and '{RPEnd}' \
        and 201ccp.RPStartDate <= '{RPStart}' \
        and datediff(201ccp.CContactDate, 101pb.AntenatalAppDate)  <= 3 \
        and 202cap.COMonReading >= 0"
    # Capture denominator rows in the reporting period.
    ,"Denominator" : "SELECT MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , Person_ID_Mother, UniqPregID, '' AS Person_ID_Baby, RecordNumber, OrgCodeProvider \
        , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking \
      where RPStartDate between '{RPStart}' and '{RPEnd}' \
      and AntenatalAppDate between '{RPStart}' and '{RPEnd}'"
}
SLBMeasures['SLB1.P2'] = SLB1_P2

# COMMAND ----------

# DBTITLE 1,No longer used - Element 1 Outcome Indicator 3
# # Percentage of women who have a CO level >=4ppm at booking and <4ppm at the 36 week appointment.
# SLB1_3 = {
#     "Indicator" : "SBL_Element1_OutcomeIndicator3"
#     ,"CountOf" : "UniqPregID"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, at36weeks.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#         from (Select distinct 201qccp.OrgCodeProvider, 201qccp.Person_ID_Mother, 201qccp.UniqPregID, 201qccp.CContactDate \
#           from {DatabaseSchema}.MSD201CareContactPreg as 201qccp \
#           Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202qcap \
#           on 201qccp.UniqPregID = 202qcap.UniqPregID and 201qccp.Person_ID_Mother = 202qcap.Person_ID_Mother and 201qccp.CareConID = 202qcap.CareConID \
#           where 201qccp.RPStartDate between '{RPStart}' and '{RPEnd}' \
#             and 201qccp.CContactDate  between '{RPStart}' and '{RPEnd}' \
#             and 201qccp.GestAgeCContactDate between 245 and 258 \
#             and 202qcap.RPStartDate between '{RPStart}' and '{RPEnd}' \
#             and 202qcap.COMonReading < 4) as at36weeks \
#         inner join {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         on at36weeks.UniqPregID = 101pb.UniqPregID and at36weeks.Person_ID_Mother = 101pb.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
#         on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
#         on 201ccp.UniqPregID = 202cap.UniqPregID and 201ccp.CareConID = 202cap.CareConID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother \
#       where \
#         101pb.RPStartDate <= '{RPStart}' \
#         and datediff(101pb.AntenatalAppDate, 201ccp.CContactDate)  between -3 and 3 \
#         and at36weeks.CContactDate > 201ccp.CContactDate \
#         and 202cap.COMonReading >= 4"
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, at36weeks.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#         from (Select distinct 201qccp.OrgCodeProvider, 201qccp.Person_ID_Mother, 201qccp.UniqPregID, 201qccp.CContactDate \
#           from {DatabaseSchema}.MSD201CareContactPreg as 201qccp \
#           Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202qcap \
#           on 201qccp.UniqPregID = 202qcap.UniqPregID and 201qccp.Person_ID_Mother = 202qcap.Person_ID_Mother and 201qccp.CareConID = 202qcap.CareConID \
#           where 201qccp.RPStartDate between '{RPStart}' and '{RPEnd}' \
#             and 201qccp.CContactDate  between '{RPStart}' and '{RPEnd}' \
#             and 201qccp.GestAgeCContactDate between 245 and 258 \
#             and 202qcap.RPStartDate between '{RPStart}' and '{RPEnd}' \
#             and 202qcap.COMonReading >= 0) as at36weeks \
#         inner join {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         on at36weeks.UniqPregID = 101pb.UniqPregID and at36weeks.Person_ID_Mother = 101pb.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
#         on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD202CareActivityPreg  as 202cap \
#         on 201ccp.UniqPregID = 202cap.UniqPregID and 201ccp.CareConID = 202cap.CareConID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother and 201ccp.RPStartDate = 202cap.RPStartDate \
#       where \
#         101pb.RPStartDate <= '{RPStart}' \
#         and datediff(101pb.AntenatalAppDate, 201ccp.CContactDate)  between -3 and 3 \
#         and at36weeks.CContactDate > 201ccp.CContactDate \
#         and 202cap.COMonReading >= 4"
# }
# SLBMeasures['SLB1.3'] = SLB1_3

# COMMAND ----------

# DBTITLE 1,Element 1 Process Indicator a.ii - formerly Element 1 Process Indicator 3
# Percentage of women where CO measurement at 36 weeks is recorded
SLB1_P3 = {
    "Indicator" : "SBL_Element1_ProcessIndicator_a.ii"
    ,"CountOf" : "UniqPregID"
    # Capture numerator rows in the reporting period.
    ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
        from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
        on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother and 101pb.RecordNumber = 201ccp.RecordNumber \
        Inner Join {DatabaseSchema}.MSD202CareActivityPreg as 202cap \
        on 201ccp.UniqPregID = 202cap.UniqPregID and 201ccp.Person_ID_Mother = 202cap.Person_ID_Mother and 201ccp.CareConID = 202cap.CareConID \
      where \
        101pb.RPEndDate = '{RPEnd}' \
        and 201ccp.CContactDate between '{RPStart}' and '{RPEnd}' \
        and 201ccp.GestAgeCContactDate between 245 and 258 \
        and 202cap.RPEndDate = '{RPEnd}' \
        and 202cap.COMonReading >= 0"
    # Capture denominator rows in the reporting period.
    ,"Denominator" : "SELECT MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
        from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD201CareContactPreg as 201ccp \
        on 101pb.UniqPregID = 201ccp.UniqPregID and 101pb.Person_ID_Mother = 201ccp.Person_ID_Mother and 101pb.RecordNumber = 201ccp.RecordNumber \
      where \
        201ccp.RPEndDate = '{RPEnd}' \
        and 201ccp.CContactDate between '{RPStart}' and '{RPEnd}' \
        and 201ccp.GestAgeCContactDate between 245 and 258"
}
SLBMeasures['SLB1.P3'] = SLB1_P3

# COMMAND ----------

# DBTITLE 1,Element 2 Process Indicator b - formerly Element 2 Process Indicator 2
# Percentage of pregnancies where an SGA fetus is antenatally detected and this is recorded on the provider\\\'s MIS and included in their MSDS submission to NHS Digital
SLB2_P2 = {
    "Indicator" : "SBL_Element2_ProcessIndicator_b"
    ,"CountOf" : "UniqPregID"
    # Capture numerator rows in the reporting period.
    ,"Numerator" : "SELECT pb101.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , pb101.Person_ID_Mother, pb101.UniqPregID, '' AS Person_ID_Baby, pb101.RecordNumber, pb101.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, pb101.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
        from {DatabaseSchema}.MSD101pregnancybooking as pb101 \
        inner join \
        ( \
          select bd1.UniqPregID, bd1.Person_ID_Mother, bd1.RecordNumber from \
          (select UniqPregID, Person_ID_Mother, RecordNumber, RPStartDate from \
          {DatabaseSchema}.msd401babydemographics \
          where RPStartDate between '{RPStart}' and '{RPEnd}' \
          and PersonBirthDateBaby between '{RPStart}' and '{RPEnd}') as bd1 \
          left join {DatabaseSchema}.msd106diagnosispreg as dp106 \
          on bd1.UniqPregID = dp106.UniqPregID \
          where Diag IN ('267258002', '722525006', '722835006', '206164002', '249051004', '22033007', '276606009', '276607000') \
          and bd1.RPStartDate >= dp106.RPStartDate \
          union all \
          select bd2.UniqPregID, bd2.Person_ID_Mother, bd2.RecordNumber from \
          (select UniqPregID, Person_ID_Mother, RecordNumber, RPStartDate from \
          {DatabaseSchema}.msd401babydemographics \
          where RPStartDate between '{RPStart}' and '{RPEnd}' \
          and PersonBirthDateBaby between '{RPStart}' and '{RPEnd}') as bd2 \
          left join {DatabaseSchema}.msd109findingobsmother as dp109 \
          on bd2.UniqPregID = dp109.UniqPregID \
          where FindingCode IN ('267258002', '722525006', '722835006', '206164002', '249051004', '22033007', '276606009', '276607000') \
          and bd2.RPStartDate >= dp109.RPStartDate \
          union all \
          select bd3.UniqPregID, bd3.Person_ID_Mother, bd3.RecordNumber from \
          (select UniqPregID, Person_ID_Mother, RecordNumber, RPStartDate from \
          {DatabaseSchema}.msd401babydemographics \
          where RPStartDate between '{RPStart}' and '{RPEnd}' \
          and PersonBirthDateBaby between '{RPStart}' and '{RPEnd}') as bd3 \
          Inner Join {DatabaseSchema}.MSD201CareContactPreg as ccp201 \
          on bd3.UniqPregID = ccp201.UniqPregID \
          Inner Join {DatabaseSchema}.MSD202CareActivityPreg as cap202 \
          on ccp201.UniqPregID = cap202.UniqPregID and ccp201.CareConID = cap202.CareConID and ccp201.RPStartDate = cap202.RPStartDate \
          where cap202.FindingCode IN ('267258002', '722525006', '722835006', '206164002', '249051004', '22033007', '276606009', '276607000') \
          and bd3.RPStartDate >= ccp201.RPStartDate \
        ) as u \
        on pb101.UniqPregID = u.UniqPregID and pb101.Person_ID_Mother = u.Person_ID_Mother and pb101.RecordNumber = u.RecordNumber"
    # Capture denominator rows in the reporting period.
    ,"Denominator" : "SELECT pb101.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , pb101.Person_ID_Mother, pb101.UniqPregID, '' AS Person_ID_Baby, pb101.RecordNumber, pb101.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
        from {DatabaseSchema}.MSD101pregnancybooking as pb101 \
        inner join {DatabaseSchema}.msd401babydemographics as bd401 \
        on pb101.UniqPregID = bd401.UniqPregID and pb101.Person_ID_Mother = bd401.Person_ID_Mother and pb101.RecordNumber = bd401.RecordNumber \
            where bd401.RPStartDate between '{RPStart}' and '{RPEnd}' \
            and bd401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}'" \
}
SLBMeasures['SLB2.P2'] = SLB2_P2

# COMMAND ----------

# DBTITLE 1,Element 3 Process Indicator a - formerly Element 3 Process Indicator 2
SLB3_P2 = {
    "Indicator" : "SBL_Element3_ProcessIndicator_a"
    ,"CountOf" : "Person_ID_Mother"
    # Capture numerator rows in the reporting period.
    # FindingCode = 245761000000108 is CTG scan ; other codes are for Reduced Fetal Movement
    ,"Numerator" : "SELECT pb101.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , pb101.Person_ID_Mother, pb101.UniqPregID, '' AS Person_ID_Baby, pb101.RecordNumber, pb101.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, pb101.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as pb101 \
        Inner Join (select UniqPregID, Person_ID_Mother, RecordNumber, RPStartDate FROM {DatabaseSchema}.MSD202CareActivityPreg  \
                      where RPStartDate Between '{RPStart}' and '{RPEnd}' \
                      and FindingCode in (276369006, 289432001, 249038009, 276372004, 163540009, 363093002, 276370007) \
                    union all \
                      select UniqPregID, Person_ID_Mother, RecordNumber, RPStartDate FROM {DatabaseSchema}.msd106diagnosispreg \
                      where RPStartDate Between '{RPStart}' and '{RPEnd}' \
                      and DiagDate Between '{RPStart}' and '{RPEnd}' \
                      and Diag in (276369006, 289432001, 249038009, 276372004, 163540009, 363093002, 276370007) \
                   ) as rfm \
        on pb101.UniqPregID = rfm.UniqPregID and pb101.Person_ID_Mother = rfm.Person_ID_Mother and pb101.RecordNumber = rfm.RecordNumber \
        Inner Join {DatabaseSchema}.MSD202CareActivityPreg as cap202 \
        on rfm.UniqPregID = cap202.UniqPregID and rfm.Person_ID_Mother = cap202.Person_ID_Mother and rfm.RPStartDate = cap202.RPStartDate \
        where cap202.FindingCode = 245761000000108"
    # Capture denominator rows in the reporting period.
    # Codes are for Reduced Fetal Movement
    ,"Denominator" : "SELECT pb101.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate, '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , pb101.Person_ID_Mother, pb101.UniqPregID, '' AS Person_ID_Baby, pb101.RecordNumber, pb101.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, pb101.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother, '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as pb101 \
        Inner Join (select UniqPregID, Person_ID_Mother, RecordNumber FROM {DatabaseSchema}.MSD202CareActivityPreg  \
                      where RPStartDate Between '{RPStart}' and '{RPEnd}' \
                      and FindingCode in (276369006, 289432001, 249038009, 276372004, 163540009, 363093002, 276370007) \
                    union all \
                      select UniqPregID, Person_ID_Mother, RecordNumber FROM {DatabaseSchema}.msd106diagnosispreg \
                      where RPStartDate Between '{RPStart}' and '{RPEnd}' \
                      and DiagDate Between '{RPStart}' and '{RPEnd}' \
                      and Diag in (276369006, 289432001, 249038009, 276372004, 163540009, 363093002, 276370007) \
                   ) as rfm \
        on pb101.UniqPregID = rfm.UniqPregID and pb101.Person_ID_Mother = rfm.Person_ID_Mother and pb101.RecordNumber = rfm.RecordNumber"
}
SLBMeasures['SLB3.P2'] = SLB3_P2

# COMMAND ----------

# DBTITLE 1,Element 5 Outcome Indicator l.a - formerly Element 5 Outcome Indicator 1a
# The incidence of women with a singleton pregnancy giving birth (liveborn) as a % of all singleton live births(>22 weeks): In the late second trimester (from 22+1 to 23+6 weeks)
SLB5_1a = {
    "Indicator" : "SBL_Element5_OutcomeIndicator_l.a"
    ,"CountOf" : "Person_ID_Mother"
    # Capture numerator rows in the reporting period.
    ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
        , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
        , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
            on 101pb.UniqPregID = MSD401.UniqPregID \
                and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
                and 101pb.RecordNumber = MSD401.RecordNumber \
        Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
            on MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
              and MSD401.UniqPregID = MSD301.UniqPregID \
              and MSD401.RecordNumber = MSD301.RecordNumber \
        where MSD301.BirthsPerLabAndDel = 1 \
            and MSD401.PregOutcome IN ('01','02','03','04') and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
            and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
            and MSD401.GestationLengthBirth between 155 and 167 "
          
    # Capture denominator rows in the reporting period.
    ,"Denominator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
        , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
        , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
            on 101pb.UniqPregID = MSD401.UniqPregID \
                and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
                and 101pb.RecordNumber = MSD401.RecordNumber \
        Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
            on MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
              and MSD401.UniqPregID = MSD301.UniqPregID \
              and MSD401.RecordNumber = MSD301.RecordNumber \
      where MSD301.BirthsPerLabAndDel = 1 \
          and MSD401.PregOutcome IN ('01','02','03','04') and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
          and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
          and MSD401.GestationLengthBirth > 154"        
}
SLBMeasures['SLB5.1a'] = SLB5_1a

# COMMAND ----------

# DBTITLE 1,Element 5 Outcome Indicator l.b - formerly Element 5 Outcome Indicator 1b
# The incidence of women with a singleton pregnancy giving birth (liveborn) as a % of all singleton live births(>22 weeks): Preterm (from 24+0 to 36+6 weeks)
SLB5_1b = {
    "Indicator" : "SBL_Element5_OutcomeIndicator_l.b"
    ,"CountOf" : "Person_ID_Mother"
    # Capture numerator rows in the reporting period.
    ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
        , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
        , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
            on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
                and 101pb.RecordNumber = MSD401.RecordNumber \
        Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
            on MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
              and MSD401.UniqPregID = MSD301.UniqPregID \
              and MSD401.RecordNumber = MSD301.RecordNumber \
      where MSD301.BirthsPerLabAndDel = 1 \
          and MSD401.PregOutcome IN ('01','02','03','04') and MSD401.GestationLengthBirth between 168 and 258 \
          and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
          and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' "
                   
    # Capture denominator rows in the reporting period.
    ,"Denominator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
        , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
        , 101pb.Person_ID_Mother, 101pb.UniqPregID, '' AS Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
        , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
        , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
        , current_timestamp() as CreatedAt \
      from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
        Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
            on 101pb.UniqPregID = MSD401.UniqPregID \
                and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
                and 101pb.RecordNumber = MSD401.RecordNumber \
        Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
            on MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
              and MSD401.UniqPregID = MSD301.UniqPregID \
              and MSD401.RecordNumber = MSD301.RecordNumber \
      where MSD301.BirthsPerLabAndDel = 1 \
          and MSD401.PregOutcome IN ('01','02','03','04') and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
          and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
          and MSD401.GestationLengthBirth > 154"      
}
SLBMeasures['SLB5.1b'] = SLB5_1b

# COMMAND ----------

# DBTITLE 1,No longer used - Element 5 Process Indicator 1
# # Percentage of singleton live births (less than 34+0 weeks) receiving a full course of antenatal corticosteroids, within seven days of birth.
# SLB5_1 = {
#     "Indicator" : "SBL_Element5_ProcessIndicator1"
#     ,"CountOf" : "Person_ID_Baby"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.UniqPregID = MSD301.UniqPregID \
#               and MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#               and MSD401.RPStartDate = MSD301.RPStartDate \
#         Inner Join {DatabaseSchema}.MSD302CareActivityLabDel  as MSD302 \
#             on MSD401.UniqPregID = MSD302.UniqPregID \
#               and MSD401.Person_ID_Mother = MSD302.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as MSD201 \
#             on MSD401.UniqPregID = MSD201.UniqPregID and MSD401.Person_ID_Mother = MSD201.Person_ID_Mother and MSD401.RPStartDate = MSD201.RPStartDate \
#         left Join {DatabaseSchema}.MSD202CareActivityPreg  as MSD202 \
#             on MSD201.UniqPregID = MSD202.UniqPregID and MSD201.Person_ID_Mother = MSD202.Person_ID_Mother and MSD201.CareConID = MSD202.CareConID and MSD201.RPStartDate = MSD202.RPStartDate\
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PregOutcome = '01' \
#           and MSD401.GestationLengthBirth < 238 \
#           and ((MSD302.ProcedureCode = '434611000124106' and datediff(MSD401.PersonBirthDateBaby, MSD302.ClinInterDateMother) between 0 and 7) \
#             or (MSD202.ProcedureCode = '434611000124106' and datediff(MSD401.PersonBirthDateBaby, MSD201.CContactDate) between 0 and 7))"
#           
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID \
#                 and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#               and MSD401.UniqPregID = MSD301.UniqPregID \
#               and MSD401.RPStartDate = MSD301.RPStartDate \
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PregOutcome = '01' \
#           and MSD401.GestationLengthBirth < 238"
# }
# SLBMeasures['SLB5.1'] = SLB5_1

# COMMAND ----------

# DBTITLE 1,No longer used - Element 5 Process Indicator 2
# # Percentage of singleton live births (less than 34+0 weeks) occurring more than seven days after completion of their first course of antenatal corticosteroids.
# SLB5_2 = {
#     "Indicator" : "SBL_Element5_ProcessIndicator2"
#     ,"CountOf" : "Person_ID_Baby"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.UniqPregID = MSD301.UniqPregID \
#               and MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#               and MSD401.RPStartDate = MSD301.RPStartDate \
#         Inner Join {DatabaseSchema}.MSD302CareActivityLabDel  as MSD302 \
#             on MSD401.UniqPregID = MSD302.UniqPregID \
#               and MSD401.Person_ID_Mother = MSD302.Person_ID_Mother \
#         Inner Join {DatabaseSchema}.MSD201CareContactPreg as MSD201 \
#             on MSD401.UniqPregID = MSD201.UniqPregID and MSD401.Person_ID_Mother = MSD201.Person_ID_Mother and MSD401.RPStartDate = MSD201.RPStartDate \
#         left Join {DatabaseSchema}.MSD202CareActivityPreg  as MSD202 \
#             on MSD201.UniqPregID = MSD202.UniqPregID and MSD201.Person_ID_Mother = MSD202.Person_ID_Mother and MSD201.CareConID = MSD202.CareConID and MSD201.RPStartDate = MSD202.RPStartDate\
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PregOutcome = '01' \
#           and MSD401.GestationLengthBirth < 238 \
#           and MSD302.ProcedureCode = '434611000124106' \
#           and ((MSD302.ProcedureCode = '434611000124106' and datediff(MSD401.PersonBirthDateBaby, MSD302.ClinInterDateMother) > 7) \
#             or (MSD202.ProcedureCode = '434611000124106' and datediff(MSD401.PersonBirthDateBaby, MSD201.CContactDate) > 7))"
#           
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.UniqPregID = MSD301.UniqPregID \
#               and MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#               and MSD401.RPStartDate = MSD301.RPStartDate \
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PregOutcome = '01' \
#           and MSD401.GestationLengthBirth < 238"
# }
# SLBMeasures['SLB5.2'] = SLB5_2

# COMMAND ----------

# DBTITLE 1,No longer used - Element 5 Process Indicator 3
# # Percentage of singleton live births (less than 30+0 weeks) receiving magnesium sulphate within 24 hours prior to birth
# SLB5_3 = {
#     "Indicator" : "SBL_Element5_ProcessIndicator3"
#     ,"CountOf" : "Person_ID_Baby"
#     # Capture numerator rows in the reporting period.
#     ,"Numerator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.UniqPregID = MSD301.UniqPregID \
#                 and MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#                 and MSD401.RPStartDate = MSD301.RPStartDate \
#         Inner Join {DatabaseSchema}.MSD302CareActivityLabDel as MSD302 \
#             on MSD302.UniqPregID = MSD301.UniqPregID and MSD302.Person_ID_Mother = MSD301.Person_ID_Mother \
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PregOutcome = '01' and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.GestationLengthBirth < 210 \
#           and MSD302.ProcedureCode = '144351000000105'  \
#           and MSD302.ClinInterDateMother between MSD401.PersonBirthDateBaby and date_add(MSD401.PersonBirthDateBaby, -1) "
#     
#     # Capture denominator rows in the reporting period.
#     ,"Denominator" : "SELECT 101pb.MSD101_ID AS KeyValue, '{RPStart}' AS RPStartDate, '{RPEnd}' AS RPEndDate \
#         , '{Indicator}' AS Indicator, '{IndicatorFamily}' AS IndicatorFamily \
#         , 101pb.Person_ID_Mother, 101pb.UniqPregID, MSD401.Person_ID_Baby, 101pb.RecordNumber, 101pb.OrgCodeProvider \
#         , '' as LADCD, '' as LADNM, 101pb.OrgSiteIDBooking, '' as OrgSiteNameBooking, '' as CCGResponsibilityMother \
#         , '' as CCGName, '' as OrgSiteIDActualDelivery, '' as OrgSiteNameActualDelivery \
#         , current_timestamp() as CreatedAt \
#       from {DatabaseSchema}.MSD101pregnancybooking as 101pb \
#         Inner Join {DatabaseSchema}.MSD401BabyDemographics as MSD401 \
#             on 101pb.UniqPregID = MSD401.UniqPregID and 101pb.Person_ID_Mother = MSD401.Person_ID_Mother \
#                 and 101pb.RecordNumber = MSD401.RecordNumber \
#         Inner Join {DatabaseSchema}.MSD301LabourDelivery  as MSD301 \
#             on MSD401.UniqPregID = MSD301.UniqPregID and MSD401.Person_ID_Mother = MSD301.Person_ID_Mother \
#                 and MSD401.RPStartDate = MSD301.RPStartDate \
#       where MSD301.BirthsPerLabAndDel = 1 \
#           and MSD401.PregOutcome = '01' and MSD401.RPStartDate between '{RPStart}' and '{RPEnd}' \
#           and MSD401.PersonBirthDateBaby between '{RPStart}' and '{RPEnd}' \
#           and MSD401.GestationLengthBirth < 210 "
#     
# }
# SLBMeasures['SLB5.3'] = SLB5_3

# COMMAND ----------

# DBTITLE 1,For each measure, get numerator and denominator data
for key, value in SLBMeasures.items():
  Indicator = value["Indicator"]
  NumeratorStatement = ("INSERT INTO {outSchema}.SLB_Numerator_Raw " + value["Numerator"]).format(DatabaseSchema=dbSchema, outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, Indicator=Indicator, IndicatorFamily=IndicatorFamily)
  print(NumeratorStatement)
  spark.sql(NumeratorStatement)
  DenominatorStatement = ("INSERT INTO {outSchema}.SLB_Denominator_Raw " + value["Denominator"]).format(DatabaseSchema=dbSchema, outSchema=outSchema, RPStart=RPBegindate, RPEnd=RPEnddate, Indicator=Indicator, IndicatorFamily=IndicatorFamily)
  print(DenominatorStatement)
  spark.sql(DenominatorStatement)



# COMMAND ----------

# DBTITLE 1,Add geographies to numerator
# MAGIC %sql
# MAGIC -- I think ctegp002 should have an additional order on EndDateGMPReg desc
# MAGIC ; with cteladv2 as (
# MAGIC   select row_number() over (partition by ladcd order by DSS_ONS_PUBLISHED_DATE desc, DSS_RECORD_START_DATE desc) as rn
# MAGIC   , LADCD, LADNM from $dss_corporate.ONS_LSOA_CCG_LAD_V02
# MAGIC   where '$RPEnddate' >= DSS_ONS_PUBLISHED_DATE and DSS_RECORD_END_DATE is null
# MAGIC )
# MAGIC ,ctelad as (
# MAGIC   select * from cteladv2 where rn = 1
# MAGIC )
# MAGIC ,cteorg as (
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where ORG_OPEN_DATE <= '$RPEnddate' and (ORG_CLOSE_DATE is NULL or ORG_CLOSE_DATE >= '$RPBegindate')
# MAGIC   and BUSINESS_END_DATE is NULL 
# MAGIC )
# MAGIC ,cteclosed as ( -- If organisation not open during reporting period, resolve to current name
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where BUSINESS_END_DATE is NULL
# MAGIC   and org_type_code='TR'
# MAGIC )
# MAGIC ,ctebd as (
# MAGIC   select row_number() over (partition by UniqPregID, Person_ID_Mother order by RPStartDate desc, RecordNumber desc) as rn, UniqPregID, Person_ID_Mother, OrgSiteIDActualDelivery
# MAGIC   from $dbSchema.msd401babydemographics
# MAGIC   where RPEndDate <= '$RPEnddate'
# MAGIC )
# MAGIC ,cteBabyDemo as (
# MAGIC   select UniqPregID, Person_ID_Mother, OrgSiteIDActualDelivery from ctebd where rn=1
# MAGIC )
# MAGIC ,ctegp002 as (
# MAGIC   select row_number() over (partition by RecordNumber order by case when EndDateGMPReg is null then 1 else 0 end desc, MSD002_ID desc) as rn
# MAGIC   , RecordNumber, StartDateGMPReg, EndDateGMPReg, RPEndDate, 
# MAGIC     --Since July, all CCG are referred as SubICB, so picking the corresponding field. Maintaining the column name as SLB tables have same name and to avoid any breaks.
# MAGIC  CASE
# MAGIC   WHEN $month_id <= 1467  then CCGResponsibilityMother
# MAGIC   ELSE OrgIDSubICBLocGP
# MAGIC   END as CCGResponsibilityMother
# MAGIC   from $dbSchema.msd002gp
# MAGIC   where RPEndDate = '$RPEnddate' and (EndDateGMPReg is null or (StartDateGMPReg is null and EndDateGMPReg > '$RPStartdate') or (StartDateGMPReg <= '$RPEnddate' and EndDateGMPReg >= '$RPStartdate'))
# MAGIC )
# MAGIC , ctegp as (
# MAGIC   select RecordNumber, StartDateGMPReg, EndDateGMPReg, RPEndDate, CCGResponsibilityMother from ctegp002 where rn=1
# MAGIC )
# MAGIC MERGE INTO $outSchema.SLB_Numerator_Raw as nr
# MAGIC USING (select DISTINCT snr.KeyValue, snr.Person_ID_Mother, 001md.LAD_UAMother, lad.LADNM, bd.OrgSiteIDActualDelivery, COALESCE(org1.NAME, orgc1.NAME) as OrgSiteNameActualDelivery
# MAGIC        , COALESCE(org2.NAME, orgc2.NAME) as OrgSiteNameBooking, 002gp.CCGResponsibilityMother, COALESCE(org3.NAME, orgc3.NAME) as CCGName
# MAGIC from $outSchema.SLB_Numerator_Raw as snr
# MAGIC left join $dbSchema.msd001motherdemog as 001md on snr.Person_ID_Mother = 001md.Person_ID_Mother and snr.RecordNumber = 001md.RecordNumber
# MAGIC left join ctegp as 002gp on snr.RecordNumber = 002gp.RecordNumber 
# MAGIC left join cteBabyDemo as bd on bd.UniqPregID = snr.UniqPregID and bd.Person_ID_Mother = snr.Person_ID_Mother
# MAGIC left join ctelad as lad on 001md.LAD_UAMother = lad.LADCD
# MAGIC left join cteorg as org1 on bd.OrgSiteIDActualDelivery = org1.ORG_CODE
# MAGIC left join cteorg as org2 on snr.OrgSiteIDBooking = org2.ORG_CODE
# MAGIC left join cteorg as org3 on 002gp.CCGResponsibilityMother = org3.ORG_CODE
# MAGIC left join cteclosed as orgc1 on bd.OrgSiteIDActualDelivery = orgc1.ORG_CODE
# MAGIC left join cteclosed as orgc2 on snr.OrgSiteIDBooking = orgc2.ORG_CODE
# MAGIC left join cteclosed as orgc3 on 002gp.CCGResponsibilityMother = orgc3.ORG_CODE
# MAGIC ) as agg
# MAGIC ON nr.KeyValue = agg.KeyValue
# MAGIC WHEN MATCHED THEN UPDATE SET nr.LADCD = agg.LAD_UAMother, nr.LADNM = agg.LADNM, nr.OrgSiteNameBooking = agg.OrgSiteNameBooking 
# MAGIC   , nr.CCGResponsibilityMother = agg.CCGResponsibilityMother, nr.CCGName = agg.CCGName, nr.OrgSiteIDActualDelivery = agg.OrgSiteIDActualDelivery, nr.OrgSiteNameActualDelivery = agg.OrgSiteNameActualDelivery
# MAGIC

# COMMAND ----------

# DBTITLE 1,Add geographies to denominator
# MAGIC %sql
# MAGIC ; with cteladv2 as (
# MAGIC   select row_number() over (partition by ladcd order by DSS_ONS_PUBLISHED_DATE desc, DSS_RECORD_START_DATE desc) as rn
# MAGIC   , LADCD, LADNM from $dss_corporate.ONS_LSOA_CCG_LAD_V02
# MAGIC   where '$RPEnddate' >= DSS_ONS_PUBLISHED_DATE and DSS_RECORD_END_DATE is null
# MAGIC )
# MAGIC ,ctelad as (
# MAGIC   select * from cteladv2 where rn = 1
# MAGIC )
# MAGIC ,cteorg as (
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where ORG_OPEN_DATE <= '$RPEnddate' and (ORG_CLOSE_DATE is NULL or ORG_CLOSE_DATE >= '$RPBegindate')
# MAGIC   and BUSINESS_END_DATE is NULL 
# MAGIC )
# MAGIC ,cteclosed as ( -- If organisation not open during reporting period, resolve to current name. If still resolving to Null, may need to add to list of org_type_code
# MAGIC     select ORG_CODE, NAME
# MAGIC   from $dss_corporate.ORG_DAILY
# MAGIC   where BUSINESS_END_DATE is NULL
# MAGIC   and org_type_code='TR'
# MAGIC )
# MAGIC ,ctebd as (
# MAGIC   select row_number() over (partition by UniqPregID, Person_ID_Mother order by RPStartDate desc, RecordNumber desc) as rn, UniqPregID, Person_ID_Mother, OrgSiteIDActualDelivery
# MAGIC   from $dbSchema.msd401babydemographics
# MAGIC   where RPEndDate <= '$RPEnddate'
# MAGIC )
# MAGIC ,cteBabyDemo as (
# MAGIC   select UniqPregID, Person_ID_Mother, OrgSiteIDActualDelivery from ctebd where rn=1
# MAGIC )
# MAGIC ,ctegp002 as (
# MAGIC   select row_number() over (partition by RecordNumber order by case when EndDateGMPReg is null then 1 else 0 end desc, MSD002_ID desc) as rn
# MAGIC   , RecordNumber, StartDateGMPReg, EndDateGMPReg, RPEndDate, 
# MAGIC     --Since July, all CCG are referred as SubICB, so picking the corresponding field. Maintaining the column name as SLB tables have same name and to avoid any breaks.
# MAGIC  CASE
# MAGIC   WHEN $month_id <= 1467  then CCGResponsibilityMother
# MAGIC   ELSE OrgIDSubICBLocGP
# MAGIC   END as CCGResponsibilityMother
# MAGIC   from $dbSchema.msd002gp
# MAGIC   where RPEndDate = '$RPEnddate' and (EndDateGMPReg is null or (StartDateGMPReg is null and EndDateGMPReg > '$RPStartdate') or (StartDateGMPReg <= '$RPEnddate' and EndDateGMPReg >= '$RPStartdate'))
# MAGIC )
# MAGIC , ctegp as (
# MAGIC   select RecordNumber, StartDateGMPReg, EndDateGMPReg, RPEndDate, CCGResponsibilityMother from ctegp002 where rn=1
# MAGIC )
# MAGIC MERGE INTO $outSchema.SLB_Denominator_Raw as dr
# MAGIC -- distinct needed to avoid multiple rows causing merge to fail - particularly on DAE REF where data quality is poor
# MAGIC USING (select DISTINCT sdr.KeyValue, sdr.Person_ID_Mother, 001md.LAD_UAMother, lad.LADNM, bd.OrgSiteIDActualDelivery, COALESCE(org1.NAME, orgc1.NAME) as OrgSiteNameActualDelivery
# MAGIC        , COALESCE(org2.NAME, orgc2.NAME) as OrgSiteNameBooking, 002gp.CCGResponsibilityMother, COALESCE(org3.NAME, orgc3.NAME) as CCGName
# MAGIC from $outSchema.SLB_Denominator_Raw as sdr
# MAGIC left join $dbSchema.msd001motherdemog as 001md on sdr.Person_ID_Mother = 001md.Person_ID_Mother and sdr.RecordNumber = 001md.RecordNumber
# MAGIC left join ctegp as 002gp on sdr.RecordNumber = 002gp.RecordNumber 
# MAGIC left join cteBabyDemo as bd on bd.UniqPregID = sdr.UniqPregID and bd.Person_ID_Mother = sdr.Person_ID_Mother
# MAGIC left join ctelad as lad on 001md.LAD_UAMother = lad.LADCD
# MAGIC left join cteorg as org1 on bd.OrgSiteIDActualDelivery = org1.ORG_CODE
# MAGIC left join cteorg as org2 on sdr.OrgSiteIDBooking = org2.ORG_CODE
# MAGIC left join cteorg as org3 on 002gp.CCGResponsibilityMother = org3.ORG_CODE
# MAGIC left join cteclosed as orgc1 on bd.OrgSiteIDActualDelivery = orgc1.ORG_CODE
# MAGIC left join cteclosed as orgc2 on sdr.OrgSiteIDBooking = orgc2.ORG_CODE
# MAGIC left join cteclosed as orgc3 on 002gp.CCGResponsibilityMother = orgc3.ORG_CODE) as agg
# MAGIC ON dr.KeyValue = agg.KeyValue
# MAGIC WHEN MATCHED THEN UPDATE SET dr.LADCD = agg.LAD_UAMother, dr.LADNM = agg.LADNM, dr.OrgSiteNameBooking = agg.OrgSiteNameBooking
# MAGIC   , dr.CCGResponsibilityMother = agg.CCGResponsibilityMother, dr.CCGName = agg.CCGName, dr.OrgSiteIDActualDelivery = agg.OrgSiteIDActualDelivery, dr.OrgSiteNameActualDelivery = agg.OrgSiteNameActualDelivery
# MAGIC

# COMMAND ----------

# DBTITLE 1,Aggregate provider for each indicator
for key, value in SLBMeasures.items():
  Indicator = value["Indicator"]
  CountOf = value["CountOf"]
  print(CountOf)
  # If organisation not open during reporting period, resolve to current name
  ProviderStatement = ("with cteclosed as ( \
      select ORG_CODE, NAME \
    from {dss_corporate}.ORG_DAILY \
    where BUSINESS_END_DATE is NULL \
  ) \
  INSERT INTO {outSchema}.SLB_Provider_Aggregated \
  SELECT den.RPStartDate, den.RPEndDate, den.Indicator, den.IndicatorFamily, den.OrgCodeProvider \
        , COALESCE(den.Trust, c.NAME) AS OrgName, 'Provider' AS OrgLevel \
        , COALESCE(num.Total, 0) as Unrounded_Numerator \
        , den.Total as Unrounded_Denominator \
        , case when COALESCE(num.Total, 0) > 7 then round((COALESCE(num.Total, 0)/den.Total)*100, 1) else 0 end as Unrounded_Rate \
        , case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 end as Rounded_Numerator \
        , case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 ELSE 0 end as Rounded_Denominator \
        , round((case when COALESCE(num.Total, 0) between 1 and 7 then 5 when COALESCE(num.Total, 0) > 7 then round(COALESCE(num.Total, 0)/5,0)*5 ELSE 0 END / \
            case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 ELSE 0 end)*100, 1) as Rounded_Rate \
        , current_timestamp() as CreatedAt \
        FROM (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, OrgCodeProvider, count(DISTINCT {CountOf}) as Total from {outSchema}.SLB_Numerator_Geographies \
          where Indicator='{Indicator}' \
          group by RPStartDate, RPEndDate, Indicator, IndicatorFamily, OrgCodeProvider) AS num \
        RIGHT JOIN (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, OrgCodeProvider, Trust, count(DISTINCT {CountOf}) as Total from {outSchema}.SLB_Denominator_Geographies \
          where Indicator='{Indicator}' \
          group by RPStartDate, RPEndDate, Indicator, IndicatorFamily, OrgCodeProvider, Trust) as den \
        ON COALESCE(num.OrgCodeProvider, 'Not Known') = COALESCE(den.OrgCodeProvider, 'Not Known') \
        LEFT JOIN cteclosed as c on den.OrgCodeProvider = c.ORG_CODE").format(outSchema=outSchema, Indicator=Indicator, CountOf=CountOf, dss_corporate=dss_corporate)
  print(ProviderStatement)
  spark.sql(ProviderStatement)


# COMMAND ----------

# DBTITLE 1,Aggregate geographies
for key, value in SLBMeasures.items():
  Indicator = value["Indicator"]
  CountOf = value["CountOf"]
  print(Indicator)
  for key, value in Geographies.items():
    Geog_Org_Level = value["Org_Level"]
    Geog_Org_Name_Column = value["Name_Column"]
    Geog_Provider_Name_Column = value["Code_Column"]
    geographiesAggregationStatement = ("INSERT INTO {outSchema}.SLB_Geography_Aggregated \
      SELECT den.RPStartDate \
        , den.RPEndDate \
        , den.Indicator \
        , den.IndicatorFamily \
        , den.OrgCodeProvider AS OrgCodeProvider \
        , den.OrgName AS OrgName \
        , {Geog_Org_Level} AS OrgLevel \
        , num.Total as Unrounded_Numerator \
        , den.Total as Unrounded_Denominator \
        , case when num.Total > 7 then round((num.Total/den.Total)*100, 1) else 0 end as Unrounded_Rate \
        , case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end as Rounded_Numerator \
        , case when den.Total = 0 then 0 when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator \
        , round((case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end / \
             case when den.Total = 0 then 0 when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end)*100, 1) as Rounded_Rate \
        , current_timestamp() as CreatedAt \
        FROM (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE({Geog_Org_Name_Column}, 'Not Known') AS OrgName, COALESCE({Geog_Provider_Name}, 'Not Known') AS OrgCodeProvider \
          , count(DISTINCT {CountOf}) as Total from {outSchema}.SLB_Numerator_Geographies \
          where Indicator='{Indicator}' \
          group by RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE({Geog_Org_Name_Column}, 'Not Known'), COALESCE({Geog_Provider_Name}, 'Not Known')) AS num \
        RIGHT JOIN (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE({Geog_Org_Name_Column}, 'Not Known') AS OrgName, COALESCE({Geog_Provider_Name}, 'Not Known') AS OrgCodeProvider, \
          count(DISTINCT {CountOf}) as Total, Indicator from {outSchema}.SLB_Denominator_Geographies \
          where Indicator='{Indicator}' \
          group by RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE({Geog_Org_Name_Column}, 'Not Known'), COALESCE({Geog_Provider_Name}, 'Not Known')) as den \
        ON COALESCE(num.OrgName, 'Not Known') = COALESCE(den.OrgName, 'Not Known') and COALESCE(num.OrgCodeProvider, 'Not Known') = COALESCE(den.OrgCodeProvider, 'Not Known')").format( 
          outSchema=outSchema,Geog_Org_Level=Geog_Org_Level,Geog_Org_Name_Column=Geog_Org_Name_Column,Geog_Provider_Name=Geog_Provider_Name_Column, CountOf=CountOf, Indicator=Indicator \
        )
    print(geographiesAggregationStatement)
    spark.sql(geographiesAggregationStatement)

# COMMAND ----------

# DBTITLE 1,Add national
# should change num.* to den.* to handle the cases of there being no numerator (otherwise not included in output). Coalesce to 0 on num.Total and den.Total
for key, value in SLBMeasures.items():
  Indicator = value["Indicator"]
  CountOf = value["CountOf"]
  nationalAggregationStatement = ("INSERT INTO {outSchema}.SLB_Geography_Aggregated \
    SELECT den.RPStartDate \
        , den.RPEndDate \
        , den.Indicator \
        , den.IndicatorFamily \
        , 'National' AS OrgCodeProvider \
        , 'All Submitters' AS OrgName \
        , 'National' AS OrgLevel \
        , num.Total as Unrounded_Numerator \
        , den.Total as Unrounded_Denominator \
        , case when num.Total > 7 then round((num.Total/den.Total)*100, 1) else 0 end as Unrounded_Rate \
        , case when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end as Rounded_Numerator \
        , case when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end as Rounded_Denominator \
        , round((case when num.Total = 0 then 0 when num.Total between 1 and 7 then 5 when num.Total > 7 then round(num.Total/5,0)*5 end / \
             case when den.Total = 0 then 0 when den.Total between 1 and 7 then 5 when den.Total > 7 then round(den.Total/5,0)*5 end)*100, 1) as Rounded_Rate \
        , current_timestamp() as CreatedAt \
        FROM (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, count(DISTINCT {CountOf}) as Total from {outSchema}.SLB_Numerator_Raw \
          group by RPStartDate, RPEndDate, Indicator, IndicatorFamily) AS num \
        RIGHT JOIN (select RPStartDate, RPEndDate, Indicator, IndicatorFamily, count(DISTINCT {CountOf}) as Total from {outSchema}.SLB_Denominator_Raw group by RPStartDate, RPEndDate, Indicator, IndicatorFamily) as den \
        ON num.Indicator = den.Indicator \
        and den.Indicator='{Indicator}' \
        where den.Indicator='{Indicator}'").format(outSchema=outSchema, CountOf=CountOf, Indicator=Indicator)
  print(nationalAggregationStatement)
  spark.sql(nationalAggregationStatement)

# COMMAND ----------

# DBTITLE 1,Insert CSV formatted output into SLB_CSV
# 6 statements to populate CSV output table
# Default date format is CCYY-MM-DD which is consistent with TOS
# If decimal part of the number is 0 then strip decimal part, consistent with saving a csv in Excel
for key, value in SLBMeasures.items():
  Indicator = value["Indicator"]
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Denominator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Denominator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Provider_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Numerator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Numerator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Provider_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Rate' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Rate, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Provider_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Denominator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Denominator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Geography_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Numerator' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Numerator, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Geography_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)
  csvStatement = ("INSERT INTO {outSchema}.SLB_CSV \
    SELECT RPStartDate, RPEndDate, Indicator, IndicatorFamily, COALESCE(OrgCodeProvider, 'Not Known'), COALESCE(OrgName, 'Not Known'), OrgLevel, 'Rate' AS Currency, \
    COALESCE(REPLACE(CAST (ROUND(Rounded_Rate, 1) AS STRING), '.0', ''), '0') AS Value, current_timestamp() as CreatedAt \
    FROM {outSchema}.SLB_Geography_Aggregated \
    WHERE Indicator = '{Indicator}'").format(outSchema=outSchema, Indicator=Indicator)
  spark.sql(csvStatement)

# COMMAND ----------

dbutils.notebook.exit("Notebook: SLB ran successfully")