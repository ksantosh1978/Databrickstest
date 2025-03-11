# Databricks notebook source
# MAGIC %python
# MAGIC #dbutils.widgets.removeAll()
# MAGIC

# COMMAND ----------

# DBTITLE 1,Widgets
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The physical tables used here are created in the `create_intermediate_tables` notebook

# COMMAND ----------

# DBTITLE 1,Mother/Baby/Site Geogs table
# MAGIC %sql
# MAGIC --GENERATES TABLE OF MOTHER DETAILS (CCG AND LAD), BOOKING DETAILS (APPOINTMENT SITE NAME) AND BABY DETAILS (BIRTH SITE NAME)
# MAGIC
# MAGIC truncate table $outSchema.motherbabybooking_geog; 
# MAGIC
# MAGIC with GP as (
# MAGIC select distinct msd002.recordnumber,
# MAGIC --Since July, all CCG are referred as SubICB, so picking the corresponding field. Maintaining the column name as SLB tables have same name and to avoid any breaks.
# MAGIC  CASE
# MAGIC   WHEN $month_id <= 1467  then msd002.ccgresponsibilitymother
# MAGIC   ELSE msd002.OrgIDSubICBLocGP
# MAGIC   END as ccgresponsibilitymother
# MAGIC                            
# MAGIC from $dbSchema.msd002gp as msd002
# MAGIC where RPEndDate = '$RPEnddate'
# MAGIC and ( EndDateGMPReg is null
# MAGIC   or (StartDateGMPReg is null and EndDateGMPReg > '$RPEnddate')
# MAGIC   or (StartDateGMPReg < '$RPEnddate' and EndDateGMPReg > '$RPEnddate') 
# MAGIC   )
# MAGIC )
# MAGIC
# MAGIC ,ORG as (
# MAGIC select distinct
# MAGIC org.org_code,
# MAGIC org.name,
# MAGIC org.short_name
# MAGIC from dss_corporate.org_daily as org
# MAGIC where (business_start_date<='$RPEnddate' and (org_close_date >'$RPEnddate' or business_end_date >'$RPEnddate') or 
# MAGIC       (business_start_date<='$RPEnddate' and business_end_date is null))
# MAGIC )
# MAGIC
# MAGIC ,LAD as (
# MAGIC select distinct
# MAGIC ladv2.LADNM, ladv2.LADCD
# MAGIC from dss_corporate.ons_lsoa_ccg_lad_v02 as ladv2
# MAGIC where ladv2.dss_record_start_date <= '$RPEnddate' and (ladv2.dss_record_end_date is null or ladv2.dss_record_end_date >= '$RPEnddate') 
# MAGIC )
# MAGIC
# MAGIC ,CCG as (
# MAGIC select * from GP
# MAGIC left join Org
# MAGIC   on GP.ccgresponsibilitymother = Org.org_code
# MAGIC )
# MAGIC
# MAGIC ,motherDetails as (
# MAGIC select
# MAGIC msd001.Person_ID_Mother, msd001.orgcodeprovider,
# MAGIC msd001.UniqSubmissionID, msd001.recordnumber,
# MAGIC msd001.RPEndDate, ccg.CCGResponsibilityMother,
# MAGIC ccg.name as CCG_Name, ccg.Short_Name as CCG_Name_Short,
# MAGIC msd001.LAD_UAMother, LAD.LADNM as LAD_Name
# MAGIC from $dbSchema.msd001motherdemog as msd001
# MAGIC   left join CCG
# MAGIC     on msd001.RecordNumber = CCG.RecordNumber
# MAGIC    left join LAD
# MAGIC      on msd001.LAD_UAMother = LAD.ladcd
# MAGIC     where msd001.RPEndDate = '$RPEnddate' 
# MAGIC  )
# MAGIC
# MAGIC ,bookingDetails as (
# MAGIC select
# MAGIC msd101.Person_ID_Mother, msd101.UniqPregID,
# MAGIC msd101.UniqSubmissionID, msd101.RecordNumber,
# MAGIC msd101.RPEndDate, msd101.OrgCodePRovider,
# MAGIC msd101.OrgSiteIDBooking, org.name as OrgSiteNameBooking
# MAGIC from $dbSchema.msd101pregnancybooking as msd101
# MAGIC   left join org
# MAGIC     on msd101.Orgsiteidbooking = org.org_code
# MAGIC    where msd101.RPEndDate = '$RPEnddate' 
# MAGIC  )
# MAGIC
# MAGIC ,babyDetails as (
# MAGIC select
# MAGIC msd401.Person_ID_Baby, msd401.Person_ID_Mother,
# MAGIC msd401.UniqSubmissionID, msd401.RecordNumber,
# MAGIC msd401.PersonBirthDateBaby, msd401.RPEndDate,
# MAGIC msd401.OrgSiteIDActualDelivery, org.name as OrgSiteNameActualDelivery
# MAGIC from $dbSchema.msd401babydemographics as msd401
# MAGIC   left join org
# MAGIC     on msd401.orgsiteidactualdelivery = org.org_code
# MAGIC   where msd401.RPEndDate = '$RPEnddate' 
# MAGIC )
# MAGIC
# MAGIC insert into $outSchema.motherbabybooking_geog
# MAGIC
# MAGIC select distinct
# MAGIC 'MotherDetails' as Record_Type, 'CCG' as Sub_Type,
# MAGIC md.Person_ID_Mother as Person_ID_Mother, '' as Person_ID_Baby,
# MAGIC '' as UniqPregID, md.RecordNumber as RecordNumber,
# MAGIC md.RPEndDate as RPEndDate, coalesce(md.ccgresponsibilitymother, 'Unknown') as valuecode,
# MAGIC coalesce(md.ccg_name, 'Unknown') as valuename,
# MAGIC md.UniqSubmissionID
# MAGIC from motherDetails as md
# MAGIC
# MAGIC union all
# MAGIC select distinct
# MAGIC 'MotherDetails' as Record_Type, 'LAD' as Sub_Type,
# MAGIC md.Person_ID_Mother as Person_ID_Mother, '' as Person_ID_Baby,
# MAGIC '' as UniqPregID, md.RecordNumber as RecordNumber,
# MAGIC md.RPEndDate as RPEndDate, coalesce(md.LAD_UAMother, 'Unknown') as valuecode,
# MAGIC
# MAGIC case when md.LAD_UAMother ='ZZ201' then 'Home' 
# MAGIC     when md.LAD_UAMother ='ZZ888' then 'Non-NHS Organisation' 
# MAGIC     when md.LAD_UAMother ='ZZ203' then 'Not Known (not recorded)' 
# MAGIC     when md.LAD_UAMother ='ZZ999' then 'Other' 
# MAGIC     when md.LAD_UAMother is null then 'LOCAL AUTHORITY UNKNOWN'
# MAGIC     when md.LAD_Name is null then 'LOCAL AUTHORITY UNKNOWN' 
# MAGIC else md.LAD_name end as valuename,
# MAGIC
# MAGIC md.UniqSubmissionID
# MAGIC from motherDetails as md
# MAGIC
# MAGIC  
# MAGIC union all
# MAGIC select distinct
# MAGIC 'BookingDetails' as Record_Type, 'Booking_Site' as Sub_Type,
# MAGIC bd.Person_ID_Mother as Person_ID_Mother, '' as Person_ID_Baby,
# MAGIC bd.uniqpregid as UniqPregID, bd.recordnumber as RecordNumber,
# MAGIC bd.RPendDate as RPEndDate, coalesce(bd.orgsiteidbooking, 'Unknown') as valuecode,
# MAGIC
# MAGIC case when bd.orgsiteidbooking ='ZZ201' then 'Home' 
# MAGIC     when bd.orgsiteidbooking ='ZZ888' then 'Non-NHS Organisation' 
# MAGIC     when bd.orgsiteidbooking ='ZZ203' then 'Not Known (not recorded)' 
# MAGIC     when bd.orgsiteidbooking ='ZZ999' then 'Other' 
# MAGIC     when bd.orgsiteidbooking is null then 'BOOKING SITE UNKNOWN'
# MAGIC     when bd.orgsitenamebooking is null then 'BOOKING SITE UNKNOWN' 
# MAGIC else bd.orgsitenamebooking end as valuename,
# MAGIC
# MAGIC bd.UniqSubmissionID
# MAGIC from bookingDetails as bd
# MAGIC
# MAGIC union all
# MAGIC select distinct
# MAGIC 'BabyDetails' as Record_Type, 'Baby_Site' as Sub_Type,
# MAGIC bd.person_id_mother as Person_ID_Mother, bd.Person_ID_Baby as Person_ID_Baby,
# MAGIC '' as UniqPregID, bd.RecordNumber as RecordNumber,
# MAGIC bd.rpenddate as RPEndDate, coalesce(bd.orgsiteidactualdelivery, 'Unknown') as valuecode,
# MAGIC
# MAGIC case when bd.orgsiteidactualdelivery ='ZZ201' then 'Home' 
# MAGIC     when bd.orgsiteidactualdelivery ='ZZ888' then 'Non-NHS Organisation' 
# MAGIC     when bd.orgsiteidactualdelivery ='ZZ203' then 'Not Known (not recorded)' 
# MAGIC     when bd.orgsiteidactualdelivery ='ZZ777' then 'In Transit' 
# MAGIC     when bd.orgsiteidactualdelivery is null then 'DELIVERY SITE UNKNOWN'
# MAGIC     when bd.orgsitenameactualdelivery is null then 'DELIVERY SITE UNKNOWN' 
# MAGIC else bd.orgsitenameactualdelivery end as valuename,
# MAGIC
# MAGIC bd.UniqSubmissionID
# MAGIC from babyDetails as bd
# MAGIC
# MAGIC order by 3,6,5,2
# MAGIC

# COMMAND ----------

# DBTITLE 1,Booking base table - SQL
# MAGIC %sql 
# MAGIC
# MAGIC --#TODO: review description of the function of this code
# MAGIC
# MAGIC -- Creates a view on the msds source data tables and custom geography tables. 
# MAGIC -- View contains new record-level derivations to allow aggregation of measures at multiple levels in next steps of processing. 
# MAGIC -- Forms the base table for the set of Maternity Booking measures
# MAGIC
# MAGIC
# MAGIC truncate table $outSchema.BookingsBaseTable;  --#TODO: make this a delete from where match RPEnddate (or a drop partition?)
# MAGIC
# MAGIC insert into $outSchema.BookingsBaseTable 
# MAGIC
# MAGIC
# MAGIC select 
# MAGIC   '$RPEnddate' as ReportingPeriodEndDate,
# MAGIC   msd101.Person_ID_Mother,
# MAGIC   '' as TotalBookings,  -- placeholder needed to force one single group for this measure
# MAGIC   AgeAtBookingMother,
# MAGIC   '' as AgeAtBookingMotherAvg,  -- placeholder needed to force one single group for this measure
# MAGIC   case 
# MAGIC     when AgeAtBookingMother is null then 'Missing Value'
# MAGIC     when AgeAtBookingMother between 11 and 19 then 'Under 20'
# MAGIC     when AgeAtBookingMother between 20 and 24 then '20 to 24'
# MAGIC     when AgeAtBookingMother between 25 and 29 then '25 to 29'
# MAGIC     when AgeAtBookingMother between 30 and 34 then '30 to 34'
# MAGIC     when AgeAtBookingMother between 35 and 39 then '35 to 39'
# MAGIC     when AgeAtBookingMother between 40 and 44 then '40 to 44'
# MAGIC     when AgeAtBookingMother between 45 and 60 then '45 or Over'
# MAGIC     else 'Value outside reporting parameters'
# MAGIC   end
# MAGIC     as AgeAtBookingMotherGroup,
# MAGIC  case
# MAGIC     when PersonBMIBand is null then "Missing Value"    
# MAGIC     when PersonBMIBand = 'Less than 18.5' then "Underweight"
# MAGIC     when PersonBMIBand = '18.5 - 24.9' then "Normal"
# MAGIC     when PersonBMIBand = '25 - 29.9' then "Overweight"
# MAGIC     when PersonBMIBand in ('30 - 39.9','40 or more') then "Obese"
# MAGIC     else "Value outside reporting parameters"
# MAGIC   end
# MAGIC    as BMI,
# MAGIC   case 
# MAGIC    when ComplexSocialFactorsInd is NULL then 'Missing Value'
# MAGIC    when upper(ComplexSocialFactorsInd) ='Y' then 'Y'
# MAGIC    when upper(ComplexSocialFactorsInd) ='N' then 'N'
# MAGIC    else 'Value outside reporting parameters'
# MAGIC   end
# MAGIC    as ComplexSocialFactorsInd,  
# MAGIC  case
# MAGIC    when Rank_IMD_Decile_2015 is not null then Rank_IMD_Decile_2015
# MAGIC    when Rank_IMD_Decile_2015 is null and LSOAMother2011 is not null then "Resident Elsewhere in UK, Channel Islands or Isle of Man"
# MAGIC    when Rank_IMD_Decile_2015 is null and PostcodeDistrictMother = "ZZ99" then "Pseudo postcode recorded (includes no fixed abode or resident overseas)"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC     as DeprivationDecileAtBooking,
# MAGIC   case
# MAGIC    when upper(EthnicCategoryMother) = "A" then "White" 
# MAGIC    when upper(EthnicCategoryMother) = "B" then "White" 
# MAGIC    when upper(EthnicCategoryMother) = "C" then "White" 
# MAGIC    when upper(EthnicCategoryMother) = "D" then "Mixed" 
# MAGIC    when upper(EthnicCategoryMother) = "E" then "Mixed" 
# MAGIC    when upper(EthnicCategoryMother) = "F" then "Mixed" 
# MAGIC    when upper(EthnicCategoryMother) = "G" then "Mixed" 
# MAGIC    when upper(EthnicCategoryMother) = "H" then "Asian or Asian British" 
# MAGIC    when upper(EthnicCategoryMother) = "J" then "Asian or Asian British" 
# MAGIC    when upper(EthnicCategoryMother) = "K" then "Asian or Asian British" 
# MAGIC    when upper(EthnicCategoryMother) = "L" then "Asian or Asian British" 
# MAGIC    when upper(EthnicCategoryMother) = "M" then "Black or Black British" 
# MAGIC    when upper(EthnicCategoryMother) = "N" then "Black or Black British" 
# MAGIC    when upper(EthnicCategoryMother) = "P" then "Black or Black British" 
# MAGIC    when upper(EthnicCategoryMother) = "R" then "Any other ethnic group" 
# MAGIC    when upper(EthnicCategoryMother) = "S" then "Any other ethnic group" 
# MAGIC    when upper(EthnicCategoryMother) = "Z" then "Not Stated" 
# MAGIC    when EthnicCategoryMother = "99" then "Not known" 
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end 
# MAGIC    as EthnicCategoryMotherGroup,
# MAGIC   case
# MAGIC    when GestAgeBooking between 0 and 70 then "0 to 70 days"
# MAGIC    when GestAgeBooking between 71 and 90 then "71 to 90 days"
# MAGIC    when GestAgeBooking between 91 and 140 then "91 to 140 days"
# MAGIC    when GestAgeBooking between 141 and 999 then "141+ days"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC    as GestAgeFormalAntenatalBookingGroup,
# MAGIC   case 
# MAGIC    when COMonReading = 0 then "0 ppm"
# MAGIC    when COMonReading >0 and COMonReading < 4 then "Between 0 and 4 ppm"
# MAGIC    when COMonReading >= 4 then "4 and over ppm"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end 
# MAGIC    as CO_Concentration_Booking,
# MAGIC   case
# MAGIC     when SmokingStatusBooking_derived is not null then SmokingStatusBooking_derived
# MAGIC     else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC     as SmokingStatusGroupBooking,
# MAGIC   null as SmokingStatusGroupDelivery,
# MAGIC --   case
# MAGIC --     when SmokingStatusDelivery_derived is not null then SmokingStatusDelivery_derived
# MAGIC --     else "Missing Value / Value outside reporting parameters"
# MAGIC --   end
# MAGIC --     as SmokingStatusGroupDelivery,
# MAGIC   case
# MAGIC    when FolicAcidSupplement = '01' then "Has been taking prior to becoming pregnant"
# MAGIC    when FolicAcidSupplement = '02' then "Started taking once pregnancy confirmed"
# MAGIC    when FolicAcidSupplement = '03' then "Not taking folic acid supplement"
# MAGIC    when UPPER (FolicAcidSupplement) = 'ZZ' then "Not Stated (Person asked but declined to provide a response)"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC    as FolicAcidSupplement,
# MAGIC   AlcoholUnitsPerWeekBand,
# MAGIC   case
# MAGIC    when PreviousCaesareanSections >= 1 then "At least one Caesarean"
# MAGIC    when (PreviousLiveBirths > 0 or PreviousStillBirths > 0) and PreviousCaesareanSections = 0 then "At least one Previous Birth, zero Caesareans"
# MAGIC    when ifnull(PreviousCaesareanSections,0) = 0 and ifnull(PreviousLiveBirths,0) = 0 and ifnull(PreviousStillBirths,0) = 0 then "Zero Previous Births"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC    as PreviousCaesareanSectionsGroup,
# MAGIC  case
# MAGIC   when PreviousLiveBirths between 1 and 4 then PreviousLiveBirths
# MAGIC   when PreviousLiveBirths >= 5 then "5+"
# MAGIC   when PreviousLiveBirths = 0 then "No previous live births"
# MAGIC   else "Missing Value / Value outside reporting parameters"
# MAGIC  end
# MAGIC   as PreviousLiveBirthsGroup,
# MAGIC   geo.*,
# MAGIC   mbb.valuecode as BookingSiteCode, --NEW
# MAGIC   mbb.valuename as BookingSiteName, --NEW
# MAGIC   mbb_CCG.valuecode as Mother_CCG_Code, --NEW
# MAGIC   mbb_CCG.valuename as Mother_CCG, --NEW
# MAGIC   mbb_LAD.valuecode as Mother_LAD_Code, --NEW
# MAGIC   mbb_LAD.valuename as Mother_LAD --NEW
# MAGIC from $outSchema.MostRecentSubmissions MSD000 
# MAGIC join global_temp.MSD101PregnancyBooking_with_smokingstatusbooking_derived msd101 on MSD000.UniqSubmissionID = msd101.UniqSubmissionID
# MAGIC left join $dbSchema.msd001motherdemog msd001 on msd101.UniqSubmissionID = msd001.UniqSubmissionID and msd101.Person_ID_Mother = msd001.Person_ID_Mother 
# MAGIC left join $dbSchema.msd201carecontactpreg msd201 on msd101.UniqSubmissionID = msd201.UniqSubmissionID and msd101.UniqPregID = msd201.UniqPregID and msd101.AntenatalAppDate = msd201.CContactDate
# MAGIC left join $dbSchema.msd202careactivitypreg msd202 on msd201.UniqSubmissionID = msd202.UniqSubmissionID and msd201.CareConID = msd202.CareConID 
# MAGIC left join $outSchema.geogtlrr geo on MSD000.OrgCodeProvider = geo.Trust_ORG
# MAGIC
# MAGIC left join $outSchema.motherbabybooking_geog mbb on msd101.person_id_mother = mbb.person_id_mother and mbb.Record_Type="BookingDetails" 
# MAGIC and mbb.uniqpregid=msd101.uniqpregid 
# MAGIC and mbb.UniqSubmissionID=msd001.UniqSubmissionID --NEW
# MAGIC left join $outSchema.motherbabybooking_geog mbb_CCG on msd101.person_id_mother = mbb_CCG.person_id_mother and mbb_CCG.Record_Type="MotherDetails" and mbb_CCG.sub_type="CCG"
# MAGIC and mbb_CCG.UniqSubmissionID=msd001.UniqSubmissionID --NEW
# MAGIC left join $outSchema.motherbabybooking_geog mbb_LAD on msd101.person_id_mother = mbb_LAD.person_id_mother and mbb_LAD.Record_Type="MotherDetails" and mbb_LAD.sub_type="LAD" 
# MAGIC and mbb_LAD.UniqSubmissionID=msd001.UniqSubmissionID --NEW
# MAGIC
# MAGIC where  msd101.RPStartDate = '$RPStartdate' and  
# MAGIC        msd101.AntenatalAppDate between '$RPStartdate' and '$RPEnddate' and 
# MAGIC        msd101.Person_ID_Mother = msd001.Person_ID_Mother 
# MAGIC
# MAGIC -- #TODO: abstract the joined tables into a separate view? Makes this code cleaner and allows testing in isolation.

# COMMAND ----------

# DBTITLE 1,Robson Group View for use in Births Table - SQL
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create or replace view $outSchema.RobsonGroupTable
# MAGIC
# MAGIC as
# MAGIC
# MAGIC --GROUP 1
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 1 - Nulliparous, single cephalic, >= 37 weeks, spontaneous labour' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY     AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID    = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD302CAREACTIVITYLABDEL AS MSD302 ON MSD301.LabourDeliveryID = MSD302.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID    = MSD302.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS   AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID    = MSD401.UniqSubmissionID
# MAGIC WHERE         MSD101.PREVIOUSLIVEBIRTHS = 0
# MAGIC          AND  MSD401.FETUSPRESENTATION  = '01' 
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH  BETWEEN 259 AND 315
# MAGIC          AND  MSD301.LABOURONSETDATE       IS NOT NULL
# MAGIC          --AND (MSD302.ROMREASON        <> '01'  OR MSD302.ROMREASON IS NULL)
# MAGIC          AND (MSD302.MASTERSNOMEDCTPROCEDURECODE NOT IN 
# MAGIC          ('408818004','177129005','288191008','288190009','288189000','31208007','177135005','177136006','236971007','308037008','236958009')
# MAGIC          OR   MSD302.MASTERSNOMEDCTPROCEDURECODE IS NULL)
# MAGIC          AND (MSD301.CAESAREANDATE      > MSD301.LABOURONSETDATE
# MAGIC          OR   MSD301.CAESAREANDATE         IS NULL)
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC
# MAGIC --Find the mothers from above where they have LABOURONSETMETHOD = '1' at any point
# MAGIC          AND  MSD301.LabourDeliveryID   IN
# MAGIC         (
# MAGIC SELECT 
# MAGIC DISTINCT      MSD301.LabourDeliveryID 
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY AS MSD301
# MAGIC WHERE         MSD301.LABOURONSETMETHOD = '1'
# MAGIC          )
# MAGIC --Find the mothers from above where they do not have LABOURONSETMETHOD in ('2','3','4','5') at any point
# MAGIC          AND  MSD301.LabourDeliveryID   NOT IN
# MAGIC         (
# MAGIC SELECT 
# MAGIC DISTINCT      MSD301.LabourDeliveryID 
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY AS MSD301
# MAGIC WHERE         MSD301.LABOURONSETMETHOD  IN ('2','3','4','5')
# MAGIC          )
# MAGIC -- END GROUP 1
# MAGIC UNION ALL
# MAGIC -- GROUP 2
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 2 - Nulliparous, single cephalic, >= 37 weeks, induced or caesarean before labour' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY     AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING   AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC           AND MSD301.UniqSubmissionID        = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD302CAREACTIVITYLABDEL AS MSD302 ON MSD301.LabourDeliveryID = MSD302.LabourDeliveryID 
# MAGIC           AND MSD301.UniqSubmissionID        = MSD302.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS   AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC           AND MSD301.UniqSubmissionID        = MSD401.UniqSubmissionID
# MAGIC WHERE         MSD101.PREVIOUSLIVEBIRTHS     = 0
# MAGIC           AND MSD401.FETUSPRESENTATION      = '01' 
# MAGIC           AND MSD401.GESTATIONLENGTHBIRTH   BETWEEN 259 AND 315
# MAGIC           AND((MSD302.MASTERSNOMEDCTPROCEDURECODE       IN              ('408818004','177129005','288191008','288190009','288189000','31208007','177135005','177136006','236971007','308037008','236958009') 
# MAGIC           OR  MSD301.LABOURONSETMETHOD      IN ('2','3','4','5'))
# MAGIC          --AND((MSD302.ROMREASON = '01'       OR MSD301.LABOURONSETMETHOD IN ('2','3','4','5'))
# MAGIC           OR (MSD301.CAESAREANDATE          IS NOT NULL 
# MAGIC           AND MSD301.LABOURONSETDATE        IS NULL ))
# MAGIC           AND MSD301.RPStartDate = '$RPStartdate'
# MAGIC -- END GROUP 2
# MAGIC UNION ALL
# MAGIC --GROUP 3  
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 3 - Multiparous (excluding previous caesareans), single cephalic, >= 37 weeks, spontaneous labour' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY     AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING   AS MSD101   ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID           = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD302CAREACTIVITYLABDEL AS   MSD302 ON MSD301.LabourDeliveryID = MSD302.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID           = MSD302.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS   AS   MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID           = MSD401.UniqSubmissionID
# MAGIC WHERE        (MSD101.PREVIOUSLIVEBIRTHS        > 0      
# MAGIC          AND  MSD101.PREVIOUSLIVEBIRTHS <=40)
# MAGIC          AND  MSD101.PREVIOUSCAESAREANSECTIONS = 0 
# MAGIC          AND  MSD401.FETUSPRESENTATION         = '01' 
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH   BETWEEN 259 AND 315
# MAGIC          AND  MSD301.LABOURONSETDATE        IS NOT NULL
# MAGIC          --AND (MSD302.ROMREASON               <> '01'       OR MSD302.ROMREASON IS NULL)
# MAGIC          AND (MSD302.MASTERSNOMEDCTPROCEDURECODE        NOT IN 
# MAGIC          ('408818004','177129005','288191008','288190009','288189000','31208007','177135005','177136006','236971007','308037008','236958009')
# MAGIC          OR   MSD302.MASTERSNOMEDCTPROCEDURECODE        IS NULL)
# MAGIC          AND (MSD301.CAESAREANDATE             > MSD301.LABOURONSETDATE
# MAGIC          OR   MSD301.CAESAREANDATE                      IS NULL)
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC          
# MAGIC --Find the mothers from above where they have LABOURONSETMETHOD = '1' at any point
# MAGIC          AND  MSD301.LabourDeliveryID   IN
# MAGIC         (
# MAGIC SELECT 
# MAGIC DISTINCT      MSD301.LabourDeliveryID
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY AS MSD301 
# MAGIC WHERE         MSD301.LABOURONSETMETHOD  = '1'
# MAGIC          )
# MAGIC --Find the mothers from above where they do not have LABOURONSETMETHOD in ('2','3','4','5') at any point
# MAGIC          AND  MSD301.LabourDeliveryID   NOT IN
# MAGIC         (
# MAGIC SELECT 
# MAGIC DISTINCT      MSD301.LabourDeliveryID 
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY AS MSD301
# MAGIC WHERE         MSD301.LABOURONSETMETHOD  IN ('2','3','4','5')
# MAGIC          )
# MAGIC --END GROUP 3  
# MAGIC  UNION ALL
# MAGIC --GROUP 4  
# MAGIC SELECT         distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC     'Group 4 - Multiparous (excluding previous caesareans), single cephalic, >= 37 weeks, induced or caesarean before labour' AS RobsonGroup
# MAGIC FROM           $dbSchema.MSD301LABOURDELIVERY     AS MSD301
# MAGIC INNER JOIN     $dbSchema.MSD101PREGNANCYBOOKING   AS MSD101   ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC           AND  MSD301.UniqSubmissionID           = MSD101.UniqSubmissionID
# MAGIC INNER JOIN     $dbSchema.MSD302CAREACTIVITYLABDEL AS MSD302   ON MSD301.LabourDeliveryID = MSD302.LabourDeliveryID 
# MAGIC           AND  MSD301.UniqSubmissionID           = MSD302.UniqSubmissionID
# MAGIC INNER JOIN     $dbSchema.MSD401BABYDEMOGRAPHICS   AS MSD401   ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC           AND  MSD301.UniqSubmissionID           = MSD401.UniqSubmissionID
# MAGIC WHERE         (MSD101.PREVIOUSLIVEBIRTHS        > 0      AND MSD101.PREVIOUSLIVEBIRTHS <= 40)
# MAGIC           AND  MSD101.PREVIOUSCAESAREANSECTIONS = 0 
# MAGIC           AND  MSD401.FETUSPRESENTATION         = '01' 
# MAGIC           AND  MSD401.GESTATIONLENGTHBIRTH   BETWEEN 259 AND 315
# MAGIC          -- AND((MSD302.ROMREASON = '01'         OR MSD301.LABOURONSETMETHOD IN ('2','3','4','5'))
# MAGIC           AND((MSD302.MASTERSNOMEDCTPROCEDURECODE       IN              ('408818004','177129005','288191008','288190009','288189000','31208007','177135005','177136006','236971007','308037008','236958009') 
# MAGIC           OR   MSD301.LABOURONSETMETHOD      IN ('2','3','4','5'))
# MAGIC           OR  (MSD301.CAESAREANDATE          IS NOT NULL AND MSD301.LABOURONSETDATE IS NULL))
# MAGIC           AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC              
# MAGIC  --END GROUP 4  
# MAGIC  UNION ALL
# MAGIC  --GROUP 5 
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 5 - Previous caesarean, single cephalic >= 37 weeks' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY   AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID           = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID           = MSD401.UniqSubmissionID
# MAGIC WHERE        (MSD101.PREVIOUSLIVEBIRTHS        >  0 AND MSD101.PREVIOUSLIVEBIRTHS <= 40)
# MAGIC          AND (MSD101.PREVIOUSCAESAREANSECTIONS >= 1 AND MSD101.PREVIOUSCAESAREANSECTIONS <= 20)
# MAGIC          AND  MSD101.PREVIOUSCAESAREANSECTIONS <=       MSD101.PREVIOUSLIVEBIRTHS
# MAGIC          AND  MSD401.FETUSPRESENTATION          = '01' 
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH BETWEEN 259 AND 315
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC --END GROUP 5
# MAGIC UNION ALL
# MAGIC --GROUP 6           
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 6 - All nulliparous breeches' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY   AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD401.UniqSubmissionID
# MAGIC WHERE         MSD101.PREVIOUSLIVEBIRTHS   = 0
# MAGIC          AND  MSD401.FETUSPRESENTATION    = '02' 
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH IS NOT NULL
# MAGIC          AND  MSD301.LABOURONSETMETHOD    IS NOT NULL
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC --END GROUP 6
# MAGIC UNION ALL
# MAGIC --GROUP 7              
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 7 - All multiparous breeches (including previous caesareans)' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY   AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD401.UniqSubmissionID
# MAGIC WHERE        (MSD101.PREVIOUSLIVEBIRTHS   > 0       AND MSD101.PREVIOUSLIVEBIRTHS <= 40)
# MAGIC          AND  MSD401.FETUSPRESENTATION    = '02' 
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH           IS NOT NULL 
# MAGIC          AND  MSD101.PREVIOUSCAESAREANSECTIONS      IS NOT NULL
# MAGIC          AND  MSD301.LABOURONSETMETHOD              IS NOT NULL
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC --END GROUP 7 
# MAGIC UNION ALL
# MAGIC --GROUP 9             
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 9 - All abnormal lies (including previous caesareans)' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY   AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING AS MSD101 ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS AS MSD401 ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD401.UniqSubmissionID
# MAGIC WHERE         MSD101.PREVIOUSLIVEBIRTHS   IS NOT NULL
# MAGIC          AND  MSD401.FETUSPRESENTATION    = '03'
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH IS NOT NULL
# MAGIC          AND  MSD101.PREVIOUSCAESAREANSECTIONS      IS NOT NULL
# MAGIC          AND  MSD301.LABOURONSETMETHOD    IS NOT NULL
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC --END GROUP 9
# MAGIC UNION ALL
# MAGIC --GROUP 10
# MAGIC SELECT        distinct MSD301.UniqSubmissionID, MSD301.LabourDeliveryID,
# MAGIC               'Group 10 - All single cephalic, <= 36 weeks (including previous caesareans)' AS RobsonGroup
# MAGIC FROM          $dbSchema.MSD301LABOURDELIVERY   AS MSD301
# MAGIC INNER JOIN    $dbSchema.MSD101PREGNANCYBOOKING AS MSD101   ON MSD301.UniqPregID = MSD101.UniqPregID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD101.UniqSubmissionID
# MAGIC INNER JOIN    $dbSchema.MSD401BABYDEMOGRAPHICS AS MSD401   ON MSD301.LabourDeliveryID = MSD401.LabourDeliveryID 
# MAGIC          AND  MSD301.UniqSubmissionID      = MSD401.UniqSubmissionID
# MAGIC WHERE         MSD101.PREVIOUSLIVEBIRTHS   IS NOT NULL
# MAGIC          AND  MSD401.FETUSPRESENTATION    = '01'
# MAGIC          AND  MSD401.GESTATIONLENGTHBIRTH BETWEEN 141 AND 258
# MAGIC          AND  MSD101.PREVIOUSCAESAREANSECTIONS IS NOT NULL
# MAGIC          AND  MSD301.LABOURONSETMETHOD    IS NOT NULL
# MAGIC          AND  MSD301.RPStartDate = '$RPStartdate'
# MAGIC --End Group 10

# COMMAND ----------

# DBTITLE 1,Births Base Table - SQL
# MAGIC %sql
# MAGIC --#TODO: review description of the function of this code
# MAGIC
# MAGIC --#TODO: BirthsWithoutIntervention (https://db.ref.core.data.digital.nhs.uk/#notebook/141203/command/141206) wants adding in when the snomed has been sorted out - GB is doing this - 25072019
# MAGIC
# MAGIC -- Creates a subset of the msds source data tables and custom geography tables. 
# MAGIC -- View contains new record-level derivations to allow aggregation of measures at multiple levels in next steps of processing. 
# MAGIC -- Forms the base table for the set of Maternity Births measures
# MAGIC
# MAGIC -- MHA-3426(26/09/2022) (Apgar Derivation fix, using the global view msd405careactivity_apgar_records)
# MAGIC
# MAGIC
# MAGIC truncate table $outSchema.BirthsBaseTable;  --#TODO: make this a delete from where match RPEnddate (or a drop partition?)
# MAGIC
# MAGIC insert into $outSchema.BirthsBaseTable 
# MAGIC
# MAGIC
# MAGIC --base births table 
# MAGIC   select 
# MAGIC   '$RPEnddate' as ReportingPeriodEndDate,
# MAGIC   MSD401.Person_ID_Baby,
# MAGIC   '' as TotalBabies,  -- placeholder needed to force one single group for this measure
# MAGIC   case 
# MAGIC    when MSD405_apgar.ApgarScore between 0 and 6 and GestationLengthBirth between 259 and 315 then "0 to 6"
# MAGIC    when MSD405_apgar.ApgarScore between 7 and 10 and GestationLengthBirth between 259 and 315 then "7 to 10"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC    as ApgarScore5TermGroup7,
# MAGIC   case 
# MAGIC    when BabyFirstFeedIndCode IN ('01', '02') THEN 'Maternal or Donor Breast Milk'
# MAGIC    when BabyFirstFeedIndCode = '03' THEN 'Not Breast Milk'
# MAGIC    else 'Missing Value / Value outside reporting parameters'
# MAGIC   end
# MAGIC    as BabyFirstFeedBreastMilkStatus,
# MAGIC   case
# MAGIC    when BirthWeight_derived between 1 and 1499 and GestationLengthBirth between 259 and 315 then "Under 1500g"
# MAGIC    when BirthWeight_derived between 1500 and 1999 and GestationLengthBirth between 259 and 315 then "1500g to 1999g"
# MAGIC    when BirthWeight_derived between 2000 and 2499 and GestationLengthBirth between 259 and 315 then "2000g to 2499g"
# MAGIC    when BirthWeight_derived between 2500 and 2999 and GestationLengthBirth between 259 and 315 then "2500g to 2999g"
# MAGIC    when BirthWeight_derived between 3000 and 3499 and GestationLengthBirth between 259 and 315 then "3000g to 3499g"
# MAGIC    when BirthWeight_derived between 3500 and 3999 and GestationLengthBirth between 259 and 315 then "3500g to 3999g"
# MAGIC    when BirthWeight_derived between 4000 and 4999 and GestationLengthBirth between 259 and 315 then "4000g to 4999g"
# MAGIC    when BirthWeight_derived between 5000 and 9998 and GestationLengthBirth between 259 and 315 then "5000g and over"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end 
# MAGIC    as BirthweightTermGroup,
# MAGIC   case 
# MAGIC    when GestationLengthBirth between 141 and 195 then "27 weeks and under"
# MAGIC    when GestationLengthBirth between 196 and 223 then "28 to 31 weeks"
# MAGIC    when GestationLengthBirth between 224 and 237 then "32 to 33 weeks"
# MAGIC    when GestationLengthBirth between 238 and 258 then "34 to 36 weeks"
# MAGIC    when GestationLengthBirth between 259 and 265 then "37 weeks"
# MAGIC    when GestationLengthBirth between 266 and 272 then "38 weeks"
# MAGIC    when GestationLengthBirth between 273 and 279 then "39 weeks"
# MAGIC    when GestationLengthBirth between 280 and 286 then "40 weeks"
# MAGIC    when GestationLengthBirth between 287 and 293 then "41 weeks"
# MAGIC    when GestationLengthBirth between 294 and 300 then "42 weeks"
# MAGIC    when GestationLengthBirth between 301 and 315 then "43 weeks and over"
# MAGIC    else "Missing Value / Value outside reporting parameters"
# MAGIC   end
# MAGIC    as GestationLengthBirth,
# MAGIC   case
# MAGIC    when GestationLengthBirth between 0 and 139 then 'Missing Value / Value outside reporting parameters'
# MAGIC    when GestationLengthBirth between 141 and 258 then '<37 weeks'
# MAGIC    when GestationLengthBirth between 259 and 315 then '>=37 weeks'
# MAGIC    else 'Missing Value / Value outside reporting parameters' 
# MAGIC   end
# MAGIC    as GestationLengthBirthGroup37,
# MAGIC   case
# MAGIC    when SettingPlaceBirth ='01' then 'NHS Obstetric unit (including theatre)'
# MAGIC    when SettingPlaceBirth ='02' then 'NHS Alongside midwifery unit'
# MAGIC    when SettingPlaceBirth ='03' then 'NHS Freestanding midwifery unit (FMU)'
# MAGIC    when SettingPlaceBirth ='04' then 'Home (NHS care)'
# MAGIC    when SettingPlaceBirth ='05' then 'Home (private care)'
# MAGIC    when SettingPlaceBirth ='06' then 'Private hospital'
# MAGIC    when SettingPlaceBirth ='07' then 'Maternity assessment or triage unit/ area'
# MAGIC    when SettingPlaceBirth ='08' then 'NHS ward/health care setting without delivery facilities'
# MAGIC    when SettingPlaceBirth ='09' then 'In transit (with NHS ambulance services)'
# MAGIC    when SettingPlaceBirth ='10' then 'In transit (with private ambulance services)'
# MAGIC    when SettingPlaceBirth ='11' then 'In transit (without healthcare services present)'
# MAGIC    when SettingPlaceBirth ='12' then 'Non-domestic and non-health care setting'
# MAGIC    when SettingPlaceBirth ='98' then 'Other (not listed)'
# MAGIC    when SettingPlaceBirth ='99' then 'Not known (not recorded)'
# MAGIC    else 'Missing Value / Value outside reporting parameters'
# MAGIC   end
# MAGIC    as PlaceTypeActualDeliveryMidwifery,
# MAGIC  case
# MAGIC   when upper(SkinToSkinContact1HourInd) in ('Y', 'N') and GestationLengthBirth between 259 and 315 then upper(SkinToSkinContact1HourInd)
# MAGIC   else 'Missing Value / Value outside reporting parameters'
# MAGIC  end
# MAGIC   as SkinToSkinContact1HourTerm,
# MAGIC   RobsonGroup,
# MAGIC   geo.*,
# MAGIC   mbb.valuecode as DeliverySiteCode,
# MAGIC   mbb.valuename as DeliverySiteName,
# MAGIC   mbb_CCG.valuecode as Mother_CCG_Code,
# MAGIC   mbb_CCG.valuename as Mother_CCG,
# MAGIC   mbb_LAD.valuecode as Mother_LAD_Code,
# MAGIC   mbb_LAD.valuename as Mother_LAD,
# MAGIC   case
# MAGIC     when MSD405.BirthWeight_derived between 1 and 2499 and MSD401.GestationLengthBirth between 259 and 315 then "Under 2500g"
# MAGIC     when MSD405.BirthWeight_derived between 2500 and 9998 and MSD401.GestationLengthBirth between 259 and 315 then "2500g and over"
# MAGIC     else "Missing Value / Value outside reporting parameters"
# MAGIC   end as BirthWeightTermGroup2500,
# MAGIC   -- 1/Feb/21 there are duplicates of MSD401_ID and MSD405_ID in testdata_mat_analysis_mat_pre_clear on Ref but not on Live,
# MAGIC   -- so add tiebreakers after MSD401_ID. The query is still not deterministic but it should provide consistent results for unit testing
# MAGIC   row_number() over (partition by MSD401.Person_ID_Baby order by case when MSD405.Birthweight_derived is null or MSD405.Birthweight_derived = 9999 or MSD405.Birthweight_derived = 9999000 then 0 else 1 end desc, MSD405.CareActIDBaby asc, MSD405.MSD405_ID desc, MSD401.MSD401_ID desc, msd401.RecordNumber desc ,msd401.Rownumber desc ,MSD000.OrgCodeProvider, mbb.RecordNumber desc, msd401.UniqPregID, msd401.RowNumber, msd401.PregOutcome, msd401.NHSNumberBaby, msd401.NHSNumberStatusBaby, msd401.DeliveryMethodCode) as BirthWeightRank -- MSD405_ID, MSD401_ID not in specification, added as a tiebreaker
# MAGIC from $outSchema.MostRecentSubmissions MSD000 
# MAGIC join $dbSchema.msd401babydemographics MSD401 on MSD000.UniqSubmissionID = msd401.UniqSubmissionID 
# MAGIC left join global_temp.msd405careactivitybaby_derived MSD405 on MSD401.UniqSubmissionID = MSD405.UniqSubmissionID and MSD401.Person_ID_Baby = MSD405.Person_ID_Baby
# MAGIC
# MAGIC left join $outSchema.geogtlrr geo on MSD000.OrgCodeProvider = geo.Trust_ORG
# MAGIC left join $outSchema.RobsonGroupTable RG on MSD401.UniqSubmissionID = RG.UniqSubmissionID and MSD401.LabourDeliveryID = RG.LabourDeliveryID
# MAGIC
# MAGIC Left join $outSchema.motherbabybooking_geog as mbb on msd401.person_id_baby = mbb.person_id_baby and mbb.Record_Type="BabyDetails" and mbb.UniqSubmissionID=msd401.UniqSubmissionID
# MAGIC LEFT JOIN $outSchema.motherbabybooking_geog as mbb_ccg on msd401.person_id_mother = mbb_ccg.person_id_mother and mbb_ccg.Record_Type="MotherDetails" and mbb_ccg.sub_type="CCG" and mbb_ccg.UniqSubmissionID=msd401.UniqSubmissionID
# MAGIC LEFT JOIN $outSchema.motherbabybooking_geog as mbb_lad on msd401.person_id_mother = mbb_lad.person_id_mother and mbb_lad.Record_Type="MotherDetails" and mbb_lad.sub_type="LAD" and mbb_lad.UniqSubmissionID=msd401.UniqSubmissionID
# MAGIC LEFT JOIN global_temp.msd405careactivity_apgar_records AS MSD405_apgar ON MSD401.UniqSubmissionID = MSD405_apgar.UniqSubmissionID AND MSD401.Person_ID_Baby = MSD405_apgar.Person_ID_Baby AND MSD401.UniqPregID = MSD405_apgar.UniqPregID
# MAGIC
# MAGIC where MSD401.RPStartDate = '$RPStartdate' and MSD401.PersonBirthDateBaby between '$RPStartdate' and '$RPEnddate'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Delivery Base Table - SQL
# MAGIC %sql
# MAGIC /* Creating a seperate view for GTTL since having it as an inner subquery in the next code causes issue in the newer version of Databricks SQL.
# MAGIC    Creating GTTLGroup measure from the GTTL measure to avoid reapplying the snomed codes. */
# MAGIC create or replace temporary view GTTL
# MAGIC as
# MAGIC select *,
# MAGIC case when GenitalTractTraumaticLesion_Measure = 'No traumatic lesion reported' then 'No traumatic lesion reported'
# MAGIC      else 'At least one traumatic lesion'
# MAGIC       end
# MAGIC        as GenitalTractTraumaticLesionGroup
# MAGIC from
# MAGIC (
# MAGIC select
# MAGIC   msd401.OrgCodeProvider,
# MAGIC   msd401.LabourdeliveryID,
# MAGIC   case
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '249221003' then 1 end) > 0 then 'Labial tear'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '262935001' then 1 end) > 0 then 'Vaginal tear'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '57759005' then 1 end) > 0 then 'Perineal tear - first degree'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '6234006' then 1 end) > 0 then 'Perineal tear - second degree'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '10217006' then 1 end) > 0 then 'Perineal tear - third degree'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '399031001' then 1 end) > 0 then 'Perineal tear - fourth degree'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion IN ('15413009', '17860005',
# MAGIC                                                          '236992007', '25828002',
# MAGIC                                                          '26313002', '288194000',
# MAGIC                                                          '40219000', '85548006') then 1 end) > 0 then 'Episiotomy'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '199972004' then 1 end) > 0 then 'Cervical tear'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '237329006' then 1 end) > 0 then 'Urethral tear'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '1105801000000107' then 1 end) > 0 then 'Clitoral tear'
# MAGIC     when count(case when msd302.GenitalTractTraumaticLesion = '63407004' then 1 end) > 0 then 'Anterior incision'
# MAGIC     when count(case when msd302.MasterSnomedCTProcedureCode = '893721000000103' then 1 end) > 0 then 'Defibulation'
# MAGIC   else 'No traumatic lesion reported'
# MAGIC    end
# MAGIC     as GenitalTractTraumaticLesion_Measure
# MAGIC from $outSchema.MostRecentSubmissions MSD000 -- use instead of single 'from msd302' line above for 'curr' tables or test data
# MAGIC join $dbSchema.msd302CareActivityLabDel msd302 on MSD000.UniqSubmissionID = msd302.UniqSubmissionID -- use instead of single 'from msd302' line above for 'curr' tables or test data
# MAGIC left join $dbSchema.msd401babydemographics msd401 on msd302.UniqSubmissionID = msd401.UniqSubmissionID and msd302.LabourdeliveryID = msd401.LabourdeliveryID
# MAGIC left join $outSchema.geogtlrr geo on msd000.OrgCodeProvider = geo.Trust_ORG
# MAGIC where msd401.RPStartDate = '$RPStartdate'
# MAGIC and msd401.PersonBirthDateBaby between '$RPStartdate' and '$RPEnddate'
# MAGIC group by msd401.OrgCodeProvider, msd401.LabourdeliveryID
# MAGIC having count(case when msd401.Deliverymethodcode IN ('0','1','2','3','4') then 1 end)>0
# MAGIC    and count(case when msd401.Deliverymethodcode IN ('5','6') then 1 end)=0
# MAGIC    )

# COMMAND ----------

# MAGIC %sql
# MAGIC --#TODO: review description of the function of this code
# MAGIC
# MAGIC --#TODO: rewrite the sub-query to look less rubbish (consider 'where in' in case statement)
# MAGIC
# MAGIC -- Creates a view on the msds source data tables and custom geography tables. 
# MAGIC -- View contains new record-level derivations to allow aggregation of measures at multiple levels in next steps of processing. 
# MAGIC -- Forms the base table for the set of Maternity Births measures
# MAGIC
# MAGIC /* The monthly delivery cohort is stored in the DeliveryBaseTable.
# MAGIC Since the GenitalTractTraumaLesion and GenitalTractTraumaLesionGroup measures
# MAGIC filter by conditions not present in other measures of the DeliveryBaseTable,
# MAGIC there are NULL records (because of the left join) against those measures and this goes into the publication
# MAGIC as an empty value under Measure column.
# MAGIC To remove it we are simply ignoring those NULL records in the looper dooper notebook. */
# MAGIC  
# MAGIC truncate table $outSchema.DeliveryBaseTable; --#TODO: make this a delete from where match RPEnddate (or a drop partition?)
# MAGIC insert into $outSchema.DeliveryBaseTable 
# MAGIC
# MAGIC
# MAGIC select 
# MAGIC   '$RPEnddate' as ReportingPeriodEndDate,
# MAGIC   msd401.Person_ID_Mother,
# MAGIC   '' as TotalDeliveries,  -- placeholder needed to force one single group for this measure
# MAGIC   case
# MAGIC    when DeliveryMethodCode in ("0","1","5") then "Spontaneous"
# MAGIC    when DeliveryMethodCode in ("2", "3", "4", "6") then "Instrumental"
# MAGIC    when DeliveryMethodCode = "7" then "Elective caesarean section"
# MAGIC    when DeliveryMethodCode = "8" then "Emergency caesarean section"
# MAGIC    when DeliveryMethodCode = "9" then "Other"
# MAGIC    else "Missing Value / Value outside reporting parameters" 
# MAGIC   end
# MAGIC    as DeliveryMethodBabyGroup,
# MAGIC   GTTL.GenitalTractTraumaticLesion_Measure AS GenitalTractTraumaticLesion,
# MAGIC   GTTL.GenitalTractTraumaticLesionGroup,
# MAGIC   case
# MAGIC    when COMonReading = 0 then '0 ppm'
# MAGIC    when COMonReading >0 and COMonReading < 4 then 'Between 0 and 4 ppm'
# MAGIC    when COMonReading >= 4 then '4 and over ppm'
# MAGIC    else 'Missing Value / Value outside reporting parameters'
# MAGIC   end
# MAGIC    as CO_Concentration_Delivery,
# MAGIC   geo.*,
# MAGIC   mbb.valuecode as DeliverySiteCode, --NEW
# MAGIC   mbb.valuename as DeliverySiteName, --NEW
# MAGIC   mbb_CCG.valuecode as Mother_CCG_Code, --NEW
# MAGIC   mbb_CCG.valuename as Mother_CCG, --NEW
# MAGIC   mbb_LAD.valuecode as Mother_LAD_Code, --NEW
# MAGIC   mbb_LAD.valuename as Mother_LAD --NEW
# MAGIC
# MAGIC from $outSchema.MostRecentSubmissions MSD000 
# MAGIC join $dbSchema.msd301labourdelivery msd301 on MSD000.UniqSubmissionID = msd301.UniqSubmissionID 
# MAGIC left join $dbSchema.msd302careactivitylabdel msd302 on msd301.UniqSubmissionID = msd302.UniqSubmissionID and msd301.LabourDeliveryID = msd302.LabourDeliveryID
# MAGIC left join $dbSchema.msd201carecontactpreg msd201 on msd301.UniqSubmissionID = msd201.UniqSubmissionID and msd301.UniqPregID = msd201.UniqPregID and (msd301.LabourOnsetDate = msd201.CContactDate or msd301.CaesareanDate = msd201.CContactDate)
# MAGIC left join $dbSchema.msd202careactivitypreg msd202 on msd201.UniqSubmissionID = msd202.UniqSubmissionID and msd201.CareConID = msd202.CareConID
# MAGIC left join $dbSchema.msd401babydemographics msd401 on msd301.UniqSubmissionID = msd401.UniqSubmissionID and msd301.LabourDeliveryID = msd401.LabourDeliveryID
# MAGIC left join $outSchema.geogtlrr geo on MSD000.OrgCodeProvider = geo.Trust_ORG
# MAGIC left join GTTL on msd401.OrgCodeProvider = GTTL.OrgCodeProvider and msd401.LabourdeliveryID = GTTL.LabourdeliveryID
# MAGIC    
# MAGIC Left join $outSchema.motherbabybooking_geog as mbb on mbb.Record_Type="BabyDetails" and mbb.UniqSubmissionID=msd301.UniqSubmissionID and mbb.person_id_mother = msd301.person_id_mother
# MAGIC LEFT JOIN $outSchema.motherbabybooking_geog as mbb_ccg on msd301.person_id_mother = mbb_ccg.person_id_mother and mbb_ccg.Record_Type="MotherDetails" and mbb_ccg.sub_type="CCG" and mbb_ccg.UniqSubmissionID=msd301.UniqSubmissionID
# MAGIC LEFT JOIN $outSchema.motherbabybooking_geog as mbb_lad on msd301.person_id_mother = mbb_lad.person_id_mother and mbb_lad.Record_Type="MotherDetails" and mbb_lad.sub_type="LAD" and mbb_lad.UniqSubmissionID=msd301.UniqSubmissionID
# MAGIC
# MAGIC where msd401.RPStartDate = '$RPStartdate' and msd401.PersonBirthDateBaby between '$RPStartdate' and '$RPEnddate'

# COMMAND ----------

# DBTITLE 1,Care Plan Base Table - SQL
# MAGIC %sql
# MAGIC
# MAGIC truncate table $outSchema.CarePlanBaseTable;  --#TODO: make this a delete from where match RPEnddate (or a drop partition?)
# MAGIC
# MAGIC insert into $outSchema.CarePlanBaseTable
# MAGIC
# MAGIC Select 
# MAGIC '$RPEnddate' as ReportingPeriodEndDate,
# MAGIC OrgCodeProvider
# MAGIC ,'' as CCP_Any_Pathways
# MAGIC ,'' as PCP_Any_Pathways
# MAGIC ,'' as CCP_Antenatal
# MAGIC ,'' as PCP_Antenatal
# MAGIC ,'' as CCP_Birth
# MAGIC ,'' as PCP_Birth
# MAGIC ,'' as CCP_Postpartum
# MAGIC ,'' as PCP_Postpartum
# MAGIC
# MAGIC ,case when RN1 = 1 and ContCarePathInd = 'Y' then a.Person_ID_Mother end as CCP_Any_PathwaysCount
# MAGIC ,case when RN2 = 1 and MatPersCarePlanInd = 'Y' then a.Person_ID_Mother end as PCP_Any_PathwaysCount
# MAGIC ,case when RN3 = 1 and CarePlanType = '05' and ContCarePathInd = 'Y' then a.Person_ID_Mother end as CCP_AntenatalCount
# MAGIC ,case when RN4 = 1 and CarePlanType = '05' and MatPersCarePlanInd = 'Y' then a.Person_ID_Mother end as PCP_AntenatalCount
# MAGIC ,case when RN5 = 1 and CarePlanType = '06' and ContCarePathInd = 'Y' then a.Person_ID_Mother end as CCP_BirthCount
# MAGIC ,case when RN6 = 1 and CarePlanType = '06' and MatPersCarePlanInd = 'Y' then a.Person_ID_Mother end as PCP_BirthCount
# MAGIC ,case when RN7 = 1 and CarePlanType = '07' and ContCarePathInd = 'Y' then a.Person_ID_Mother end as CCP_PostpartumCount
# MAGIC ,case when RN8 = 1 and CarePlanType = '07' and MatPersCarePlanInd = 'Y' then a.Person_ID_Mother end as PCP_PostpartumCount
# MAGIC ,geo.*,
# MAGIC   mbb.valuecode as BookingSiteCode, --NEW
# MAGIC   mbb.valuename as BookingSiteName, --NEW
# MAGIC   mbb_CCG.valuecode as Mother_CCG_Code, --NEW
# MAGIC   mbb_CCG.valuename as Mother_CCG, --NEW
# MAGIC   mbb_LAD.valuecode as Mother_LAD_Code, --NEW
# MAGIC   mbb_LAD.valuename as Mother_LAD --NEW
# MAGIC from (
# MAGIC       Select msd000.OrgCodeProvider
# MAGIC       ,Person_ID_Mother
# MAGIC       ,CarePlanType
# MAGIC       ,MatPersCarePlanInd
# MAGIC       ,ContCarePathInd,uniqpregid,msd102.uniqsubmissionid
# MAGIC       ,case when msd102.ContCarePathInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN1
# MAGIC       ,case when msd102.MatPersCarePlanInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN2
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.ContCarePathInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN3
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.MatPersCarePlanInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN4
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.ContCarePathInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN5
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.MatPersCarePlanInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN6
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.ContCarePathInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN7
# MAGIC       ,case when msd102.CarePlanType is not null and msd102.MatPersCarePlanInd is not null then RANK () OVER ( PARTITION BY msd102.OrgCodeProvider, Person_ID_Mother, UniqPregID ORDER BY cast(CarePlanDate as int) DESC) end as RN8
# MAGIC       from $outSchema.MostRecentSubmissions MSD000 
# MAGIC       join $dbSchema.msd102matcareplan msd102 on MSD000.UniqSubmissionID = msd102.UniqSubmissionID
# MAGIC       where msd102.RPStartDate = '$RPStartdate'
# MAGIC       and msd102.CarePlanDate between '$RPStartdate' and '$RPEnddate'
# MAGIC       order by msd102.OrgCodeProvider, UniqPregID , CarePlanDate desc
# MAGIC ) a
# MAGIC left join $outSchema.geogtlrr geo on a.OrgCodeProvider = geo.Trust_ORG
# MAGIC
# MAGIC left join $outSchema.motherbabybooking_geog mbb     on a.person_id_mother = mbb.person_id_mother     and mbb.Record_Type="BookingDetails"                                    and mbb.uniqpregid=a.uniqpregid             and mbb.UniqSubmissionID=a.UniqSubmissionID --NEW
# MAGIC left join $outSchema.motherbabybooking_geog mbb_CCG on a.person_id_mother = mbb_CCG.person_id_mother and mbb_CCG.Record_Type="MotherDetails" and mbb_CCG.sub_type="CCG"      
# MAGIC and mbb_CCG.UniqSubmissionID=a.UniqSubmissionID --NEW
# MAGIC left join $outSchema.motherbabybooking_geog mbb_LAD on a.person_id_mother = mbb_LAD.person_id_mother and mbb_LAD.Record_Type="MotherDetails" and mbb_LAD.sub_type="LAD"      
# MAGIC and mbb_LAD.UniqSubmissionID=a.UniqSubmissionID --NEW
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Notebook: populate_base_tables ran successfully")