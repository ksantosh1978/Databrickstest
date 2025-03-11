# Databricks notebook source
# DBTITLE 1,Widget values
params = {"RPStartdate" : dbutils.widgets.get("RPStartdate")
          ,"RPEnddate" : dbutils.widgets.get("RPEnddate")
          ,"outSchema" : dbutils.widgets.get("outSchema")
          ,"dbSchema" : dbutils.widgets.get("dbSchema") 
          ,"dss_corporate" : dbutils.widgets.get("dss_corporate")
          ,"month_id" : dbutils.widgets.get("month_id")
         }
print(params)

# COMMAND ----------

# DBTITLE 1,msd101pregnancybooking_old_orgs_updated
# MAGIC %sql
# MAGIC -- Organisations may have merged during pregnancy. If so, use merged org code.
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW msd101pregnancybooking_old_orgs_updated AS
# MAGIC
# MAGIC SELECT
# MAGIC msd101.EFFECTIVE_FROM
# MAGIC ,msd101.EFFECTIVE_TO
# MAGIC ,msd101.AgeAtBookingMother
# MAGIC ,msd101.AlcoholUnitsBooking
# MAGIC ,msd101.AlcoholUnitsPerWeekBand
# MAGIC ,msd101.AntePathLevel
# MAGIC ,msd101.AntenatalAppDate
# MAGIC ,msd101.CigarettesPerDayBand
# MAGIC ,msd101.ComplexSocialFactorsInd
# MAGIC ,msd101.DisabilityIndMother
# MAGIC ,msd101.DischReason
# MAGIC ,msd101.DischargeDateMatService
# MAGIC ,msd101.EDDAgreed
# MAGIC ,msd101.EDDMethodAgreed
# MAGIC ,msd101.EmploymentStatusMother
# MAGIC ,msd101.EmploymentStatusPartner
# MAGIC ,msd101.FolicAcidSupplement
# MAGIC ,msd101.GestAgeBooking
# MAGIC ,msd101.LPIDMother
# MAGIC ,msd101.LangCode
# MAGIC ,msd101.LastMenstrualPeriodDate
# MAGIC ,msd101.LeadAnteProvider
# MAGIC ,msd101.MHPredictionDetectionIndMother
# MAGIC ,msd101.MSD101_ID
# MAGIC ,msd101.OrgCodeAPLC
# MAGIC ,CASE 
# MAGIC    WHEN rel.REL_TO_ORG_CODE is null THEN msd101.OrgCodeProvider
# MAGIC    ELSE rel. REL_FROM_ORG_CODE
# MAGIC  END AS OrgCodeProvider
# MAGIC ,msd101.OrgIDComm
# MAGIC ,msd101.OrgIDProvOrigin
# MAGIC ,msd101.OrgIDRecv
# MAGIC ,msd101.OrgSiteIDBooking
# MAGIC ,msd101.PersonBMIBand
# MAGIC ,msd101.PersonBMIBooking
# MAGIC ,msd101.Person_ID_Mother
# MAGIC ,msd101.PregFirstConDate
# MAGIC ,msd101.PregFirstContactCareProfType
# MAGIC ,msd101.PregnancyID
# MAGIC ,msd101.PreviousCaesareanSections
# MAGIC ,msd101.PreviousLiveBirths
# MAGIC ,msd101.PreviousLossesLessThan24Weeks
# MAGIC ,msd101.PreviousStillBirths
# MAGIC ,msd101.RPEndDate
# MAGIC ,msd101.RPStartDate
# MAGIC ,msd101.ReasonLateBooking
# MAGIC ,msd101.RecordNumber
# MAGIC ,msd101.RowNumber
# MAGIC ,msd101.SmokingStatusBooking
# MAGIC ,msd101.SmokingStatusDelivery
# MAGIC ,msd101.SmokingStatusDischarge
# MAGIC ,msd101.SourceRefMat
# MAGIC ,msd101.SupportStatusIndMother
# MAGIC ,msd101.UniqPregID
# MAGIC ,msd101.UniqSubmissionID
# MAGIC from $dbSchema.msd101pregnancybooking msd101
# MAGIC LEFT Join $dss_corporate.org_relationship_daily rel
# MAGIC ON msd101.OrgcodeProvider = rel.REL_TO_ORG_CODE 
# MAGIC AND rel.REL_IS_CURRENT = 1           --                        (which means it's a current relationship)
# MAGIC AND rel.REL_TYPE_CODE = 'P'             --                        (which specifies Successor to Predecessor relationships)
# MAGIC AND rel.REL_FROM_ORG_TYPE_CODE = 'TR'    --                        (which means it is only including Trusts)
# MAGIC AND rel.REL_OPEN_DATE <= '$RPEnddate'
# MAGIC AND (rel.REL_CLOSE_DATE IS NULL OR rel.REL_CLOSE_DATE > '$RPEnddate') 

# COMMAND ----------

# DBTITLE 1,msd001motherdemog_old_orgs_updated
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW msd001motherdemog_old_orgs_updated AS
# MAGIC
# MAGIC SELECT
# MAGIC msd001.EFFECTIVE_FROM
# MAGIC ,msd001.EFFECTIVE_TO
# MAGIC ,msd001.AgeAtDeathMother
# MAGIC ,msd001.AgeRPEndDate
# MAGIC ,msd001.CCGResidenceMother
# MAGIC ,msd001.CountyMother
# MAGIC ,msd001.DayOfWeekOfDeathMother
# MAGIC ,msd001.ElectoralWardMother
# MAGIC ,msd001.EthnicCategoryMother
# MAGIC ,msd001.LAD_UAMother
# MAGIC ,msd001.LPIDMother
# MAGIC ,msd001.LSOAMother2011
# MAGIC ,msd001.MPSConfidence
# MAGIC ,msd001.MSD001_ID
# MAGIC ,msd001.MeridianOfDeathMother
# MAGIC ,msd001.MonthOfDeathMother
# MAGIC ,msd001.NHSNumberMother
# MAGIC ,msd001.NHSNumberStatusMother
# MAGIC ,msd001.OrgCodeProvider orgCodeProvider_beforeUpdate
# MAGIC ,CASE 
# MAGIC    WHEN rel.REL_TO_ORG_CODE is null THEN msd001.OrgCodeProvider
# MAGIC    ELSE rel. REL_FROM_ORG_CODE
# MAGIC  END AS OrgCodeProvider
# MAGIC ,msd001.OrgIDLPID
# MAGIC ,msd001.OrgIDResidenceResp
# MAGIC ,msd001.PersonBirthDateMother
# MAGIC ,msd001.PersonDeathDateMother
# MAGIC ,msd001.PersonDeathTimeMother
# MAGIC ,msd001.Person_ID_Mother
# MAGIC ,msd001.Postcode
# MAGIC ,msd001.PostcodeDistrictMother
# MAGIC ,msd001.RPEndDate
# MAGIC ,msd001.RPStartDate
# MAGIC ,msd001.Rank_IMD_Decile_2015
# MAGIC ,msd001.RecordNumber
# MAGIC ,msd001.RowNumber
# MAGIC ,msd001.UniqSubmissionID
# MAGIC ,msd001.ValidNHSNoFlagMother
# MAGIC ,msd001.ValidPostcodeFlag
# MAGIC ,msd001.YearOfDeathMother
# MAGIC from $dbSchema.msd001motherdemog msd001
# MAGIC LEFT Join $dss_corporate.org_relationship_daily rel
# MAGIC ON msd001.OrgcodeProvider = rel.REL_TO_ORG_CODE 
# MAGIC AND rel.REL_IS_CURRENT = 1           --                        (which means it's a current relationship)
# MAGIC AND rel.REL_TYPE_CODE = 'P'             --                        (which specifies Successor to Predecessor relationships)
# MAGIC AND rel.REL_FROM_ORG_TYPE_CODE = 'TR'    --                        (which means it is only including Trusts)
# MAGIC AND rel.REL_OPEN_DATE <= '$RPEnddate'
# MAGIC AND (rel.REL_CLOSE_DATE IS NULL OR rel.REL_CLOSE_DATE > '$RPEnddate') 

# COMMAND ----------

# DBTITLE 1,msd102matcareplan_old_orgs_updated
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW msd102matcareplan_old_orgs_updated AS
# MAGIC SELECT
# MAGIC  msd102.EFFECTIVE_FROM
# MAGIC ,msd102.EFFECTIVE_TO
# MAGIC ,msd102.CarePlanDate
# MAGIC ,msd102.CarePlanType
# MAGIC ,msd102.CareProfLID
# MAGIC ,msd102.ContCarePathInd
# MAGIC ,msd102.MSD102_ID
# MAGIC ,msd102.MatPersCarePlanInd
# MAGIC ,msd102.OrgCodeProvider orgCodeProvider_beforeUpdate
# MAGIC ,CASE 
# MAGIC    WHEN rel.REL_TO_ORG_CODE is null THEN msd102.OrgCodeProvider
# MAGIC    ELSE rel. REL_FROM_ORG_CODE
# MAGIC  END AS OrgCodeProvider
# MAGIC ,msd102.OrgSiteIDPlannedDelivery
# MAGIC ,msd102.Person_ID_Mother
# MAGIC ,msd102.PlannedDeliverySetting
# MAGIC ,msd102.PregnancyID
# MAGIC ,msd102.RPEndDate
# MAGIC ,msd102.RPStartDate
# MAGIC ,msd102.ReasonChangeDelSettingAnt
# MAGIC ,msd102.RecordNumber
# MAGIC ,msd102.RowNumber
# MAGIC ,msd102.TeamLocalID
# MAGIC ,msd102.UniqPregID
# MAGIC ,msd102.UniqSubmissionID
# MAGIC from $dbSchema.msd102matcareplan msd102
# MAGIC LEFT Join $dss_corporate.org_relationship_daily rel
# MAGIC ON msd102.OrgcodeProvider = rel.REL_TO_ORG_CODE 
# MAGIC AND rel.REL_IS_CURRENT = 1           --                        (which means it's a current relationship)
# MAGIC AND rel.REL_TYPE_CODE = 'P'             --                        (which specifies Successor to Predecessor relationships)
# MAGIC AND rel.REL_FROM_ORG_TYPE_CODE = 'TR'    --                        (which means it is only including Trusts)
# MAGIC AND rel.REL_OPEN_DATE <= '$RPEnddate'
# MAGIC AND (rel.REL_CLOSE_DATE IS NULL OR rel.REL_CLOSE_DATE > '$RPEnddate') 

# COMMAND ----------

# DBTITLE 1,msd401BabyDemographics_old_orgs_updated
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW msd401BabyDemographics_old_orgs_updated AS
# MAGIC SELECT
# MAGIC msd401.EFFECTIVE_FROM,
# MAGIC  msd401.EFFECTIVE_TO,
# MAGIC  msd401.AgeAtBirthMother,
# MAGIC  msd401.AgeAtDeathBaby,
# MAGIC  msd401.BabyFirstFeedDate,
# MAGIC  msd401.BabyFirstFeedIndCode,
# MAGIC  msd401.BabyFirstFeedTime,
# MAGIC  msd401.BirthOrderMaternitySUS,
# MAGIC  msd401.CareProfLIDDel,
# MAGIC  msd401.DayOfBirthBaby,
# MAGIC  msd401.DayOfDeathBaby,
# MAGIC  msd401.DeliveryMethodCode,
# MAGIC  msd401.DischargeDateBabyHosp,
# MAGIC  msd401.DischargeTimeBabyHosp,
# MAGIC  msd401.EthnicCategoryBaby,
# MAGIC  msd401.FetusPresentation,
# MAGIC  msd401.GestationLengthBirth,
# MAGIC  msd401.LPIDBaby,
# MAGIC  msd401.LabourDeliveryID,
# MAGIC  msd401.LocalFetalID,
# MAGIC  msd401.MPSConfidence,
# MAGIC  msd401.MSD401_ID,
# MAGIC  msd401.MerOfBirthBaby,
# MAGIC  msd401.MeridianOfDeathBaby,
# MAGIC  msd401.MonthOfBirthBaby,
# MAGIC  msd401.MonthOfDeathBaby,
# MAGIC  msd401.NHSNumberBaby,
# MAGIC  msd401.NHSNumberStatusBaby,
# MAGIC  CASE 
# MAGIC    WHEN rel.REL_TO_ORG_CODE is null THEN msd401.OrgCodeProvider
# MAGIC    ELSE rel. REL_FROM_ORG_CODE
# MAGIC  END AS OrgCodeProvider,
# MAGIC  msd401.OrgIDLocalPatientIdBaby,
# MAGIC  msd401.OrgSiteIDActualDelivery,
# MAGIC  msd401.PersonBirthDateBaby,
# MAGIC  msd401.PersonBirthTimeBaby,
# MAGIC  msd401.PersonDeathDateBaby,
# MAGIC  msd401.PersonDeathTimeBaby,
# MAGIC  msd401.PersonPhenSex,
# MAGIC  msd401.Person_ID_Baby,
# MAGIC  msd401.Person_ID_Mother,
# MAGIC  msd401.PregOutcome,
# MAGIC  msd401.RPEndDate,
# MAGIC  msd401.RPStartDate,
# MAGIC  msd401.RecordNumber,
# MAGIC  msd401.RowNumber,
# MAGIC  msd401.SettingPlaceBirth,
# MAGIC  msd401.SkinToSkinContact1HourInd,
# MAGIC  msd401.UniqPregID,
# MAGIC  msd401.UniqSubmissionID,
# MAGIC  msd401.ValidNHSNoFlagBaby,
# MAGIC  msd401.WaterDeliveryInd,
# MAGIC  msd401.YearOfBirthBaby,
# MAGIC  msd401.YearOfDeathBaby
# MAGIC from $dbSchema.msd401BabyDemographics msd401
# MAGIC LEFT Join $dss_corporate.org_relationship_daily rel
# MAGIC ON msd401.OrgcodeProvider = rel.REL_TO_ORG_CODE 
# MAGIC AND rel.REL_IS_CURRENT = 1           --                        (which means it's a current relationship)
# MAGIC AND rel.REL_TYPE_CODE = 'P'             --                        (which specifies Successor to Predecessor relationships)
# MAGIC AND rel.REL_FROM_ORG_TYPE_CODE = 'TR'    --                        (which means it is only including Trusts)
# MAGIC AND rel.REL_OPEN_DATE <= '$RPEnddate'
# MAGIC AND (rel.REL_CLOSE_DATE IS NULL OR rel.REL_CLOSE_DATE > '$RPEnddate') 

# COMMAND ----------

dbutils.notebook.exit("Notebook: update_base_tables ran successfully")