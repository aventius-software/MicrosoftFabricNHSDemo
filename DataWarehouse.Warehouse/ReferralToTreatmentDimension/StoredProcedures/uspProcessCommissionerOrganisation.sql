CREATE PROCEDURE [ReferralToTreatmentDimension].[uspProcessCommissionerOrganisation] AS
BEGIN
	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([CommissionerOrganisationKey]) 
	
		FROM 
		[ReferralToTreatmentDimension].[CommissionerOrganisation]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [ReferralToTreatmentDimension].[CommissionerOrganisation]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'
	
	-- Create ICB geography staging table
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[ICBGeography];
	CREATE TABLE [ReferralToTreatmentStaging].[ICBGeography] 
	AS
	SELECT DISTINCT
	[ICBOrganisationCode] = [icb].[ICB23CDH]
	,[ICBCounty] = CAST([cn].[_c1] AS VARCHAR(100))

	FROM 
	[LandingLakehouse].[dbo].[icb_names_and_codes] [icb]

	INNER JOIN
	[LandingLakehouse].[dbo].[gridall] [ga]
	ON
	[icb].[ICB23CDH] = [ga].[_c42]

	INNER JOIN
	[LandingLakehouse].[dbo].[county_names_and_codes] [cn]
	ON
	[ga].[_c6] = [cn].[_c0]

	-- Create staging table with transformed data
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[CommissionerOrganisation];
	CREATE TABLE [ReferralToTreatmentStaging].[CommissionerOrganisation] 
	AS
	SELECT
	[HighWatermark] = MAX(ISNULL([staging].[DateTimeUpdated], [staging].[DateTimeLoaded]))
	,[OrganisationCode] = [staging].[CommissionerOrgCode]
	,[OrganisationName] = ISNULL([staging].[CommissionerOrgName], 'Name not specified')
	,[ICBCode] = ISNULL([staging].[CommissionerParentOrgCode], 'No ICB code')
	,[ICBName] = ISNULL([staging].[CommissionerParentName], 'No ICB name')
	,[icb].[ICBCounty]
	
	FROM
	[Staginglakehouse].[dbo].[referral_to_treatment] [staging]

	LEFT JOIN
	[ReferralToTreatmentStaging].[ICBGeography] [icb]
	ON
	[staging].[CommissionerParentOrgCode] = [icb].[ICBOrganisationCode]

	WHERE
	CAST(ISNULL([staging].[DateTimeUpdated], [staging].[DateTimeLoaded]) AS DATETIME2(0)) > @currentHighWatermark

	GROUP BY
	[staging].[CommissionerOrgCode]
	,[staging].[CommissionerOrgName]
	,[staging].[CommissionerParentOrgCode]
	,[staging].[CommissionerParentName]
	,[icb].[ICBCounty]
		
	-- Insert new records
	INSERT INTO 
	[ReferralToTreatmentDimension].[CommissionerOrganisation] (
		[CommissionerOrganisationKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[IsCurrent]
		,[ValidFromDate]
		,[ValidToDate]
		,[HighWatermark]
		,[OrganisationCode]
		,[OrganisationName]
		,[ICBCode]
		,[ICBName]
		,[ICBCounty]
	) 

	SELECT
	[CommissionerOrganisationKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[IsCurrent] = 1
	,[ValidFromDate] = CAST('19000101' AS DATE)
	,[ValidToDate] = CAST('99991231' AS DATE)
	,[HighWatermark] = [src].[HighWatermark]
	,[OrganisationCode] = [src].[OrganisationCode]
	,[OrganisationName] = [src].[OrganisationName]	
	,[ICBCode] = [src].[ICBCode]
	,[ICBName] = [src].[ICBName]
	,[ICBCounty] = [src].[ICBCounty]

	FROM
	[ReferralToTreatmentStaging].[CommissionerOrganisation] [src]

	WHERE 
	NOT EXISTS(
		SELECT
		1

		FROM
		[ReferralToTreatmentDimension].[CommissionerOrganisation] [trg]

		WHERE
		[src].[OrganisationCode] = [trg].[OrganisationCode]
	)

	-- Update existing rows	
	UPDATE
	[trg]

	SET
	[DateTimeUpdated] = GETDATE()
	,[HighWatermark] = [src].[HighWatermark]
	,[OrganisationName] = [src].[OrganisationName]	
	,[ICBCode] = [src].[ICBCode]
	,[ICBName] = [src].[ICBName]
	,[ICBCounty] = [src].[ICBCounty]

	FROM
	[ReferralToTreatmentDimension].[CommissionerOrganisation] [trg]	

	INNER JOIN
	[ReferralToTreatmentStaging].[CommissionerOrganisation] [src]
	ON
	[trg].[OrganisationCode] = [src].[OrganisationCode]
	AND (
		[trg].[OrganisationName] <> [src].[OrganisationName]		
		OR [trg].[ICBCode] <> [src].[ICBCode]
		OR [trg].[ICBName] <> [src].[ICBName]
		OR [trg].[ICBCounty] <> [src].[ICBCounty]
	)

	-- Bye
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[ICBGeography];
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[CommissionerOrganisation];
END