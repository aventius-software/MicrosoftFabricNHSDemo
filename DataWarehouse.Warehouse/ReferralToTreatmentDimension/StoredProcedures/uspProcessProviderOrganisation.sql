CREATE PROCEDURE [ReferralToTreatmentDimension].[uspProcessProviderOrganisation] AS
BEGIN
	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([ProviderOrganisationKey]) 
	
		FROM 
		[ReferralToTreatmentDimension].[ProviderOrganisation]		
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [ReferralToTreatmentDimension].[ProviderOrganisation]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'

	-- Create staging table
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[ProviderOrganisation];
	CREATE TABLE [ReferralToTreatmentStaging].[ProviderOrganisation] 
	AS
	SELECT 
	[HighWatermark] = MAX(ISNULL([DateTimeUpdated], [DateTimeLoaded]))
	,[OrganisationCode] = [ProviderOrgCode]
	,[OrganisationName] = [ProviderOrgName]
	,[ParentOrganisationCode] = [ProviderParentOrgCode]
	,[ParentOrganisationName] = [ProviderParentName]
		
	FROM
	[Staginglakehouse].[dbo].[referral_to_treatment]

	WHERE
	CAST(ISNULL([DateTimeUpdated], [DateTimeLoaded]) AS DATETIME2(0)) > @currentHighWatermark

	GROUP BY
	[ProviderOrgCode]
	,[ProviderOrgName]
	,[ProviderParentOrgCode]
	,[ProviderParentName]
	
	-- Insert new records
	INSERT INTO 
	[ReferralToTreatmentDimension].[ProviderOrganisation] (
		[ProviderOrganisationKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[IsCurrent]
		,[ValidFromDate]
		,[ValidToDate]
		,[HighWatermark]
		,[OrganisationCode]
		,[OrganisationName]
		,[ParentOrganisationCode]
		,[ParentOrganisationName]
	) 

	SELECT
	[ProviderOrganisationKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[IsCurrent] = 1
	,[ValidFromDate] = CAST('19000101' AS DATE)
	,[ValidToDate] = CAST('99991231' AS DATE)
	,[HighWatermark] = [src].[HighWatermark]
	,[OrganisationCode] = [src].[OrganisationCode]
	,[OrganisationName] = [src].[OrganisationName]
	,[ParentOrganisationCode] = [src].[ParentOrganisationCode]
	,[ParentOrganisationName] = [src].[ParentOrganisationName]

	FROM
	[ReferralToTreatmentStaging].[ProviderOrganisation] [src]

	WHERE 
	NOT EXISTS(
		SELECT
		1

		FROM
		[ReferralToTreatmentDimension].[ProviderOrganisation] [trg]

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
	,[ParentOrganisationCode] = [src].[ParentOrganisationCode]
	,[ParentOrganisationName] = [src].[ParentOrganisationName]

	FROM
	[ReferralToTreatmentDimension].[ProviderOrganisation] [trg]	

	INNER JOIN
	[ReferralToTreatmentStaging].[ProviderOrganisation] [src]
	ON
	[trg].[OrganisationCode] = [src].[OrganisationCode]
	AND (
		[trg].[OrganisationName] <> [src].[OrganisationName]
		OR [trg].[ParentOrganisationCode] <> [src].[ParentOrganisationCode]
		OR [trg].[ParentOrganisationName] <> [src].[ParentOrganisationName]
	)

	-- Done
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[ProviderOrganisation];
END