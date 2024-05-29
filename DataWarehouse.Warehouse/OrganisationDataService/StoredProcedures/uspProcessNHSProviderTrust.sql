CREATE PROCEDURE [OrganisationDataService].[uspProcessNHSProviderTrust]
	@customOptions VARCHAR(4000)
AS
BEGIN
	-- Data taken from https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/other-nhs-organisations

	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([NHSProviderTrustKey]) 
	
		FROM 
		[OrganisationDataServiceDimension].[NHSProviderTrust]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [OrganisationDataServiceDimension].[NHSProviderTrust]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'
	
	-- Create staging table with transformed data
	DROP TABLE IF EXISTS [OrganisationDataServiceStaging].[NHSProviderTrust];
	CREATE TABLE [OrganisationDataServiceStaging].[NHSProviderTrust] 
	AS
	SELECT DISTINCT
	[HighWatermark] = ISNULL([DateTimeUpdated], [DateTimeLoaded])
	,[OrganisationCode] = [_c0]
	,[OrganisationName] = ISNULL([_c1], 'Not specified')
	,[NationalGroupingCode] = ISNULL([_c2], 'N/S')
	,[HighLevelHealthGeographyCode] = ISNULL([_c3], 'N/S')
	,[AddressLine1] = [_c4]
	,[AddressLine2] = [_c5]
	,[AddressLine3] = [_c6]
	,[AddressLine4] = [_c7]
	,[AddressLine5] = [_c8]
	,[TownOrCity] = ISNULL([_c7], 'Not specified')
	,[PostcodeArea] = RTRIM(LEFT([_c9], CHARINDEX(' ', [_c9])))
	,[Postcode] = [_c9]
	,[OpenDate] = CAST([_c10] AS DATE)
	,[CloseDate] = CAST([_c11] AS DATE)
	
	FROM
	[StagingLakehouse].[dbo].[organisation_data_service_nhs_provider_trust]

	WHERE
	ISNULL([DateTimeUpdated], [DateTimeLoaded]) > @currentHighWatermark
	   		
	-- Insert new records
	INSERT INTO 
	[OrganisationDataServiceDimension].[NHSProviderTrust] (
		[NHSProviderTrustKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[IsCurrent]
		,[ValidFromDate]
		,[ValidToDate]
		,[HighWatermark]
		,[OrganisationCode]
		,[OrganisationName]
		,[NationalGroupingCode]
		,[HighLevelHealthGeographyCode]
		,[AddressLine1]
		,[AddressLine2]
		,[AddressLine3]
		,[AddressLine4]
		,[AddressLine5]
		,[TownOrCity]
		,[PostcodeArea]
		,[Postcode]
		,[OpenDate]
		,[CloseDate]
	) 

	SELECT
	[NHSProviderTrustKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[IsCurrent] = 1
	,[ValidFromDate] = CAST('19000101' AS DATE)
	,[ValidToDate] = CAST('99991231' AS DATE)
	,[HighWatermark] = [src].[HighWatermark]
	,[OrganisationCode] = [src].[OrganisationCode]
	,[OrganisationName] = [src].[OrganisationName]
	,[NationalGroupingCode] = [src].[NationalGroupingCode]
	,[HighLevelHealthGeographyCode] = [src].[HighLevelHealthGeographyCode]
	,[AddressLine1] = [src].[AddressLine1]
	,[AddressLine2] = [src].[AddressLine2]
	,[AddressLine3] = [src].[AddressLine3]
	,[AddressLine4] = [src].[AddressLine4]
	,[AddressLine5] = [src].[AddressLine5]
	,[TownOrCity] = [src].[TownOrCity]
	,[PostcodeArea] = [src].[PostcodeArea]
	,[Postcode] = [src].[Postcode]
	,[OpenDate] = [src].[OpenDate]
	,[CloseDate] = [src].[CloseDate]

	FROM
	[OrganisationDataServiceStaging].[NHSProviderTrust] [src]

	WHERE 
	NOT EXISTS(
		SELECT
		1

		FROM
		[OrganisationDataServiceDimension].[NHSProviderTrust] [trg]

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
	,[NationalGroupingCode] = [src].[NationalGroupingCode]
	,[HighLevelHealthGeographyCode] = [src].[HighLevelHealthGeographyCode]
	,[AddressLine1] = [src].[AddressLine1]
	,[AddressLine2] = [src].[AddressLine2]
	,[AddressLine3] = [src].[AddressLine3]
	,[AddressLine4] = [src].[AddressLine4]
	,[AddressLine5] = [src].[AddressLine5]
	,[TownOrCity] = [src].[TownOrCity]
	,[PostcodeArea] = [src].[PostcodeArea]
	,[Postcode] = [src].[Postcode]
	,[OpenDate] = [src].[OpenDate]
	,[CloseDate] = [src].[CloseDate]

	FROM
	[OrganisationDataServiceDimension].[NHSProviderTrust] [trg]	

	INNER JOIN
	[OrganisationDataServiceStaging].[NHSProviderTrust] [src]
	ON
	[trg].[OrganisationCode] = [src].[OrganisationCode]
	AND (
		[trg].[OrganisationName] <> [src].[OrganisationName]		
		OR [trg].[NationalGroupingCode] <> [src].[NationalGroupingCode]
		OR [trg].[HighLevelHealthGeographyCode] <> [src].[HighLevelHealthGeographyCode]
		OR NOT EXISTS(SELECT [trg].[AddressLine1] INTERSECT SELECT [src].[AddressLine1])
		OR NOT EXISTS(SELECT [trg].[AddressLine2] INTERSECT SELECT [src].[AddressLine2])
		OR NOT EXISTS(SELECT [trg].[AddressLine3] INTERSECT SELECT [src].[AddressLine3])
		OR NOT EXISTS(SELECT [trg].[AddressLine4] INTERSECT SELECT [src].[AddressLine4])
		OR NOT EXISTS(SELECT [trg].[AddressLine5] INTERSECT SELECT [src].[AddressLine5])
		OR NOT EXISTS(SELECT [trg].[TownOrCity] INTERSECT SELECT [src].[TownOrCity])
		OR NOT EXISTS(SELECT [trg].[PostcodeArea] INTERSECT SELECT [src].[PostcodeArea])
		OR NOT EXISTS(SELECT [trg].[Postcode] INTERSECT SELECT [src].[Postcode])
		OR [trg].[OpenDate] <> [src].[OpenDate]
		OR NOT EXISTS(SELECT [trg].[CloseDate] INTERSECT SELECT [src].[CloseDate])
	)

	-- Bye
	DROP TABLE IF EXISTS [OrganisationDataServiceStaging].[NHSProviderTrust];
END