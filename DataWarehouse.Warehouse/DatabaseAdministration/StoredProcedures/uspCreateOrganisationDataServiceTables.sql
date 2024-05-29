CREATE PROCEDURE [DatabaseAdministration].[uspCreateOrganisationDataServiceTables] AS
BEGIN
	/************************************************************************
	
	Creates all the required tables
	
	References
	https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/other-nhs-organisations
	https://learn.microsoft.com/en-us/fabric/data-warehouse/tables
	https://learn.microsoft.com/en-us/fabric/data-warehouse/table-constraints

	************************************************************************/

	-- First create schemas    
	EXEC [DatabaseAdministration].[uspCreateSchemasForDataset] @dataset = 'OrganisationDataService';
	
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'OrganisationDataServiceDimension' AND TABLE_NAME = 'NHSProviderTrust')
	BEGIN
		DROP TABLE IF EXISTS [OrganisationDataServiceDimension].[NHSProviderTrust];
		CREATE TABLE [OrganisationDataServiceDimension].[NHSProviderTrust](
			[NHSProviderTrustKey]			INT				NOT NULL
			,[DateTimeLoaded]				DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]				DATETIME2(0)	NULL
			,[DateTimeDeleted]				DATETIME2(0)	NULL
			,[IsCurrent]					BIT				NOT NULL
			,[ValidFromDate]				DATE			NOT NULL
			,[ValidToDate]					DATE			NOT NULL
			,[HighWatermark]				DATETIME2(0)	NOT NULL
			,[OrganisationCode]				VARCHAR(5)		NOT NULL
			,[OrganisationName]				VARCHAR(100)	NOT NULL
			,[NationalGroupingCode]			VARCHAR(5)		NOT NULL
			,[HighLevelHealthGeographyCode]	VARCHAR(5)		NOT NULL
			,[AddressLine1]					VARCHAR(35)		NULL
			,[AddressLine2]					VARCHAR(35)		NULL
			,[AddressLine3]					VARCHAR(35)		NULL
			,[AddressLine4]					VARCHAR(35)		NULL
			,[AddressLine5]					VARCHAR(35)		NULL
			,[TownOrCity]					VARCHAR(35)		NULL
			,[PostcodeArea]					VARCHAR(4)		NULL
			,[Postcode]						VARCHAR(8)		NULL
			,[OpenDate]						DATE			NOT NULL
			,[CloseDate]					DATE			NULL
		);

		-- Add the primary key
		ALTER TABLE [OrganisationDataServiceDimension].[NHSProviderTrust]
		ADD CONSTRAINT [PK_[NHSProviderTrust] PRIMARY KEY NONCLUSTERED ([NHSProviderTrustKey]) NOT ENFORCED;
	END
END