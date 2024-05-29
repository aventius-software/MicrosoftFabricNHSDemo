CREATE PROCEDURE [DatabaseAdministration].[uspCreateReferralToTreatmentTables] AS
BEGIN
	/************************************************************************
	
	Creates all the required tables
	
	References
	https://learn.microsoft.com/en-us/fabric/data-warehouse/tables
	https://learn.microsoft.com/en-us/fabric/data-warehouse/table-constraints

	************************************************************************/

	-- First create schemas    
	EXEC [DatabaseAdministration].[uspCreateSchemasForDataset] @dataset = 'ReferralToTreatment';
	
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ReferralToTreatmentDimension' AND TABLE_NAME = 'CommissionerOrganisation')
	BEGIN
		DROP TABLE IF EXISTS [ReferralToTreatmentDimension].[CommissionerOrganisation];
		CREATE TABLE [ReferralToTreatmentDimension].[CommissionerOrganisation](
			[CommissionerOrganisationKey]	INT				NOT NULL
			,[DateTimeLoaded]				DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]				DATETIME2(0)	NULL
			,[DateTimeDeleted]				DATETIME2(0)	NULL
			,[IsCurrent]					BIT				NOT NULL
			,[ValidFromDate]				DATE			NOT NULL
			,[ValidToDate]					DATE			NOT NULL
			,[HighWatermark]				DATETIME2(0)	NOT NULL
			,[OrganisationCode]				VARCHAR(50)		NOT NULL
			,[OrganisationName]				VARCHAR(500)	NOT NULL
			,[ICBCode]						VARCHAR(50)		NOT NULL
			,[ICBName]						VARCHAR(500)	NOT NULL
			,[ICBCounty]					VARCHAR(500)	NOT NULL
		);
		
		-- Add the primary key
		ALTER TABLE [ReferralToTreatmentDimension].[CommissionerOrganisation] 
		ADD CONSTRAINT [PK_CommissionerOrganisation] PRIMARY KEY NONCLUSTERED ([CommissionerOrganisationKey]) NOT ENFORCED;
	END

	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ReferralToTreatmentDimension' AND TABLE_NAME = 'ProviderOrganisation')
	BEGIN
		DROP TABLE IF EXISTS [ReferralToTreatmentDimension].[ProviderOrganisation];
		CREATE TABLE [ReferralToTreatmentDimension].[ProviderOrganisation](
			[ProviderOrganisationKey]	INT				NOT NULL
			,[DateTimeLoaded]			DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]			DATETIME2(0)	NULL
			,[DateTimeDeleted]			DATETIME2(0)	NULL
			,[IsCurrent]				BIT				NOT NULL
			,[ValidFromDate]			DATE			NOT NULL
			,[ValidToDate]				DATE			NOT NULL
			,[HighWatermark]			DATETIME2(0)	NOT NULL
			,[OrganisationCode]			VARCHAR(50)		NOT NULL
			,[OrganisationName]			VARCHAR(500)	NOT NULL
			,[ParentOrganisationCode]	VARCHAR(50)		NOT NULL
			,[ParentOrganisationName]	VARCHAR(500)	NOT NULL
		);

		-- Add the primary key
		ALTER TABLE [ReferralToTreatmentDimension].[ProviderOrganisation] 		
		ADD CONSTRAINT [PK_ProviderOrganisation] PRIMARY KEY NONCLUSTERED ([ProviderOrganisationKey]) NOT ENFORCED;
	END
	
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ReferralToTreatmentDimension' AND TABLE_NAME = 'WaitingStatus')
	BEGIN
		DROP TABLE IF EXISTS [ReferralToTreatmentDimension].[WaitingStatus];
		CREATE TABLE [ReferralToTreatmentDimension].[WaitingStatus](
			[WaitingStatusKey]		    INT				NOT NULL
			,[DateTimeLoaded]			DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]			DATETIME2(0)	NULL
			,[DateTimeDeleted]			DATETIME2(0)	NULL
			,[IsCurrent]				BIT				NOT NULL
			,[ValidFromDate]			DATE			NOT NULL
			,[ValidToDate]				DATE			NOT NULL
			,[HighWatermark]			DATETIME2(0)	NOT NULL
			,[WaitingStatusType]	    VARCHAR(50)		NOT NULL
			,[WaitingStatusDescription]	VARCHAR(500)	NOT NULL
			,[WaitingStatusCategory]	VARCHAR(50)		NOT NULL
		);

		-- Add the primary key
		ALTER TABLE [ReferralToTreatmentDimension].[WaitingStatus] 		
		ADD CONSTRAINT [PK_WaitingStatus] PRIMARY KEY NONCLUSTERED ([WaitingStatusKey]) NOT ENFORCED;
	END

	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ReferralToTreatmentDimension' AND TABLE_NAME = 'TreatmentFunction')
	BEGIN
		DROP TABLE IF EXISTS [ReferralToTreatmentDimension].[TreatmentFunction];
		CREATE TABLE [ReferralToTreatmentDimension].[TreatmentFunction](
			[TreatmentFunctionKey]		INT				NOT NULL
			,[DateTimeLoaded]			DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]			DATETIME2(0)	NULL
			,[DateTimeDeleted]			DATETIME2(0)	NULL
			,[IsCurrent]				BIT				NOT NULL
			,[ValidFromDate]			DATE			NOT NULL
			,[ValidToDate]				DATE			NOT NULL
			,[HighWatermark]			DATETIME2(0)	NOT NULL
			,[TreatmentFunctionCode]	VARCHAR(50)		NOT NULL
			,[TreatmentFunctionName]	VARCHAR(500)	NOT NULL			
		);

		-- Add the primary key
		ALTER TABLE [ReferralToTreatmentDimension].[TreatmentFunction] 		
		ADD CONSTRAINT [PK_TreatmentFunction] PRIMARY KEY NONCLUSTERED ([TreatmentFunctionKey]) NOT ENFORCED;
	END

	-- Create fact table
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ReferralToTreatmentFact' AND TABLE_NAME = 'PatientsWaitingMonthly')
	BEGIN
		DROP TABLE IF EXISTS [ReferralToTreatmentFact].[PatientsWaitingMonthly];
		CREATE TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly](
			[PatientsWaitingMonthlyKey]			INT				NOT NULL
			,[DateTimeLoaded]					DATETIME2(0)	NOT NULL
			,[DateTimeUpdated]					DATETIME2(0)	NULL
			,[DateTimeDeleted]					DATETIME2(0)	NULL
			,[HighWatermark]					DATETIME2(0)	NOT NULL
			,[ChronologicalCalendarKey]			INT				NOT NULL
			,[ProviderOrganisationKey]			INT				NOT NULL
			,[CommissionerOrganisationKey]		INT				NOT NULL
			,[WaitingStatusKey]					INT				NOT NULL
			,[TreatmentFunctionKey]				INT				NOT NULL
			,[ChronologicalWeekTimeBandingKey]	INT				NOT NULL
			,[NumberOfPatientsWaiting]			INT				NULL
		);

		-- Add the primary key
		ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly] 		
		ADD CONSTRAINT [PK_PatientsWaitingMonthly] PRIMARY KEY NONCLUSTERED ([PatientsWaitingMonthlyKey]) NOT ENFORCED;

		-- Add foreign keys
		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_ChronlogicalCalendar] FOREIGN KEY ([ChronologicalCalendarKey]) 
		--REFERENCES [Chronological].[Calendar] ([ChronologicalCalendarKey]) NOT ENFORCED;

		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_ChronlogicalWeekTimeBanding] FOREIGN KEY ([ChronologicalWeekTimeBandingKey]) 
		--REFERENCES [Chronological].[WeekTimeBanding] ([ChronologicalWeekTimeBandingKey]) NOT ENFORCED;

		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_ProviderOrganisation] FOREIGN KEY ([ProviderOrganisationKey]) 
		--REFERENCES [ReferralToTreatmentDimension].[ProviderOrganisation] ([ProviderOrganisationKey]) NOT ENFORCED;

		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_CommissionerOrganisation] FOREIGN KEY ([CommissionerOrganisationKey]) 
		--REFERENCES [ReferralToTreatmentDimension].[CommissionerOrganisation] ([CommissionerOrganisationKey]) NOT ENFORCED;

		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_WaitingStatus] FOREIGN KEY ([WaitingStatusKey]) 
		--REFERENCES [ReferralToTreatmentDimension].[WaitingStatus] ([WaitingStatusKey]) NOT ENFORCED;

		--ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly]
		--ADD CONSTRAINT [FK_TreatmentFunction] FOREIGN KEY ([TreatmentFunctionKey]) 
		--REFERENCES [ReferralToTreatmentDimension].[TreatmentFunction] ([TreatmentFunctionKey]) NOT ENFORCED;		
	END	
END