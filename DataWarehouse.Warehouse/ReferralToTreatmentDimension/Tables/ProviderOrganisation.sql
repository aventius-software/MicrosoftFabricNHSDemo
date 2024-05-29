CREATE TABLE [ReferralToTreatmentDimension].[ProviderOrganisation] (

	[ProviderOrganisationKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[IsCurrent] bit NOT NULL, 
	[ValidFromDate] date NOT NULL, 
	[ValidToDate] date NOT NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[OrganisationCode] varchar(50) NOT NULL, 
	[OrganisationName] varchar(500) NOT NULL, 
	[ParentOrganisationCode] varchar(50) NOT NULL, 
	[ParentOrganisationName] varchar(500) NOT NULL
);


GO
ALTER TABLE [ReferralToTreatmentDimension].[ProviderOrganisation] ADD CONSTRAINT PK_ProviderOrganisation primary key NONCLUSTERED ([ProviderOrganisationKey]);