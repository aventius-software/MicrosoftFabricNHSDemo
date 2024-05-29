CREATE TABLE [ReferralToTreatmentDimension].[CommissionerOrganisation] (

	[CommissionerOrganisationKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[IsCurrent] bit NOT NULL, 
	[ValidFromDate] date NOT NULL, 
	[ValidToDate] date NOT NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[OrganisationCode] varchar(50) NOT NULL, 
	[OrganisationName] varchar(500) NOT NULL, 
	[ICBCode] varchar(50) NOT NULL, 
	[ICBName] varchar(500) NOT NULL, 
	[ICBCounty] varchar(100) NULL
);


GO
ALTER TABLE [ReferralToTreatmentDimension].[CommissionerOrganisation] ADD CONSTRAINT PK_CommissionerOrganisation primary key NONCLUSTERED ([CommissionerOrganisationKey]);